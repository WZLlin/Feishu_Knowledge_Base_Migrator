"""管线编排器：把各阶段串成可断点续跑的流水线，全程以台账为权威。

阶段（对应 Stage）：
  ingest   : connector.discover -> upsert_discovered；再对 DISCOVERED 逐个
             fetch + 文本提取 + 精确去重(SHA256 查台账) -> EXTRACTED / SKIPPED_DUPLICATE
  dedup    : 对 EXTRACTED 建 MinHash 近似索引，标近似重复 -> DEDUPED
  classify : 对 DEDUPED(或 EXTRACTED) 调分类器 -> CLASSIFIED；
             高置信且 not needs_review 自动 -> CONFIRMED
  load     : 对 CONFIRMED 写飞书 -> LOADED

每步只处理「处于对应上一阶段」的条目，故可反复运行、断点续跑。
文本正文缓存到 work_dir/text/<key哈希>.txt，避免撑大台账。
"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Callable, Optional

# 进度回调：progress(done, total, msg)。默认 None，不影响 CLI/测试。
ProgressCb = Optional[Callable[[int, int, str], None]]


def _report(cb: ProgressCb, done: int, total: int, msg: str = "") -> None:
    if cb:
        cb(done, total, msg)

from ..ledger import Ledger
from ..models import DedupVerdict, SourceItem, Stage
from ..taxonomy import Taxonomy
from ..utils.naming import canonical_name
from .classify import Classifier
from .dedup import NearDuplicateIndex
from .extract import extract_text


class Orchestrator:
    def __init__(self, ledger: Ledger, taxonomy: Taxonomy, work_dir: str,
                 confidence_threshold: float = 0.85):
        self.led = ledger
        self.tx = taxonomy
        self.work_dir = work_dir
        self.text_dir = os.path.join(work_dir, "text")
        os.makedirs(self.text_dir, exist_ok=True)
        self.threshold = confidence_threshold

    # ── 文本缓存 ──────────────────────────────────────────

    def _text_path(self, stable_key: str) -> str:
        h = hashlib.sha1(stable_key.encode()).hexdigest()
        return os.path.join(self.text_dir, f"{h}.txt")

    def _save_text(self, stable_key: str, text: str) -> None:
        with open(self._text_path(stable_key), "w", encoding="utf-8") as f:
            f.write(text)

    def load_text(self, stable_key: str) -> str:
        p = self._text_path(stable_key)
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return f.read()
        return ""

    # ── 阶段 1：ingest ────────────────────────────────────

    def ingest(self, connector, progress: ProgressCb = None) -> dict:
        """列举 + 抽取 + 精确去重。返回统计。"""
        stats = {"discovered": 0, "extracted": 0, "exact_dup": 0, "failed": 0}
        # 1) 盘点
        _report(progress, 0, 0, "开始盘点源系统…")
        for item in connector.discover():
            if self.led.upsert_discovered(item):
                stats["discovered"] += 1
        _report(progress, 0, stats["discovered"], f"盘点完成，发现 {stats['discovered']} 项，开始抽取")
        # 2) 对 DISCOVERED 逐个下载 + 提取
        pending = self.led.items_in_stage(Stage.DISCOVERED)
        total = len(pending)
        for done, row in enumerate(pending, 1):
            key = row["stable_key"]
            _report(progress, done, total, f"抽取 {done}/{total}: {row['original_name'] or key}")
            item = self._row_to_item(row)
            try:
                item = connector.fetch(item)
            except Exception as e:  # noqa: BLE001
                self.led.update(key, stage=Stage.FAILED, error_detail=f"fetch: {e}")
                stats["failed"] += 1
                continue
            if item.raw_metadata.get("skip_reason"):
                self.led.update(key, stage=Stage.FAILED,
                                error_detail=item.raw_metadata["skip_reason"])
                stats["failed"] += 1
                continue
            # 精确去重：sha256 已在台账中出现且已入库 -> 跳过
            if item.content_sha256:
                dups = [r for r in self.led.find_by_sha(item.content_sha256)
                        if r["stable_key"] != key
                        and r["stage"] in (Stage.LOADED.value, Stage.CONFIRMED.value,
                                           Stage.CLASSIFIED.value, Stage.DEDUPED.value,
                                           Stage.EXTRACTED.value)]
                if dups:
                    self.led.update(
                        key, stage=Stage.SKIPPED_DUPLICATE.value,
                        dedup_verdict=DedupVerdict.EXACT_DUPLICATE.value,
                        dedup_cluster_id=item.content_sha256,
                        content_sha256=item.content_sha256,
                        local_blob_path=item.local_blob_path,
                    )
                    stats["exact_dup"] += 1
                    continue
            # 文本提取
            result = extract_text(item.local_blob_path) if item.local_blob_path else None
            text = result.text if result else ""
            note = "" if (result and result.ok) else (result.note if result else "no blob")
            self._save_text(key, text)
            self.led.update(
                key, stage=Stage.EXTRACTED.value,
                content_sha256=item.content_sha256,
                local_blob_path=item.local_blob_path,
                dedup_cluster_id=item.content_sha256,
                error_detail=note or None,
            )
            stats["extracted"] += 1
        return stats

    # ── 阶段 2：dedup（近似）──────────────────────────────

    def dedup_pass(self, threshold: float = 0.75, progress: ProgressCb = None) -> dict:
        stats = {"unique": 0, "near_dup": 0}
        idx = NearDuplicateIndex(threshold=threshold)
        rows = self.led.items_in_stage(Stage.EXTRACTED)
        total = len(rows)
        for done, row in enumerate(rows, 1):
            key = row["stable_key"]
            _report(progress, done, total, f"近似去重 {done}/{total}")
            text = self.load_text(key)
            res = idx.query(text)
            if res.verdict == DedupVerdict.NEAR_DUPLICATE:
                self.led.update(
                    key, stage=Stage.DEDUPED.value,
                    dedup_verdict=DedupVerdict.NEAR_DUPLICATE.value,
                    error_detail=f"近似重复 of {res.matched_key} sim={res.similarity:.2f}",
                )
                stats["near_dup"] += 1
            else:
                idx.add(key, text)
                self.led.update(key, stage=Stage.DEDUPED.value,
                                dedup_verdict=DedupVerdict.UNIQUE.value)
                stats["unique"] += 1
        return stats

    # ── 阶段 3：classify ──────────────────────────────────

    # 走批量的最小条数：低于此数直接逐条（批量的轮询开销不划算）
    _BATCH_MIN = 8

    def _apply_classification(self, row, r) -> bool:
        """把一条分类结果 r 回写台账（算规范名 + 置信路由）。返回是否自动确认。

        供批量与逐条两条路径共用，避免逻辑重复。
        """
        key = row["stable_key"]
        cat = self.tx.get(r.category)
        doc_type = r.doc_type or (cat.doc_types[0] if cat and cat.doc_types else "doc")
        ext = os.path.splitext(row["original_name"] or "")[1]
        cname = canonical_name(doc_type=doc_type, title=r.title or row["original_name"],
                               doc_date=r.doc_date, version="1", ext=ext)
        auto = (r.confidence >= self.threshold and not r.needs_human_review
                and r.category != self.tx.triage_path)
        self.led.update(
            key,
            stage=(Stage.CONFIRMED if auto else Stage.CLASSIFIED).value,
            category=r.category, confidence=r.confidence, canonical_name=cname,
            metadata_json=json.dumps(r.model_dump(), ensure_ascii=False, default=str),
        )
        return auto

    def _mark_near_dup_to_review(self, key: str) -> None:
        """近似重复项默认进人工复核，不自动分类归档。"""
        self.led.update(key, stage=Stage.CLASSIFIED.value,
                        category=self.tx.triage_path, confidence=0.0)

    def classify_pass(self, classifier: Classifier, progress: ProgressCb = None,
                      use_batch: Optional[bool] = None) -> dict:
        """AI 分类。use_batch=None 时读配置 KBM_CLAUDE_USE_BATCH（默认 True）。

        满足「配置开启 + classifier 在线 + 待分类数≥阈值」时走 Message Batches
        （token 5 折）；批量端点不可用（中转网关不代理/超时）时整批回退逐条
        classify_one（带 prompt cache，功能不打折）。
        """
        stats = {"classified": 0, "auto_confirmed": 0, "to_review": 0}
        rows = self.led.items_in_stage(Stage.DEDUPED)
        total = len(rows)
        # 近似重复项先摘出（不送模型）；其余进待分类集
        to_model = []
        for row in rows:
            if row["dedup_verdict"] == DedupVerdict.NEAR_DUPLICATE.value:
                self._mark_near_dup_to_review(row["stable_key"])
                stats["classified"] += 1
                stats["to_review"] += 1
            else:
                to_model.append(row)

        if use_batch is None:
            try:
                from ..config import get_settings
                use_batch = get_settings().claude_use_batch
            except Exception:  # noqa: BLE001  取配置失败不阻断，退回逐条
                use_batch = False

        want_batch = use_batch and classifier.online and len(to_model) >= self._BATCH_MIN
        if want_batch:
            from .classify import BatchUnavailable
            items = [(row["stable_key"], row["original_name"] or "", self.load_text(row["stable_key"]))
                     for row in to_model]
            try:
                results = classifier.classify_batch(items, progress=progress)
                for done, row in enumerate(to_model, 1):
                    _report(progress, done, len(to_model),
                            f"回填分类 {done}/{len(to_model)}: {row['original_name'] or ''}")
                    r = results.get(row["stable_key"])
                    if r is None:  # 理论上不会发生（classify_batch 已补齐），保险起见
                        r = classifier.classify_one(self.load_text(row["stable_key"]),
                                                    row["original_name"] or "")
                    auto = self._apply_classification(row, r)
                    stats["classified"] += 1
                    stats["auto_confirmed" if auto else "to_review"] += 1
                return stats
            except BatchUnavailable as e:
                _report(progress, 0, len(to_model), f"批量不可用，回退逐条：{e}")

        # 逐条路径（离线 / 小批量 / 批量回退）
        for done, row in enumerate(to_model, 1):
            key = row["stable_key"]
            _report(progress, done, len(to_model),
                    f"AI 分类 {done}/{len(to_model)}: {row['original_name'] or ''}")
            r = classifier.classify_one(self.load_text(key), row["original_name"] or "")
            auto = self._apply_classification(row, r)
            stats["classified"] += 1
            stats["auto_confirmed" if auto else "to_review"] += 1
        return stats

    # ── 人工确认（Web 控制台调用）────────────────────────

    def confirm(self, stable_key: str, category: str,
                canonical_name_override: Optional[str] = None) -> None:
        fields = {"stage": Stage.CONFIRMED.value, "category": category, "confidence": 1.0}
        if canonical_name_override:
            fields["canonical_name"] = canonical_name_override
        self.led.update(stable_key, **fields)

    def reject_as_duplicate(self, stable_key: str) -> None:
        self.led.update(stable_key, stage=Stage.SKIPPED_DUPLICATE.value,
                        dedup_verdict=DedupVerdict.NEAR_DUPLICATE.value)

    # ── 阶段 4：load（写飞书）────────────────────────────

    def load_pass(self, writer, folder_map: dict[str, str],
                  progress: ProgressCb = None) -> dict:
        """folder_map: {category_path: feishu_folder_token}。返回统计。

        writer 为 FeishuWriter；为空(None)时进入 dry-run，仅打印将执行的动作。
        """
        stats = {"loaded": 0, "failed": 0, "dry_run": 0}
        rows = self.led.items_in_stage(Stage.CONFIRMED)
        total = len(rows)
        for done, row in enumerate(rows, 1):
            key = row["stable_key"]
            _report(progress, done, total, f"写飞书 {done}/{total}: {row['original_name'] or ''}")
            category = row["category"] or self.tx.triage_path
            folder = folder_map.get(category) or folder_map.get(self.tx.triage_path, "")
            name = row["canonical_name"] or row["original_name"]
            if writer is None:
                stats["dry_run"] += 1
                continue
            try:
                # 幂等护栏：upload_file 非幂等，重试会产生重复 Drive 文件。
                # 若已有 feishu_token（上次上传成功、仅后续步骤失败），跳过重传，
                # 只重跑对外分享收紧。无 token 才走完整上传。
                file_token = row["feishu_token"]
                if file_token:
                    _report(progress, done, total, f"已存在 token，跳过重传：{name}")
                else:
                    file_token = writer.upload_file(row["local_blob_path"], folder, name)
                writer.lock_down_external(file_token, "file")
                self.led.update(key, stage=Stage.LOADED.value, feishu_token=file_token)
                stats["loaded"] += 1
            except Exception as e:  # noqa: BLE001
                self.led.update(key, stage=Stage.FAILED.value, error_detail=f"load: {e}")
                stats["failed"] += 1
        return stats

    def retry_failed_loads(self, writer, folder_map: dict[str, str],
                           progress: ProgressCb = None) -> dict:
        """重试写飞书失败的条目：先把 load 阶段失败项重排回 CONFIRMED，再跑 load_pass。

        只回捞 error_detail 以 "load: " 开头的 FAILED（不动 fetch/ingest 失败）。
        依赖 load_pass 的幂等护栏：已上传成功（有 feishu_token）的条目重试时不重传。
        """
        n = self.led.requeue_failed(error_prefix="load: ", target_stage=Stage.CONFIRMED)
        _report(progress, 0, n, f"重排 {n} 条写飞书失败项 -> CONFIRMED")
        stats = self.load_pass(writer, folder_map, progress=progress)
        stats["requeued"] = n
        return stats

    # ── 阶段 5：把已上传云文件挂进 Wiki 节点 ─────────────

    def move_loaded_to_wiki(self, writer, targets: dict, user_token: str = "",
                            progress: ProgressCb = None) -> dict:
        """把 stage=LOADED 的文件挂入 Wiki 对应分类节点（以用户身份重传后挂载）。

        targets: bootstrap --wiki 产出，需含 space_id + wiki_node_map{分类: node_token}。

        **关键约束（实测）**：Wiki 空间由用户 OAuth 创建、归用户所有；只有【用户本人
        拥有的文档】能 move_docs_to_wiki 挂进去。load 阶段的文件是租户(应用)上传、租户
        拥有的，即便把用户加 full_access、甚至 transfer_owner，move 仍报 131006 no move
        permission。故此处用 user_token 从本地 blob 重新上传（文件归用户）再挂载，成功后
        把租户旧副本删入回收站（去重、可恢复）。

        每条按 category 找父节点，upload_as_user_and_mount 挂入，回写 wiki_node_token。
        **stage 仍保持 LOADED**（不回退）；wiki_node_token 是否存在即幂等标记：已挂入重跑跳过。

        writer 为 None 时 dry-run，仅统计不真实调用。
        失败不翻 FAILED（避免与 load 失败混淆），仅记 error_detail="wiki: ..."，
        下次重跑自动重试（仍无 wiki_node_token；writer 侧已回滚孤儿用户副本）。
        """
        stats = {"mounted": 0, "skipped": 0, "failed": 0, "dry_run": 0}
        space_id = targets.get("space_id") or ""
        node_map = targets.get("wiki_node_map") or {}
        rows = self.led.items_in_stage(Stage.LOADED)
        total = len(rows)
        for done, row in enumerate(rows, 1):
            key = row["stable_key"]
            name = row["canonical_name"] or row["original_name"] or key
            _report(progress, done, total, f"挂入 Wiki {done}/{total}: {name}")
            if row["wiki_node_token"]:          # 幂等：已挂入
                stats["skipped"] += 1
                continue
            blob = row["local_blob_path"]
            if not blob or not os.path.exists(blob):   # 无本地副本，无从以用户身份重传
                stats["skipped"] += 1
                continue
            category = row["category"] or self.tx.triage_path
            parent = node_map.get(category) or node_map.get(self.tx.triage_path, "")
            if writer is None:
                stats["dry_run"] += 1
                continue
            try:
                res = writer.upload_as_user_and_mount(space_id, blob, name, parent,
                                                      user_token=user_token)
                wiki_token = res.get("wiki_token")
                if not wiki_token:               # 未确认挂载成功：不记幂等标记，下次重试
                    raise RuntimeError("未返回 wiki_token（挂载未确认）")
                self.led.update(key, wiki_node_token=wiki_token, error_detail=None)
                stats["mounted"] += 1
                # 成功后清理租户旧副本（进回收站，可恢复），避免云空间与 Wiki 重复
                old = row["feishu_token"]
                if old:
                    try:
                        writer.delete_drive_file(old, "file")
                    except Exception:  # noqa: BLE001  清理失败不影响迁移成功事实
                        pass
            except Exception as e:  # noqa: BLE001
                self.led.update(key, error_detail=f"wiki: {e}")
                stats["failed"] += 1
        return stats

    # ── 工具 ──────────────────────────────────────────────

    @staticmethod
    def _row_to_item(row) -> SourceItem:
        from ..models import SourceType
        return SourceItem(
            source_type=SourceType(row["source_type"]),
            source_id=row["source_id"],
            source_path=row["source_path"] or "",
            original_name=row["original_name"] or "",
            size=row["size"] or 0,
            content_sha256=row["content_sha256"],
            local_blob_path=row["local_blob_path"],
            raw_metadata=json.loads(row["metadata_json"]) if row["metadata_json"] else {},
        )
