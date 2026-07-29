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
import re
from datetime import date
from functools import wraps
from typing import Callable, Optional

# 进度回调：progress(done, total, msg)。默认 None，不影响 CLI/测试。
ProgressCb = Optional[Callable[[int, int, str], None]]


def _report(cb: ProgressCb, done: int, total: int, msg: str = "") -> None:
    if cb:
        cb(done, total, msg)


def _tracked(run_type: str):
    """把一次顶层管线调用记录为可审计批次，不改变原方法返回结构。"""
    def decorate(fn):
        @wraps(fn)
        def wrapped(self, *args, **kwargs):
            run_id = self.led.start_pipeline_run(
                run_type,
                structure_version_id=str(
                    kwargs.get("structure_version_id") or ""
                ),
            )
            try:
                result = fn(self, *args, **kwargs)
            except Exception as exc:
                self.led.finish_pipeline_run(
                    run_id, error=f"{type(exc).__name__}: {exc}",
                )
                raise
            self.led.finish_pipeline_run(
                run_id, stats=result if isinstance(result, dict) else {},
            )
            return result
        return wrapped
    return decorate

from ..ledger import Ledger
from ..models import DedupVerdict, SourceItem, Stage
from ..taxonomy import Taxonomy
from ..utils.naming import canonical_name
from ..utils.identity import resolve_identity
from ..utils.governance import add_months, governance_fields
from .classify import Classifier
from .dedup import NearDuplicateIndex, SemanticIndex
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

    @_tracked("ingest")
    def ingest(self, connector, progress: ProgressCb = None) -> dict:
        """列举 + 抽取 + 精确去重。返回统计。"""
        stats = {"discovered": 0, "changed": 0, "missing": 0, "extracted": 0, "exact_dup": 0, "failed": 0}
        from ..ledger import _now
        scan_started_at = _now()
        seen_keys: set[str] = set()
        # 1) 盘点
        _report(progress, 0, 0, "开始盘点源系统…")
        source_type = getattr(connector, "source_name", "")
        try:
            from ..models import SourceType
            source_type = SourceType(source_type)
        except ValueError as e:
            raise ValueError(f"连接器未声明合法 source_name: {source_type!r}") from e
        for item in connector.discover():
            if item.source_type != source_type:
                raise ValueError(
                    f"连接器 source_name={source_type.value!r} 却发现了 "
                    f"{item.source_type.value!r} 条目"
                )
            result = self.led.record_discovered(item)
            seen_keys.add(item.stable_key())
            if result.created:
                stats["discovered"] += 1
            if result.changed:
                stats["changed"] += 1
        if source_type.value == "local" and getattr(connector, "root", ""):
            stats["missing"] = self.led.mark_missing_local_sources(
                connector.root, seen_keys, scan_started_at)
        _report(progress, 0, stats["discovered"], f"盘点完成，发现 {stats['discovered']} 项，开始抽取")
        # 2) 只对当前来源的 DISCOVERED 下载 + 提取，避免跨连接器误处理。
        pending = self.led.items_in_stage(Stage.DISCOVERED, source_type)
        total = len(pending)
        for done, row in enumerate(pending, 1):
            key = row["stable_key"]
            _report(progress, done, total, f"抽取 {done}/{total}: {row['original_name'] or key}")
            item = self._row_to_item(row)
            try:
                item = connector.fetch(item)
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(key, "fetch", str(e))
                stats["failed"] += 1
                continue
            # 仅在连接器明确说明权限采集成功时更新快照；无权限/接口失败时保留旧值。
            if item.raw_metadata.get("permissions_collected"):
                self.led.replace_source_permissions(key, item.permissions)
            if item.raw_metadata.get("skip_reason"):
                self.led.mark_failed(key, "fetch", item.raw_metadata["skip_reason"],
                                     retryable=False)
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
            # 文本提取。失败进入结构化队列，避免空正文被静默推进到分类阶段。
            try:
                result = extract_text(item.local_blob_path) if item.local_blob_path else None
                if not result or not result.ok:
                    note = result.note if result else "no blob"
                    self.led.update(
                        key, content_sha256=item.content_sha256,
                        local_blob_path=item.local_blob_path,
                        dedup_cluster_id=item.content_sha256,
                        extraction_ok=0, extraction_note=note,
                        extracted_text_chars=0,
                    )
                    non_retryable = note.startswith("unsupported ext")
                    self.led.mark_failed(
                        key, "extract", note, retryable=not non_retryable,
                    )
                    stats["failed"] += 1
                    continue
                self._save_text(key, result.text)
                self.led.update(
                    key, stage=Stage.EXTRACTED.value,
                    content_sha256=item.content_sha256,
                    local_blob_path=item.local_blob_path,
                    dedup_cluster_id=item.content_sha256,
                    error_detail=result.note or None,
                    failed_stage=None,
                    extraction_ok=1, extraction_note=result.note or "",
                    extracted_text_chars=len(result.text),
                )
                stats["extracted"] += 1
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(key, "extract", str(e))
                stats["failed"] += 1
        return stats

    # ── 阶段 1b：群聊会话存档 -> 会话片段 -> 标准 items 管线 ──

    @_tracked("ingest_chat")
    def ingest_chat(self, connector, chat_id: str, chat_name: str = "",
                    limit: int = 1000, max_batches: int = 1000,
                    progress: ProgressCb = None) -> dict:
        """群聊会话存档迁移：把一个群的历史消息按自然日聚合成「会话片段」，
        每片段渲染为 .md 落 work_dir，作为 SourceItem(WECOM_CHAT) 登记进 items 表并置
        EXTRACTED，随后复用既有 dedup/classify/confirm/load/push-to-wiki 全流程。

        增量：以 chat_migrations.last_message_seq 为游标，只拉新消息（seq 不推进即停）。
        幂等：按天分片，source_id=chat_id:YYYY-MM-DD；已进入后续阶段(已分类/确认/入库)的
        旧片段重跑跳过，不回退。member_snapshot/last_message_seq/status 回写 chat_migrations。

        connector 需实现 fetch_messages(seq, limit)（ChatArchiveConnector；离线抛 RuntimeError）。
        """
        from ..models import SourceType
        from ..connectors.wecom_chat import aggregate_messages

        stats = {"batches": 0, "messages": 0, "segments": 0, "skipped_existing": 0,
                 "files": 0, "files_skipped": 0}
        chat_dir = os.path.join(self.work_dir, "chat")
        file_dir = os.path.join(chat_dir, "files")
        os.makedirs(file_dir, exist_ok=True)
        # 仅当连接器在线且实现 fetch_media 时下载群文件（离线/旧假连接器向后兼容跳过）
        media_ok = getattr(connector, "online", False) and hasattr(connector, "fetch_media")

        row = self.led.get_chat(chat_id)
        seq = int(row["last_message_seq"]) if row and row["last_message_seq"] else 0
        name = chat_name or (row["chat_name_original"] if row else "") or ""
        self.led.upsert_chat(chat_id, chat_name_original=name,
                             migration_status="in_progress")

        # 1) 按 seq 游标增量拉取所有新消息批次
        all_msgs: list[dict] = []
        for _ in range(max_batches):
            msgs, new_seq = connector.fetch_messages(seq, limit)
            if not msgs:
                break
            all_msgs.extend(msgs)
            stats["batches"] += 1
            _report(progress, stats["batches"], 0,
                     f"拉取会话批次 {stats['batches']}，累计 {len(all_msgs)} 条")
            if new_seq <= seq:      # 游标未推进：防死循环
                seq = new_seq
                break
            seq = new_seq
            if len(msgs) < limit:   # 不足一批，已到末尾
                break
        stats["messages"] = len(all_msgs)

        # 2) 聚合为会话片段（按自然日）
        segments = aggregate_messages(chat_id, all_msgs)
        members: set[str] = set()
        done_stages = (Stage.CLASSIFIED.value, Stage.CONFIRMED.value,
                       Stage.LOADED.value, Stage.SKIPPED_DUPLICATE.value)
        for done, seg in enumerate(segments, 1):
            members.update(seg.participants)
            sid = f"{chat_id}:{seg.date}"
            item = SourceItem(
                source_type=SourceType.WECOM_CHAT, source_id=sid,
                source_path=f"wecom_chat://{chat_id}/{seg.date}",
                original_name=f"群聊_{name or chat_id}_{seg.date}.md",
                raw_metadata={"chat_id": chat_id, "date": seg.date,
                              "participants": seg.participants, "files": seg.files},
            )
            key = item.stable_key()
            _report(progress, done, len(segments), f"会话片段 {seg.date}")
            existing = self.led.get(key)
            if existing and existing["stage"] in done_stages:
                stats["skipped_existing"] += 1   # 已进入后续阶段，幂等跳过
            else:
                text = seg.to_text()
                blob = os.path.join(chat_dir, f"{chat_id}_{seg.date}.md")
                with open(blob, "w", encoding="utf-8") as f:
                    f.write(text)
                sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
                item.local_blob_path = blob
                item.content_sha256 = sha
                self.led.upsert_discovered(item)      # 幂等登记（已存在则不覆盖）
                self._save_text(key, text)
                self.led.update(key, stage=Stage.EXTRACTED.value, content_sha256=sha,
                                local_blob_path=blob, dedup_cluster_id=sha, error_detail=None)
                stats["segments"] += 1

            # 群文件：下载媒体 -> 抽取 -> 登记为 WECOM_CHAT 文件项进标准管线
            if media_ok:
                for f in seg.files:
                    self._ingest_chat_file(connector, chat_id, name or chat_id,
                                           seg.date, f, file_dir, done_stages, stats)

        # 3) 回写群聊台账（增量游标 + 成员快照 + 状态）
        self.led.upsert_chat(
            chat_id, last_message_seq=str(seq), member_snapshot=sorted(members),
            migration_status="completed" if (segments or row) else "partial",
        )
        return stats

    def _ingest_chat_file(self, connector, chat_id: str, chat_name: str, date: str,
                          f: dict, file_dir: str, done_stages: tuple, stats: dict) -> None:
        """下载一条群文件消息的媒体，抽取文本，登记为 WECOM_CHAT 文件项(EXTRACTED)。

        幂等：已进入后续阶段的同 sdkfileid 文件跳过；精确去重：sha 命中已入库者标
        SKIPPED_DUPLICATE。sdkfileid 缺失或下载失败则记 files_skipped，不阻断整批。
        """
        from ..models import SourceType

        sdkfileid = f.get("sdkfileid") or ""
        filename = f.get("filename") or sdkfileid or "群文件"
        if not sdkfileid:
            stats["files_skipped"] += 1
            return
        fitem = SourceItem(
            source_type=SourceType.WECOM_CHAT, source_id=f"{chat_id}:file:{sdkfileid}",
            source_path=f"wecom_chat://{chat_id}/file/{sdkfileid}",
            original_name=filename, size=int(f.get("filesize", 0) or 0),
            raw_metadata={"chat_id": chat_id, "kind": "file", "date": date,
                          "sdkfileid": sdkfileid},
        )
        key = fitem.stable_key()
        existing = self.led.get(key)
        if existing and existing["stage"] in done_stages:
            stats["files_skipped"] += 1        # 幂等跳过
            return
        try:
            data = connector.fetch_media(sdkfileid)
        except Exception as e:  # noqa: BLE001  单个文件下载失败不阻断整批
            self.led.upsert_discovered(fitem)
            self.led.mark_failed(key, "fetch", f"群文件下载失败 {e}")
            stats["files_skipped"] += 1
            return
        sha = hashlib.sha256(data).hexdigest()
        # 精确去重：sha 已在库且已入后续阶段 -> 跳过
        dups = [r for r in self.led.find_by_sha(sha)
                if r["stable_key"] != key
                and r["stage"] in (Stage.LOADED.value, Stage.CONFIRMED.value,
                                   Stage.CLASSIFIED.value, Stage.DEDUPED.value,
                                   Stage.EXTRACTED.value)]
        local = os.path.join(file_dir, f"{sdkfileid[:32]}_{filename}")
        with open(local, "wb") as fp:
            fp.write(data)
        fitem.local_blob_path = local
        fitem.content_sha256 = sha
        self.led.upsert_discovered(fitem)
        if dups:
            self.led.update(key, stage=Stage.SKIPPED_DUPLICATE.value,
                            dedup_verdict=DedupVerdict.EXACT_DUPLICATE.value,
                            dedup_cluster_id=sha, content_sha256=sha,
                            local_blob_path=local)
            stats["files_skipped"] += 1
            return
        result = extract_text(local)
        if not result or not result.ok:
            note = result.note if result else "no blob"
            self.led.update(key, content_sha256=sha, local_blob_path=local,
                            dedup_cluster_id=sha, extraction_ok=0,
                            extraction_note=note, extracted_text_chars=0)
            self.led.mark_failed(
                key, "extract", note,
                retryable=not note.startswith("unsupported ext"),
            )
            stats["files_skipped"] += 1
            return
        self._save_text(key, result.text)
        self.led.update(key, stage=Stage.EXTRACTED.value, content_sha256=sha,
                        local_blob_path=local, dedup_cluster_id=sha,
                        error_detail=result.note or None, failed_stage=None,
                        extraction_ok=1, extraction_note=result.note or "",
                        extracted_text_chars=len(result.text))
        stats["files"] += 1

    # ── 阶段 2：dedup（近似）──────────────────────────────

    @_tracked("dedup")
    def dedup_pass(self, threshold: float = 0.75, progress: ProgressCb = None,
                   index: NearDuplicateIndex | None = None) -> dict:
        """近似去重，并将历史唯一条目加入候选索引。

        历史正文仍位于 work_dir/text；缓存已清理的旧条目不会参与近似比较，但不影响
        当前批次继续处理。语义去重仍可作为更高成本的周期性补充。
        """
        stats = {"unique": 0, "near_dup": 0, "historical_indexed": 0, "failed": 0}
        idx = index or NearDuplicateIndex(threshold=threshold)
        historical_stages = (Stage.DEDUPED, Stage.CLASSIFIED, Stage.CONFIRMED, Stage.LOADED)
        for stage in historical_stages:
            for old in self.led.items_in_stage(stage):
                if old["dedup_verdict"] != DedupVerdict.UNIQUE.value:
                    continue
                text = self.load_text(old["stable_key"])
                if text.strip():
                    idx.add(old["stable_key"], text)
                    stats["historical_indexed"] += 1
        rows = self.led.items_in_stage(Stage.EXTRACTED)
        total = len(rows)
        for done, row in enumerate(rows, 1):
            key = row["stable_key"]
            _report(progress, done, total, f"近似去重 {done}/{total}")
            try:
                text = self.load_text(key)
                res = idx.query(text)
                if res.verdict == DedupVerdict.NEAR_DUPLICATE:
                    self.led.update(
                        key, stage=Stage.DEDUPED.value,
                        dedup_verdict=DedupVerdict.NEAR_DUPLICATE.value,
                        error_detail=f"近似重复 of {res.matched_key} sim={res.similarity:.2f}",
                        failed_stage=None,
                    )
                    stats["near_dup"] += 1
                else:
                    idx.add(key, text)
                    self.led.update(key, stage=Stage.DEDUPED.value,
                                    dedup_verdict=DedupVerdict.UNIQUE.value,
                                    failed_stage=None)
                    stats["unique"] += 1
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(key, "dedup", str(e))
                stats["failed"] += 1
        return stats

    # ── 阶段 2b：semantic（语义近邻，周期性疑似重复审查）────

    # 语义审查覆盖的阶段：已过精确/近似去重、仍在库的条目（不含已判重/失败）
    _SEMANTIC_STAGES = (Stage.DEDUPED, Stage.CLASSIFIED, Stage.CONFIRMED, Stage.LOADED)

    @_tracked("semantic")
    def semantic_pass(self, progress: ProgressCb = None, index=None,
                      cos_threshold: float = 0.90) -> dict:
        """语义去重（第三层）：对已过精确/近似去重的条目建向量索引，把 cos≥阈值 的
        相似对里的一方标 SEMANTIC_CANDIDATE 进人工队列——**不改 stage、不自动删**。

        重依赖（sentence-transformers + faiss）缺失时该层自动跳过（available=False），
        不阻断管线。index 可注入（测试/自定义模型）；缺省内建 SemanticIndex。

        pending_review() 以 dedup_verdict='semantic_candidate' 直接纳入人工队列，
        故标记只写 dedup_verdict + error_detail，保留原 stage 不回退。
        """
        stats = {"available": True, "indexed": 0, "pairs": 0, "candidates": 0}
        idx = index if index is not None else SemanticIndex(cos_threshold=cos_threshold)
        if not idx.available:
            stats["available"] = False
            _report(progress, 0, 0, "语义去重不可用：缺 sentence-transformers/faiss，跳过")
            return stats

        # 收集候选集：跨相关阶段、排除已判近似重复的条目，取有正文者
        rows = []
        seen_keys: set[str] = set()
        for st in self._SEMANTIC_STAGES:
            for row in self.led.items_in_stage(st):
                key = row["stable_key"]
                if key in seen_keys:
                    continue
                if row["dedup_verdict"] == DedupVerdict.NEAR_DUPLICATE.value:
                    continue
                seen_keys.add(key)
                rows.append(row)

        items = [(r["stable_key"], self.load_text(r["stable_key"])) for r in rows]
        items = [(k, t) for k, t in items if t.strip()]
        stats["indexed"] = len(items)
        _report(progress, 0, len(items), f"语义索引：{len(items)} 条")
        if not items:
            return stats

        idx.build(items)
        pairs = idx.candidates()
        stats["pairs"] = len(pairs)
        # 每对保留一方（key_a，通常更早），把另一方(key_b)标为疑似进人工队列
        marked: set[str] = set()
        for done, (key_a, key_b, sim) in enumerate(pairs, 1):
            _report(progress, done, len(pairs), f"语义疑似 {done}/{len(pairs)}")
            if key_b in marked:
                continue
            self.led.update(
                key_b, dedup_verdict=DedupVerdict.SEMANTIC_CANDIDATE.value,
                error_detail=f"语义疑似重复 of {key_a} cos={sim:.2f}",
            )
            marked.add(key_b)
        stats["candidates"] = len(marked)
        return stats

    # ── 阶段 3：classify ──────────────────────────────────

    # 走批量的最小条数：低于此数直接逐条（批量的轮询开销不划算）
    _BATCH_MIN = 8

    def _apply_classification(self, row, r, classifier_version: str = "") -> bool:
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
        fields = governance_fields(cat, r.doc_date)
        self.led.update(
            key,
            stage=(Stage.CONFIRMED if auto else Stage.CLASSIFIED).value,
            category=r.category, confidence=r.confidence, canonical_name=cname,
            metadata_json=json.dumps(r.model_dump(), ensure_ascii=False, default=str),
            classifier_version=classifier_version,
            **fields,
        )
        return auto

    def _mark_near_dup_to_review(self, key: str) -> None:
        """近似重复项默认进人工复核，不自动分类归档。"""
        self.led.update(key, stage=Stage.CLASSIFIED.value,
                        category=self.tx.triage_path, confidence=0.0)

    @_tracked("classify")
    def classify_pass(self, classifier: Classifier, progress: ProgressCb = None,
                      use_batch: Optional[bool] = None) -> dict:
        """AI 分类。use_batch=None 时读配置 KBM_CLAUDE_USE_BATCH（默认 True）。

        满足「配置开启 + classifier 在线 + 待分类数≥阈值」时走 Message Batches
        （token 5 折）；批量端点不可用（中转网关不代理/超时）时整批回退逐条
        classify_one（带 prompt cache，功能不打折）。
        """
        stats = {"classified": 0, "auto_confirmed": 0, "to_review": 0, "failed": 0}
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
        classifier_version = (
            f"anthropic:{classifier.model}" if classifier.online
            else "offline:heuristic-v1"
        )
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
                    auto = self._apply_classification(row, r, classifier_version)
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
            try:
                r = classifier.classify_one(self.load_text(key), row["original_name"] or "")
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(key, "classify", str(e))
                stats["failed"] += 1
                continue
            auto = self._apply_classification(row, r, classifier_version)
            stats["classified"] += 1
            stats["auto_confirmed" if auto else "to_review"] += 1
        return stats

    # ── 人工确认（Web 控制台调用）────────────────────────

    def confirm(self, stable_key: str, category: str,
                canonical_name_override: Optional[str] = None) -> None:
        row = self.led.get(stable_key)
        if not row:
            raise KeyError(stable_key)
        if category not in self.tx.category_paths():
            raise ValueError(f"非法分类：{category}")
        metadata = json.loads(row["metadata_json"] or "{}") if row else {}
        fields = {"stage": Stage.CONFIRMED.value, "category": category, "confidence": 1.0}
        fields.update(governance_fields(self.tx.get(category), metadata.get("doc_date")))
        if canonical_name_override:
            fields["canonical_name"] = canonical_name_override
        self.led.update(stable_key, **fields)
        if row:
            self.led.record_classification_feedback(stable_key, row["category"], category,
                                                    row["confidence"])

    def triage_topic_signals(self, limit: int = 20) -> list[tuple[str, int]]:
        """对待整理正文做轻量词频聚合，辅助人工判断是否应增设目录。"""
        counts: dict[str, int] = {}
        for row in self.led.pending_review():
            if row["category"] != self.tx.triage_path:
                continue
            terms = set(re.findall(r"[\u4e00-\u9fff]{2,}|[A-Za-z][A-Za-z0-9_-]{2,}", self.load_text(row["stable_key"])))
            for term in terms:
                counts[term] = counts.get(term, 0) + 1
        return sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:limit]

    def classification_calibration(self, *, target_precision: float = 0.90,
                                   min_samples: int = 5) -> dict:
        """用人工确认反馈评估自动确认阈值，数据不足时不擅自调整。"""
        rows = self.led.classification_feedback_rows()
        candidates = []
        for threshold in (0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95):
            selected = [r for r in rows if float(r["confidence"] or 0) >= threshold]
            matches = sum(
                r["suggested_category"] == r["confirmed_category"] for r in selected
            )
            candidates.append({
                "threshold": threshold,
                "samples": len(selected),
                "precision": round(matches / len(selected), 3) if selected else None,
                "coverage": round(len(selected) / len(rows), 3) if rows else 0.0,
            })
        eligible = [
            c for c in candidates
            if c["samples"] >= min_samples
            and c["precision"] is not None
            and c["precision"] >= target_precision
        ]
        recommendation = eligible[0]["threshold"] if eligible else self.threshold
        return {
            "feedback_samples": len(rows),
            "target_precision": target_precision,
            "min_samples": min_samples,
            "recommended_threshold": recommendation,
            "current_threshold": self.threshold,
            "auto_applicable": bool(eligible),
            "candidates": candidates,
        }

    @staticmethod
    def _cluster_terms(text: str) -> set[str]:
        latin = {
            word.lower() for word in re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}", text)
        }
        chinese_runs = re.findall(r"[\u4e00-\u9fff]{2,}", text)
        bigrams = {
            run[i:i + 2] for run in chinese_runs for i in range(len(run) - 1)
        }
        return latin | bigrams

    def triage_topic_clusters(self, *, limit: int = 20,
                              similarity: float = 0.20) -> list[dict]:
        """按正文词项 Jaccard 相似度聚合待整理文档，产出可解释的知识缺口簇。"""
        docs = []
        for row in self.led.pending_review():
            if row["category"] != self.tx.triage_path:
                continue
            text = f"{row['original_name'] or ''}\n{self.load_text(row['stable_key'])}"
            docs.append({
                "key": row["stable_key"],
                "name": row["original_name"] or row["stable_key"],
                "terms": self._cluster_terms(text),
            })
        clusters: list[list[dict]] = []
        for doc in docs:
            best, best_score = None, 0.0
            for cluster in clusters:
                union_terms = set().union(*(member["terms"] for member in cluster))
                union = doc["terms"] | union_terms
                score = len(doc["terms"] & union_terms) / len(union) if union else 0.0
                if score > best_score:
                    best, best_score = cluster, score
            if best is not None and best_score >= similarity:
                best.append(doc)
            else:
                clusters.append([doc])
        result = []
        for number, cluster in enumerate(
                sorted(clusters, key=lambda c: (-len(c), c[0]["name"])), 1):
            frequency: dict[str, int] = {}
            for member in cluster:
                for term in member["terms"]:
                    frequency[term] = frequency.get(term, 0) + 1
            shared = sorted(frequency, key=lambda t: (-frequency[t], t))[:8]
            result.append({
                "cluster": number,
                "size": len(cluster),
                "terms": shared,
                "items": [{"key": d["key"], "name": d["name"]} for d in cluster],
            })
        return result[:limit]

    def complete_review(self, stable_key: str, *, actor: str = "",
                        reviewed_on: date | None = None) -> str:
        row = self.led.get(stable_key)
        if not row:
            raise KeyError(stable_key)
        category = self.tx.get(row["category"] or "")
        months = category.review_months if category else 12
        next_due = add_months(reviewed_on or date.today(), months).isoformat()
        self.led.complete_review(stable_key, next_due, actor=actor)
        return next_due

    def reject_as_duplicate(self, stable_key: str) -> None:
        if not self.led.get(stable_key):
            raise KeyError(stable_key)
        self.led.update(stable_key, stage=Stage.SKIPPED_DUPLICATE.value,
                        dedup_verdict=DedupVerdict.NEAR_DUPLICATE.value)

    def sync_source_permissions(self, writer, stable_key: str, target_token: str,
                                target_type: str, identity_map: dict[str, str]) -> dict:
        """把来源权限映射为飞书协作者，并撤销本工具管理的过期授权。

        撤销范围严格限定为 permission_syncs 中本工具曾成功授予的主体，不碰人工
        在飞书侧添加的协作者。
        """
        stats = {"granted": 0, "updated": 0, "revoked": 0,
                 "skipped": 0, "unmapped": [], "failed": 0}
        item = self.led.get(stable_key)
        grants = [(row["principal"], row["role"]) for row in self.led.source_permissions(stable_key)]
        if item and item["owner"]:
            grants.append((item["owner"], "full_access"))
        if item and item["steward"]:
            grants.append((item["steward"], "edit"))
        rank = {"view": 1, "edit": 2, "full_access": 3}
        merged: dict[str, str] = {}
        for principal, perm in grants:
            if rank.get(perm, 0) > rank.get(merged.get(principal, ""), 0):
                merged[principal] = perm
        previous_rows = self.led.managed_permissions(
            stable_key, target_token, target_type,
        )
        previous: dict[str, object] = {}
        for old in previous_rows:
            previous[old["principal"]] = old
        for principal, perm in merged.items():
            open_id = resolve_identity(identity_map, principal)
            if not open_id:
                stats["unmapped"].append(principal)
                continue
            if self.led.permission_sync_succeeded(stable_key, target_token, target_type,
                                                   principal, perm):
                stats["skipped"] += 1
                continue
            try:
                old = previous.get(principal)
                if old:
                    writer.update_collaborator(
                        target_token, target_type, open_id, perm=perm,
                    )
                    self.led.close_managed_permission(
                        stable_key, target_token, target_type, principal, "superseded",
                    )
                    stats["updated"] += 1
                else:
                    writer.add_collaborator(
                        target_token, target_type, open_id, perm=perm,
                    )
                    stats["granted"] += 1
                self.led.record_permission_sync(stable_key, target_token, target_type,
                                                principal, open_id, perm, "succeeded")
            except Exception as e:  # noqa: BLE001
                self.led.record_permission_sync(stable_key, target_token, target_type,
                                                principal, open_id, perm, "failed", str(e))
                stats["failed"] += 1
        for principal, old in previous.items():
            if principal in merged:
                continue
            try:
                writer.remove_collaborator(
                    target_token, target_type, old["feishu_open_id"],
                )
                self.led.close_managed_permission(
                    stable_key, target_token, target_type, principal, "revoked",
                )
                stats["revoked"] += 1
            except Exception:  # noqa: BLE001
                # 保持 succeeded，后续同步仍会再次尝试撤销。
                stats["failed"] += 1
        return stats

    @_tracked("archive")
    def archive_due_items(self, writer, archive_folder_token: str, *, commit: bool = False,
                          reason: str = "retention_due", progress: ProgressCb = None,
                          wiki_space_id: str = "", wiki_archive_node: str = "",
                          user_token: str = "",
                          structure_version_id: str = "") -> dict:
        """将保留期到期的 Drive 文件或 Wiki 节点移入对应的 99 归档目录。"""
        stats = {"archived": 0, "wiki_archived": 0, "dry_run": 0,
                 "skipped": 0, "failed": 0}
        rows = self.led.governance_items(triage_path=self.tx.triage_path)["archive_due"]
        for done, row in enumerate(rows, 1):
            key = row["stable_key"]
            _report(progress, done, len(rows), f"归档 {done}/{len(rows)}: {row['original_name'] or key}")
            if row["wiki_node_token"]:
                if not (wiki_space_id and wiki_archive_node):
                    stats["skipped"] += 1
                    continue
                if not commit:
                    stats["dry_run"] += 1
                    continue
                try:
                    writer.move_wiki_node(
                        wiki_space_id, row["wiki_node_token"],
                        wiki_archive_node, user_token=user_token,
                    )
                    self.led.mark_archived(key, reason)
                    stats["archived"] += 1
                    stats["wiki_archived"] += 1
                except Exception as e:  # noqa: BLE001
                    self.led.mark_failed(
                        key, "archive", str(e), preserve_stage=True,
                    )
                    stats["failed"] += 1
                continue
            if not row["feishu_token"]:
                stats["skipped"] += 1
                continue
            if not commit:
                stats["dry_run"] += 1
                continue
            try:
                writer.move_file(row["feishu_token"], archive_folder_token, "file")
                self.led.mark_archived(key, reason)
                stats["archived"] += 1
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(key, "archive", str(e))
                stats["failed"] += 1
        return stats

    def finalize_source_change(self, change_id: int, writer, archive_folder_token: str,
                               *, commit: bool = False) -> dict:
        """新版本已入库后，将旧 Drive 副本归档并完成变更事件。"""
        change = self.led.get_source_change(change_id)
        if not change or change["status"] != "materialized":
            raise ValueError("变更不存在或尚未物化")
        note = change["resolution_note"] or ""
        if "derived=" not in note:
            raise ValueError("变更缺少派生版本关联")
        derived_key = note.split("derived=", 1)[1].strip()
        old, new = self.led.get(change["stable_key"]), self.led.get(derived_key)
        if not new or new["stage"] != Stage.LOADED.value:
            raise ValueError("新版本尚未完成写入")
        result = {"old_key": change["stable_key"], "new_key": derived_key, "dry_run": not commit}
        if not commit:
            return result
        if old and old["feishu_token"] and not old["wiki_node_token"]:
            try:
                writer.move_file(old["feishu_token"], archive_folder_token, "file")
                self.led.mark_archived(old["stable_key"], f"superseded_by:{derived_key}")
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(
                    old["stable_key"], "archive", str(e), preserve_stage=True,
                )
                raise
        self.led.complete_source_change(change_id, f"superseded_by={derived_key}")
        result["completed"] = True
        return result

    def resolve_missing_source(self, stable_key: str, action: str, writer=None,
                               archive_folder_token: str = "", commit: bool = False) -> dict:
        row = self.led.get(stable_key)
        if not row or not row["source_missing_at"]:
            raise ValueError("条目未标记为源端删除")
        if action == "keep":
            self.led.resolve_missing_source(stable_key, "keep")
            return {"kept": True}
        if action != "archive":
            raise ValueError("action 必须为 keep/archive")
        if not commit:
            return {"dry_run": True, "token": row["feishu_token"]}
        if row["feishu_token"] and not row["wiki_node_token"]:
            try:
                writer.move_file(row["feishu_token"], archive_folder_token, "file")
                self.led.mark_archived(stable_key, "source_deleted")
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(
                    stable_key, "archive", str(e), preserve_stage=True,
                )
                raise
        self.led.resolve_missing_source(stable_key, "archived")
        return {"archived": True}

    # ── 阶段 4：load（写飞书）────────────────────────────

    @_tracked("load")
    def load_pass(self, writer, folder_map: dict[str, str],
                  progress: ProgressCb = None, identity_map: dict[str, str] | None = None,
                  *, structure_version_id: str = "",
                  target_node_map: dict[str, str] | None = None,
                  target_resolver: Callable | None = None) -> dict:
        """folder_map: {category_path: feishu_folder_token}。返回统计。

        writer 为 FeishuWriter；为空(None)时进入 dry-run，仅打印将执行的动作。
        """
        stats = {"loaded": 0, "failed": 0, "dry_run": 0,
                 "permissions_granted": 0, "permissions_unmapped": 0, "permissions_failed": 0}
        identity_map = identity_map or {}
        target_node_map = target_node_map or {}
        rows = self.led.items_in_stage(Stage.CONFIRMED)
        total = len(rows)
        for done, row in enumerate(rows, 1):
            key = row["stable_key"]
            _report(progress, done, total, f"写飞书 {done}/{total}: {row['original_name'] or ''}")
            category = row["category"] or self.tx.triage_path
            resolved = target_resolver(row) if target_resolver else {}
            folder = (
                resolved.get("remote_token")
                or folder_map.get(category)
                or folder_map.get(self.tx.triage_path, "")
            )
            node_id = (
                resolved.get("node_id")
                or target_node_map.get(category)
                or target_node_map.get(self.tx.triage_path, "")
            )
            assignment_source = resolved.get("assignment_source") or "category"
            name = row["canonical_name"] or row["original_name"]
            if writer is None:
                stats["dry_run"] += 1
                continue
            try:
                if not folder:
                    raise RuntimeError(f"结构版本缺少分类目标目录：{category}")
                if structure_version_id:
                    if not node_id:
                        raise RuntimeError(f"结构版本缺少分类目标节点：{category}")
                    self.led.assign_structure_target(
                        key, structure_version_id, node_id,
                        source=assignment_source, confidence=row["confidence"],
                    )
                # 幂等护栏：upload_file 非幂等，重试会产生重复 Drive 文件。
                # 若已有 feishu_token（上次上传成功、仅后续步骤失败），跳过重传，
                # 只重跑对外分享收紧。无 token 才走完整上传。
                file_token = row["feishu_token"]
                if file_token:
                    _report(progress, done, total, f"已存在 token，跳过重传：{name}")
                else:
                    file_token = writer.upload_file(row["local_blob_path"], folder, name)
                writer.lock_down_external(file_token, "file")
                perm_stats = self.sync_source_permissions(
                    writer, key, file_token, "file", identity_map)
                stats["permissions_granted"] += perm_stats["granted"]
                stats["permissions_unmapped"] += len(perm_stats["unmapped"])
                stats["permissions_failed"] += perm_stats["failed"]
                self.led.update(key, stage=Stage.LOADED.value, feishu_token=file_token)
                stats["loaded"] += 1
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(key, "load", str(e))
                stats["failed"] += 1
        return stats

    def retry_failed_loads(self, writer, folder_map: dict[str, str],
                           progress: ProgressCb = None,
                           identity_map: dict[str, str] | None = None,
                           *, structure_version_id: str = "",
                           target_node_map: dict[str, str] | None = None,
                           target_resolver: Callable | None = None) -> dict:
        """重试写飞书失败的条目：先把 load 阶段失败项重排回 CONFIRMED，再跑 load_pass。

        只回捞 error_detail 以 "load: " 开头的 FAILED（不动 fetch/ingest 失败）。
        依赖 load_pass 的幂等护栏：已上传成功（有 feishu_token）的条目重试时不重传。
        """
        n = self.led.requeue_failed(error_prefix="load: ", target_stage=Stage.CONFIRMED)
        _report(progress, 0, n, f"重排 {n} 条写飞书失败项 -> CONFIRMED")
        stats = self.load_pass(
            writer, folder_map, progress=progress, identity_map=identity_map,
            structure_version_id=structure_version_id,
            target_node_map=target_node_map,
            target_resolver=target_resolver,
        )
        stats["requeued"] = n
        return stats

    # ── 阶段 5：把已上传云文件挂进 Wiki 节点 ─────────────

    @_tracked("wiki")
    def move_loaded_to_wiki(self, writer, targets: dict, user_token: str = "",
                            progress: ProgressCb = None,
                            identity_map: dict[str, str] | None = None,
                            *, structure_version_id: str = "",
                            target_resolver: Callable | None = None) -> dict:
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
        失败保留 LOADED（文件入库事实不回退），同时以 failed_stage=wiki 进入统一失败清单；
        下次重跑或 retry-failed --stage wiki 均可重试。
        """
        stats = {"mounted": 0, "skipped": 0, "failed": 0, "dry_run": 0,
                 "permissions_granted": 0, "permissions_unmapped": 0}
        identity_map = identity_map or {}
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
                self.led.mark_failed(
                    key, "wiki", "缺少本地文件副本，无法以用户身份重传",
                    retryable=False, preserve_stage=True,
                )
                stats["failed"] += 1
                continue
            category = row["category"] or self.tx.triage_path
            resolved = target_resolver(row) if target_resolver else {}
            parent = (
                resolved.get("remote_token")
                or node_map.get(category)
                or node_map.get(self.tx.triage_path, "")
            )
            if writer is None:
                stats["dry_run"] += 1
                continue
            try:
                if not parent:
                    raise RuntimeError(f"结构版本缺少 Wiki 分类目标：{category}")
                if structure_version_id and resolved.get("node_id"):
                    self.led.assign_structure_target(
                        key, structure_version_id, resolved["node_id"],
                        source=resolved.get("assignment_source") or "category",
                        confidence=row["confidence"],
                    )
                res = writer.upload_as_user_and_mount(space_id, blob, name, parent,
                                                      user_token=user_token)
                wiki_token = res.get("wiki_token")
                if not wiki_token:               # 未确认挂载成功：不记幂等标记，下次重试
                    raise RuntimeError("未返回 wiki_token（挂载未确认）")
                self.led.update(
                    key, wiki_node_token=wiki_token, error_detail=None,
                    failed_stage=None,
                )
                perm_stats = self.sync_source_permissions(
                    writer, key, wiki_token, "wiki", identity_map)
                stats["permissions_granted"] += perm_stats["granted"]
                stats["permissions_unmapped"] += len(perm_stats["unmapped"])
                stats["mounted"] += 1
                # 成功后清理租户旧副本（进回收站，可恢复），避免云空间与 Wiki 重复
                old = row["feishu_token"]
                if old:
                    try:
                        writer.delete_drive_file(old, "file")
                    except Exception:  # noqa: BLE001  清理失败不影响迁移成功事实
                        pass
            except Exception as e:  # noqa: BLE001
                self.led.mark_failed(
                    key, "wiki", str(e), preserve_stage=True,
                )
                stats["failed"] += 1
        return stats

    # ── 群聊治理：成员→协作者映射 + 群名打标 ────────────────

    def map_chat_collaborators(self, writer, chat_id: str, user_map: dict[str, str],
                               perm: str = "view", progress: ProgressCb = None) -> dict:
        """按群成员快照给该群产出的每个飞书文档逐个加协作者（默认 view）。

        user_map: {wecom_userid: feishu_open_id}（本地 JSON 维护）。未命中映射的成员
        进 unmapped 人工清单，不阻断。每个群文档单独加（决策：per-doc 粒度）：优先用
        wiki_node_token(obj_type=wiki)，否则 feishu_token(obj_type=file)。

        writer 为 None 时 dry-run：仅统计将加的 (文档, 成员) 数，不真实调用。
        """
        stats = {"docs": 0, "members": 0, "granted": 0, "failed": 0,
                 "unmapped": [], "dry_run": 0}
        chat = self.led.get_chat(chat_id)
        if not chat:
            stats["error"] = f"台账无群 {chat_id} 记录"
            return stats
        members = json.loads(chat["member_snapshot"]) if chat["member_snapshot"] else []
        mapped: list[str] = []
        for uid in members:
            fid = user_map.get(uid)
            if fid:
                mapped.append(fid)
            else:
                stats["unmapped"].append(uid)
        stats["members"] = len(mapped)

        docs = [r for r in self.led.items_for_chat(chat_id)
                if r["wiki_node_token"] or r["feishu_token"]]
        stats["docs"] = len(docs)
        total = len(docs) * max(len(mapped), 1)
        done = 0
        for row in docs:
            token = row["wiki_node_token"] or row["feishu_token"]
            obj_type = "wiki" if row["wiki_node_token"] else "file"
            for fid in mapped:
                done += 1
                _report(progress, done, total,
                        f"加协作者 {row['original_name'] or token}")
                if writer is None:
                    stats["dry_run"] += 1
                    continue
                try:
                    writer.add_collaborator(token, obj_type, fid, perm=perm)
                    stats["granted"] += 1
                except Exception as e:  # noqa: BLE001  单个失败进清单不阻断
                    stats["failed"] += 1
                    stats["unmapped"].append(f"{fid}@{obj_type}:{e}")
        return stats

    def tag_chat_group(self, group_connector, chat_id: str,
                       feishu_url: str = "", progress: ProgressCb = None) -> dict:
        """群名打标「原群名[已备份]」（尽力改名，失败降级发通知，再失败记 manual）。

        group_connector 为 WeComGroupConnector；结果 tag_status 回写 chat_migrations。
        group_connector 为 None 时 dry-run：仅回显将执行的动作。
        """
        chat = self.led.get_chat(chat_id)
        original = (chat["chat_name_original"] if chat else "") or ""
        if group_connector is None:
            _report(progress, 0, 1, f"[dry-run] 将打标群 {chat_id}「{original}[已备份]」")
            return {"tag_status": "dry_run", "detail": f"预览：{original}[已备份]"}
        _report(progress, 0, 1, f"群名打标 {chat_id}…")
        res = group_connector.tag_group(chat_id, original, feishu_url=feishu_url)
        self.led.upsert_chat(chat_id, tag_status=res.get("tag_status"),
                             error_detail=res.get("detail"))
        _report(progress, 1, 1, f"群名打标：{res.get('tag_status')} — {res.get('detail')}")
        return res

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
