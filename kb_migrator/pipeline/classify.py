"""AI 分类 + 元数据抽取（Claude 结构化输出）。

关键设计：
- `category` 用 taxonomy 合法路径的 **enum 约束**，模型不可能编造目录；
- 单次调用同时产出分类 + 元数据 + 置信度 + 理由 + 是否需人工复核；
- 静态前缀（目录定义 / schema / few-shot）放在 system，打 prompt cache 断点，
  每份文档只变动用户消息，缓存读仅 1 折；schema 全程稳定，避免语法重编译；
- 批量走 Message Batches API（输入输出各 5 折），实时单条走 Messages API；
- 无 API Key 时降级为关键词启发式分类器，保证管线离线可跑与可测。

置信路由由编排器执行：confidence ≥ 阈值 且 not needs_human_review → 自动确认；
否则进人工确认队列。
"""
from __future__ import annotations

import json
import time
from typing import Callable, Optional

from ..models import ClassificationResult
from ..taxonomy import Taxonomy

_MAX_DOC_CHARS = 6000   # 送模型的正文截断长度（首尾各取，控制 token）

# batch 进度回调：progress(done, total, msg)
BatchProgressCb = Optional[Callable[[int, int, str], None]]


class BatchUnavailable(RuntimeError):
    """批量端点不可用/超时/提交失败——调用方应据此回退逐条 classify_one。

    典型触发：中转网关不代理 batches 端点（404/NotFound）、SDK 无 batches、
    轮询超时。**不用于**单条 result 失败（那类在结果回填时降级为启发式）。
    """


def _report_batch(cb: BatchProgressCb, done: int, total: int, msg: str = "") -> None:
    if cb:
        cb(done, total, msg)


def _build_schema(category_enum: list[str]) -> dict:
    """结构化输出 JSON Schema。category 强约束为合法目录路径枚举。"""
    return {
        "type": "object",
        "properties": {
            "category": {"type": "string", "enum": category_enum},
            "confidence": {"type": "number"},
            "rationale": {"type": "string"},
            "needs_human_review": {"type": "boolean"},
            "title": {"type": "string"},
            "doc_type": {"type": "string"},
            "doc_date": {"type": ["string", "null"]},
            "tags": {"type": "array", "items": {"type": "string"}},
            "summary": {"type": "string"},
            "obsolete_flag": {"type": "boolean"},
        },
        "required": [
            "category", "confidence", "rationale", "needs_human_review",
            "title", "doc_type", "tags", "summary", "obsolete_flag",
        ],
        "additionalProperties": False,
    }


def _system_prompt(tx: Taxonomy) -> str:
    """静态前缀：目录定义 + 分类规则。作为 prompt cache 命中的稳定内容。"""
    lines = [
        "你是组织知识库的文档分类助手。请把给定文档归入下列固定目录之一，"
        "并抽取元数据。只能从给定目录路径中选择，不得自造目录。",
        "",
        "【可选目录】",
    ]
    for c in tx.categories:
        lines.append(f"- {c.path}（适用类型: {', '.join(c.doc_types) or '通用'}）")
    lines.append(f"- {tx.triage_path}（无法可靠归类时选此项，并设 needs_human_review=true）")
    lines += [
        "",
        "【规则】",
        "1. category 必须是上述路径之一（逐字一致）。",
        "2. confidence 为你对该分类的把握 0~1；不确定就调低并 needs_human_review=true。",
        "3. title 为规范化的中文标题；doc_type 用英文小写标签（如 policy/meeting/template）。",
        "4. doc_date 尽量抽取文档自身日期(YYYY-MM-DD)，无则 null。",
        "5. obsolete_flag：出现『作废/已废止/被…替代/草案待批/有效期已过』等信号时置 true。",
        "6. summary 为 2-3 句中文摘要。tags 为 3-6 个主题词。",
    ]
    return "\n".join(lines)


def _truncate(text: str) -> str:
    if len(text) <= _MAX_DOC_CHARS:
        return text
    head = text[: _MAX_DOC_CHARS // 2]
    tail = text[-_MAX_DOC_CHARS // 2 :]
    return f"{head}\n...(中略)...\n{tail}"


class Classifier:
    def __init__(self, taxonomy: Taxonomy, api_key: str = "", model: str = "claude-sonnet-5",
                 base_url: str = "", auth_style: str = "auto"):
        self.tx = taxonomy
        self.model = model
        self.category_enum = taxonomy.category_paths()
        self.schema = _build_schema(self.category_enum)
        self.system = _system_prompt(taxonomy)
        self._client = None
        if api_key:
            try:
                import anthropic

                kwargs: dict = {}
                if base_url:
                    kwargs["base_url"] = base_url
                # sk- 短格式中转多用 Bearer(auth_token)；官方长 key 用 x-api-key。
                # auto：非官方(sk-ant- 之外)且配了自建网关 → 走 Bearer。
                use_bearer = auth_style == "bearer" or (
                    auth_style == "auto" and base_url and not api_key.startswith("sk-ant-")
                )
                if use_bearer:
                    kwargs["auth_token"] = api_key
                else:
                    kwargs["api_key"] = api_key
                self._client = anthropic.Anthropic(**kwargs)
            except ImportError:
                self._client = None

    @property
    def online(self) -> bool:
        return self._client is not None

    # ── 单条分类 ──────────────────────────────────────────

    def classify_one(self, text: str, filename: str = "") -> ClassificationResult:
        if not self.online:
            return self._heuristic(text, filename)
        return self._claude_one(text, filename)

    def _claude_one(self, text: str, filename: str) -> ClassificationResult:
        user = f"文件名：{filename}\n\n正文：\n{_truncate(text)}"
        # tool-use 强制结构化输出：定义一个 record_classification 工具，schema 即入参。
        tool = {
            "name": "record_classification",
            "description": "记录文档分类与元数据",
            "input_schema": self.schema,
        }
        resp = self._client.messages.create(
            model=self.model,
            max_tokens=1024,
            system=[{
                "type": "text",
                "text": self.system,
                # 缓存断点打在静态前缀，随后每份文档的 user 消息不进缓存
                "cache_control": {"type": "ephemeral"},
            }],
            tools=[tool],
            tool_choice={"type": "tool", "name": "record_classification"},
            messages=[{"role": "user", "content": user}],
        )
        for block in resp.content:
            if getattr(block, "type", None) == "tool_use":
                return self._coerce(block.input)
        # 未拿到 tool_use（refusal/max_tokens 等）→ 兜底进人工
        return ClassificationResult(
            category=self.tx.triage_path, confidence=0.0,
            rationale="模型未返回结构化结果", needs_human_review=True,
        )

    def _coerce(self, data: dict) -> ClassificationResult:
        # enum 已由 schema 约束，这里做大小写/非法值兜底
        cat = data.get("category", "")
        if cat not in self.category_enum:
            cat = self.tx.triage_path
            data["needs_human_review"] = True
        data["category"] = cat
        return ClassificationResult(**{
            k: data.get(k) for k in ClassificationResult.model_fields if k in data
        })

    # ── 批量分类（Message Batches API）────────────────────

    def build_batch_requests(self, items: list[tuple[str, str, str]]) -> list[dict]:
        """items: [(custom_id, filename, text)]，产出 batch 请求体列表。

        真正提交交由调用方（编排器）用 client.messages.batches.create，
        以便统一管理轮询与结果回填台账。此处只负责构造，保证 schema 稳定。
        """
        tool = {
            "name": "record_classification",
            "description": "记录文档分类与元数据",
            "input_schema": self.schema,
        }
        reqs = []
        for custom_id, filename, text in items:
            reqs.append({
                "custom_id": custom_id,
                "params": {
                    "model": self.model,
                    "max_tokens": 1024,
                    "system": [{"type": "text", "text": self.system,
                                "cache_control": {"type": "ephemeral"}}],
                    "tools": [tool],
                    "tool_choice": {"type": "tool", "name": "record_classification"},
                    "messages": [{"role": "user",
                                  "content": f"文件名：{filename}\n\n正文：\n{_truncate(text)}"}],
                },
            })
        return reqs

    def classify_batch(self, items: list[tuple[str, str, str]], *,
                       progress: BatchProgressCb = None,
                       poll_interval: float = 5.0,
                       poll_timeout: float = 1800.0) -> dict[str, ClassificationResult]:
        """批量分类：提交 Message Batches → 轮询至结束 → 按 custom_id 回填。

        items: [(custom_id, filename, text)]。返回 {custom_id: ClassificationResult}，
        覆盖所有传入 custom_id（单条 result 失败/过期时降级为启发式，标 needs_review）。

        提交/轮询层面的失败（网关不支持 batches、超时等）抛 BatchUnavailable，
        由编排器捕获后整批回退逐条 classify_one（后者带 prompt cache，功能不打折）。
        结果乱序返回，**一律以 custom_id 建键，绝不按位置对齐**。
        """
        if not self.online:
            raise BatchUnavailable("classifier 离线，无法走批量")
        if not items:
            return {}
        # custom_id -> (filename, text)，供单条失败时的启发式兜底
        lookup = {cid: (fn, tx) for cid, fn, tx in items}

        try:
            batches = self._client.messages.batches
        except AttributeError as e:  # SDK 版本过旧/中转封装无此属性
            raise BatchUnavailable(f"SDK 无 messages.batches: {e}") from e

        try:
            batch = batches.create(requests=self.build_batch_requests(items))
        except Exception as e:  # noqa: BLE001  网关 404/NotFound/鉴权等 → 回退
            raise BatchUnavailable(f"batch 提交失败: {e}") from e

        # 轮询直到 processing_status == "ended"
        _report_batch(progress, 0, len(items), f"batch 已提交 id={getattr(batch, 'id', '?')}，等待处理…")
        deadline = time.monotonic() + poll_timeout
        while True:
            try:
                cur = batches.retrieve(batch.id)
            except Exception as e:  # noqa: BLE001
                raise BatchUnavailable(f"batch 轮询失败: {e}") from e
            if getattr(cur, "processing_status", None) == "ended":
                break
            if time.monotonic() >= deadline:
                raise BatchUnavailable(f"batch 轮询超时（>{poll_timeout}s）")
            counts = getattr(cur, "request_counts", None)
            done = (getattr(counts, "succeeded", 0) + getattr(counts, "errored", 0)
                    + getattr(counts, "canceled", 0) + getattr(counts, "expired", 0)) if counts else 0
            _report_batch(progress, done, len(items), "batch 处理中…")
            time.sleep(poll_interval)

        # 回填结果，按 custom_id 建键
        out: dict[str, ClassificationResult] = {}
        try:
            for res in batches.results(batch.id):
                cid = getattr(res, "custom_id", None)
                if cid is None:
                    continue
                out[cid] = self._parse_batch_result(res, lookup.get(cid, ("", "")))
        except Exception as e:  # noqa: BLE001  结果拉取整体失败 → 回退逐条
            raise BatchUnavailable(f"batch 结果拉取失败: {e}") from e

        # 兜底：任何未出现在结果中的 custom_id 用启发式补齐（避免漏项）
        for cid, (fn, tx) in lookup.items():
            if cid not in out:
                out[cid] = self._heuristic(tx, fn)
        _report_batch(progress, len(items), len(items), f"batch 回填完成，共 {len(out)} 条")
        return out

    def _parse_batch_result(self, res, fallback_item: tuple[str, str]) -> ClassificationResult:
        """解析单条 batch result；成功取 tool_use，否则启发式兜底（标 needs_review）。"""
        result = getattr(res, "result", None)
        if getattr(result, "type", None) == "succeeded":
            message = getattr(result, "message", None)
            for block in getattr(message, "content", []) or []:
                if getattr(block, "type", None) == "tool_use":
                    return self._coerce(dict(block.input))
        # errored / expired / canceled / 无 tool_use → 启发式，强制人工复核
        fn, tx = fallback_item
        return self._heuristic(tx, fn)

    # ── 离线启发式兜底（无 API Key 时）────────────────────

    def _heuristic(self, text: str, filename: str) -> ClassificationResult:
        """关键词启发式：仅用于离线联调/演示，一律标记需人工复核。"""
        blob = f"{filename}\n{text}".lower()
        rules = [
            ("01 制度与流程", ["制度", "流程", "policy", "规定", "管理办法", "sop"]),
            ("03 会议与结论", ["会议", "纪要", "结论", "minutes", "meeting", "决议"]),
            ("05 模板与规范", ["模板", "template", "规范", "标准", "checklist"]),
            ("02 项目资料", ["项目", "project", "需求", "方案", "prd"]),
            ("04 业务经验与方法", ["经验", "复盘", "方法", "指南", "best practice"]),
            ("06 参考资料", ["参考", "reference", "资料", "手册", "manual"]),
        ]
        hit, score = self.tx.triage_path, 0
        for path, kws in rules:
            s = sum(kw in blob for kw in kws)
            if s > score:
                hit, score = path, s
        title = filename or (text.strip().splitlines()[0][:40] if text.strip() else "未命名")
        return ClassificationResult(
            category=hit if hit in self.category_enum else self.tx.triage_path,
            confidence=0.4 if score else 0.0,
            rationale=f"离线启发式命中关键词数={score}（需人工复核）",
            needs_human_review=True,
            title=title,
            doc_type="", tags=[], summary="", obsolete_flag=False,
        )
