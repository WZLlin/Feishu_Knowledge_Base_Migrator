"""语义去重接线（离线）：注入假向量索引，验证 semantic_pass 标疑似重复进人工队列。

要点：
- 不改 stage、不自动删；只把相似对的一方标 dedup_verdict=semantic_candidate；
- 被标项通过 pending_review() 进人工确认队列；
- 缺重依赖（sentence-transformers/faiss）时 available=False，优雅跳过不阻断。
"""
from kb_migrator.ledger import Ledger
from kb_migrator.models import DedupVerdict, SourceItem, SourceType, Stage
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy

TAX = "config/taxonomy.yaml"


class _FakeIndex:
    """脚本化语义索引：available=True，build 记录入参，candidates 回放预置相似对。"""
    def __init__(self, pairs):
        self._pairs = pairs
        self.built = None

    @property
    def available(self):
        return True

    def build(self, items):
        self.built = items

    def candidates(self):
        return self._pairs


class _Unavailable:
    available = False

    def build(self, items):  # pragma: no cover - 不应被调用
        raise AssertionError("不可用索引不应 build")

    def candidates(self):  # pragma: no cover
        return []


def _orch(tmp_path):
    led = Ledger(str(tmp_path / "ledger.db"))
    orch = Orchestrator(led, Taxonomy.load(TAX), str(tmp_path / "work"))
    return led, orch


def _add(led, orch, key_id, text, stage=Stage.DEDUPED):
    item = SourceItem(source_type=SourceType.LOCAL, source_id=key_id,
                      source_path=f"/{key_id}.txt", original_name=f"{key_id}.txt")
    led.upsert_discovered(item)
    key = item.stable_key()
    orch._save_text(key, text)
    led.update(key, stage=stage.value, dedup_verdict=DedupVerdict.UNIQUE.value)
    return key


def test_semantic_pass_flags_candidate_into_review(tmp_path):
    led, orch = _orch(tmp_path)
    k1 = _add(led, orch, "a", "远程办公管理制度 第一版")
    k2 = _add(led, orch, "b", "远程办公管理办法 修订版")

    idx = _FakeIndex(pairs=[(k1, k2, 0.95)])
    stats = orch.semantic_pass(index=idx)

    assert stats["available"] is True
    assert stats["indexed"] == 2          # 两条有正文的候选进索引
    assert stats["candidates"] == 1
    # 索引确实收到 (key, text) 列表
    assert {k for k, _ in idx.built} == {k1, k2}

    # 被标的一方（k2）：verdict=semantic_candidate，stage 不变（仍 DEDUPED）
    row = led.get(k2)
    assert row["dedup_verdict"] == DedupVerdict.SEMANTIC_CANDIDATE.value
    assert row["stage"] == Stage.DEDUPED.value
    assert "语义疑似重复" in (row["error_detail"] or "")
    # 另一方（k1）保留，未被标
    assert led.get(k1)["dedup_verdict"] == DedupVerdict.UNIQUE.value

    # 进人工确认队列
    review_keys = {r["stable_key"] for r in led.pending_review()}
    assert k2 in review_keys
    led.close()


def test_semantic_pass_degrades_when_unavailable(tmp_path):
    led, orch = _orch(tmp_path)
    _add(led, orch, "a", "任意正文")
    stats = orch.semantic_pass(index=_Unavailable())
    assert stats["available"] is False
    assert stats["candidates"] == 0
    led.close()
