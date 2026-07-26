"""批量分类（Message Batches）：桩掉 anthropic 客户端，验证提交-轮询-回填。

要点：
- 结果**乱序**返回，回填必须按 custom_id 建键（非位置）；
- 单条 errored/缺失 → 降级启发式，强制 needs_human_review；
- 提交层失败 → BatchUnavailable，供编排器整批回退逐条。
"""
from types import SimpleNamespace

import pytest

from kb_migrator.pipeline.classify import BatchUnavailable, Classifier
from kb_migrator.taxonomy import Taxonomy


def _tx():
    return Taxonomy.load("config/taxonomy.yaml")


def _tool_result(custom_id, category):
    block = SimpleNamespace(type="tool_use", input={
        "category": category, "confidence": 0.95, "needs_human_review": False,
        "title": "标题", "rationale": "ok", "doc_type": "policy",
        "tags": ["a"], "summary": "s", "obsolete_flag": False,
    })
    msg = SimpleNamespace(content=[block])
    return SimpleNamespace(custom_id=custom_id,
                           result=SimpleNamespace(type="succeeded", message=msg))


def _errored_result(custom_id):
    return SimpleNamespace(custom_id=custom_id,
                           result=SimpleNamespace(type="errored", message=None))


class _FakeBatches:
    def __init__(self, results, statuses=("in_progress", "ended")):
        self._results = results
        self._statuses = list(statuses)
        self.created_with = None

    def create(self, requests):
        self.created_with = requests
        return SimpleNamespace(id="batch_1")

    def retrieve(self, batch_id):
        status = self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]
        return SimpleNamespace(
            processing_status=status,
            request_counts=SimpleNamespace(succeeded=1, errored=1, canceled=0, expired=0),
        )

    def results(self, batch_id):
        return list(self._results)


class _FakeClient:
    def __init__(self, batches):
        self.messages = SimpleNamespace(batches=batches)


def _clf(client):
    clf = Classifier(_tx())          # 无 key -> 离线
    clf._client = client             # 注入桩 -> online=True
    return clf


def test_batch_backfills_by_custom_id_unordered():
    tx = _tx()
    valid = tx.category_paths()[0]
    # 结果故意乱序：先返回 b 再返回 a
    fake = _FakeBatches([_tool_result("b", valid), _tool_result("a", valid)])
    clf = _clf(_FakeClient(fake))
    assert clf.online

    items = [("a", "甲.docx", "制度正文"), ("b", "乙.docx", "会议纪要")]
    out = clf.classify_batch(items, poll_interval=0.01)

    assert set(out) == {"a", "b"}                 # 按 custom_id 建键
    assert out["a"].category == valid
    assert out["b"].category == valid
    assert fake.created_with is not None and len(fake.created_with) == 2


def test_batch_errored_item_degrades_to_review():
    tx = _tx()
    valid = tx.category_paths()[0]
    fake = _FakeBatches([_tool_result("a", valid), _errored_result("b")])
    clf = _clf(_FakeClient(fake))

    out = clf.classify_batch([("a", "甲", "x"), ("b", "乙", "y")], poll_interval=0.01)
    assert out["a"].needs_human_review is False
    assert out["b"].needs_human_review is True    # errored -> 启发式兜底


def test_batch_missing_custom_id_is_filled():
    tx = _tx()
    valid = tx.category_paths()[0]
    # 只返回 a 的结果，b 缺失 -> classify_batch 应补齐 b
    fake = _FakeBatches([_tool_result("a", valid)])
    clf = _clf(_FakeClient(fake))

    out = clf.classify_batch([("a", "甲", "x"), ("b", "乙", "y")], poll_interval=0.01)
    assert set(out) == {"a", "b"}
    assert out["b"].needs_human_review is True


def test_batch_submit_failure_raises_unavailable():
    class _Boom(_FakeBatches):
        def create(self, requests):
            raise RuntimeError("404 Not Found")

    clf = _clf(_FakeClient(_Boom([])))
    with pytest.raises(BatchUnavailable):
        clf.classify_batch([("a", "甲", "x")], poll_interval=0.01)


def test_offline_classifier_raises_unavailable():
    clf = Classifier(_tx())           # 离线
    assert not clf.online
    with pytest.raises(BatchUnavailable):
        clf.classify_batch([("a", "甲", "x")])
