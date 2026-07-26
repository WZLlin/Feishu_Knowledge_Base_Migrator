"""把已上传云文件挂进 Wiki 节点：move_loaded_to_wiki 编排 + writer 轮询。

- 只处理 stage=LOADED 且有 feishu_token 的条目，按 category 找 wiki 父节点；
- wiki_node_token 是否存在即幂等标记：已挂入的重跑跳过；
- dry-run（writer=None）不真实调用；失败保留 LOADED、记 error_detail="wiki: "；
- writer.mount_doc_to_wiki：同步返回 wiki_token 直接用；返回 task_id 则轮询任务端点。
"""
from kb_migrator.ledger import Ledger
from kb_migrator.models import SourceItem, SourceType, Stage
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy


def _mk(tmp_path):
    led = Ledger(str(tmp_path / "ledger.db"))
    tx = Taxonomy.load("config/taxonomy.yaml")
    orch = Orchestrator(led, tx, str(tmp_path / "work"))
    return led, orch


def _loaded(led, tmp_path, sid, name, cat, token="tok_"):
    blob = tmp_path / f"blob_{sid}_{name}"
    blob.write_text("x", encoding="utf-8")     # 需真实存在（编排器会 os.path.exists 校验）
    item = SourceItem(source_type=SourceType.LOCAL, source_id=sid,
                      source_path=f"/x/{name}", original_name=name)
    led.upsert_discovered(item)
    key = item.stable_key()
    led.update(key, stage=Stage.LOADED.value, category=cat,
               feishu_token=token + sid, local_blob_path=str(blob))
    return key


_TARGETS = {
    "mode": "wiki", "space_id": "spc_1",
    "wiki_node_map": {"01 制度与流程": "node_policy", "06 参考资料": "node_ref"},
}


class _FakeWriter:
    """记录每次挂入（本地路径→父节点）、删除的租户旧副本，按名返回 wiki_token。"""
    def __init__(self, fail_on=None):
        self.calls = []
        self.deleted = []
        self.fail_on = fail_on or set()          # 命中的 name 抛错

    def upload_as_user_and_mount(self, space_id, local_path, name, parent_wiki_token="",
                                 user_token="", sensitive=False, poll_timeout=60.0):
        self.calls.append((space_id, local_path, name, parent_wiki_token))
        if name in self.fail_on:
            raise RuntimeError("挂载失败")
        return {"wiki_token": "wiki_" + name, "obj_token": "usrfile_" + name}

    def delete_drive_file(self, token, obj_type="file", user_token=""):
        self.deleted.append(token)


def test_move_loaded_to_wiki_maps_parent_and_records(tmp_path):
    led, orch = _mk(tmp_path)
    ka = _loaded(led, tmp_path, "a", "甲.docx", "01 制度与流程")
    kb = _loaded(led, tmp_path, "b", "乙.docx", "06 参考资料")

    w = _FakeWriter()
    stats = orch.move_loaded_to_wiki(w, _TARGETS, user_token="ut")
    assert stats["mounted"] == 2 and stats["failed"] == 0
    # 父节点按 category 正确映射（按文件名建键）
    parents = {c[2]: c[3] for c in w.calls}
    assert parents["甲.docx"] == "node_policy"
    assert parents["乙.docx"] == "node_ref"
    # 回写 wiki_node_token，stage 仍保持 LOADED
    assert led.get(ka)["wiki_node_token"] == "wiki_甲.docx"
    assert led.get(ka)["stage"] == Stage.LOADED.value
    # 成功后删除租户旧副本（feishu_token）
    assert "tok_a" in w.deleted and "tok_b" in w.deleted
    led.close()


def test_idempotent_skip_already_mounted(tmp_path):
    led, orch = _mk(tmp_path)
    _loaded(led, tmp_path, "a", "甲.docx", "01 制度与流程")
    w = _FakeWriter()
    orch.move_loaded_to_wiki(w, _TARGETS, user_token="ut")      # 第一次挂入
    stats = orch.move_loaded_to_wiki(w, _TARGETS, user_token="ut")  # 再跑
    assert stats["mounted"] == 0 and stats["skipped"] == 1
    assert len(w.calls) == 1                    # 不重复调用
    led.close()


def test_dry_run_no_writer(tmp_path):
    led, orch = _mk(tmp_path)
    ka = _loaded(led, tmp_path, "a", "甲.docx", "01 制度与流程")
    stats = orch.move_loaded_to_wiki(None, _TARGETS)
    assert stats["dry_run"] == 1 and stats["mounted"] == 0
    assert led.get(ka)["wiki_node_token"] is None
    led.close()


def test_failure_keeps_loaded_and_records_error(tmp_path):
    led, orch = _mk(tmp_path)
    ka = _loaded(led, tmp_path, "a", "甲.docx", "01 制度与流程")
    w = _FakeWriter(fail_on={"甲.docx"})
    stats = orch.move_loaded_to_wiki(w, _TARGETS, user_token="ut")
    assert stats["failed"] == 1 and stats["mounted"] == 0
    row = led.get(ka)
    assert row["stage"] == Stage.LOADED.value          # 不回退
    assert row["wiki_node_token"] is None
    assert row["error_detail"].startswith("wiki: ")     # 记录，下次重跑自动重试
    assert w.deleted == []                              # 失败不删租户副本
    led.close()


def test_no_blob_skipped(tmp_path):
    led, orch = _mk(tmp_path)
    # LOADED 但无本地 blob → 无从以用户身份重传，跳过
    item = SourceItem(source_type=SourceType.LOCAL, source_id="x",
                      source_path="/x/丙.docx", original_name="丙.docx")
    led.upsert_discovered(item)
    led.update(item.stable_key(), stage=Stage.LOADED.value, category="01 制度与流程",
               feishu_token="tok_x")
    w = _FakeWriter()
    stats = orch.move_loaded_to_wiki(w, _TARGETS, user_token="ut")
    assert stats["skipped"] == 1 and stats["mounted"] == 0
    led.close()


# ── writer.mount_doc_to_wiki 同步/异步两条路径 ──────────────

class _FakeClient:
    """按预置脚本返回 move_docs_to_wiki 及任务轮询响应。"""
    def __init__(self, script):
        self.script = list(script)
        self.paths = []

    def call(self, method, path, **kw):
        self.paths.append((method, path))
        return self.script.pop(0)


def test_mount_sync_returns_wiki_token():
    from kb_migrator.feishu.writer import FeishuWriter
    w = FeishuWriter(_FakeClient([{"data": {"wiki_token": "wiki_sync", "applied": True}}]))
    out = w.mount_doc_to_wiki("spc", "file", "obj1", "parent")
    assert out["wiki_token"] == "wiki_sync"


def test_mount_async_polls_task(monkeypatch):
    from kb_migrator.feishu import writer as writer_mod
    from kb_migrator.feishu.writer import FeishuWriter
    monkeypatch.setattr(writer_mod.time, "sleep", lambda *_: None)   # 别真睡
    c = _FakeClient([
        {"data": {"task_id": "t1"}},                                  # 提交，异步
        {"data": {"task": {"move_result": []}}},                      # 轮询：未就绪
        {"data": {"task": {"move_result": [{"node": {"wiki_token": "wiki_async"},
                                            "status": 0}]}}},           # 就绪
    ])
    w = FeishuWriter(c)
    out = w.mount_doc_to_wiki("spc", "file", "obj1", "parent", poll_timeout=30.0)
    assert out["wiki_token"] == "wiki_async"
    assert any(p[1].endswith("/wiki/v2/tasks/t1") for p in c.paths)
