"""群聊治理（离线）：成员→协作者映射 + 群名打标接线。

要点：
- per-doc 粒度：给该群产出的每个飞书文档，逐个已映射成员加协作者；
- 映射源 = 本地 {wecom_userid: feishu_open_id}；未命中成员进 unmapped 人工清单，不阻断；
- writer=None 走 dry-run（只统计不真调）；group_connector=None 时打标为 dry_run。
"""
from kb_migrator.ledger import Ledger
from kb_migrator.models import DedupVerdict, SourceItem, SourceType, Stage
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy

TAX = "config/taxonomy.yaml"


class _FakeWriter:
    """记录 add_collaborator 调用；便于断言 (token, obj_type, fid, perm)。"""
    def __init__(self):
        self.calls = []

    def add_collaborator(self, token, obj_type, member_id, perm="view"):
        self.calls.append((token, obj_type, member_id, perm))


class _FakeGroup:
    def tag_group(self, chat_id, original, feishu_url=""):
        return {"tag_status": "renamed", "detail": f"{original}[已备份]"}


def _orch(tmp_path):
    led = Ledger(str(tmp_path / "ledger.db"))
    orch = Orchestrator(led, Taxonomy.load(TAX), str(tmp_path / "work"))
    return led, orch


def _chat_doc(led, orch, source_id, *, feishu_token=None, wiki_node_token=None):
    """造一条该群的 WECOM_CHAT 文档条目，并回填飞书 token。"""
    item = SourceItem(source_type=SourceType.WECOM_CHAT, source_id=source_id,
                      source_path=source_id, original_name=f"{source_id}.md")
    led.upsert_discovered(item)
    key = item.stable_key()
    orch._save_text(key, "正文")
    fields = {"stage": Stage.LOADED.value, "dedup_verdict": DedupVerdict.UNIQUE.value}
    if feishu_token:
        fields["feishu_token"] = feishu_token
    if wiki_node_token:
        fields["wiki_node_token"] = wiki_node_token
    led.update(key, **fields)
    return key


def test_map_collaborators_per_doc_per_member(tmp_path):
    led, orch = _orch(tmp_path)
    # 群成员快照：userA/userB 已映射，userC 未映射
    led.upsert_chat("chat42", chat_name_original="上线群",
                    member_snapshot=["userA", "userB", "userC"])
    # 两个群文档：一个已入云空间(file)，一个已挂 Wiki 节点(wiki)
    _chat_doc(led, orch, "chat42:2026-01-01", feishu_token="fk_file")
    _chat_doc(led, orch, "chat42:2026-01-02", wiki_node_token="wk_node")

    user_map = {"userA": "ou_a", "userB": "ou_b"}   # userC 缺失
    writer = _FakeWriter()
    stats = orch.map_chat_collaborators(writer, "chat42", user_map)

    assert stats["docs"] == 2
    assert stats["members"] == 2                     # 仅已映射的两人
    assert stats["granted"] == 4                     # 2 文档 × 2 成员
    assert stats["failed"] == 0
    assert stats["unmapped"] == ["userC"]            # 未映射成员进清单
    # file 文档用 feishu_token/obj_type=file；wiki 文档用 wiki_node_token/obj_type=wiki
    assert ("fk_file", "file", "ou_a", "view") in writer.calls
    assert ("fk_file", "file", "ou_b", "view") in writer.calls
    assert ("wk_node", "wiki", "ou_a", "view") in writer.calls
    assert ("wk_node", "wiki", "ou_b", "view") in writer.calls
    led.close()


def test_map_collaborators_dry_run_when_no_writer(tmp_path):
    led, orch = _orch(tmp_path)
    led.upsert_chat("chat42", member_snapshot=["userA"])
    _chat_doc(led, orch, "chat42:2026-01-01", feishu_token="fk_file")

    stats = orch.map_chat_collaborators(None, "chat42", {"userA": "ou_a"})
    assert stats["dry_run"] == 1                      # 1 文档 × 1 成员，仅预览
    assert stats["granted"] == 0
    led.close()


def test_map_collaborators_skips_docs_without_token(tmp_path):
    led, orch = _orch(tmp_path)
    led.upsert_chat("chat42", member_snapshot=["userA"])
    # 无 feishu_token / wiki_node_token 的文档不计入（尚未入库，无处加协作者）
    item = SourceItem(source_type=SourceType.WECOM_CHAT,
                      source_id="chat42:2026-01-03", source_path="chat42:2026-01-03",
                      original_name="x.md")
    led.upsert_discovered(item)
    led.update(item.stable_key(), stage=Stage.CONFIRMED.value)

    writer = _FakeWriter()
    stats = orch.map_chat_collaborators(writer, "chat42", {"userA": "ou_a"})
    assert stats["docs"] == 0
    assert writer.calls == []
    led.close()


def test_tag_chat_group_writes_status(tmp_path):
    led, orch = _orch(tmp_path)
    led.upsert_chat("chat42", chat_name_original="上线群")
    res = orch.tag_chat_group(_FakeGroup(), "chat42", feishu_url="https://feishu/x")
    assert res["tag_status"] == "renamed"
    # 回写台账
    assert led.get_chat("chat42")["tag_status"] == "renamed"
    led.close()


def test_tag_chat_group_dry_run_without_connector(tmp_path):
    led, orch = _orch(tmp_path)
    led.upsert_chat("chat42", chat_name_original="上线群")
    res = orch.tag_chat_group(None, "chat42")
    assert res["tag_status"] == "dry_run"
    assert "上线群[已备份]" in res["detail"]
    led.close()
