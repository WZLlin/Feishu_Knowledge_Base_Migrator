"""群聊会话存档迁移（离线）：假连接器喂脚本化消息批次，验证 ingest_chat。

要点：
- 按自然日聚合出会话片段，各自成 EXTRACTED 的 items 条目（复用标准管线）；
- last_message_seq / member_snapshot / migration_status 回写 chat_migrations；
- 幂等：已推进到后续阶段（已分类/确认/入库）的旧片段重跑时跳过、不回退。
"""
import json
from datetime import datetime, timezone

from kb_migrator.ledger import Ledger
from kb_migrator.models import SourceType, Stage
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy

TAX = "config/taxonomy.yaml"


def _ms(y, mo, d, h, mi):
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp() * 1000)


# 跨两个自然日（UTC）：01-01 两条文本 + 01-02 一条文本、一条文件、一条撤回(应剔除)
_MSGS = [
    {"seq": 1, "from": "userA", "msgtime": _ms(2026, 1, 1, 9, 0),
     "msgtype": "text", "text": {"content": "早上好"}},
    {"seq": 2, "from": "userB", "msgtime": _ms(2026, 1, 1, 10, 30),
     "msgtype": "text", "text": {"content": "上线计划已确认"}},
    {"seq": 3, "from": "userA", "msgtime": _ms(2026, 1, 2, 8, 15),
     "msgtype": "text", "text": {"content": "第二天继续"}},
    {"seq": 4, "from": "userC", "msgtime": _ms(2026, 1, 2, 8, 20),
     "msgtype": "file", "file": {"sdkfileid": "F1", "filename": "方案.docx",
                                 "filesize": 100}},
    {"seq": 5, "from": "userA", "msgtime": _ms(2026, 1, 2, 8, 25),
     "msgtype": "revoke"},          # 系统类：聚合时剔除，但游标仍前进
]


class _FakeChat:
    """脚本化会话存档连接器：单批返回全部消息（<limit 触发结束）。"""
    def __init__(self, msgs, new_seq):
        self._msgs = msgs
        self._new_seq = new_seq
        self.calls = []

    def fetch_messages(self, seq=0, limit=1000):
        self.calls.append(seq)
        return list(self._msgs), self._new_seq


class _FakeChatWithMedia(_FakeChat):
    """在线连接器：额外实现 fetch_media，用于验证群文件下载接线。"""
    online = True

    def __init__(self, msgs, new_seq, media):
        super().__init__(msgs, new_seq)
        self._media = media          # {sdkfileid: bytes}
        self.media_calls = []

    def fetch_media(self, sdkfileid):
        self.media_calls.append(sdkfileid)
        return self._media[sdkfileid]


def _orch(tmp_path):
    led = Ledger(str(tmp_path / "ledger.db"))
    tx = Taxonomy.load(TAX)
    return led, Orchestrator(led, tx, str(tmp_path / "work"))


def test_ingest_chat_creates_segment_items_and_cursor(tmp_path):
    led, orch = _orch(tmp_path)
    stats = orch.ingest_chat(_FakeChat(_MSGS, new_seq=5), "chat42", chat_name="上线群")

    assert stats["messages"] == 5
    assert stats["segments"] == 2          # 两个自然日
    assert stats["skipped_existing"] == 0

    # 每个自然日一条 WECOM_CHAT 的 EXTRACTED 条目
    for date in ("2026-01-01", "2026-01-02"):
        key = f"{SourceType.WECOM_CHAT.value}:chat42:{date}"
        row = led.get(key)
        assert row is not None and row["stage"] == Stage.EXTRACTED.value
        assert row["content_sha256"] and row["local_blob_path"]

    # 台账 chat_migrations 回写：游标 / 成员快照 / 状态
    chat = led.get_chat("chat42")
    assert chat["last_message_seq"] == "5"
    assert chat["migration_status"] == "completed"
    assert chat["chat_name_original"] == "上线群"
    assert json.loads(chat["member_snapshot"]) == ["userA", "userB", "userC"]
    led.close()


def test_ingest_chat_segment_text_content(tmp_path):
    led, orch = _orch(tmp_path)
    orch.ingest_chat(_FakeChat(_MSGS, new_seq=5), "chat42")
    text = orch.load_text(f"{SourceType.WECOM_CHAT.value}:chat42:2026-01-02")
    assert "第二天继续" in text
    assert "[文件] 方案.docx" in text        # 文件消息进正文
    assert "revoke" not in text              # 撤回类被剔除
    led.close()


def test_ingest_chat_downloads_group_files_when_online(tmp_path):
    led, orch = _orch(tmp_path)
    blob = "file-bytes-方案".encode("utf-8")
    conn = _FakeChatWithMedia(_MSGS, new_seq=5, media={"F1": blob})
    stats = orch.ingest_chat(conn, "chat42")

    assert stats["files"] == 1                 # 群文件被下载并登记
    assert conn.media_calls == ["F1"]          # 按 sdkfileid 拉取媒体
    # 文件项：WECOM_CHAT 的 EXTRACTED 条目，键 = wecom_chat:chat42:file:F1
    fkey = f"{SourceType.WECOM_CHAT.value}:chat42:file:F1"
    frow = led.get(fkey)
    assert frow is not None and frow["stage"] == Stage.EXTRACTED.value
    assert frow["original_name"] == "方案.docx"
    assert frow["content_sha256"]
    # 落盘的二进制与下载一致
    with open(frow["local_blob_path"], "rb") as fp:
        assert fp.read() == blob
    led.close()


def test_ingest_chat_offline_skips_files(tmp_path):
    # 旧式假连接器无 online/fetch_media：向后兼容，不下载群文件、不报错
    led, orch = _orch(tmp_path)
    stats = orch.ingest_chat(_FakeChat(_MSGS, new_seq=5), "chat42")
    assert stats["files"] == 0
    led.close()


def test_ingest_chat_idempotent_skips_advanced_segments(tmp_path):
    led, orch = _orch(tmp_path)
    orch.ingest_chat(_FakeChat(_MSGS, new_seq=5), "chat42")

    # 人工/后续阶段把 01-01 片段推进到 CLASSIFIED
    advanced = f"{SourceType.WECOM_CHAT.value}:chat42:2026-01-01"
    led.update(advanced, stage=Stage.CLASSIFIED.value)

    # 重跑（游标已是 5，假连接器仍回放同批）：已推进的片段跳过、不回退
    stats2 = orch.ingest_chat(_FakeChat(_MSGS, new_seq=5), "chat42")
    assert stats2["skipped_existing"] == 1
    assert led.get(advanced)["stage"] == Stage.CLASSIFIED.value   # 未被回退
    # 未推进的 01-02 片段仍为 EXTRACTED
    other = f"{SourceType.WECOM_CHAT.value}:chat42:2026-01-02"
    assert led.get(other)["stage"] == Stage.EXTRACTED.value
    led.close()
