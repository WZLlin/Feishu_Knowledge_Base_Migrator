from kb_migrator.ledger import Ledger
from kb_migrator.models import SourceItem, SourceType, Stage


def _item(sid="a", sha="deadbeef"):
    return SourceItem(source_type=SourceType.LOCAL, source_id=sid,
                      source_path=f"/x/{sid}", original_name=f"{sid}.docx",
                      size=10, content_sha256=sha)


def test_upsert_is_idempotent():
    led = Ledger(":memory:")
    assert led.upsert_discovered(_item()) is True
    assert led.upsert_discovered(_item()) is False   # 二次不重复插入
    assert led.stage_counts() == {Stage.DISCOVERED.value: 1}


def test_update_and_stage_transition():
    led = Ledger(":memory:")
    led.upsert_discovered(_item())
    led.update("local:a", stage=Stage.EXTRACTED, category="01 制度与流程", confidence=0.9)
    row = led.get("local:a")
    assert row["stage"] == "extracted"
    assert row["category"] == "01 制度与流程"


def test_find_by_sha_supports_exact_dedup():
    led = Ledger(":memory:")
    led.upsert_discovered(_item("a", "same"))
    led.upsert_discovered(_item("b", "same"))
    assert len(led.find_by_sha("same")) == 2


def test_chat_ledger_idempotency_and_cursor():
    led = Ledger(":memory:")
    led.upsert_chat("chat1", chat_name_original="项目群",
                    member_snapshot=["u1", "u2"], last_message_seq="100")
    assert led.chat_is_completed("chat1") is False
    led.upsert_chat("chat1", migration_status="completed", last_message_seq="250")
    assert led.chat_is_completed("chat1") is True
    assert led.get_chat("chat1")["last_message_seq"] == "250"
