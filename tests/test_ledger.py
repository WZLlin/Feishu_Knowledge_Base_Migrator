from kb_migrator.ledger import Ledger
from kb_migrator.models import Permission, SourceItem, SourceType, Stage


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


def test_discovery_change_is_audited_without_resetting_stage():
    led = Ledger(":memory:")
    item = _item()
    led.upsert_discovered(item)
    led.update("local:a", stage=Stage.LOADED, feishu_token="old-file")

    changed = _item()
    changed.size = 20
    result = led.record_discovered(changed)

    assert result.created is False
    assert result.changed is True
    assert led.get("local:a")["stage"] == Stage.LOADED.value
    assert led.get("local:a")["feishu_token"] == "old-file"
    events = led.pending_source_changes()
    assert len(events) == 1
    assert events[0]["stable_key"] == "local:a"

    changed_again = _item()
    changed_again.size = 30
    led.record_discovered(changed_again)
    events = led.pending_source_changes()
    assert len(events) == 2
    assert '"size": 20' in events[1]["old_snapshot_json"]


def test_chat_ledger_idempotency_and_cursor():
    led = Ledger(":memory:")
    led.upsert_chat("chat1", chat_name_original="项目群",
                    member_snapshot=["u1", "u2"], last_message_seq="100")
    assert led.chat_is_completed("chat1") is False
    led.upsert_chat("chat1", migration_status="completed", last_message_seq="250")
    assert led.chat_is_completed("chat1") is True
    assert led.get_chat("chat1")["last_message_seq"] == "250"


def test_source_permissions_replace_prior_snapshot():
    led = Ledger(":memory:")
    led.upsert_discovered(_item())
    led.replace_source_permissions("local:a", [
        Permission(principal="a@example.com", role="view"),
        Permission(principal="b@example.com", role="edit"),
    ])
    led.replace_source_permissions("local:a", [
        Permission(principal="b@example.com", role="view"),
    ])
    rows = led.source_permissions("local:a")
    assert [(r["principal"], r["role"]) for r in rows] == [("b@example.com", "view")]


def test_governance_queues_filter_active_items():
    led = Ledger(":memory:")
    led.upsert_discovered(_item())
    led.update("local:a", stage=Stage.LOADED, category="90 待整理",
               review_due_at="2026-01-01", retention_due_at="2026-01-02", owner="")
    queues = led.governance_items(as_of="2026-01-03")
    assert [r["stable_key"] for r in queues["review_due"]] == ["local:a"]
    assert [r["stable_key"] for r in queues["archive_due"]] == ["local:a"]
    assert [r["stable_key"] for r in queues["unowned"]] == ["local:a"]
    assert [r["stable_key"] for r in queues["triage"]] == ["local:a"]


def test_classification_feedback_summary():
    led = Ledger(":memory:")
    led.record_classification_feedback("a", "90 待整理", "01 制度与流程", 0.2)
    led.record_classification_feedback("b", "01 制度与流程", "01 制度与流程", 0.9)
    row = led.classification_feedback_summary()[0]
    assert row["confirmed_count"] == 2 and row["matches"] == 1


def test_governance_health_and_review_event():
    led = Ledger(":memory:")
    led.upsert_discovered(_item())
    led.update("local:a", stage=Stage.LOADED, category="01 制度与流程",
               review_due_at="2026-01-01", owner="owner@example.com")
    health = led.governance_health(as_of="2026-01-02")
    assert health["owner_coverage"] == 100.0
    assert health["review_current"] == 0.0

    led.complete_review("local:a", "2027-01-02", actor="reviewer")
    assert led.get("local:a")["review_due_at"] == "2027-01-02"
    event = led.conn.execute(
        "SELECT * FROM governance_events WHERE stable_key='local:a'"
    ).fetchone()
    assert event["event_type"] == "review_completed"
    assert event["actor"] == "reviewer"


def test_pipeline_runs_and_item_state_events_are_audited():
    led = Ledger(":memory:")
    led.upsert_discovered(_item())
    led.update("local:a", stage=Stage.EXTRACTED, extraction_ok=1,
               extracted_text_chars=12)
    event = led.conn.execute(
        "SELECT * FROM item_events WHERE stable_key='local:a'"
    ).fetchone()
    assert event["event_type"] == "state_changed"
    assert '"to": "extracted"' in event["detail_json"]

    run_id = led.start_pipeline_run("dedup")
    led.finish_pipeline_run(run_id, stats={"unique": 1})
    run = led.recent_pipeline_runs(1)[0]
    assert run["status"] == "success"
    assert '"unique": 1' in run["stats_json"]


def test_materialize_source_change_creates_new_revision():
    led = Ledger(":memory:")
    led.upsert_discovered(_item())
    changed = _item(); changed.size = 99
    led.record_discovered(changed)
    key = led.materialize_source_change(led.pending_source_changes()[0]["id"])
    assert key == "local:a#rev1"
    assert led.get(key)["stage"] == Stage.DISCOVERED.value
    assert led.pending_source_changes() == []


def test_mark_missing_local_sources_only_marks_unseen_entries():
    led = Ledger(":memory:")
    first, second = _item("a"), _item("b")
    first.source_path, second.source_path = "C:/knowledge/a.docx", "C:/knowledge/b.docx"
    led.upsert_discovered(first); led.upsert_discovered(second)
    assert led.mark_missing_local_sources("C:/knowledge", {first.stable_key()}, "9999-01-01") == 1
    assert [r["stable_key"] for r in led.missing_source_items()] == [second.stable_key()]
