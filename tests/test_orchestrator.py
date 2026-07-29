"""编排器端到端离线测试：本地文件夹 → 分类 → 人工确认 → load(dry-run)。"""
from datetime import date
import shutil

from docx import Document

from kb_migrator.connectors.local_folder import LocalFolderConnector
from kb_migrator.ledger import Ledger
from kb_migrator.models import Permission, SourceItem, SourceType, Stage
from kb_migrator.pipeline.classify import Classifier
from kb_migrator.models import DedupVerdict
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy

TAX = "config/taxonomy.yaml"


def _mk(path, paras):
    d = Document()
    for p in paras:
        d.add_paragraph(p)
    d.save(path)


def test_full_pipeline_offline(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    _mk(src / "制度.docx", ["远程办公管理制度", "本管理办法规定流程与审批。"])
    _mk(src / "纪要.docx", ["项目会议纪要", "会议结论：按期上线。"])
    # 精确重复
    shutil.copyfile(src / "制度.docx", src / "制度副本.docx")

    led = Ledger(str(tmp_path / "ledger.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "work"), confidence_threshold=0.85)

    ing = orch.ingest(LocalFolderConnector(str(src)))
    assert ing["discovered"] == 3
    assert ing["exact_dup"] == 1          # 副本被 SHA256 精确去重
    assert ing["extracted"] == 2

    orch.dedup_pass()
    clf = Classifier(tx, api_key="")       # 离线启发式
    cls = orch.classify_pass(clf)
    # 离线一律 needs_review，故都进人工队列，不自动确认
    assert cls["auto_confirmed"] == 0
    assert cls["to_review"] == 2
    assert all(r["classifier_version"] == "offline:heuristic-v1"
               for r in led.pending_review())

    # 人工确认一条
    keys = [r["stable_key"] for r in led.pending_review()]
    orch.confirm(keys[0], "01 制度与流程")

    # load dry-run
    load = orch.load_pass(writer=None, folder_map={})
    assert load["dry_run"] == 1
    led.close()


class _LoadWriter:
    def __init__(self):
        self.uploads = []

    def upload_file(self, path, folder_token, name):
        self.uploads.append((path, folder_token, name))
        return "file-loaded"

    def lock_down_external(self, token, obj_type):
        return None


def test_load_uses_per_item_structure_target_resolver(tmp_path):
    blob = tmp_path / "policy.pdf"
    blob.write_bytes(b"pdf")
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(
        source_type=SourceType.LOCAL, source_id="policy",
        source_path=str(blob), original_name="2026-policy.pdf",
        local_blob_path=str(blob),
    )
    led.upsert_discovered(item)
    led.update(
        item.stable_key(), stage=Stage.CONFIRMED.value,
        category="01 制度与流程", confidence=0.95,
    )
    writer = _LoadWriter()

    stats = orch.load_pass(
        writer, {}, structure_version_id="version-split",
        target_resolver=lambda row: {
            "remote_token": "folder-2026",
            "node_id": "node-2026",
            "assignment_source": "split_rule",
        },
    )

    assert stats["loaded"] == 1
    assert writer.uploads[0][1] == "folder-2026"
    assignment = led.conn.execute(
        "SELECT * FROM item_target_assignments WHERE stable_key=?",
        (item.stable_key(),),
    ).fetchone()
    assert assignment["node_id"] == "node-2026"
    assert assignment["assignment_source"] == "split_rule"
    run = led.recent_pipeline_runs(1)[0]
    assert run["run_type"] == "load"
    assert run["structure_version_id"] == "version-split"
    led.close()


def test_resume_is_idempotent(tmp_path):
    """重复 ingest 不重复处理已完成条目（断点续跑）。"""
    src = tmp_path / "src"
    src.mkdir()
    _mk(src / "a.docx", ["制度内容", "流程说明"])
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    conn = LocalFolderConnector(str(src))
    first = orch.ingest(conn)
    second = orch.ingest(conn)             # 二次运行
    assert first["discovered"] == 1
    assert second["discovered"] == 0       # 幂等：无新增
    led.close()


def test_ingest_only_fetches_current_connector_source(tmp_path):
    """本地扫描不能意外 fetch 之前遗留的 SharePoint DISCOVERED 条目。"""
    src = tmp_path / "src"
    src.mkdir()
    _mk(src / "a.docx", ["制度内容"])
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    led.upsert_discovered(SourceItem(
        source_type=SourceType.SHAREPOINT, source_id="drive/item",
        source_path="https://sharepoint/item", original_name="remote.docx",
    ))

    stats = orch.ingest(LocalFolderConnector(str(src)))

    assert stats["extracted"] == 1
    assert led.get("sharepoint:drive/item")["stage"] == Stage.DISCOVERED.value
    led.close()


def test_extract_failure_enters_retry_queue(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "broken.docx").write_bytes(b"not-a-docx")
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))

    stats = orch.ingest(LocalFolderConnector(str(src)))

    row = led.get(next(iter(r["stable_key"] for r in led.failed_items("extract"))))
    assert stats["failed"] == 1
    assert row["stage"] == Stage.FAILED.value
    assert row["failed_stage"] == "extract"
    assert row["extraction_ok"] == 0
    assert row["extraction_note"]
    assert led.requeue_failures("extract", Stage.DISCOVERED) == 1
    led.close()


class _HistoricalIndex:
    """轻量索引替身：用于验证编排器会先加入历史唯一文本。"""
    def __init__(self):
        self.items = {}

    def add(self, key, text):
        self.items[key] = text

    def query(self, text):
        from kb_migrator.pipeline.dedup import NearDupResult
        for key, old in self.items.items():
            if old == text:
                return NearDupResult(DedupVerdict.NEAR_DUPLICATE, key, 1.0)
        return NearDupResult(DedupVerdict.UNIQUE)


class _BrokenIndex:
    def add(self, key, text):
        pass

    def query(self, text):
        raise RuntimeError("索引损坏")


def test_dedup_failure_enters_retry_queue(tmp_path):
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.LOCAL, source_id="new",
                      source_path="/new", original_name="new.txt")
    led.upsert_discovered(item)
    orch._save_text(item.stable_key(), "正文")
    led.update(item.stable_key(), stage=Stage.EXTRACTED)

    stats = orch.dedup_pass(index=_BrokenIndex())

    row = led.get(item.stable_key())
    assert stats["failed"] == 1
    assert row["failed_stage"] == "dedup"
    assert row["stage"] == Stage.FAILED.value
    led.close()


def test_dedup_compares_new_items_against_historical_unique_text(tmp_path):
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    old = SourceItem(source_type=SourceType.LOCAL, source_id="old", source_path="/old",
                     original_name="old.txt")
    new = SourceItem(source_type=SourceType.LOCAL, source_id="new", source_path="/new",
                     original_name="new.txt")
    led.upsert_discovered(old)
    led.upsert_discovered(new)
    orch._save_text(old.stable_key(), "同一份历史制度")
    orch._save_text(new.stable_key(), "同一份历史制度")
    led.update(old.stable_key(), stage=Stage.LOADED,
               dedup_verdict=DedupVerdict.UNIQUE.value)
    led.update(new.stable_key(), stage=Stage.EXTRACTED)

    stats = orch.dedup_pass(index=_HistoricalIndex())

    assert stats["historical_indexed"] == 1
    assert stats["near_dup"] == 1
    assert led.get(new.stable_key())["dedup_verdict"] == DedupVerdict.NEAR_DUPLICATE.value
    led.close()


class _PermissionWriter:
    def __init__(self):
        self.calls = []

    def add_collaborator(self, token, obj_type, member_id, perm="view"):
        self.calls.append((token, obj_type, member_id, perm))

    def update_collaborator(self, token, obj_type, member_id, perm="view"):
        self.calls.append(("update", token, obj_type, member_id, perm))

    def remove_collaborator(self, token, obj_type, member_id):
        self.calls.append(("remove", token, obj_type, member_id))


def test_source_permissions_are_mapped_and_idempotently_synced(tmp_path):
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.SHAREPOINT, source_id="drive/a",
                      source_path="https://sp/a", original_name="a.docx")
    led.upsert_discovered(item)
    led.replace_source_permissions(item.stable_key(), [
        Permission(principal="Alice@Example.com", role="view"),
        Permission(principal="unknown@example.com", role="edit"),
    ])
    writer = _PermissionWriter()

    first = orch.sync_source_permissions(
        writer, item.stable_key(), "file-token", "file", {"alice@example.com": "ou_alice"})
    second = orch.sync_source_permissions(
        writer, item.stable_key(), "file-token", "file", {"alice@example.com": "ou_alice"})

    assert first == {
        "granted": 1, "updated": 0, "revoked": 0, "skipped": 0,
        "unmapped": ["unknown@example.com"], "failed": 0,
    }
    assert second["skipped"] == 1
    assert second["unmapped"] == ["unknown@example.com"]
    assert writer.calls == [("file-token", "file", "ou_alice", "view")]
    led.close()


def test_permission_sync_updates_roles_and_revokes_only_managed_stale_grants(tmp_path):
    led = Ledger(str(tmp_path / "l.db")); tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.SHAREPOINT, source_id="drive/a",
                      source_path="https://sp/a", original_name="a.docx")
    led.upsert_discovered(item)
    led.replace_source_permissions(item.stable_key(), [
        Permission(principal="alice@example.com", role="view"),
        Permission(principal="bob@example.com", role="view"),
    ])
    writer = _PermissionWriter()
    identity = {"alice@example.com": "ou_alice", "bob@example.com": "ou_bob"}
    orch.sync_source_permissions(writer, item.stable_key(), "tok", "file", identity)

    led.replace_source_permissions(item.stable_key(), [
        Permission(principal="alice@example.com", role="edit"),
    ])
    stats = orch.sync_source_permissions(
        writer, item.stable_key(), "tok", "file", identity,
    )

    assert stats["updated"] == 1
    assert stats["revoked"] == 1
    assert ("update", "tok", "file", "ou_alice", "edit") in writer.calls
    assert ("remove", "tok", "file", "ou_bob") in writer.calls
    assert {r["principal"] for r in led.managed_permissions(
        item.stable_key(), "tok", "file")} == {"alice@example.com"}
    led.close()


def test_owner_and_steward_receive_governance_permissions(tmp_path):
    led = Ledger(str(tmp_path / "l.db")); tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.LOCAL, source_id="a", source_path="/a", original_name="a")
    led.upsert_discovered(item)
    led.update(item.stable_key(), owner="owner@example.com", steward="steward@example.com")
    writer = _PermissionWriter()
    stats = orch.sync_source_permissions(writer, item.stable_key(), "tok", "file", {
        "owner@example.com": "ou_owner", "steward@example.com": "ou_steward"})
    assert stats["granted"] == 2
    assert ("tok", "file", "ou_owner", "full_access") in writer.calls
    assert ("tok", "file", "ou_steward", "edit") in writer.calls
    led.close()


def test_confirm_applies_taxonomy_governance_dates(tmp_path):
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.LOCAL, source_id="a", source_path="/a",
                      original_name="a.docx")
    led.upsert_discovered(item)
    led.update(item.stable_key(), stage=Stage.CLASSIFIED,
               metadata_json='{"doc_date":"2026-01-15"}')

    orch.confirm(item.stable_key(), "01 制度与流程")
    row = led.get(item.stable_key())

    assert row["review_due_at"] == "2026-04-15"
    assert row["retention_due_at"] == "2036-01-15"
    led.close()


def test_complete_review_reschedules_from_review_date(tmp_path):
    led = Ledger(str(tmp_path / "l.db")); tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.LOCAL, source_id="a",
                      source_path="/a", original_name="a.docx")
    led.upsert_discovered(item)
    led.update(item.stable_key(), stage=Stage.LOADED,
               category="01 制度与流程", review_due_at="2025-01-01")
    next_due = orch.complete_review(
        item.stable_key(), actor="owner", reviewed_on=date(2026, 1, 31),
    )
    assert next_due == "2026-04-30"
    assert led.get(item.stable_key())["review_due_at"] == next_due
    led.close()


def test_feedback_calibration_and_triage_clustering(tmp_path):
    led = Ledger(str(tmp_path / "l.db")); tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"), confidence_threshold=0.85)
    for i in range(5):
        led.record_classification_feedback(
            f"good{i}", "01 制度与流程", "01 制度与流程", 0.9,
        )
        led.record_classification_feedback(
            f"bad{i}", "90 待整理", "01 制度与流程", 0.6,
        )
    calibration = orch.classification_calibration()
    assert calibration["auto_applicable"] is True
    assert calibration["recommended_threshold"] == 0.7

    for sid, text in (("x", "员工入职培训流程与课程安排"),
                      ("y", "员工入职培训课程和考试安排")):
        item = SourceItem(source_type=SourceType.LOCAL, source_id=sid,
                          source_path=f"/{sid}", original_name=f"{sid}.txt")
        led.upsert_discovered(item)
        led.update(item.stable_key(), stage=Stage.CLASSIFIED,
                   category=tx.triage_path)
        orch._save_text(item.stable_key(), text)
    clusters = orch.triage_topic_clusters(similarity=0.1)
    assert clusters[0]["size"] == 2
    led.close()


class _ArchiveWriter:
    def __init__(self):
        self.moves = []
        self.wiki_moves = []

    def move_file(self, token, folder, obj_type):
        self.moves.append((token, folder, obj_type))
        return {"task_id": "task"}

    def move_wiki_node(self, space_id, node_token, parent, user_token=""):
        self.wiki_moves.append((space_id, node_token, parent, user_token))
        return {"node": {"node_token": node_token}}


def test_archive_moves_only_eligible_drive_files_after_commit(tmp_path):
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.LOCAL, source_id="a", source_path="/a",
                      original_name="a.docx")
    led.upsert_discovered(item)
    led.update(item.stable_key(), stage=Stage.LOADED, feishu_token="file-token",
               retention_due_at="2020-01-01")
    writer = _ArchiveWriter()

    preview = orch.archive_due_items(writer, "archive-folder")
    result = orch.archive_due_items(writer, "archive-folder", commit=True)

    assert preview["dry_run"] == 1 and writer.moves == [("file-token", "archive-folder", "file")]
    assert result["archived"] == 1
    assert led.get(item.stable_key())["archived_at"]
    assert led.governance_items()["archive_due"] == []
    led.close()


def test_archive_moves_wiki_node_with_user_identity(tmp_path):
    led = Ledger(str(tmp_path / "l.db")); tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.LOCAL, source_id="wiki",
                      source_path="/wiki", original_name="wiki.docx")
    led.upsert_discovered(item)
    led.update(item.stable_key(), stage=Stage.LOADED, feishu_token="obj",
               wiki_node_token="wik-node", retention_due_at="2020-01-01")
    writer = _ArchiveWriter()

    result = orch.archive_due_items(
        writer, "", commit=True, wiki_space_id="space",
        wiki_archive_node="archive-node", user_token="user-token",
    )

    assert result["wiki_archived"] == 1
    assert writer.wiki_moves == [
        ("space", "wik-node", "archive-node", "user-token"),
    ]
    assert led.get(item.stable_key())["archived_at"]
    led.close()


def test_finalize_source_change_archives_old_loaded_version(tmp_path):
    led = Ledger(str(tmp_path / "l.db")); tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    base = SourceItem(source_type=SourceType.LOCAL, source_id="a", source_path="/a", original_name="a.docx")
    led.upsert_discovered(base); led.update(base.stable_key(), stage=Stage.LOADED, feishu_token="old")
    changed = base.model_copy(update={"size": 2}); led.record_discovered(changed)
    cid = led.pending_source_changes()[0]["id"]; new_key = led.materialize_source_change(cid)
    led.update(new_key, stage=Stage.LOADED, feishu_token="new")
    writer = _ArchiveWriter()
    result = orch.finalize_source_change(cid, writer, "archive", commit=True)
    assert result["completed"] is True
    assert writer.moves == [("old", "archive", "file")]
    assert led.get(base.stable_key())["archived_at"]
    assert led.get_source_change(cid)["status"] == "completed"
    led.close()


def test_resolve_missing_keep_closes_queue(tmp_path):
    led = Ledger(str(tmp_path / "l.db")); tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    item = SourceItem(source_type=SourceType.LOCAL, source_id="a", source_path="C:/root/a", original_name="a")
    led.upsert_discovered(item)
    led.mark_missing_local_sources("C:/root", set(), "9999-01-01")
    assert orch.resolve_missing_source(item.stable_key(), "keep") == {"kept": True}
    assert led.missing_source_items() == []
    led.close()
