import pytest

from kb_migrator.ledger import Ledger
from kb_migrator.models import SourceItem, SourceType, Stage
from kb_migrator.structure import StructureConflict, StructureService
from kb_migrator.structure.relocation import ItemRelocationExecutor
from kb_migrator.structure.reconcile import StructureReconciler
from kb_migrator.taxonomy import Taxonomy


def service(tmp_path):
    led = Ledger(str(tmp_path / "ledger.db"))
    tx = Taxonomy.load("config/taxonomy.yaml")
    return led, StructureService(led, tx, str(tmp_path / "targets.json"))


def test_seed_save_rename_keeps_stable_node_id(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        original = draft["nodes"][0]
        nodes = [dict(node) for node in draft["nodes"]]
        nodes[0]["display_name"] = "制度与流程（新版）"

        saved = structures.save_draft(
            draft["id"], draft["revision"], nodes, actor="tester"
        )

        assert saved["revision"] == draft["revision"] + 1
        assert saved["nodes"][0]["node_id"] == original["node_id"]
        assert saved["nodes"][0]["semantic_key"] == original["semantic_key"]
        assert saved["nodes"][0]["display_name"] == "制度与流程（新版）"
    finally:
        led.close()


def test_optimistic_revision_conflict(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        structures.save_draft(draft["id"], draft["revision"], draft["nodes"])
        with pytest.raises(StructureConflict):
            structures.save_draft(draft["id"], draft["revision"], draft["nodes"])
    finally:
        led.close()


def test_validation_detects_cycle_and_requires_system_nodes(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        nodes = [dict(node) for node in draft["nodes"] if node["node_kind"] != "archive"]
        nodes[0]["parent_node_id"] = nodes[1]["node_id"]
        nodes[1]["parent_node_id"] = nodes[0]["node_id"]
        result = structures.validate_nodes(nodes)
        codes = {item["code"] for item in result["errors"]}
        assert "cycle" in codes
        assert "archive_required" in codes
    finally:
        led.close()


def test_split_rules_route_items_and_report_history_impact(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        source = draft["nodes"][0]
        split = structures.split_node(
            draft["id"], draft["revision"], source["node_id"], [
                {
                    "display_name": "PDF 文档",
                    "assignment_rule": {
                        "field": "doc_type", "operator": "equals",
                        "value": "pdf", "priority": 10,
                    },
                },
                {
                    "display_name": "其他文档",
                    "assignment_rule": {"fallback": True, "priority": 999},
                },
            ],
            actor="owner",
        )
        pdf_node = next(n for n in split["nodes"] if n["display_name"] == "PDF 文档")
        fallback_node = next(
            n for n in split["nodes"] if n["display_name"] == "其他文档"
        )
        pdf = structures.resolve_item_target({
            "category": source["display_name"],
            "original_name": "制度.pdf",
            "metadata_json": "{}",
        }, split["id"])
        other = structures.resolve_item_target({
            "category": source["display_name"],
            "original_name": "制度.docx",
            "metadata_json": "{}",
        }, split["id"])
        assert pdf["node_id"] == pdf_node["node_id"]
        assert pdf["assignment_source"] == "split_rule"
        assert other["node_id"] == fallback_node["node_id"]

        item = SourceItem(
            source_type=SourceType.LOCAL, source_id="history",
            source_path="/history", original_name="历史.pdf",
        )
        led.upsert_discovered(item)
        led.update(
            item.stable_key(), category=source["display_name"],
            stage=Stage.LOADED.value, feishu_token="old-file",
        )
        impact = structures.split_impact(split["id"])
        assert impact["total_items"] == 1
        assert impact["loaded_relocation_candidates"] == 1
        assert impact["splits"][0]["history_policy"] == "preview_only"
        health = structures.health(split["id"])
        assert health["validation"]["valid"] is True
        assert health["split_impact"]["total_items"] == 1
    finally:
        led.close()


def test_split_rule_validation_rejects_multiple_fallbacks(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        parent = draft["nodes"][0]["node_id"]
        nodes = [dict(node) for node in draft["nodes"]]
        for index in range(2):
            nodes.append({
                "node_id": f"fallback-{index}", "parent_node_id": parent,
                "semantic_key": f"fallback.{index}",
                "display_name": f"兜底 {index}", "node_kind": "category",
                "assignment_rule": {"fallback": True, "priority": index + 1},
            })
        result = structures.validate_nodes(nodes)
        assert "multiple_rule_fallbacks" in {
            error["code"] for error in result["errors"]
        }
    finally:
        led.close()


def test_split_without_fallback_adds_triage_fallback(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        source = draft["nodes"][0]
        split = structures.split_node(
            draft["id"], draft["revision"], source["node_id"], [
                {
                    "display_name": "PDF",
                    "assignment_rule": {
                        "field": "doc_type", "operator": "equals",
                        "value": "pdf",
                    },
                },
                {
                    "display_name": "Word",
                    "assignment_rule": {
                        "field": "doc_type", "operator": "equals",
                        "value": "docx",
                    },
                },
            ],
        )
        fallback = next(
            node for node in split["nodes"]
            if node.get("parent_node_id") == source["node_id"]
            and (node.get("assignment_rule") or {}).get("fallback")
        )
        assert fallback["display_name"] == structures.taxonomy.triage_path
        resolved = structures.resolve_item_target({
            "category": source["display_name"],
            "original_name": "未知.bin",
            "metadata_json": "{}",
        }, split["id"])
        assert resolved["node_id"] == fallback["node_id"]
    finally:
        led.close()


def test_multi_user_approval_freezes_until_required_count(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        first = structures.approve(
            draft["id"], actor="alice", required_approvals=2,
            comment="业务确认",
        )
        assert first["status"] == "reviewing"
        assert first["approval_count"] == 1
        with pytest.raises(StructureConflict):
            structures.save_draft(
                first["id"], first["revision"], first["nodes"]
            )

        second = structures.approve(
            first["id"], actor="bob", required_approvals=2,
            comment="治理确认",
        )
        assert second["status"] == "approved"
        assert second["approval_count"] == 2
        assert {item["actor"] for item in second["approvals"]} == {"alice", "bob"}
    finally:
        led.close()


def _active_split_with_history(tmp_path):
    led, structures = service(tmp_path)
    first = structures.ensure_draft()
    source = first["nodes"][0]
    structures.approve(first["id"])
    writer = FakeWriter()
    initial = StructureReconciler(structures, writer).apply(first["id"])
    active = structures.active_version()
    source = next(n for n in active["nodes"] if n["node_id"] == source["node_id"])

    item = SourceItem(
        source_type=SourceType.LOCAL, source_id="historical-policy",
        source_path="/historical-policy.pdf",
        original_name="历史制度.pdf",
    )
    led.upsert_discovered(item)
    led.update(
        item.stable_key(), category=source["display_name"],
        stage=Stage.LOADED.value, feishu_token="file-history",
    )
    led.assign_structure_target(
        item.stable_key(), active["id"], source["node_id"]
    )
    writer.add_file(
        source["binding"]["remote_token"], "历史制度.pdf", "file-history"
    )

    draft = structures.ensure_draft()
    split = structures.split_node(
        draft["id"], draft["revision"], source["node_id"], [
            {
                "display_name": "PDF 文档",
                "assignment_rule": {
                    "field": "doc_type", "operator": "equals",
                    "value": "pdf", "priority": 10,
                },
            },
            {
                "display_name": "其他文档",
                "assignment_rule": {"fallback": True, "priority": 999},
            },
        ],
    )
    structures.approve(split["id"])
    StructureReconciler(structures, writer).apply(
        split["id"], root_token=initial["root_token"]
    )
    return led, structures, writer, item.stable_key(), source["node_id"]


def test_historical_relocation_plan_executes_and_rolls_back(tmp_path):
    led, structures, writer, stable_key, source_node_id = (
        _active_split_with_history(tmp_path)
    )
    try:
        active = structures.active_version()
        target = next(
            n for n in active["nodes"] if n["display_name"] == "PDF 文档"
        )
        plan = structures.create_item_relocation_plan(
            active["id"], actor="planner"
        )
        assert plan["summary"]["total"] == 1
        assert plan["actions"][0]["target_node_id"] == target["node_id"]

        approved = structures.approve_item_relocation_plan(
            plan["id"], actor="approver"
        )
        result = ItemRelocationExecutor(structures, writer).execute(
            approved["id"]
        )
        assert result["status"] == "completed"
        assert any(
            item["token"] == "file-history"
            for item in writer.list_drive_children(
                target["binding"]["remote_token"]
            )
        )
        assert led.get(stable_key)["target_node_id"] == target["node_id"]

        rolled_back = ItemRelocationExecutor(structures, writer).rollback(
            approved["id"]
        )
        assert rolled_back["status"] == "rolled_back"
        assert led.get(stable_key)["target_node_id"] == source_node_id
        source = next(
            n for n in active["nodes"] if n["node_id"] == source_node_id
        )
        assert any(
            item["token"] == "file-history"
            for item in writer.list_drive_children(
                source["binding"]["remote_token"]
            )
        )
    finally:
        led.close()


def test_historical_relocation_conflict_blocks_all_moves(tmp_path):
    led, structures, writer, stable_key, source_node_id = (
        _active_split_with_history(tmp_path)
    )
    try:
        active = structures.active_version()
        target = next(
            n for n in active["nodes"] if n["display_name"] == "PDF 文档"
        )
        writer.add_file(
            target["binding"]["remote_token"], "历史制度.pdf", "file-conflict"
        )
        plan = structures.create_item_relocation_plan(active["id"])
        structures.approve_item_relocation_plan(plan["id"])
        executor = ItemRelocationExecutor(structures, writer)

        with pytest.raises(StructureConflict, match="同名冲突"):
            executor.execute(plan["id"])

        source = next(
            n for n in active["nodes"] if n["node_id"] == source_node_id
        )
        assert any(
            item["token"] == "file-history"
            for item in writer.list_drive_children(
                source["binding"]["remote_token"]
            )
        )
        assert led.get(stable_key)["target_node_id"] == source_node_id
        failed = structures.get_item_relocation_plan(plan["id"])
        assert failed["summary"]["conflicts"] == 1
    finally:
        led.close()


def test_historical_relocation_blocks_permission_expansion(tmp_path):
    led, structures, writer, stable_key, source_node_id = (
        _active_split_with_history(tmp_path)
    )
    try:
        active = structures.active_version()
        target = next(
            n for n in active["nodes"] if n["display_name"] == "PDF 文档"
        )
        source = next(
            n for n in active["nodes"] if n["node_id"] == source_node_id
        )

        def permissions(token, obj_type, user_token=""):
            if token == target["binding"]["remote_token"]:
                return {
                    "external_access": True,
                    "link_share_entity": "anyone_readable",
                }
            return {
                "external_access": False,
                "link_share_entity": "closed",
            }

        writer.get_public_permission = permissions
        plan = structures.create_item_relocation_plan(active["id"])
        structures.approve_item_relocation_plan(plan["id"])

        with pytest.raises(StructureConflict, match="权限扩大"):
            ItemRelocationExecutor(structures, writer).execute(plan["id"])

        assert any(
            item["token"] == "file-history"
            for item in writer.list_drive_children(
                source["binding"]["remote_token"]
            )
        )
        assert led.get(stable_key)["target_node_id"] == source_node_id
    finally:
        led.close()


def test_historical_relocation_exact_hash_deduplicates_without_move(tmp_path):
    led, structures, writer, stable_key, source_node_id = (
        _active_split_with_history(tmp_path)
    )
    try:
        digest = "a" * 64
        led.update(stable_key, content_sha256=digest)
        active = structures.active_version()
        target = next(
            n for n in active["nodes"] if n["display_name"] == "PDF 文档"
        )
        writer.add_file(
            target["binding"]["remote_token"], "历史制度.pdf", "file-existing"
        )
        writer.children[target["binding"]["remote_token"]][-1][
            "content_sha256"
        ] = digest

        plan = structures.create_item_relocation_plan(active["id"])
        structures.approve_item_relocation_plan(plan["id"])
        result = ItemRelocationExecutor(structures, writer).execute(plan["id"])

        assert result["status"] == "completed"
        action = structures.get_item_relocation_plan(plan["id"])["actions"][0]
        assert action["detail"]["preflight"] == "exact_duplicate_in_target"
        assert action["detail"]["remote_moved"] is False
        assert led.get(stable_key)["target_node_id"] == target["node_id"]
        source = next(
            n for n in active["nodes"] if n["node_id"] == source_node_id
        )
        assert any(
            item["token"] == "file-history"
            for item in writer.list_drive_children(
                source["binding"]["remote_token"]
            )
        )
    finally:
        led.close()


def test_historical_relocation_resumes_after_lost_move_response(tmp_path):
    led, structures, writer, stable_key, source_node_id = (
        _active_split_with_history(tmp_path)
    )
    try:
        active = structures.active_version()
        target = next(
            n for n in active["nodes"] if n["display_name"] == "PDF 文档"
        )
        plan = structures.create_item_relocation_plan(active["id"])
        structures.approve_item_relocation_plan(plan["id"])
        original_move = writer.move_file

        def move_then_lose_response(token, folder, obj_type="file"):
            original_move(token, folder, obj_type)
            writer.move_file = original_move
            raise RuntimeError("connection lost after remote move")

        writer.move_file = move_then_lose_response
        executor = ItemRelocationExecutor(structures, writer)
        with pytest.raises(RuntimeError, match="connection lost"):
            executor.execute(plan["id"])
        assert led.get(stable_key)["target_node_id"] == source_node_id
        assert any(
            item["token"] == "file-history"
            for item in writer.list_drive_children(
                target["binding"]["remote_token"]
            )
        )

        resumed = executor.execute(plan["id"])
        assert resumed["status"] == "completed"
        assert led.get(stable_key)["target_node_id"] == target["node_id"]
        action = structures.get_item_relocation_plan(plan["id"])["actions"][0]
        assert action["status"] == "completed"
        assert action["detail"]["remote_moved"] is True
        relocation = structures.find_relocation(
            active["id"], "item_split", "file-history",
            node_id=target["node_id"],
        )
        assert relocation["status"] == "completed"
    finally:
        led.close()


def test_remote_snapshot_diff_maps_existing_and_creates_missing(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        first = draft["nodes"][0]
        snapshot = structures.save_remote_snapshot(
            "drive",
            [{
                "remote_token": "fld-existing",
                "parent_token": "root-token",
                "display_name": first["display_name"],
                "node_type": "folder",
            }, {
                "remote_token": "fld-external",
                "parent_token": "root-token",
                "display_name": "外部维护",
                "node_type": "folder",
            }],
            root_token="root-token",
        )
        plan = structures.create_diff_plan(draft["id"], snapshot["id"])
        action_types = [item["action_type"] for item in plan["actions"]]
        assert "MAP" in action_types
        assert "CREATE" in action_types
        assert "REMOTE_ONLY" in action_types
        assert plan["summary"]["blocking_conflicts"] == 0
    finally:
        led.close()


class FakeWriter:
    def __init__(self):
        self.created = []
        self.children = {}

    def create_folder(self, name, parent_token=""):
        token = f"folder-{len(self.created) + 1}"
        self.created.append((token, name, parent_token))
        self.children.setdefault(parent_token, []).append({
            "token": token, "name": name, "type": "folder",
        })
        self.children.setdefault(token, [])
        return token

    def list_drive_children(self, folder_token, user_token=""):
        return list(self.children.get(folder_token, []))

    def move_file(self, file_token, folder_token, obj_type="file"):
        item = None
        for children in self.children.values():
            for candidate in list(children):
                if candidate["token"] == file_token:
                    item = candidate
                    children.remove(candidate)
                    break
            if item:
                break
        if item is None:
            raise KeyError(file_token)
        self.children.setdefault(folder_token, []).append(item)
        return {}

    def add_file(self, folder_token, name, token):
        self.children.setdefault(folder_token, []).append({
            "token": token, "name": name, "type": "file",
        })


def test_approved_structure_can_be_applied_and_activated(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        approved = structures.approve(draft["id"], actor="owner")
        writer = FakeWriter()

        result = StructureReconciler(structures, writer).apply(approved["id"])

        assert result["status"] == "active"
        plan = structures.get_change_plan(result["plan_id"])
        assert plan["status"] == "completed"
        assert {
            action["status"] for action in plan["actions"]
        } <= {"completed", "skipped"}
        assert len(writer.created) == len(draft["nodes"]) + 1  # 根目录 + 规划节点
        active = structures.active_version()
        assert active["id"] == approved["id"]
        assert all(node["binding"] for node in active["nodes"])
        routes, node_ids = structures.routing_map(mode="drive")
        assert routes["90 待整理"]
        assert node_ids["99 归档"]
    finally:
        led.close()


def test_merge_draft_generates_merge_and_retire_actions(tmp_path):
    led, structures = service(tmp_path)
    try:
        first = structures.ensure_draft()
        structures.approve(first["id"])
        writer = FakeWriter()
        initial = StructureReconciler(structures, writer).apply(first["id"])

        draft = structures.ensure_draft()
        target, source = draft["nodes"][0], draft["nodes"][1]
        merged = structures.merge_nodes(
            draft["id"], draft["revision"], target["node_id"],
            [source["node_id"]], actor="owner",
        )
        assert source["display_name"] in merged["nodes"][0]["aliases"]
        assert not any(n["node_id"] == source["node_id"] for n in merged["nodes"])
        assert merged["transformations"][0]["transformation_type"] == "merge"

        plan = structures.create_diff_plan(
            merged["id"], structures.latest_snapshot("drive")["id"]
        )
        action_types = [action["action_type"] for action in plan["actions"]]
        assert "MERGE" in action_types
        assert "RETIRE" in action_types
        assert not any(
            action["action_type"] == "REMOTE_ONLY"
            and action["remote_token"] == source["binding"]["remote_token"]
            for action in plan["actions"]
        )
        assert initial["root_token"]
    finally:
        led.close()


def test_drive_merge_creates_independent_history_relocation_plan(tmp_path):
    led, structures = service(tmp_path)
    try:
        first = structures.ensure_draft()
        structures.approve(first["id"])
        writer = FakeWriter()
        initial = StructureReconciler(structures, writer).apply(first["id"])
        active = structures.active_version()
        target, source = active["nodes"][0], active["nodes"][1]
        source_token = source["binding"]["remote_token"]
        target_token = target["binding"]["remote_token"]
        writer.add_file(source_token, "项目说明.docx", "file-project")
        item = SourceItem(
            source_type=SourceType.LOCAL, source_id="merge-history",
            source_path="/项目说明.docx", original_name="项目说明.docx",
        )
        led.upsert_discovered(item)
        led.update(
            item.stable_key(), category=source["display_name"],
            stage=Stage.LOADED.value, feishu_token="file-project",
        )
        led.assign_structure_target(
            item.stable_key(), active["id"], source["node_id"]
        )

        draft = structures.ensure_draft()
        structures.merge_nodes(
            draft["id"], draft["revision"], target["node_id"],
            [source["node_id"]],
        )
        structures.approve(draft["id"])
        plan = structures.create_diff_plan(
            draft["id"], history_scope="relocate_history"
        )
        plan = structures.approve_change_plan(plan["id"], actor="owner")
        result = StructureReconciler(structures, writer).apply(
            draft["id"], plan_id=plan["id"],
            root_token=initial["root_token"]
        )

        assert result["merged_children"] == 0
        assert result["relocation_candidates"] == 1
        assert any(
            item["token"] == "file-project"
            for item in writer.list_drive_children(source_token)
        )
        relocation = structures.get_item_relocation_plan(
            result["relocation_plan_id"]
        )
        structures.approve_item_relocation_plan(relocation["id"])
        ItemRelocationExecutor(structures, writer).execute(relocation["id"])
        assert writer.list_drive_children(source_token) == []
        assert any(item["token"] == "file-project"
                   for item in writer.list_drive_children(target_token))
        assert any(token == source_token for token, _, _ in writer.created)
        assert structures.get_version(draft["id"])["transformations"][0]["status"] == "completed"
    finally:
        led.close()


def test_drive_rename_preserves_history_until_independent_relocation(tmp_path):
    led, structures = service(tmp_path)
    try:
        first = structures.ensure_draft()
        structures.approve(first["id"])
        writer = FakeWriter()
        initial = StructureReconciler(structures, writer).apply(first["id"])
        active = structures.active_version()
        original = active["nodes"][0]
        old_token = original["binding"]["remote_token"]
        writer.add_file(old_token, "制度正文.docx", "file-policy")
        item = SourceItem(
            source_type=SourceType.LOCAL, source_id="rename-history",
            source_path="/制度正文.docx", original_name="制度正文.docx",
        )
        led.upsert_discovered(item)
        led.update(
            item.stable_key(), category=original["display_name"],
            stage=Stage.LOADED.value, feishu_token="file-policy",
        )
        led.assign_structure_target(
            item.stable_key(), active["id"], original["node_id"]
        )

        draft = structures.ensure_draft()
        nodes = [dict(node) for node in draft["nodes"]]
        nodes[0]["display_name"] = "制度流程中心"
        saved = structures.save_draft(draft["id"], draft["revision"], nodes)
        structures.approve(saved["id"])
        plan = structures.create_diff_plan(
            saved["id"], history_scope="relocate_history"
        )
        plan = structures.approve_change_plan(plan["id"], actor="owner")
        result = StructureReconciler(structures, writer).apply(
            saved["id"], plan_id=plan["id"],
            root_token=initial["root_token"]
        )

        renamed = structures.get_version(saved["id"])["nodes"][0]
        assert renamed["binding"]["remote_token"] != old_token
        assert [item["token"] for item in writer.list_drive_children(old_token)] == [
            "file-policy"
        ]
        relocation = structures.get_item_relocation_plan(
            result["relocation_plan_id"]
        )
        structures.approve_item_relocation_plan(relocation["id"])
        ItemRelocationExecutor(structures, writer).execute(relocation["id"])
        assert writer.list_drive_children(old_token) == []
        assert any(
            item["token"] == "file-policy"
            for item in writer.list_drive_children(
                renamed["binding"]["remote_token"]
            )
        )
    finally:
        led.close()


def test_default_structure_publish_preserves_historical_content(tmp_path):
    led, structures = service(tmp_path)
    try:
        first = structures.ensure_draft()
        structures.approve(first["id"])
        writer = FakeWriter()
        initial = StructureReconciler(structures, writer).apply(first["id"])
        active = structures.active_version()
        original = active["nodes"][0]
        old_token = original["binding"]["remote_token"]
        writer.add_file(old_token, "历史制度.docx", "file-history-safe")

        draft = structures.ensure_draft()
        nodes = [dict(node) for node in draft["nodes"]]
        nodes[0]["display_name"] = "制度流程新目录"
        saved = structures.save_draft(draft["id"], draft["revision"], nodes)
        structures.approve(saved["id"])
        result = StructureReconciler(structures, writer).apply(
            saved["id"], root_token=initial["root_token"]
        )

        current = structures.get_version(saved["id"])["nodes"][0]
        assert result["history_scope"] == "unmigrated_only"
        assert current["binding"]["remote_token"] != old_token
        assert [item["token"] for item in writer.list_drive_children(old_token)] == [
            "file-history-safe"
        ]
        assert writer.list_drive_children(
            current["binding"]["remote_token"]
        ) == []
    finally:
        led.close()


def test_retry_scope_preserves_old_route_unless_retries_are_included(tmp_path):
    led, structures = service(tmp_path)
    try:
        first = structures.ensure_draft()
        structures.approve(first["id"])
        writer = FakeWriter()
        initial = StructureReconciler(structures, writer).apply(first["id"])
        active_v1 = structures.active_version()
        node_v1 = active_v1["nodes"][0]
        old_token = node_v1["binding"]["remote_token"]

        item = SourceItem(
            source_type=SourceType.LOCAL, source_id="retry-scope",
            source_path="/retry.docx", original_name="retry.docx",
        )
        led.upsert_discovered(item)
        led.update(
            item.stable_key(), category=node_v1["display_name"],
            stage=Stage.FAILED.value, failed_stage="load", retryable=1,
        )
        led.assign_structure_target(
            item.stable_key(), active_v1["id"], node_v1["node_id"]
        )

        draft_v2 = structures.ensure_draft()
        nodes = [dict(node) for node in draft_v2["nodes"]]
        nodes[0]["display_name"] = "制度流程 V2"
        structures.save_draft(
            draft_v2["id"], draft_v2["revision"], nodes
        )
        structures.approve(draft_v2["id"])
        StructureReconciler(structures, writer).apply(
            draft_v2["id"], root_token=initial["root_token"]
        )
        active_v2 = structures.active_version()
        preserved = structures.resolve_retry_target(
            led.get(item.stable_key()), active_v2["id"]
        )
        assert preserved["remote_token"] == old_token

        draft_v3 = structures.ensure_draft()
        nodes = [dict(node) for node in draft_v3["nodes"]]
        nodes[0]["display_name"] = "制度流程 V3"
        structures.save_draft(
            draft_v3["id"], draft_v3["revision"], nodes
        )
        structures.approve(draft_v3["id"])
        plan = structures.create_diff_plan(
            draft_v3["id"], history_scope="include_retries"
        )
        structures.approve_change_plan(plan["id"])
        StructureReconciler(structures, writer).apply(
            draft_v3["id"], plan_id=plan["id"],
            root_token=initial["root_token"],
        )
        active_v3 = structures.active_version()
        included = structures.resolve_retry_target(
            led.get(item.stable_key()), active_v3["id"]
        )
        assert included["remote_token"] == active_v3["nodes"][0]["binding"]["remote_token"]
        assert included["remote_token"] != old_token
    finally:
        led.close()


def test_remote_mapping_adoption_decisions_and_version_restore(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        snapshot = structures.save_remote_snapshot("drive", [
            {
                "remote_token": "remote-map", "parent_token": "root",
                "display_name": "映射目录", "file_count": 3,
            },
            {
                "remote_token": "remote-adopt", "parent_token": "root",
                "display_name": "采纳目录", "file_count": 2,
            },
            {
                "remote_token": "remote-external", "parent_token": "root",
                "display_name": "外部目录",
            },
        ], root_token="root")
        mapped = structures.map_remote_node(
            draft["id"], draft["revision"], draft["nodes"][0]["node_id"],
            "remote-map", actor="planner",
        )
        assert mapped["nodes"][0]["binding"]["remote_token"] == "remote-map"
        adopted = structures.adopt_remote_node(
            draft["id"], mapped["revision"], "remote-adopt", actor="planner"
        )
        assert any(
            node["display_name"] == "采纳目录"
            and node["binding"]["remote_token"] == "remote-adopt"
            for node in adopted["nodes"]
        )
        structures.set_remote_decision(
            "drive", "remote-external", "external", actor="owner"
        )
        refreshed = structures.get_snapshot(snapshot["id"])
        decisions = {
            node["remote_token"]: (node.get("decision") or {}).get("decision")
            for node in refreshed["nodes"]
        }
        assert decisions == {
            "remote-map": "mapped",
            "remote-adopt": "adopted",
            "remote-external": "external",
        }
        assert len(structures.list_versions()) == 1
        audit = structures.audit(draft["id"])
        assert {
            event["event_type"] for event in audit["events"]
        }.issuperset({"remote_node_mapped", "remote_node_adopted"})
    finally:
        led.close()


def test_wiki_leaf_documents_are_not_reported_as_remote_directories(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        led.conn.execute(
            "UPDATE structure_versions SET mode='wiki' WHERE id=?",
            (draft["id"],),
        )
        led.conn.commit()
        node = structures.get_version(draft["id"])["nodes"][0]
        structures.bind_node(
            draft["id"], node["node_id"], "wiki", "wiki-category"
        )
        snapshot = structures.save_remote_snapshot("wiki", [
            {
                "remote_token": "wiki-category", "parent_token": "",
                "display_name": node["display_name"], "node_type": "docx",
                "has_children": True,
            },
            {
                "remote_token": "wiki-content", "parent_token": "wiki-category",
                "display_name": "制度正文.docx", "node_type": "file",
                "has_children": False,
            },
        ], space_id="space")
        plan = structures.create_diff_plan(draft["id"], snapshot["id"])

        assert plan["summary"]["remote_content_nodes"] == 1
        assert not any(
            action["action_type"] == "REMOTE_ONLY"
            and action["remote_token"] == "wiki-content"
            for action in plan["actions"]
        )
    finally:
        led.close()


def test_wiki_content_file_binding_is_repaired_by_explicit_map(tmp_path):
    led, structures = service(tmp_path)
    try:
        draft = structures.ensure_draft()
        led.conn.execute(
            "UPDATE structure_versions SET mode='wiki' WHERE id=?",
            (draft["id"],),
        )
        led.conn.commit()
        node = structures.get_version(draft["id"])["nodes"][0]
        structures.bind_node(
            draft["id"], node["node_id"], "wiki", "content-file"
        )
        snapshot = structures.save_remote_snapshot("wiki", [
            {
                "remote_token": "content-file", "parent_token": "other-parent",
                "display_name": "错误绑定的正文.docx", "node_type": "file",
                "has_children": False,
            },
            {
                "remote_token": "correct-structure", "parent_token": "",
                "display_name": node["display_name"], "node_type": "docx",
                "has_children": True,
            },
        ], space_id="space")

        plan = structures.create_diff_plan(draft["id"], snapshot["id"])

        action = next(
            item for item in plan["actions"]
            if item["node_id"] == node["node_id"]
        )
        assert action["action_type"] == "MAP"
        assert action["remote_token"] == "correct-structure"
        assert action["before"]["previous_remote_token"] == "content-file"
        assert plan["summary"]["blocking_conflicts"] == 0
    finally:
        led.close()


def test_restore_active_version_into_current_draft(tmp_path):
    led, structures = service(tmp_path)
    try:
        first = structures.ensure_draft()
        original_name = first["nodes"][0]["display_name"]
        structures.approve(first["id"])
        StructureReconciler(structures, FakeWriter()).apply(first["id"])
        active = structures.active_version()

        draft = structures.ensure_draft()
        nodes = [dict(node) for node in draft["nodes"]]
        nodes[0]["display_name"] = "临时调整名称"
        changed = structures.save_draft(
            draft["id"], draft["revision"], nodes, actor="editor"
        )
        restored = structures.restore_version_to_draft(
            active["id"], changed["id"], changed["revision"], actor="owner"
        )

        assert restored["nodes"][0]["display_name"] == original_name
        audit = structures.audit(restored["id"])
        assert audit["events"][-1]["event_type"] == "version_restored"
        assert audit["events"][-1]["detail"]["source_version_id"] == active["id"]
    finally:
        led.close()


def test_merge_name_conflict_blocks_independent_relocation(tmp_path):
    led, structures = service(tmp_path)
    try:
        first = structures.ensure_draft()
        structures.approve(first["id"])
        writer = FakeWriter()
        initial = StructureReconciler(structures, writer).apply(first["id"])
        active = structures.active_version()
        target, source = active["nodes"][0], active["nodes"][1]
        target_token = target["binding"]["remote_token"]
        source_token = source["binding"]["remote_token"]
        writer.add_file(target_token, "同名.docx", "file-target")
        writer.add_file(source_token, "同名.docx", "file-source")
        item = SourceItem(
            source_type=SourceType.LOCAL, source_id="merge-conflict",
            source_path="/同名.docx", original_name="同名.docx",
        )
        led.upsert_discovered(item)
        led.update(
            item.stable_key(), category=source["display_name"],
            stage=Stage.LOADED.value, feishu_token="file-source",
        )
        led.assign_structure_target(
            item.stable_key(), active["id"], source["node_id"]
        )

        draft = structures.ensure_draft()
        structures.merge_nodes(
            draft["id"], draft["revision"], target["node_id"],
            [source["node_id"]],
        )
        structures.approve(draft["id"])
        plan = structures.create_diff_plan(
            draft["id"], history_scope="relocate_history"
        )
        plan = structures.approve_change_plan(plan["id"], actor="owner")
        result = StructureReconciler(structures, writer).apply(
            draft["id"], plan_id=plan["id"],
            root_token=initial["root_token"]
        )
        relocation = structures.get_item_relocation_plan(
            result["relocation_plan_id"]
        )
        structures.approve_item_relocation_plan(relocation["id"])
        with pytest.raises(StructureConflict, match="同名冲突"):
            ItemRelocationExecutor(structures, writer).execute(relocation["id"])

        assert any(i["token"] == "file-source"
                   for i in writer.list_drive_children(source_token))
        assert any(i["token"] == "file-target"
                   for i in writer.list_drive_children(target_token))
        assert structures.get_version(draft["id"])["status"] == "active"
    finally:
        led.close()
