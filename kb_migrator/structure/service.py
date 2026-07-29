"""目录结构草稿、版本、绑定、快照与差异计划。

规划节点使用永久 ``node_id``；可变目录名只作为显示字段。飞书 token 保存在 binding
中，避免重命名后破坏分类结果和历史迁移路由。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from ..ledger import Ledger
from ..taxonomy import Taxonomy


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _semantic_key(path: str, triage: str, archive: str) -> str:
    if path == triage:
        return "system.triage"
    if path == archive:
        return "system.archive"
    digest = hashlib.sha256(path.encode("utf-8")).hexdigest()[:12]
    return f"category.{digest}"


def _stable_node_id(semantic_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"kb-migrator:{semantic_key}"))


class StructureConflict(RuntimeError):
    """结构草稿被其他保存操作更新。"""


class StructureValidationError(ValueError):
    def __init__(self, result: dict):
        super().__init__("目录结构校验失败")
        self.result = result


class StructureService:
    def __init__(self, ledger: Ledger, taxonomy: Taxonomy, targets_file: str):
        self.ledger = ledger
        self.conn = ledger.conn
        self.taxonomy = taxonomy
        self.targets_file = targets_file

    # ── 版本初始化与读取 ──────────────────────────────────

    def _legacy_targets(self) -> dict:
        if self.targets_file and os.path.exists(self.targets_file):
            with open(self.targets_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {
            "mode": "", "root_token": "", "space_id": "",
            "folder_map": {}, "wiki_node_map": {},
        }

    def ensure_seeded(self) -> dict:
        row = self.conn.execute(
            "SELECT id FROM structure_versions ORDER BY created_at LIMIT 1"
        ).fetchone()
        if row:
            return self.get_version(row["id"])

        targets = self._legacy_targets()
        mode = targets.get("mode") or "drive"
        has_remote = bool(
            targets.get("root_token") or targets.get("space_id")
            or targets.get("folder_map") or targets.get("wiki_node_map")
        )
        version_id = str(uuid.uuid4())
        now = _now()
        status = "active" if has_remote else "draft"
        self.conn.execute(
            """INSERT INTO structure_versions
               (id,name,mode,status,revision,root_name,created_by,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (version_id, "初始目录结构", mode, status, 1,
             self.taxonomy.space_name, "system", now, now),
        )
        categories = {c.path: c for c in self.taxonomy.categories}
        drive_map = targets.get("folder_map") or {}
        wiki_map = targets.get("wiki_node_map") or {}
        for order, path in enumerate(self.taxonomy.all_folder_paths()):
            semantic = _semantic_key(
                path, self.taxonomy.triage_path, self.taxonomy.archive_path
            )
            category = categories.get(path)
            node_id = _stable_node_id(semantic)
            kind = (
                "triage" if path == self.taxonomy.triage_path
                else "archive" if path == self.taxonomy.archive_path
                else "category"
            )
            self.conn.execute(
                """INSERT INTO structure_nodes
                   (version_id,node_id,parent_node_id,semantic_key,display_name,
                    sort_order,node_kind,owner,steward,review_months,
                    retention_years,status,aliases_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (version_id, node_id, None, semantic, path, order, kind,
                 category.owner if category else "",
                 category.steward if category else "",
                 category.review_months if category else 12,
                 category.retention_years if category else 5,
                 "active", "[]"),
            )
            token = drive_map.get(path) if mode == "drive" else wiki_map.get(path)
            if token:
                self.conn.execute(
                    """INSERT INTO structure_bindings
                       (version_id,node_id,target_mode,remote_token,
                        parent_remote_token,binding_status,created_at,updated_at)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (version_id, node_id, mode, token,
                     targets.get("root_token", "") if mode == "drive" else "",
                     "bound", now, now),
                )
        self._event(version_id, "legacy_imported", "system", {
            "status": status,
            "mode": mode,
            "bindings": len(drive_map if mode == "drive" else wiki_map),
        }, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def active_version(self) -> dict | None:
        self.ensure_seeded()
        row = self.conn.execute(
            "SELECT id FROM structure_versions WHERE status='active' "
            "ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        return self.get_version(row["id"]) if row else None

    def list_versions(self, limit: int = 50) -> list[dict]:
        """返回轻量版本历史，避免版本选择器加载每个版本的完整节点树。"""
        self.ensure_seeded()
        rows = self.conn.execute(
            """SELECT v.*,
                      (SELECT COUNT(*) FROM structure_nodes n
                       WHERE n.version_id=v.id) AS node_count,
                      (SELECT COUNT(*) FROM structure_approvals a
                       WHERE a.version_id=v.id
                         AND a.decision='approved') AS approval_count
               FROM structure_versions v
               ORDER BY v.updated_at DESC LIMIT ?""",
            (max(1, min(int(limit), 200)),),
        ).fetchall()
        return [dict(row) for row in rows]

    def ensure_draft(self, *, mode: str = "", actor: str = "") -> dict:
        self.ensure_seeded()
        row = self.conn.execute(
            "SELECT id FROM structure_versions WHERE status IN ('draft','reviewing') "
            "ORDER BY CASE status WHEN 'draft' THEN 0 ELSE 1 END, "
            "updated_at DESC LIMIT 1"
        ).fetchone()
        if row:
            return self.get_version(row["id"])

        base_row = self.conn.execute(
            "SELECT id FROM structure_versions WHERE status IN ('approved','failed','active') "
            "ORDER BY CASE status WHEN 'approved' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, "
            "updated_at DESC "
            "LIMIT 1"
        ).fetchone()
        if base_row is None:
            base_row = self.conn.execute(
                "SELECT id FROM structure_versions ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        base = self.get_version(base_row["id"])
        version_id = str(uuid.uuid4())
        now = _now()
        target_mode = mode or base["mode"]
        self.conn.execute(
            """INSERT INTO structure_versions
               (id,name,mode,status,revision,base_version_id,remote_snapshot_id,
                root_name,created_by,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (version_id, f"{base['name']} · 调整草稿", target_mode, "draft", 1,
             base["id"], base.get("remote_snapshot_id"), base["root_name"],
             actor, now, now),
        )
        for node in base["nodes"]:
            self._insert_node(version_id, node)
        if target_mode == base["mode"]:
            self.conn.execute(
                """INSERT INTO structure_bindings
                   SELECT ?,node_id,target_mode,remote_token,parent_remote_token,
                          binding_status,created_at,?
                   FROM structure_bindings WHERE version_id=?""",
                (version_id, now, base["id"]),
            )
        self._event(version_id, "draft_created", actor, {
            "base_version_id": base["id"],
        }, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def get_version(self, version_id: str) -> dict:
        row = self.conn.execute(
            "SELECT * FROM structure_versions WHERE id=?", (version_id,)
        ).fetchone()
        if not row:
            raise KeyError(version_id)
        bindings = {
            r["node_id"]: dict(r)
            for r in self.conn.execute(
                "SELECT * FROM structure_bindings WHERE version_id=?",
                (version_id,),
            ).fetchall()
        }
        counts = {
            r["category"]: r["c"]
            for r in self.conn.execute(
                "SELECT category,COUNT(*) c FROM items "
                "WHERE category IS NOT NULL GROUP BY category"
            ).fetchall()
        }
        nodes = []
        for node in self.conn.execute(
            "SELECT * FROM structure_nodes WHERE version_id=? "
            "ORDER BY sort_order,display_name", (version_id,)
        ).fetchall():
            item = dict(node)
            item["aliases"] = json.loads(item.pop("aliases_json") or "[]")
            item["assignment_rule"] = json.loads(
                item.pop("assignment_rule_json") or "{}"
            )
            item["binding"] = bindings.get(item["node_id"])
            item["item_count"] = sum(
                counts.get(name, 0)
                for name in [item["display_name"], *item["aliases"]]
            )
            nodes.append(item)
        result = dict(row)
        result["nodes"] = nodes
        result["approvals"] = [
            dict(approval) for approval in self.conn.execute(
                "SELECT * FROM structure_approvals WHERE version_id=? "
                "ORDER BY created_at", (version_id,)
            ).fetchall()
        ]
        result["approval_count"] = len(result["approvals"])
        result["transformations"] = []
        for transform in self.conn.execute(
            "SELECT * FROM structure_transformations WHERE version_id=? "
            "ORDER BY created_at", (version_id,)
        ).fetchall():
            item = dict(transform)
            item["source_node_ids"] = json.loads(
                item.pop("source_node_ids_json") or "[]"
            )
            item["rule"] = json.loads(item.pop("rule_json") or "{}")
            result["transformations"].append(item)
        return result

    # ── 草稿保存与校验 ──────────────────────────────────

    def save_draft(self, version_id: str, revision: int, nodes: list[dict],
                   *, name: str = "", root_name: str = "", actor: str = "") -> dict:
        current = self.get_version(version_id)
        if current["status"] != "draft":
            raise StructureConflict("只有草稿版本可以编辑")
        if int(current["revision"]) != int(revision):
            raise StructureConflict(
                f"结构已被更新：当前 revision={current['revision']}"
            )
        normalized = self._normalize_nodes(nodes)
        previous_by_id = {node["node_id"]: node for node in current["nodes"]}
        for node in normalized:
            previous = previous_by_id.get(node["node_id"])
            if previous and previous["display_name"] != node["display_name"]:
                aliases = set(node.get("aliases") or [])
                aliases.add(previous["display_name"])
                node["aliases"] = sorted(aliases)
        validation = self.validate_nodes(normalized)
        if validation["errors"]:
            raise StructureValidationError(validation)

        self.conn.execute(
            "DELETE FROM structure_nodes WHERE version_id=?", (version_id,)
        )
        for node in normalized:
            self._insert_node(version_id, node)
        node_ids = {node["node_id"] for node in normalized}
        if node_ids:
            placeholders = ",".join("?" for _ in node_ids)
            self.conn.execute(
                f"DELETE FROM structure_bindings WHERE version_id=? "
                f"AND node_id NOT IN ({placeholders})",
                (version_id, *sorted(node_ids)),
            )
        else:
            self.conn.execute(
                "DELETE FROM structure_bindings WHERE version_id=?", (version_id,)
            )
        now = _now()
        self.conn.execute(
            """UPDATE structure_versions
               SET revision=revision+1,name=?,root_name=?,updated_at=?
               WHERE id=?""",
            (name.strip() or current["name"], root_name.strip() or current["root_name"],
             now, version_id),
        )
        self._cancel_open_change_plans(version_id, now)
        after_by_id = {node["node_id"]: node for node in normalized}
        added = sorted(set(after_by_id) - set(previous_by_id))
        removed = sorted(set(previous_by_id) - set(after_by_id))
        renamed = [
            {
                "node_id": node_id,
                "before": previous_by_id[node_id]["display_name"],
                "after": after_by_id[node_id]["display_name"],
            }
            for node_id in sorted(set(previous_by_id) & set(after_by_id))
            if previous_by_id[node_id]["display_name"]
            != after_by_id[node_id]["display_name"]
        ]
        moved = [
            {
                "node_id": node_id,
                "before": previous_by_id[node_id].get("parent_node_id"),
                "after": after_by_id[node_id].get("parent_node_id"),
            }
            for node_id in sorted(set(previous_by_id) & set(after_by_id))
            if (previous_by_id[node_id].get("parent_node_id") or None)
            != (after_by_id[node_id].get("parent_node_id") or None)
        ]
        reordered = [
            node_id
            for node_id in sorted(set(previous_by_id) & set(after_by_id))
            if int(previous_by_id[node_id].get("sort_order") or 0)
            != int(after_by_id[node_id].get("sort_order") or 0)
        ]
        policy_fields = (
            "owner", "steward", "review_months", "retention_years",
            "assignment_rule",
        )
        policy_changed = [
            node_id
            for node_id in sorted(set(previous_by_id) & set(after_by_id))
            if any(
                previous_by_id[node_id].get(field)
                != after_by_id[node_id].get(field)
                for field in policy_fields
            )
        ]
        self._event(version_id, "draft_saved", actor, {
            "node_count": len(normalized), "revision": revision + 1,
            "changes": {
                "added": added,
                "removed": removed,
                "renamed": renamed,
                "moved": moved,
                "reordered": reordered,
                "policy_changed": policy_changed,
            },
        }, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def restore_version_to_draft(
        self, source_version_id: str, target_draft_id: str, revision: int,
        *, actor: str = "",
    ) -> dict:
        """把历史版本复制到当前草稿；历史版本和飞书现状均不会被修改。"""
        source = self.get_version(source_version_id)
        target = self.get_version(target_draft_id)
        if source_version_id == target_draft_id:
            raise StructureConflict("不能从当前草稿恢复当前草稿")
        if target["status"] != "draft":
            raise StructureConflict("恢复目标必须是可编辑草稿")
        if source["mode"] != target["mode"]:
            raise StructureConflict("不能在云盘与 Wiki 两种目标形态之间直接恢复")
        restored = self.save_draft(
            target_draft_id,
            revision,
            [dict(node) for node in source["nodes"]],
            name=f"{source['name']} · 恢复草稿",
            root_name=source["root_name"],
            actor=actor,
        )
        now = _now()
        self.conn.execute(
            "DELETE FROM structure_bindings WHERE version_id=?",
            (target_draft_id,),
        )
        self.conn.execute(
            """INSERT INTO structure_bindings
               SELECT ?,node_id,target_mode,remote_token,parent_remote_token,
                      binding_status,created_at,?
               FROM structure_bindings WHERE version_id=?""",
            (target_draft_id, now, source_version_id),
        )
        self.conn.execute(
            "DELETE FROM structure_transformations WHERE version_id=?",
            (target_draft_id,),
        )
        for transform in source.get("transformations") or []:
            self.conn.execute(
                """INSERT INTO structure_transformations
                   (id,version_id,transformation_type,source_node_ids_json,
                    target_node_id,rule_json,status,error_detail,created_at,
                    updated_at)
                   VALUES (?,?,?,?,?,?,'pending',NULL,?,?)""",
                (
                    str(uuid.uuid4()), target_draft_id,
                    transform["transformation_type"],
                    _json(transform.get("source_node_ids") or []),
                    transform["target_node_id"],
                    _json(transform.get("rule") or {}), now, now,
                ),
            )
        self._event(target_draft_id, "version_restored", actor, {
            "source_version_id": source_version_id,
            "source_status": source["status"],
        }, commit=False)
        self.conn.commit()
        return self.get_version(restored["id"])

    def validate_version(self, version_id: str) -> dict:
        return self.validate_nodes(self.get_version(version_id)["nodes"])

    def validate_nodes(self, nodes: list[dict]) -> dict:
        errors: list[dict] = []
        warnings: list[dict] = []
        ids = [str(n.get("node_id", "")).strip() for n in nodes]
        semantics = [str(n.get("semantic_key", "")).strip() for n in nodes]
        id_set = set(ids)
        if len(id_set) != len(ids):
            errors.append({"code": "duplicate_node_id", "message": "存在重复节点 ID"})
        if len(set(semantics)) != len(semantics):
            errors.append({"code": "duplicate_semantic_key", "message": "存在重复语义分类键"})

        children: dict[str | None, list[dict]] = defaultdict(list)
        for node in nodes:
            node_id = str(node.get("node_id", "")).strip()
            name = str(node.get("display_name", "")).strip()
            parent = node.get("parent_node_id") or None
            if not node_id or not name:
                errors.append({
                    "code": "required", "node_id": node_id,
                    "message": "节点 ID 和目录名称不能为空",
                })
            if any(ch in name for ch in ("/", "\\")):
                errors.append({
                    "code": "invalid_name", "node_id": node_id,
                    "message": f"目录名称不能包含 / 或 \\：{name}",
                })
            if parent and parent not in id_set:
                errors.append({
                    "code": "orphan", "node_id": node_id,
                    "message": f"父节点不存在：{name}",
                })
            if parent == node_id:
                errors.append({
                    "code": "cycle", "node_id": node_id,
                    "message": f"节点不能以自身为父节点：{name}",
                })
            children[parent].append(node)

        for siblings in children.values():
            names = Counter(str(n.get("display_name", "")).strip().casefold()
                            for n in siblings)
            for duplicate, count in names.items():
                if duplicate and count > 1:
                    errors.append({
                        "code": "duplicate_sibling_name",
                        "message": f"同一层级存在重名目录：{duplicate}",
                    })
            if len(siblings) > 1500:
                errors.append({
                    "code": "too_many_siblings",
                    "message": "单层目录超过飞书 1500 节点限制",
                })
            ruled = [n for n in siblings if n.get("assignment_rule")]
            fallback_count = sum(
                bool((n.get("assignment_rule") or {}).get("fallback"))
                for n in ruled
            )
            if fallback_count > 1:
                errors.append({
                    "code": "multiple_rule_fallbacks",
                    "message": "同一拆分目录下只能设置一个兜底规则",
                })
            if ruled and fallback_count == 0:
                errors.append({
                    "code": "missing_rule_fallback",
                    "message": "拆分规则必须设置兜底目录，避免未匹配文件静默留在原目录",
                })
            for node in ruled:
                rule_error = self._validate_assignment_rule(
                    node.get("assignment_rule") or {}
                )
                if rule_error:
                    errors.append({
                        "code": "invalid_assignment_rule",
                        "node_id": node.get("node_id"),
                        "message": rule_error,
                    })

        parent_of = {
            str(n.get("node_id")): (n.get("parent_node_id") or None) for n in nodes
        }
        for node_id in id_set:
            seen: set[str] = set()
            cursor = node_id
            depth = 0
            while cursor:
                if cursor in seen:
                    errors.append({
                        "code": "cycle", "node_id": node_id,
                        "message": "目录层级存在循环引用",
                    })
                    break
                seen.add(cursor)
                cursor = parent_of.get(cursor)
                depth += 1
                if depth > 8:
                    warnings.append({
                        "code": "deep_tree", "node_id": node_id,
                        "message": "目录超过 8 层，建议适当扁平化",
                    })
                    break

        kinds = Counter(str(n.get("node_kind", "category")) for n in nodes)
        for kind, label in (("triage", "待整理"), ("archive", "归档")):
            if kinds[kind] != 1:
                errors.append({
                    "code": f"{kind}_required",
                    "message": f"必须且只能有一个“{label}”系统目录",
                })
        return {
            "valid": not errors, "errors": errors, "warnings": warnings,
            "node_count": len(nodes),
        }

    def approve(self, version_id: str, *, actor: str = "",
                required_approvals: int = 1, comment: str = "") -> dict:
        version = self.get_version(version_id)
        if version["status"] not in ("draft", "reviewing"):
            raise StructureConflict("只有草稿或审批中的版本可以确认")
        validation = self.validate_nodes(version["nodes"])
        if validation["errors"]:
            raise StructureValidationError(validation)
        required = max(1, int(required_approvals or 1))
        existing_required = int(version.get("required_approvals") or 1)
        if version["status"] == "reviewing" and required != existing_required:
            raise StructureConflict("审批开始后不能修改所需审批人数")
        if version["status"] == "reviewing":
            required = existing_required
        actor = actor.strip() or ("local-owner" if required == 1 else "")
        if not actor:
            raise StructureConflict("多人审批必须填写审批人")
        now = _now()
        self.conn.execute(
            """INSERT INTO structure_approvals
               (version_id,actor,decision,comment,created_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT(version_id,actor) DO UPDATE SET
                 decision=excluded.decision,comment=excluded.comment,
                 created_at=excluded.created_at""",
            (version_id, actor, "approved", comment.strip(), now),
        )
        approval_count = self.conn.execute(
            "SELECT COUNT(*) c FROM structure_approvals "
            "WHERE version_id=? AND decision='approved'", (version_id,)
        ).fetchone()["c"]
        status = "approved" if approval_count >= required else "reviewing"
        self.conn.execute(
            """UPDATE structure_versions
               SET status=?,required_approvals=?,approved_by=?,
                   approved_at=?,updated_at=?
               WHERE id=?""",
            (
                status, required, actor if status == "approved" else None,
                now if status == "approved" else None, now, version_id,
            ),
        )
        self._event(version_id, "approval_recorded", actor, {
            **validation, "comment": comment.strip(),
            "approval_count": approval_count,
            "required_approvals": required,
            "status": status,
        }, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def split_node(self, version_id: str, revision: int, source_node_id: str,
                   children: list[dict], *, actor: str = "") -> dict:
        """给分类节点增加规则子目录；只影响后续路由，历史文件仅进入影响预览。"""
        version = self.get_version(version_id)
        if version["status"] != "draft":
            raise StructureConflict("只有草稿版本可以拆分目录")
        if int(version["revision"]) != int(revision):
            raise StructureConflict(
                f"结构已被更新：当前 revision={version['revision']}"
            )
        source = next(
            (n for n in version["nodes"] if n["node_id"] == source_node_id), None
        )
        if not source:
            raise KeyError(source_node_id)
        if source["node_kind"] in ("triage", "archive"):
            raise ValueError("待整理和归档系统目录不能配置自动拆分")
        if len(children) < 2:
            raise ValueError("拆分至少需要两个目标子目录")
        children = [dict(child) for child in children]
        if not any(
            (child.get("assignment_rule") or {}).get("fallback")
            for child in children
        ):
            children.append({
                "display_name": self.taxonomy.triage_path,
                "assignment_rule": {"fallback": True, "priority": 9999},
            })

        nodes = [dict(node) for node in version["nodes"]]
        created: list[dict] = []
        existing_names = {
            n["display_name"].casefold()
            for n in nodes if n.get("parent_node_id") == source_node_id
        }
        for offset, spec in enumerate(children):
            name = str(spec.get("display_name") or "").strip()
            if not name:
                raise ValueError("拆分目标目录名称不能为空")
            if name.casefold() in existing_names:
                raise ValueError(f"拆分目标目录重名：{name}")
            existing_names.add(name.casefold())
            node_id = str(spec.get("node_id") or uuid.uuid4())
            rule = dict(spec.get("assignment_rule") or {})
            rule.setdefault("priority", offset + 1)
            created_node = {
                "node_id": node_id,
                "parent_node_id": source_node_id,
                "semantic_key": str(
                    spec.get("semantic_key") or f"split.{uuid.uuid4()}"
                ),
                "display_name": name,
                "sort_order": len(nodes) + offset,
                "node_kind": "category",
                "owner": str(spec.get("owner") or source.get("owner") or ""),
                "steward": str(
                    spec.get("steward") or source.get("steward") or ""
                ),
                "review_months": int(
                    spec.get("review_months")
                    or source.get("review_months") or 12
                ),
                "retention_years": int(
                    spec.get("retention_years")
                    or source.get("retention_years") or 5
                ),
                "status": "active", "aliases": [],
                "assignment_rule": rule,
            }
            created.append(created_node)
            nodes.append(created_node)

        saved = self.save_draft(version_id, revision, nodes, actor=actor)
        transform_id = str(uuid.uuid4())
        now = _now()
        self.conn.execute(
            """INSERT INTO structure_transformations
               (id,version_id,transformation_type,source_node_ids_json,
                target_node_id,rule_json,status,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                transform_id, version_id, "split", _json([source_node_id]),
                source_node_id, _json({
                    "source_display_name": source["display_name"],
                    "child_node_ids": [node["node_id"] for node in created],
                    "history_policy": "preview_only",
                }), "pending", now, now,
            ),
        )
        self._event(version_id, "node_split", actor, {
            "transformation_id": transform_id,
            "source_node_id": source_node_id,
            "child_node_ids": [node["node_id"] for node in created],
        }, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def merge_nodes(self, version_id: str, revision: int, target_node_id: str,
                    source_node_ids: list[str], *, actor: str = "",
                    policy_resolutions: dict | None = None) -> dict:
        """在草稿中合并目录，保留目标稳定 ID，并记录远程内容搬迁意图。"""
        version = self.get_version(version_id)
        if version["status"] != "draft":
            raise StructureConflict("只有草稿版本可以合并目录")
        if int(version["revision"]) != int(revision):
            raise StructureConflict(
                f"结构已被更新：当前 revision={version['revision']}"
            )
        by_id = {node["node_id"]: dict(node) for node in version["nodes"]}
        target = by_id.get(target_node_id)
        sources = [
            by_id[node_id] for node_id in dict.fromkeys(source_node_ids)
            if node_id in by_id and node_id != target_node_id
        ]
        if not target or not sources:
            raise ValueError("请选择一个目标目录和至少一个来源目录")
        protected = [n["display_name"] for n in sources
                     if n["node_kind"] in ("triage", "archive")]
        if protected:
            raise ValueError(f"系统目录不能作为合并来源：{', '.join(protected)}")

        parent_of = {
            node["node_id"]: node.get("parent_node_id") for node in version["nodes"]
        }
        for source in sources:
            cursor = target_node_id
            while cursor:
                if cursor == source["node_id"]:
                    raise ValueError("不能把父目录合并到其子目录")
                cursor = parent_of.get(cursor)

        source_ids = {source["node_id"] for source in sources}
        aliases = set(target.get("aliases") or [])
        policy_conflicts: list[dict] = []
        policy_resolutions = policy_resolutions or {}
        for source in sources:
            aliases.add(source["display_name"])
            aliases.update(source.get("aliases") or [])
        for field in ("owner", "steward"):
            candidates = list(dict.fromkeys(
                value for value in [
                    target.get(field) or "",
                    *(source.get(field) or "" for source in sources),
                ] if value
            ))
            if len(candidates) <= 1:
                if candidates:
                    target[field] = candidates[0]
                continue
            selected = str(policy_resolutions.get(field) or "").strip()
            if selected not in candidates:
                raise StructureConflict(
                    f"合并目录的 {field} 不一致，必须人工选择："
                    f"{', '.join(candidates)}"
                )
            target[field] = selected
            policy_conflicts.append({
                "field": field,
                "candidates": candidates,
                "resolution": selected,
                "resolved_by": actor,
            })
        target["aliases"] = sorted(aliases)
        target["retention_years"] = max(
            [int(target.get("retention_years") or 5)]
            + [int(source.get("retention_years") or 5) for source in sources]
        )
        target["review_months"] = min(
            [int(target.get("review_months") or 12)]
            + [int(source.get("review_months") or 12) for source in sources]
        )
        normalized = []
        for node in version["nodes"]:
            if node["node_id"] in source_ids:
                continue
            item = dict(node)
            if item["node_id"] == target_node_id:
                item = target
            if item.get("parent_node_id") in source_ids:
                item["parent_node_id"] = target_node_id
            normalized.append(item)

        source_details = [{
            "node_id": source["node_id"],
            "display_name": source["display_name"],
            "aliases": source.get("aliases") or [],
            "binding": source.get("binding"),
            "item_count": source.get("item_count", 0),
            "owner": source.get("owner", ""),
            "steward": source.get("steward", ""),
            "review_months": source.get("review_months"),
            "retention_years": source.get("retention_years"),
        } for source in sources]
        saved = self.save_draft(
            version_id, revision, normalized, actor=actor
        )
        transform_id = str(uuid.uuid4())
        now = _now()
        self.conn.execute(
            """INSERT INTO structure_transformations
               (id,version_id,transformation_type,source_node_ids_json,
                target_node_id,rule_json,status,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (transform_id, version_id, "merge", _json(sorted(source_ids)),
             target_node_id, _json({
                 "sources": source_details,
                 "policy": "target_wins_no_permission_downgrade",
                 "policy_conflicts": policy_conflicts,
                 "effective_review_months": target["review_months"],
                 "effective_retention_years": target["retention_years"],
             }), "pending", now, now),
        )
        self._event(version_id, "nodes_merged", actor, {
            "transformation_id": transform_id,
            "source_node_ids": sorted(source_ids),
            "target_node_id": target_node_id,
        }, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def routing_map(self, version_id: str = "", *,
                    mode: str = "drive") -> tuple[dict[str, str], dict[str, str]]:
        """返回 ``分类/别名 -> token`` 与 ``分类/别名 -> 稳定 node_id``。"""
        if not version_id:
            active = self.active_version()
            if not active:
                return {}, {}
            version_id = active["id"]
        version = self.get_version(version_id)
        tokens: dict[str, str] = {}
        node_ids: dict[str, str] = {}
        for node in version["nodes"]:
            binding = node.get("binding") or {}
            if binding.get("target_mode") != mode or not binding.get("remote_token"):
                continue
            for key in [node["display_name"], *(node.get("aliases") or [])]:
                tokens[key] = binding["remote_token"]
                node_ids[key] = node["node_id"]
        return tokens, node_ids

    def resolve_item_target(self, item: Any, version_id: str = "", *,
                            mode: str = "drive") -> dict:
        """按分类别名找到基准目录，再按其规则子目录为单条知识选择稳定目标。"""
        if not version_id:
            active = self.active_version()
            if not active:
                return {}
            version_id = active["id"]
        version = self.get_version(version_id)
        row = dict(item)
        category = str(row.get("category") or self.taxonomy.triage_path)
        base = next((
            node for node in version["nodes"]
            if category in [node["display_name"], *(node.get("aliases") or [])]
        ), None)
        if not base:
            base = next((
                node for node in version["nodes"]
                if node["node_kind"] == "triage"
            ), None)
        if not base:
            return {}

        candidates = [
            node for node in version["nodes"]
            if node.get("parent_node_id") == base["node_id"]
            and node.get("assignment_rule")
        ]
        candidates.sort(key=lambda node: (
            int((node.get("assignment_rule") or {}).get("priority") or 1000),
            int(node.get("sort_order") or 0),
        ))
        fallback = next((
            node for node in candidates
            if (node.get("assignment_rule") or {}).get("fallback")
        ), None)
        selected = next((
            node for node in candidates
            if not (node.get("assignment_rule") or {}).get("fallback")
            and self._rule_matches(node["assignment_rule"], row)
        ), fallback)
        if not selected and candidates:
            selected = next((
                node for node in version["nodes"]
                if node["node_kind"] == "triage"
            ), None)
        selected = selected or base
        binding = selected.get("binding") or {}
        if binding and binding.get("target_mode") != mode:
            return {}
        return {
            "structure_version_id": version_id,
            "node_id": selected["node_id"],
            "remote_token": binding.get("remote_token") or "",
            "display_name": selected["display_name"],
            "assignment_source": (
                "split_rule" if selected["node_id"] != base["node_id"]
                else "category"
            ),
            "base_node_id": base["node_id"],
        }

    def resolve_retry_target(self, item: Any, version_id: str = "", *,
                             mode: str = "drive") -> dict:
        """按发布计划作用范围决定失败重试沿用旧路由还是切到新结构。

        默认 ``unmigrated_only`` 保留已经分配过结构版本的失败项路由；
        ``include_retries`` 和 ``relocate_history`` 才把失败重试切到当前版本。
        尚未分配过结构版本的条目属于未迁移内容，始终使用当前版本。
        """
        row = dict(item)
        if not version_id:
            active = self.active_version()
            version_id = active["id"] if active else ""
        if not version_id:
            return {}
        plan = self.latest_change_plan(version_id)
        scope = (plan or {}).get("history_scope") or "unmigrated_only"
        assigned_version = str(row.get("structure_version_id") or "")
        target_version = (
            assigned_version
            if scope == "unmigrated_only" and assigned_version
            else version_id
        )
        try:
            return self.resolve_item_target(row, target_version, mode=mode)
        except KeyError:
            return self.resolve_item_target(row, version_id, mode=mode)

    def split_impact(self, version_id: str) -> dict:
        """统计规则命中和已迁移重定位候选，不执行远程搬迁。"""
        version = self.get_version(version_id)
        split_sources = {
            transform["target_node_id"]: transform
            for transform in version.get("transformations") or []
            if transform["transformation_type"] == "split"
        }
        results = []
        total_candidates = 0
        total_loaded = 0
        for source_id, transform in split_sources.items():
            source = next(
                (n for n in version["nodes"] if n["node_id"] == source_id), None
            )
            if not source:
                continue
            names = {source["display_name"], *(source.get("aliases") or [])}
            placeholders = ",".join("?" for _ in names)
            rows = self.conn.execute(
                f"SELECT * FROM items WHERE category IN ({placeholders})",
                tuple(sorted(names)),
            ).fetchall()
            by_target: Counter = Counter()
            unmatched = 0
            loaded = 0
            for row in rows:
                resolved = self.resolve_item_target(
                    row, version_id, mode=version["mode"]
                )
                target_id = resolved.get("node_id") or source_id
                by_target[target_id] += 1
                if target_id == source_id:
                    unmatched += 1
                current_id = row["target_node_id"] or source_id
                if (
                    (row["feishu_token"] or row["wiki_node_token"])
                    and current_id != target_id
                ):
                    loaded += 1
            total_candidates += len(rows)
            total_loaded += loaded
            results.append({
                "transformation_id": transform["id"],
                "source_node_id": source_id,
                "source_display_name": source["display_name"],
                "total_items": len(rows),
                "matched_by_target": dict(by_target),
                "unmatched_items": unmatched,
                "loaded_relocation_candidates": loaded,
                "history_policy": "preview_only",
            })
        return {
            "version_id": version_id,
            "splits": results,
            "total_items": total_candidates,
            "loaded_relocation_candidates": total_loaded,
        }

    def health(self, version_id: str) -> dict:
        version = self.get_version(version_id)
        validation = self.validate_nodes(version["nodes"])
        node_ids = {node["node_id"] for node in version["nodes"]}
        children = Counter(
            node.get("parent_node_id") for node in version["nodes"]
            if node.get("parent_node_id")
        )
        depth_by_id: dict[str, int] = {}
        parent_by_id = {
            node["node_id"]: node.get("parent_node_id")
            for node in version["nodes"]
        }
        for node_id in node_ids:
            depth, cursor, seen = 1, parent_by_id.get(node_id), {node_id}
            while cursor and cursor not in seen:
                seen.add(cursor)
                depth += 1
                cursor = parent_by_id.get(cursor)
            depth_by_id[node_id] = depth
        split = self.split_impact(version_id)
        bound = sum(bool(node.get("binding")) for node in version["nodes"])
        owner = sum(bool(node.get("owner")) for node in version["nodes"])
        steward = sum(bool(node.get("steward")) for node in version["nodes"])
        empty = sum(
            int(node.get("item_count") or 0) == 0
            and children.get(node["node_id"], 0) == 0
            for node in version["nodes"]
        )
        return {
            "version_id": version_id,
            "status": version["status"],
            "validation": validation,
            "node_count": len(version["nodes"]),
            "max_depth": max(depth_by_id.values(), default=0),
            "binding_coverage": {
                "bound": bound, "total": len(version["nodes"]),
            },
            "governance_coverage": {
                "owner": owner, "steward": steward,
                "total": len(version["nodes"]),
            },
            "empty_leaf_nodes": empty,
            "split_impact": split,
            "pending_transformations": sum(
                transform["status"] == "pending"
                for transform in version.get("transformations") or []
            ),
            "approval": {
                "count": version["approval_count"],
                "required": int(version.get("required_approvals") or 1),
                "approvals": version["approvals"],
            },
        }

    def suggest_structure(self, version_id: str) -> dict:
        """基于分类分布、人工反馈和飞书现状生成只读建议。"""
        version = self.get_version(version_id)
        snapshot = self.latest_snapshot(version["mode"])
        known_names = {
            name.casefold()
            for node in version["nodes"]
            for name in [node["display_name"], *(node.get("aliases") or [])]
            if name
        }
        bound_tokens = {
            (node.get("binding") or {}).get("remote_token")
            for node in version["nodes"]
            if node.get("binding")
        }
        feedback = {
            row["confirmed_category"]: int(row["confirmed_count"])
            for row in self.ledger.classification_feedback_summary()
        }
        suggestions: list[dict] = []
        for category in self.taxonomy.categories:
            if category.path.casefold() not in known_names:
                suggestions.append({
                    "type": "ADD_CATEGORY",
                    "title": f"补充分类目录：{category.path}",
                    "reason": "taxonomy 中存在，但当前规划结构没有对应名称或别名",
                    "confidence": 1.0,
                    "proposal": {
                        "display_name": category.path,
                        "owner": category.owner,
                        "steward": category.steward,
                        "review_months": category.review_months,
                        "retention_years": category.retention_years,
                    },
                })
        for node in version["nodes"]:
            item_count = int(node.get("item_count") or 0)
            if item_count >= 100 and not any(
                child.get("parent_node_id") == node["node_id"]
                for child in version["nodes"]
            ):
                suggestions.append({
                    "type": "CONSIDER_SPLIT",
                    "title": f"考虑拆分：{node['display_name']}",
                    "reason": (
                        f"当前已有 {item_count} 份文件，可按项目、部门、年份或"
                        "文档类型建立人工确认的拆分规则"
                    ),
                    "confidence": min(0.95, 0.6 + item_count / 1000),
                    "proposal": {"node_id": node["node_id"]},
                })
            feedback_count = feedback.get(node["display_name"], 0)
            if feedback_count >= 10 and not node.get("assignment_rule"):
                suggestions.append({
                    "type": "ADD_ROUTING_RULE",
                    "title": f"沉淀路由规则：{node['display_name']}",
                    "reason": f"已有 {feedback_count} 条人工确认记录可作为规则样本",
                    "confidence": min(0.9, 0.5 + feedback_count / 100),
                    "proposal": {"node_id": node["node_id"]},
                })
        for remote in (snapshot or {}).get("nodes") or []:
            if (
                remote["remote_token"] in bound_tokens
                or (remote.get("decision") or {}).get("decision") == "external"
            ):
                continue
            exact = next((
                node for node in version["nodes"]
                if node["display_name"].casefold()
                == remote["display_name"].casefold()
                and not node.get("binding")
            ), None)
            suggestions.append({
                "type": "MAP_REMOTE" if exact else "REVIEW_REMOTE",
                "title": (
                    f"映射飞书现有目录：{remote['display_name']}"
                    if exact else f"处置飞书现有目录：{remote['display_name']}"
                ),
                "reason": (
                    "规划结构存在同名未绑定节点，建议人工确认 token 后映射"
                    if exact else
                    "该节点尚未纳入规划，可选择采纳、外部管理、待合并或待归档"
                ),
                "confidence": 0.9 if exact else 0.55,
                "proposal": {
                    "remote_token": remote["remote_token"],
                    "node_id": exact["node_id"] if exact else "",
                },
            })
        suggestions.sort(
            key=lambda item: (-float(item["confidence"]), item["title"])
        )
        return {
            "version_id": version_id,
            "generated_at": _now(),
            "suggestions": suggestions,
            "summary": dict(Counter(
                item["type"] for item in suggestions
            )),
            "notice": "建议仅供编辑参考，不会自动保存草稿或修改飞书",
        }

    # ── 历史文件重定位计划 ────────────────────────────────

    def create_item_relocation_plan(self, version_id: str, *,
                                    actor: str = "") -> dict:
        """为所有受结构变化影响的历史文件生成独立计划。

        计划覆盖改名、移动、合并和拆分。它只比较台账记录的历史绑定与当前
        激活结构，不调用飞书，也不会在结构发布阶段隐式移动任何内容。
        """
        version = self.get_version(version_id)
        if version["status"] != "active":
            raise StructureConflict("只能为当前已激活的结构版本生成历史重定位计划")
        node_by_id = {node["node_id"]: node for node in version["nodes"]}
        plan_id = str(uuid.uuid4())
        now = _now()
        candidates: dict[str, dict] = {}
        unresolved = 0
        rows = self.conn.execute(
            """SELECT * FROM items
               WHERE stage=? AND archived_at IS NULL
                 AND (feishu_token IS NOT NULL OR wiki_node_token IS NOT NULL)
               ORDER BY stable_key""",
            ("loaded",),
        ).fetchall()
        version_cache: dict[str, dict | None] = {version_id: version}
        for row in rows:
            item = dict(row)
            resolved = self.resolve_item_target(
                item, version_id, mode=version["mode"]
            )
            target_id = resolved.get("node_id") or ""
            target_token = resolved.get("remote_token") or ""
            object_token = (
                item.get("wiki_node_token")
                if version["mode"] == "wiki"
                else item.get("feishu_token")
            ) or ""
            assigned_version_id = str(item.get("structure_version_id") or "")
            current_id = str(item.get("target_node_id") or "")
            source_parent = ""
            if assigned_version_id and current_id:
                if assigned_version_id not in version_cache:
                    try:
                        version_cache[assigned_version_id] = self.get_version(
                            assigned_version_id
                        )
                    except KeyError:
                        version_cache[assigned_version_id] = None
                assigned_version = version_cache[assigned_version_id]
                current = next((
                    node for node in (assigned_version or {}).get("nodes", [])
                    if node["node_id"] == current_id
                ), None)
                source_parent = (
                    ((current or {}).get("binding") or {}).get("remote_token")
                    or ""
                )
            if not source_parent and current_id:
                current = node_by_id.get(current_id)
                source_parent = (
                    ((current or {}).get("binding") or {}).get("remote_token")
                    or ""
                )
            if not target_id or not target_token or not object_token:
                unresolved += 1
                continue
            if not source_parent:
                unresolved += 1
                continue
            if source_parent == target_token:
                continue
            candidates[item["stable_key"]] = {
                "stable_key": item["stable_key"],
                "source_node_id": current_id,
                "target_node_id": target_id,
                "source_parent_token": source_parent,
                "target_parent_token": target_token,
                "object_token": object_token,
                "object_type": (
                    "wiki" if version["mode"] == "wiki" else "file"
                ),
                "display_name": (
                    item.get("canonical_name")
                    or item.get("original_name")
                    or item["stable_key"]
                ),
                "status": "pending",
                "detail": {
                    "category": item.get("category") or "",
                    "content_sha256": item.get("content_sha256") or "",
                    "size": int(item.get("size") or 0),
                    "rule_target": resolved.get("display_name") or "",
                    "assignment_source": (
                        resolved.get("assignment_source") or "structure_change"
                    ),
                    "source_structure_version_id": assigned_version_id,
                    "relocation_reason": (
                        "split" if current_id != target_id
                        else "rename_or_move"
                    ),
                },
            }

        summary = {
            "total": len(candidates),
            "selected": len(candidates),
            "pending": sum(
                action["status"] == "pending"
                for action in candidates.values()
            ),
            "already_moved": sum(
                action["status"] == "already_moved"
                for action in candidates.values()
            ),
            "unresolved": unresolved,
            "conflicts": 0,
            "completed": 0,
        }
        self.conn.execute(
            """INSERT INTO item_relocation_plans
               (id,version_id,target_mode,status,revision,created_by,
                summary_json,created_at,updated_at)
               VALUES (?,?,?,'draft',1,?,?,?,?)""",
            (
                plan_id, version_id, version["mode"], actor,
                _json(summary), now, now,
            ),
        )
        for order, action in enumerate(candidates.values()):
            self.conn.execute(
                """INSERT INTO item_relocation_actions
                   (plan_id,action_order,stable_key,source_node_id,target_node_id,
                    source_parent_token,target_parent_token,object_token,
                    object_type,display_name,selected,status,detail_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?)""",
                (
                    plan_id, order, action["stable_key"],
                    action["source_node_id"], action["target_node_id"],
                    action["source_parent_token"], action["target_parent_token"],
                    action["object_token"], action["object_type"],
                    action["display_name"], action["status"],
                    _json(action["detail"]),
                ),
            )
        self._event(version_id, "item_relocation_plan_created", actor, {
            "plan_id": plan_id, **summary,
        }, commit=False)
        self.conn.commit()
        return self.get_item_relocation_plan(plan_id)

    def get_item_relocation_plan(self, plan_id: str) -> dict:
        row = self.conn.execute(
            "SELECT * FROM item_relocation_plans WHERE id=?", (plan_id,)
        ).fetchone()
        if not row:
            raise KeyError(plan_id)
        result = dict(row)
        result["summary"] = json.loads(result.pop("summary_json") or "{}")
        result["actions"] = []
        for action in self.conn.execute(
            "SELECT * FROM item_relocation_actions WHERE plan_id=? "
            "ORDER BY action_order", (plan_id,)
        ).fetchall():
            item = dict(action)
            item["selected"] = bool(item["selected"])
            item["detail"] = json.loads(item.pop("detail_json") or "{}")
            result["actions"].append(item)
        return result

    def latest_item_relocation_plan(self, version_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT id FROM item_relocation_plans WHERE version_id=? "
            "ORDER BY created_at DESC LIMIT 1", (version_id,)
        ).fetchone()
        return self.get_item_relocation_plan(row["id"]) if row else None

    def select_item_relocations(self, plan_id: str, revision: int,
                                stable_keys: list[str], *,
                                actor: str = "") -> dict:
        plan = self.get_item_relocation_plan(plan_id)
        if plan["status"] != "draft":
            raise StructureConflict("只有草稿重定位计划可以调整选择")
        if int(plan["revision"]) != int(revision):
            raise StructureConflict(
                f"重定位计划已更新：当前 revision={plan['revision']}"
            )
        available = {action["stable_key"] for action in plan["actions"]}
        selected = set(stable_keys)
        unknown = selected - available
        if unknown:
            raise ValueError(f"计划中不存在条目：{sorted(unknown)[0]}")
        self.conn.execute(
            """UPDATE item_relocation_actions
               SET selected=0,
                   status=CASE WHEN status IN ('completed','rolled_back')
                               THEN status ELSE 'excluded' END
               WHERE plan_id=?""",
            (plan_id,),
        )
        if selected:
            placeholders = ",".join("?" for _ in selected)
            self.conn.execute(
                f"""UPDATE item_relocation_actions
                    SET selected=1,
                        status=CASE WHEN status IN ('completed','rolled_back')
                                    THEN status ELSE 'pending' END
                    """
                f"WHERE plan_id=? AND stable_key IN ({placeholders})",
                (plan_id, *sorted(selected)),
            )
        summary = self._refresh_item_relocation_summary(plan_id)
        self.conn.execute(
            """UPDATE item_relocation_plans
               SET revision=revision+1,summary_json=?,updated_at=? WHERE id=?""",
            (_json(summary), _now(), plan_id),
        )
        self._event(plan["version_id"], "item_relocation_selection_updated",
                    actor, {"plan_id": plan_id, "selected": len(selected)},
                    commit=False)
        self.conn.commit()
        return self.get_item_relocation_plan(plan_id)

    def approve_item_relocation_plan(self, plan_id: str, *,
                                     actor: str = "") -> dict:
        plan = self.get_item_relocation_plan(plan_id)
        if plan["status"] != "draft":
            raise StructureConflict("只有草稿重定位计划可以审批")
        selected = [action for action in plan["actions"] if action["selected"]]
        if not selected:
            raise StructureConflict("请至少选择一条历史文件重定位动作")
        conflicts = [
            action for action in selected if action["status"] == "conflict"
        ]
        if conflicts:
            raise StructureConflict(
                f"仍有 {len(conflicts)} 条同名冲突，请取消选择或处理冲突后再审批"
            )
        actor = actor.strip() or "local-owner"
        now = _now()
        self.conn.execute(
            """UPDATE item_relocation_plans
               SET status='approved',approved_by=?,approved_at=?,updated_at=?
               WHERE id=?""",
            (actor, now, now, plan_id),
        )
        self._event(plan["version_id"], "item_relocation_plan_approved",
                    actor, {"plan_id": plan_id, "selected": len(selected)},
                    commit=False)
        self.conn.commit()
        return self.get_item_relocation_plan(plan_id)

    def set_item_relocation_plan_status(self, plan_id: str, status: str, *,
                                        error: str = "") -> dict:
        allowed = {
            "draft", "approved", "ready", "running",
            "completed", "failed", "rolled_back",
        }
        if status not in allowed:
            raise ValueError(f"非法重定位计划状态：{status}")
        plan = self.get_item_relocation_plan(plan_id)
        summary = self._refresh_item_relocation_summary(plan_id)
        if error:
            summary["last_error"] = error
        self.conn.execute(
            """UPDATE item_relocation_plans
               SET status=?,summary_json=?,updated_at=? WHERE id=?""",
            (status, _json(summary), _now(), plan_id),
        )
        self.conn.commit()
        return self.get_item_relocation_plan(plan_id)

    def update_item_relocation_action(self, plan_id: str, stable_key: str,
                                      status: str, *, error: str = "",
                                      detail: dict | None = None) -> None:
        row = self.conn.execute(
            "SELECT detail_json FROM item_relocation_actions "
            "WHERE plan_id=? AND stable_key=?", (plan_id, stable_key)
        ).fetchone()
        if not row:
            raise KeyError(stable_key)
        previous = json.loads(row["detail_json"] or "{}")
        previous.update(detail or {})
        self.conn.execute(
            """UPDATE item_relocation_actions
               SET status=?,error_detail=?,detail_json=?,executed_at=?
               WHERE plan_id=? AND stable_key=?""",
            (
                status, error or None, _json(previous),
                _now() if status in ("completed", "rolled_back") else None,
                plan_id, stable_key,
            ),
        )
        self.conn.commit()

    def _refresh_item_relocation_summary(self, plan_id: str) -> dict:
        rows = self.conn.execute(
            "SELECT selected,status,COUNT(*) c FROM item_relocation_actions "
            "WHERE plan_id=? GROUP BY selected,status", (plan_id,)
        ).fetchall()
        summary = {
            "total": 0, "selected": 0, "pending": 0, "ready": 0,
            "conflicts": 0, "completed": 0, "failed": 0,
            "already_moved": 0, "excluded": 0,
        }
        for row in rows:
            count = int(row["c"])
            summary["total"] += count
            if row["selected"]:
                summary["selected"] += count
            else:
                summary["excluded"] += count
            key = str(row["status"])
            summary_key = "conflicts" if key == "conflict" else key
            if summary_key in summary:
                summary[summary_key] += count
        return summary

    def bind_node(self, version_id: str, node_id: str, target_mode: str,
                  remote_token: str, *, parent_remote_token: str = "",
                  status: str = "bound", actor: str = "",
                  commit: bool = True) -> None:
        if not any(
            n["node_id"] == node_id for n in self.get_version(version_id)["nodes"]
        ):
            raise KeyError(node_id)
        now = _now()
        self.conn.execute(
            """INSERT INTO structure_bindings
               (version_id,node_id,target_mode,remote_token,parent_remote_token,
                binding_status,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(version_id,node_id,target_mode) DO UPDATE SET
                 remote_token=excluded.remote_token,
                 parent_remote_token=excluded.parent_remote_token,
                 binding_status=excluded.binding_status,
                 updated_at=excluded.updated_at""",
            (version_id, node_id, target_mode, remote_token,
             parent_remote_token, status, now, now),
        )
        self._event(version_id, "node_bound", actor, {
            "node_id": node_id, "mode": target_mode, "remote_token": remote_token,
        }, commit=False)
        if commit:
            self.conn.commit()

    def map_remote_node(self, version_id: str, revision: int, node_id: str,
                        remote_token: str, *, actor: str = "") -> dict:
        version = self.get_version(version_id)
        if version["status"] != "draft":
            raise StructureConflict("只有草稿版本可以调整远程映射")
        if int(version["revision"]) != int(revision):
            raise StructureConflict(
                f"结构已被更新：当前 revision={version['revision']}"
            )
        node = next(
            (item for item in version["nodes"] if item["node_id"] == node_id),
            None,
        )
        if not node:
            raise KeyError(node_id)
        snapshot = self.latest_snapshot(version["mode"])
        remote = next((
            item for item in (snapshot or {}).get("nodes", [])
            if item["remote_token"] == remote_token
        ), None)
        if not remote:
            raise KeyError(remote_token)
        duplicate = self.conn.execute(
            """SELECT node_id FROM structure_bindings
               WHERE version_id=? AND target_mode=? AND remote_token=?
                 AND node_id<>?""",
            (version_id, version["mode"], remote_token, node_id),
        ).fetchone()
        if duplicate:
            raise StructureConflict("该飞书节点已映射到其他规划目录")
        self.bind_node(
            version_id, node_id, version["mode"], remote_token,
            parent_remote_token=remote.get("parent_token") or "",
            actor=actor, commit=False,
        )
        now = _now()
        self.conn.execute(
            "UPDATE structure_versions SET revision=revision+1,updated_at=? "
            "WHERE id=?",
            (now, version_id),
        )
        self._cancel_open_change_plans(version_id, now)
        self.set_remote_decision(
            version["mode"], remote_token, "mapped",
            planned_node_id=node_id, actor=actor, commit=False,
        )
        self._event(version_id, "remote_node_mapped", actor, {
            "node_id": node_id, "remote_token": remote_token,
        }, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def adopt_remote_node(self, version_id: str, revision: int,
                          remote_token: str, *,
                          parent_node_id: str = "",
                          actor: str = "") -> dict:
        version = self.get_version(version_id)
        if version["status"] != "draft":
            raise StructureConflict("只有草稿版本可以采纳飞书节点")
        if int(version["revision"]) != int(revision):
            raise StructureConflict(
                f"结构已被更新：当前 revision={version['revision']}"
            )
        snapshot = self.latest_snapshot(version["mode"])
        remote = next((
            item for item in (snapshot or {}).get("nodes", [])
            if item["remote_token"] == remote_token
        ), None)
        if not remote:
            raise KeyError(remote_token)
        if any(
            (node.get("binding") or {}).get("remote_token") == remote_token
            for node in version["nodes"]
        ):
            raise StructureConflict("该飞书节点已被规划结构采纳")
        if parent_node_id and not any(
            node["node_id"] == parent_node_id for node in version["nodes"]
        ):
            raise KeyError(parent_node_id)
        node_id = str(uuid.uuid4())
        nodes = [dict(node) for node in version["nodes"]]
        nodes.append({
            "node_id": node_id,
            "parent_node_id": parent_node_id or None,
            "semantic_key": f"remote.{version['mode']}.{node_id}",
            "display_name": remote["display_name"],
            "sort_order": len(nodes),
            "node_kind": "category",
            "owner": "", "steward": "",
            "review_months": 12, "retention_years": 5,
            "status": "active", "aliases": [],
            "assignment_rule": {},
        })
        saved = self.save_draft(
            version_id, revision, nodes, actor=actor
        )
        self.bind_node(
            version_id, node_id, version["mode"], remote_token,
            parent_remote_token=remote.get("parent_token") or "",
            actor=actor, commit=False,
        )
        self.set_remote_decision(
            version["mode"], remote_token, "adopted",
            planned_node_id=node_id, actor=actor, commit=False,
        )
        self._event(version_id, "remote_node_adopted", actor, {
            "node_id": node_id, "remote_token": remote_token,
        }, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def set_remote_decision(self, mode: str, remote_token: str,
                            decision: str, *, planned_node_id: str = "",
                            actor: str = "", note: str = "",
                            commit: bool = True) -> dict:
        allowed = {
            "managed", "mapped", "adopted", "external",
            "merge_pending", "archive_pending",
        }
        if mode not in ("drive", "wiki"):
            raise ValueError(f"不支持的目标形态：{mode}")
        if decision not in allowed:
            raise ValueError(f"非法远程节点决策：{decision}")
        snapshot = self.latest_snapshot(mode)
        if not snapshot or not any(
            node["remote_token"] == remote_token
            for node in snapshot.get("nodes") or []
        ):
            raise KeyError(remote_token)
        now = _now()
        self.conn.execute(
            """INSERT INTO remote_node_decisions
               (target_mode,remote_token,decision,planned_node_id,actor,note,
                updated_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(target_mode,remote_token) DO UPDATE SET
                 decision=excluded.decision,
                 planned_node_id=excluded.planned_node_id,
                 actor=excluded.actor,note=excluded.note,
                 updated_at=excluded.updated_at""",
            (
                mode, remote_token, decision, planned_node_id or None,
                actor, note, now,
            ),
        )
        if commit:
            self.conn.commit()
        return {
            "target_mode": mode, "remote_token": remote_token,
            "decision": decision,
            "planned_node_id": planned_node_id or None,
            "actor": actor, "note": note, "updated_at": now,
        }

    def _cancel_open_change_plans(self, version_id: str, now: str = "") -> None:
        now = now or _now()
        self.conn.execute(
            """UPDATE structure_change_plans
               SET status='cancelled',updated_at=?,
                   error_detail='draft_changed'
               WHERE version_id=? AND status IN ('preview','approved')""",
            (now, version_id),
        )
        self.conn.execute(
            """UPDATE structure_change_actions
               SET status='cancelled',executed_at=?
               WHERE plan_id IN (
                   SELECT id FROM structure_change_plans
                   WHERE version_id=? AND status='cancelled'
                     AND error_detail='draft_changed'
               ) AND status='pending'""",
            (now, version_id),
        )

    def begin_relocation(self, version_id: str, operation_type: str,
                         source_token: str, target_token: str, *,
                         node_id: str = "", detail: dict | None = None) -> dict:
        row = self.conn.execute(
            """SELECT * FROM structure_relocations
               WHERE version_id=? AND operation_type=? AND source_token=?
                 AND target_token=?""",
            (version_id, operation_type, source_token, target_token),
        ).fetchone()
        if row:
            return dict(row)
        relocation_id = str(uuid.uuid4())
        now = _now()
        self.conn.execute(
            """INSERT INTO structure_relocations
               (id,version_id,operation_type,node_id,source_token,target_token,
                status,detail_json,created_at,updated_at)
               VALUES (?,?,?,?,?,?,'moving',?,?,?)""",
            (relocation_id, version_id, operation_type, node_id,
             source_token, target_token, _json(detail or {}), now, now),
        )
        self.conn.commit()
        return dict(self.conn.execute(
            "SELECT * FROM structure_relocations WHERE id=?", (relocation_id,)
        ).fetchone())

    def find_relocation(self, version_id: str, operation_type: str,
                        source_token: str, *, node_id: str = "") -> dict | None:
        sql = (
            "SELECT * FROM structure_relocations WHERE version_id=? "
            "AND operation_type=? AND source_token=?"
        )
        args: list[Any] = [version_id, operation_type, source_token]
        if node_id:
            sql += " AND node_id=?"
            args.append(node_id)
        sql += " ORDER BY created_at DESC LIMIT 1"
        row = self.conn.execute(sql, args).fetchone()
        return dict(row) if row else None

    def complete_relocation(self, relocation_id: str,
                            *, detail: dict | None = None) -> None:
        row = self.conn.execute(
            "SELECT detail_json FROM structure_relocations WHERE id=?",
            (relocation_id,),
        ).fetchone()
        previous = json.loads(row["detail_json"] or "{}") if row else {}
        previous.update(detail or {})
        self.conn.execute(
            """UPDATE structure_relocations
               SET status='completed',detail_json=?,updated_at=? WHERE id=?""",
            (_json(previous), _now(), relocation_id),
        )
        self.conn.commit()

    def complete_transformation(self, transformation_id: str,
                                *, error: str = "") -> None:
        status = "failed" if error else "completed"
        self.conn.execute(
            """UPDATE structure_transformations
               SET status=?,error_detail=?,updated_at=? WHERE id=?""",
            (status, error or None, _now(), transformation_id),
        )
        self.conn.commit()

    def remap_merged_items(self, version_id: str, source_node_ids: list[str],
                           target_node_id: str,
                           *, target_remote_token: str = "",
                           source_categories: list[str] | None = None) -> int:
        source_categories = source_categories or []
        if not source_node_ids and not source_categories:
            return 0
        predicates = []
        args: list[str] = []
        if source_node_ids:
            placeholders = ",".join("?" for _ in source_node_ids)
            predicates.append(f"target_node_id IN ({placeholders})")
            args.extend(source_node_ids)
        if source_categories:
            placeholders = ",".join("?" for _ in source_categories)
            predicates.append(f"category IN ({placeholders})")
            args.extend(source_categories)
        rows = self.conn.execute(
            f"SELECT stable_key,confidence,feishu_token,wiki_node_token FROM items "
            f"WHERE {' OR '.join(predicates)}",
            tuple(args),
        ).fetchall()
        for row in rows:
            self.ledger.assign_structure_target(
                row["stable_key"], version_id, target_node_id,
                source="merge_relocation", confidence=row["confidence"],
            )
            source_token = (
                row["wiki_node_token"] or row["feishu_token"]
                or f"item:{row['stable_key']}"
            )
            relocation = self.begin_relocation(
                version_id, "item_assignment_remap",
                source_token, target_remote_token or target_node_id,
                node_id=target_node_id,
                detail={"stable_key": row["stable_key"]},
            )
            self.complete_relocation(
                relocation["id"], detail={"assignment_updated": True}
            )
        return len(rows)

    def set_status(self, version_id: str, status: str, *,
                   actor: str = "", detail: dict | None = None) -> dict:
        allowed = {
            "draft", "reviewing", "approved", "applying",
            "active", "superseded", "failed",
        }
        if status not in allowed:
            raise ValueError(f"非法结构状态：{status}")
        if not self.conn.execute(
            "SELECT 1 FROM structure_versions WHERE id=?", (version_id,)
        ).fetchone():
            raise KeyError(version_id)
        self.conn.execute(
            "UPDATE structure_versions SET status=?,updated_at=? WHERE id=?",
            (status, _now(), version_id),
        )
        self._event(version_id, f"status_{status}", actor, detail or {}, commit=False)
        self.conn.commit()
        return self.get_version(version_id)

    def activate(self, version_id: str, *, root_token: str = "",
                 space_id: str = "", actor: str = "") -> dict:
        version = self.get_version(version_id)
        if version["status"] not in ("approved", "applying"):
            raise StructureConflict("只有已确认或执行中的版本可以激活")
        unbound = [n["display_name"] for n in version["nodes"] if not n.get("binding")]
        if unbound:
            raise StructureConflict(f"仍有未绑定目标节点：{', '.join(unbound[:5])}")
        now = _now()
        self.conn.execute(
            "UPDATE structure_versions SET status='superseded',updated_at=? "
            "WHERE status='active' AND id<>?",
            (now, version_id),
        )
        self.conn.execute(
            "UPDATE structure_versions SET status='active',updated_at=? WHERE id=?",
            (now, version_id),
        )
        self._event(version_id, "activated", actor, {
            "root_token": root_token, "space_id": space_id,
        }, commit=False)
        self.conn.commit()
        active = self.get_version(version_id)
        self.export_legacy_targets(active, root_token=root_token, space_id=space_id)
        return active

    def export_legacy_targets(self, version: dict, *, root_token: str = "",
                              space_id: str = "") -> dict:
        """继续产出旧编排器可读取的路径映射，稳定节点映射仍以 SQLite 为准。"""
        existing = self._legacy_targets()
        result = {
            "mode": version["mode"],
            "root_token": root_token or existing.get("root_token") or "",
            "space_id": space_id or existing.get("space_id") or "",
            "folder_map": {},
            "wiki_node_map": {},
            "structure_version_id": version["id"],
        }
        target = (
            result["folder_map"] if version["mode"] == "drive"
            else result["wiki_node_map"]
        )
        for node in version["nodes"]:
            binding = node.get("binding") or {}
            token = binding.get("remote_token")
            if not token:
                continue
            for key in [node["display_name"], *(node.get("aliases") or [])]:
                target[key] = token
        os.makedirs(os.path.dirname(self.targets_file) or ".", exist_ok=True)
        with open(self.targets_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return result

    # ── 远程快照与差异 ──────────────────────────────────

    def save_remote_snapshot(self, mode: str, nodes: list[dict], *,
                             root_token: str = "", space_id: str = "",
                             status: str = "ready", error_detail: str = "") -> dict:
        snapshot_id = str(uuid.uuid4())
        normalized = []
        for order, node in enumerate(nodes):
            token = str(node.get("remote_token") or node.get("token") or "").strip()
            if not token:
                continue
            normalized.append({
                "remote_token": token,
                "parent_token": str(node.get("parent_token") or ""),
                "display_name": str(
                    node.get("display_name") or node.get("name")
                    or node.get("title") or token
                ),
                "node_type": str(node.get("node_type") or "folder"),
                "has_children": bool(node.get("has_children") or node.get("has_child")),
                "file_count": int(node.get("file_count") or 0),
                "remote_updated_at": str(
                    node.get("remote_updated_at")
                    or (node.get("raw") or {}).get("updated_at")
                    or ""
                ),
                "sort_order": int(node.get("sort_order", order)),
                "raw": node.get("raw") or {},
            })
        tree_hash = hashlib.sha256(_json(normalized).encode("utf-8")).hexdigest()
        self.conn.execute(
            """INSERT INTO remote_structure_snapshots
               (id,target_mode,root_token,space_id,status,tree_hash,node_count,
                error_detail,created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (snapshot_id, mode, root_token, space_id, status, tree_hash,
             len(normalized), error_detail, _now()),
        )
        for node in normalized:
            self.conn.execute(
                """INSERT INTO remote_structure_nodes
                   (snapshot_id,remote_token,parent_token,display_name,node_type,
                    has_children,file_count,remote_updated_at,sort_order,raw_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (snapshot_id, node["remote_token"], node["parent_token"],
                 node["display_name"], node["node_type"],
                 int(node["has_children"]), node["file_count"],
                 node["remote_updated_at"], node["sort_order"],
                 _json(node["raw"])),
            )
        self.conn.commit()
        return self.get_snapshot(snapshot_id)

    def latest_snapshot(self, mode: str = "") -> dict | None:
        sql = "SELECT id FROM remote_structure_snapshots"
        args: tuple = ()
        if mode:
            sql += " WHERE target_mode=?"
            args = (mode,)
        sql += " ORDER BY created_at DESC LIMIT 1"
        row = self.conn.execute(sql, args).fetchone()
        return self.get_snapshot(row["id"]) if row else None

    def audit(self, version_id: str) -> dict:
        self.get_version(version_id)  # 明确校验版本存在
        events = []
        for row in self.conn.execute(
            "SELECT * FROM structure_events WHERE version_id=? ORDER BY created_at",
            (version_id,),
        ).fetchall():
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json") or "{}")
            events.append(item)
        relocations = []
        for row in self.conn.execute(
            "SELECT * FROM structure_relocations WHERE version_id=? "
            "ORDER BY created_at", (version_id,)
        ).fetchall():
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json") or "{}")
            relocations.append(item)
        item_plans = [
            self.get_item_relocation_plan(row["id"])
            for row in self.conn.execute(
                "SELECT id FROM item_relocation_plans WHERE version_id=? "
                "ORDER BY created_at", (version_id,)
            ).fetchall()
        ]
        return {
            "version_id": version_id,
            "events": events,
            "relocations": relocations,
            "transformations": self.get_version(version_id)["transformations"],
            "item_relocation_plans": item_plans,
        }

    def get_snapshot(self, snapshot_id: str) -> dict:
        row = self.conn.execute(
            "SELECT * FROM remote_structure_snapshots WHERE id=?", (snapshot_id,)
        ).fetchone()
        if not row:
            raise KeyError(snapshot_id)
        result = dict(row)
        result["nodes"] = [
            {**dict(node), "raw": json.loads(node["raw_json"] or "{}")}
            for node in self.conn.execute(
                "SELECT * FROM remote_structure_nodes WHERE snapshot_id=? "
                "ORDER BY sort_order,display_name", (snapshot_id,)
            ).fetchall()
        ]
        for node in result["nodes"]:
            node.pop("raw_json", None)
        decisions = {
            row["remote_token"]: dict(row)
            for row in self.conn.execute(
                "SELECT * FROM remote_node_decisions WHERE target_mode=?",
                (result["target_mode"],),
            ).fetchall()
        }
        managed = {}
        for binding in self.conn.execute(
            """SELECT b.remote_token,b.node_id,b.version_id,v.status,v.updated_at
               FROM structure_bindings b
               JOIN structure_versions v ON v.id=b.version_id
               WHERE b.target_mode=?
               ORDER BY CASE v.status
                          WHEN 'draft' THEN 0 WHEN 'reviewing' THEN 1
                          WHEN 'approved' THEN 2 WHEN 'active' THEN 3
                          ELSE 4 END,
                        v.updated_at DESC""",
            (result["target_mode"],),
        ).fetchall():
            managed.setdefault(binding["remote_token"], dict(binding))
        for node in result["nodes"]:
            decision = decisions.get(node["remote_token"])
            binding = managed.get(node["remote_token"])
            if binding:
                decision = {
                    "target_mode": result["target_mode"],
                    "remote_token": node["remote_token"],
                    "decision": (
                        (decision or {}).get("decision")
                        if (decision or {}).get("decision")
                        in ("mapped", "adopted")
                        else "managed"
                    ),
                    "planned_node_id": binding["node_id"],
                    "version_id": binding["version_id"],
                    "updated_at": binding["updated_at"],
                }
            node["decision"] = decision
        return result

    def get_change_plan(self, plan_id: str) -> dict:
        row = self.conn.execute(
            "SELECT * FROM structure_change_plans WHERE id=?", (plan_id,)
        ).fetchone()
        if not row:
            raise KeyError(plan_id)
        result = dict(row)
        result["summary"] = json.loads(result.pop("summary_json") or "{}")
        result["actions"] = []
        for action in self.conn.execute(
            "SELECT * FROM structure_change_actions WHERE plan_id=? "
            "ORDER BY action_order", (plan_id,)
        ).fetchall():
            item = dict(action)
            item["before"] = json.loads(item.pop("before_json") or "{}")
            item["after"] = json.loads(item.pop("after_json") or "{}")
            item["depends_on"] = json.loads(
                item.pop("depends_on_json") or "[]"
            )
            item["rollbackable"] = bool(item["rollbackable"])
            result["actions"].append(item)
        return result

    def latest_change_plan(self, version_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT id FROM structure_change_plans WHERE version_id=? "
            "ORDER BY created_at DESC LIMIT 1", (version_id,)
        ).fetchone()
        return self.get_change_plan(row["id"]) if row else None

    def update_change_plan(self, plan_id: str, revision: int, *,
                           history_scope: str, actor: str = "") -> dict:
        plan = self.get_change_plan(plan_id)
        if plan["status"] != "preview":
            raise StructureConflict("只有预览中的结构计划可以调整")
        if int(plan["revision"]) != int(revision):
            raise StructureConflict(
                f"结构计划已被更新：当前 revision={plan['revision']}"
            )
        allowed = {"unmigrated_only", "include_retries", "relocate_history"}
        if history_scope not in allowed:
            raise ValueError(f"非法历史作用范围：{history_scope}")
        now = _now()
        self.conn.execute(
            """UPDATE structure_change_plans
               SET revision=revision+1,history_scope=?,updated_at=?
               WHERE id=?""",
            (history_scope, now, plan_id),
        )
        self._event(plan["version_id"], "change_plan_updated", actor, {
            "plan_id": plan_id, "history_scope": history_scope,
            "revision": revision + 1,
        }, commit=False)
        self.conn.commit()
        return self.get_change_plan(plan_id)

    def approve_change_plan(self, plan_id: str, *, actor: str = "") -> dict:
        plan = self.get_change_plan(plan_id)
        version = self.get_version(plan["version_id"])
        if plan["status"] != "preview":
            raise StructureConflict("只有预览中的结构计划可以审批")
        if version["status"] != "approved":
            raise StructureConflict("请先完成结构版本审批")
        latest_snapshot = self.latest_snapshot(version["mode"])
        latest_snapshot_id = (
            latest_snapshot["id"] if latest_snapshot else None
        )
        if plan.get("remote_snapshot_id") != latest_snapshot_id:
            raise StructureConflict(
                "飞书目录快照已变化，请重新生成并检查差异计划"
            )
        if plan["summary"].get("blocking_conflicts"):
            raise StructureConflict("结构计划仍有阻断冲突")
        actor = actor.strip() or "local-owner"
        now = _now()
        self.conn.execute(
            """UPDATE structure_change_plans
               SET status='approved',approved_by=?,approved_at=?,updated_at=?
               WHERE id=?""",
            (actor, now, now, plan_id),
        )
        self._event(plan["version_id"], "change_plan_approved", actor, {
            "plan_id": plan_id,
            "history_scope": plan["history_scope"],
        }, commit=False)
        self.conn.commit()
        return self.get_change_plan(plan_id)

    def cancel_change_plan(self, plan_id: str, *, actor: str = "") -> dict:
        plan = self.get_change_plan(plan_id)
        if plan["status"] not in ("preview", "approved", "failed"):
            raise StructureConflict("执行中的或已完成的结构计划不能取消")
        now = _now()
        self.conn.execute(
            """UPDATE structure_change_plans
               SET status='cancelled',updated_at=? WHERE id=?""",
            (now, plan_id),
        )
        self.conn.execute(
            """UPDATE structure_change_actions
               SET status='cancelled',executed_at=?
               WHERE plan_id=? AND status IN ('pending','ready')""",
            (now, plan_id),
        )
        self._event(plan["version_id"], "change_plan_cancelled", actor, {
            "plan_id": plan_id,
        }, commit=False)
        self.conn.commit()
        return self.get_change_plan(plan_id)

    def set_change_plan_status(self, plan_id: str, status: str, *,
                               error: str = "") -> dict:
        allowed = {
            "preview", "approved", "applying", "completed",
            "failed", "cancelled",
        }
        if status not in allowed:
            raise ValueError(f"非法结构计划状态：{status}")
        plan = self.get_change_plan(plan_id)
        now = _now()
        self.conn.execute(
            """UPDATE structure_change_plans
               SET status=?,updated_at=?,applied_at=CASE WHEN ?='completed'
                    THEN ? ELSE applied_at END,error_detail=?
               WHERE id=?""",
            (status, now, status, now, error or None, plan_id),
        )
        self._event(plan["version_id"], f"change_plan_{status}", "", {
            "plan_id": plan_id, "error": error,
        }, commit=False)
        self.conn.commit()
        return self.get_change_plan(plan_id)

    def update_change_action(self, plan_id: str, action_order: int,
                             status: str, *, error: str = "") -> None:
        now = _now()
        self.conn.execute(
            """UPDATE structure_change_actions
               SET status=?,error_detail=?,
                   started_at=CASE WHEN ?='running' THEN ? ELSE started_at END,
                   executed_at=CASE WHEN ? IN ('completed','failed','skipped')
                                    THEN ? ELSE executed_at END
               WHERE plan_id=? AND action_order=?""",
            (
                status, error or None, status, now, status, now,
                plan_id, int(action_order),
            ),
        )
        self.conn.commit()

    def create_diff_plan(self, version_id: str, snapshot_id: str = "",
                         *, actor: str = "",
                         history_scope: str = "unmigrated_only") -> dict:
        if history_scope not in {
            "unmigrated_only", "include_retries", "relocate_history",
        }:
            raise ValueError(f"非法历史作用范围：{history_scope}")
        version = self.get_version(version_id)
        snapshot = (
            self.get_snapshot(snapshot_id) if snapshot_id
            else self.latest_snapshot(version["mode"])
        )
        remote_nodes = snapshot["nodes"] if snapshot else []
        remote_by_token = {n["remote_token"]: n for n in remote_nodes}
        remote_by_parent_name: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for node in remote_nodes:
            remote_by_parent_name[
                (node["parent_token"] or "", node["display_name"].casefold())
            ].append(node)

        binding_by_node = {
            n["node_id"]: n.get("binding") for n in version["nodes"]
            if n.get("binding")
        }
        root_parent = ""
        if snapshot and version["mode"] == "drive":
            root_parent = snapshot.get("root_token") or ""
        actions: list[dict] = []
        consumed_remote: set[str] = set()
        node_by_id = {n["node_id"]: n for n in version["nodes"]}

        def planned_path(node: dict) -> str:
            parts = [node["display_name"]]
            parent_id = node.get("parent_node_id")
            seen = {node["node_id"]}
            while parent_id and parent_id not in seen:
                seen.add(parent_id)
                parent = node_by_id.get(parent_id)
                if not parent:
                    break
                parts.append(parent["display_name"])
                parent_id = parent.get("parent_node_id")
            parts.append(version["root_name"])
            return "/".join(reversed(parts))

        def remote_path(remote: dict | None) -> str:
            if not remote:
                return ""
            parts = [str(remote.get("display_name") or "")]
            parent_token = str(remote.get("parent_token") or "")
            seen = {str(remote.get("remote_token") or "")}
            while parent_token and parent_token not in seen:
                seen.add(parent_token)
                parent = remote_by_token.get(parent_token)
                if not parent:
                    break
                parts.append(parent["display_name"])
                parent_token = str(parent.get("parent_token") or "")
            return "/".join(reversed([part for part in parts if part]))

        def add(action_type: str, node: dict, *, remote: dict | None = None,
                before: dict | None = None, after: dict | None = None):
            before_data = before or remote or {}
            after_data = after or {
                "display_name": node["display_name"],
                "parent_node_id": node.get("parent_node_id"),
            }
            actions.append({
                "action_type": action_type,
                "node_id": node["node_id"],
                "display_name": node["display_name"],
                "remote_token": (remote or {}).get("remote_token", ""),
                "source_parent_token": (remote or {}).get("parent_token", ""),
                "target_parent_token": target_parent(node),
                "before": {
                    **before_data,
                    "path": before_data.get("path") or remote_path(remote),
                },
                "after": {
                    **after_data,
                    "path": after_data.get("path") or planned_path(node),
                    "governance": {
                        "owner": node.get("owner") or "",
                        "steward": node.get("steward") or "",
                        "review_months": node.get("review_months"),
                        "retention_years": node.get("retention_years"),
                    },
                    "permission_impact": (
                        "no_change" if action_type == "NOOP"
                        else "requires_remote_preflight"
                    ),
                },
                "item_count": int(node.get("item_count", 0)),
            })

        def target_parent(node: dict) -> str:
            parent_id = node.get("parent_node_id")
            if not parent_id:
                return root_parent
            parent_binding = binding_by_node.get(parent_id) or {}
            return parent_binding.get("remote_token") or f"planned:{parent_id}"

        for node in version["nodes"]:
            binding = binding_by_node.get(node["node_id"])
            if binding:
                remote = remote_by_token.get(binding["remote_token"])
                if not remote:
                    add("CONFLICT", node, before={
                        "reason": "绑定的飞书节点已不存在",
                        "remote_token": binding["remote_token"],
                    })
                    continue
                desired_parent = target_parent(node)
                if (
                    version["mode"] == "wiki"
                    and remote.get("node_type") == "file"
                    and not remote.get("has_children")
                ):
                    # 旧版自动映射可能把规划目录错误绑定到普通内容文件。
                    # 若目标层级有唯一同名结构节点，将其作为显式 MAP 修复动作，
                    # 由用户审批后再替换绑定，而不是制造无法处理的重命名冲突。
                    replacements = [
                        candidate
                        for candidate in remote_by_parent_name.get(
                            (desired_parent, node["display_name"].casefold()), []
                        )
                        if candidate["remote_token"] != remote["remote_token"]
                        and candidate.get("node_type") != "file"
                    ]
                    if len(replacements) == 1:
                        replacement = replacements[0]
                        consumed_remote.add(replacement["remote_token"])
                        add("MAP", node, remote=replacement, before={
                            **replacement,
                            "reason": "当前绑定指向普通内容文件，将改为同名结构节点",
                            "previous_remote_token": remote["remote_token"],
                            "previous_remote_name": remote["display_name"],
                        })
                    else:
                        add("CONFLICT", node, remote=remote, before={
                            **remote,
                            "reason": (
                                "当前绑定指向普通内容文件，且未找到唯一同名结构节点"
                            ),
                            "candidates": [
                                candidate["remote_token"]
                                for candidate in replacements
                            ],
                        })
                    continue
                consumed_remote.add(remote["remote_token"])
                changed = False
                if remote["display_name"] != node["display_name"]:
                    conflicts = [
                        candidate for candidate in remote_by_parent_name.get(
                            (desired_parent, node["display_name"].casefold()), []
                        )
                        if candidate["remote_token"] != remote["remote_token"]
                    ]
                    if conflicts:
                        add("CONFLICT", node, remote=remote, before={
                            "reason": "目标层级已存在重命名后的同名节点",
                            "candidates": [
                                candidate["remote_token"] for candidate in conflicts
                            ],
                        })
                    else:
                        add("RENAME", node, remote=remote)
                    changed = True
                desired_parent = target_parent(node)
                if desired_parent and remote["parent_token"] != desired_parent:
                    add("MOVE", node, remote=remote)
                    changed = True
                if not changed:
                    add("NOOP", node, remote=remote)
                continue

            candidates = remote_by_parent_name.get(
                (target_parent(node), node["display_name"].casefold()), []
            )
            if len(candidates) == 1:
                consumed_remote.add(candidates[0]["remote_token"])
                add("MAP", node, remote=candidates[0])
            elif len(candidates) > 1:
                add("CONFLICT", node, before={
                    "reason": "同一层级存在多个同名飞书节点",
                    "candidates": [c["remote_token"] for c in candidates],
                })
            else:
                add("CREATE", node)

        for transform in version.get("transformations") or []:
            if transform["transformation_type"] == "split":
                if transform["status"] == "completed":
                    continue
                source = node_by_id.get(transform["target_node_id"])
                if not source:
                    continue
                impact = next((
                    entry for entry in self.split_impact(version_id)["splits"]
                    if entry["transformation_id"] == transform["id"]
                ), {})
                actions.append({
                    "action_type": "SPLIT_RULE",
                    "node_id": source["node_id"],
                    "display_name": source["display_name"],
                    "remote_token": (
                        (source.get("binding") or {}).get("remote_token") or ""
                    ),
                    "source_parent_token": "",
                    "target_parent_token": "",
                    "before": {
                        "transformation_id": transform["id"],
                        "history_policy": "preview_only",
                    },
                    "after": {
                        "matched_by_target": impact.get("matched_by_target", {}),
                        "unmatched_items": impact.get("unmatched_items", 0),
                        "loaded_relocation_candidates": impact.get(
                            "loaded_relocation_candidates", 0
                        ),
                    },
                    "item_count": int(impact.get("total_items", 0)),
                })
                continue
            if transform["transformation_type"] != "merge":
                continue
            if transform["status"] == "completed":
                continue
            target = node_by_id.get(transform["target_node_id"])
            if not target:
                continue
            target_binding = binding_by_node.get(target["node_id"]) or {}
            source_details = {
                item["node_id"]: item
                for item in (transform.get("rule") or {}).get("sources", [])
            }
            for source_id in transform["source_node_ids"]:
                source = source_details.get(source_id) or {}
                source_binding = source.get("binding") or {}
                source_token = source_binding.get("remote_token") or ""
                if not source_token:
                    continue  # 从未发布过的规划节点只做逻辑合并
                remote = remote_by_token.get(source_token)
                if not remote:
                    actions.append({
                        "action_type": "CONFLICT", "node_id": target["node_id"],
                        "display_name": source.get("display_name") or source_id,
                        "remote_token": source_token,
                        "source_parent_token": "",
                        "target_parent_token": target_binding.get("remote_token", ""),
                        "before": {
                            "reason": "待合并的飞书来源目录不存在",
                            "transformation_id": transform["id"],
                        },
                        "after": {}, "item_count": int(source.get("item_count", 0)),
                    })
                    continue
                consumed_remote.add(source_token)
                if not target_binding.get("remote_token"):
                    actions.append({
                        "action_type": "CONFLICT", "node_id": target["node_id"],
                        "display_name": source.get("display_name") or source_id,
                        "remote_token": source_token,
                        "source_parent_token": remote["parent_token"],
                        "target_parent_token": "",
                        "before": {
                            "reason": "合并目标尚未绑定，请先发布目标目录",
                            "transformation_id": transform["id"],
                        },
                        "after": {}, "item_count": int(source.get("item_count", 0)),
                    })
                    continue
                actions.append({
                    "action_type": "MERGE", "node_id": target["node_id"],
                    "display_name": (
                        f"{source.get('display_name') or source_id} → "
                        f"{target['display_name']}"
                    ),
                    "remote_token": source_token,
                    "source_parent_token": remote["parent_token"],
                    "target_parent_token": target_binding["remote_token"],
                    "before": {
                        **remote, "source_node_id": source_id,
                        "transformation_id": transform["id"],
                        "policy_conflicts": (
                            transform.get("rule") or {}
                        ).get("policy_conflicts", []),
                    },
                    "after": {
                        "target_node_id": target["node_id"],
                        "target_token": target_binding["remote_token"],
                    },
                    "item_count": int(source.get("item_count", 0)),
                })
                actions.append({
                    "action_type": "RETIRE", "node_id": target["node_id"],
                    "display_name": source.get("display_name") or source_id,
                    "remote_token": source_token,
                    "source_parent_token": remote["parent_token"],
                    "target_parent_token": target_binding["remote_token"],
                    "before": {
                        "source_node_id": source_id,
                        "transformation_id": transform["id"],
                    },
                    "after": {"status": "retired", "delete": False},
                    "item_count": 0,
                })

        remote_content_nodes = 0
        for remote in remote_nodes:
            if remote["remote_token"] not in consumed_remote:
                if (
                    version["mode"] == "wiki"
                    and remote.get("node_type") != "folder"
                    and not remote.get("has_children")
                    and remote.get("parent_token") in consumed_remote
                ):
                    # Wiki 的普通文档也是 node。已管理分类节点下的叶子文档
                    # 属于内容现状，不应误报为“飞书独有目录”结构动作。
                    remote_content_nodes += 1
                    continue
                actions.append({
                    "action_type": "REMOTE_ONLY", "node_id": "",
                    "display_name": remote["display_name"],
                    "remote_token": remote["remote_token"],
                    "source_parent_token": remote["parent_token"],
                    "target_parent_token": "", "before": remote, "after": {},
                    "item_count": 0,
                })

        for order, action in enumerate(actions):
            action["action_order"] = order
            action.setdefault("depends_on", [])
            action.setdefault("rollbackable", False)
            node = node_by_id.get(action.get("node_id") or "")
            if node:
                action["before"].setdefault(
                    "path",
                    remote_path(remote_by_token.get(action.get("remote_token") or "")),
                )
                action["after"].setdefault("path", planned_path(node))
                action["after"].setdefault("governance", {
                    "owner": node.get("owner") or "",
                    "steward": node.get("steward") or "",
                    "review_months": node.get("review_months"),
                    "retention_years": node.get("retention_years"),
                })
                action["after"].setdefault(
                    "permission_impact", "requires_remote_preflight"
                )
        primary_by_node = {}
        for action in actions:
            if action["action_type"] in {
                "CREATE", "MAP", "RENAME", "MOVE", "NOOP",
            }:
                primary_by_node.setdefault(
                    action["node_id"], action["action_order"]
                )
        for action in actions:
            node = node_by_id.get(action.get("node_id") or "")
            parent_id = node.get("parent_node_id") if node else ""
            parent_order = primary_by_node.get(parent_id)
            if parent_order is not None and parent_order != action["action_order"]:
                action["depends_on"].append(parent_order)
            if action["action_type"] == "RETIRE":
                merge = next((
                    candidate for candidate in actions
                    if candidate["action_type"] == "MERGE"
                    and candidate["remote_token"] == action["remote_token"]
                ), None)
                if merge:
                    action["depends_on"].append(merge["action_order"])
            action["depends_on"] = sorted(set(action["depends_on"]))

        counts = Counter(a["action_type"] for a in actions)
        policy_warnings = sum(
            len((transform.get("rule") or {}).get("policy_conflicts", []))
            for transform in version.get("transformations") or []
            if transform["status"] != "completed"
        )
        summary = {
            "counts": dict(counts),
            "planned_nodes": len(version["nodes"]),
            "remote_nodes": len(remote_nodes),
            "remote_content_nodes": remote_content_nodes,
            "affected_items": sum(
                a["item_count"] for a in actions
                if a["action_type"] not in ("NOOP", "REMOTE_ONLY")
            ),
            "blocking_conflicts": counts.get("CONFLICT", 0),
            "policy_warnings": policy_warnings,
            "permission_preflights": sum(
                action["action_type"] not in ("NOOP", "REMOTE_ONLY")
                for action in actions
            ),
            "history_scope": history_scope,
            "scope_items": {
                "unmigrated": int(self.conn.execute(
                    """SELECT COUNT(*) c FROM items
                       WHERE stage NOT IN ('loaded','failed','skipped_duplicate')"""
                ).fetchone()["c"]),
                "retryable": int(self.conn.execute(
                    """SELECT COUNT(*) c FROM items
                       WHERE failed_stage IS NOT NULL AND retryable=1"""
                ).fetchone()["c"]),
                "historical": int(self.conn.execute(
                    "SELECT COUNT(*) c FROM items WHERE stage='loaded'"
                ).fetchone()["c"]),
            },
        }
        plan_id = str(uuid.uuid4())
        now = _now()
        self.conn.execute(
            """INSERT INTO structure_change_plans
               (id,version_id,remote_snapshot_id,status,revision,history_scope,
                created_by,summary_json,created_at,updated_at)
               VALUES (?,?,?,'preview',1,?,?,?,?,?)""",
            (plan_id, version_id, snapshot["id"] if snapshot else None,
             history_scope, actor, _json(summary), now, now),
        )
        for action in actions:
            self.conn.execute(
                """INSERT INTO structure_change_actions
                   (plan_id,action_order,action_type,display_name,node_id,remote_token,
                    source_parent_token,target_parent_token,before_json,after_json,
                    item_count,depends_on_json,rollbackable,status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'pending')""",
                (plan_id, action["action_order"], action["action_type"],
                 action["display_name"], action["node_id"],
                 action["remote_token"], action["source_parent_token"],
                 action["target_parent_token"], _json(action["before"]),
                 _json(action["after"]), action["item_count"],
                 _json(action["depends_on"]), int(action["rollbackable"])),
            )
        self._event(version_id, "diff_previewed", actor, {
            "plan_id": plan_id, **summary,
        }, commit=False)
        self.conn.commit()
        return self.get_change_plan(plan_id)

    # ── 内部工具 ────────────────────────────────────────

    def _normalize_nodes(self, nodes: list[dict]) -> list[dict]:
        result = []
        for order, node in enumerate(nodes):
            item = dict(node)
            item["node_id"] = str(item.get("node_id") or uuid.uuid4())
            item["parent_node_id"] = item.get("parent_node_id") or None
            item["display_name"] = str(item.get("display_name") or "").strip()
            item["semantic_key"] = str(
                item.get("semantic_key") or f"user.{uuid.uuid4()}"
            )
            item["sort_order"] = int(item.get("sort_order", order))
            item["node_kind"] = str(item.get("node_kind") or "category")
            item["owner"] = str(item.get("owner") or "")
            item["steward"] = str(item.get("steward") or "")
            item["review_months"] = int(item.get("review_months") or 12)
            item["retention_years"] = int(item.get("retention_years") or 5)
            item["status"] = str(item.get("status") or "active")
            item["aliases"] = list(item.get("aliases") or [])
            item["assignment_rule"] = dict(item.get("assignment_rule") or {})
            result.append(item)
        return result

    def _insert_node(self, version_id: str, node: dict) -> None:
        self.conn.execute(
            """INSERT INTO structure_nodes
               (version_id,node_id,parent_node_id,semantic_key,display_name,
                sort_order,node_kind,owner,steward,review_months,
                retention_years,status,aliases_json,assignment_rule_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (version_id, node["node_id"], node.get("parent_node_id"),
             node["semantic_key"], node["display_name"],
             int(node.get("sort_order", 0)), node.get("node_kind", "category"),
             node.get("owner", ""), node.get("steward", ""),
             int(node.get("review_months") or 12),
             int(node.get("retention_years") or 5),
             node.get("status", "active"), _json(node.get("aliases") or []),
             _json(node.get("assignment_rule") or {})),
        )

    @staticmethod
    def _validate_assignment_rule(rule: dict) -> str:
        if rule.get("fallback"):
            return ""
        field = str(rule.get("field") or "").strip()
        operator = str(rule.get("operator") or "").strip()
        if not field:
            return "拆分规则缺少字段"
        if not (
            field in {"original_name", "source_path", "doc_type", "year"}
            or field.startswith("metadata.")
        ):
            return f"不支持的拆分字段：{field}"
        if operator not in {"equals", "contains", "prefix", "regex", "in"}:
            return f"不支持的拆分操作符：{operator}"
        value = rule.get("value")
        if value is None or value == "" or value == []:
            return "拆分规则缺少匹配值"
        try:
            int(rule.get("priority") or 1000)
        except (TypeError, ValueError):
            return "拆分规则优先级必须是整数"
        if operator == "regex":
            try:
                re.compile(str(value))
            except re.error as exc:
                return f"正则表达式无效：{exc}"
        return ""

    @staticmethod
    def _rule_value(field: str, row: dict) -> Any:
        metadata = row.get("metadata_json") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (TypeError, json.JSONDecodeError):
                metadata = {}
        if field.startswith("metadata."):
            value: Any = metadata
            for part in field.split(".")[1:]:
                value = value.get(part) if isinstance(value, dict) else None
            return value
        if field == "doc_type":
            return (
                metadata.get("doc_type")
                or os.path.splitext(str(row.get("original_name") or ""))[1].lstrip(".")
            )
        if field == "year":
            value = metadata.get("year")
            if value:
                return value
            match = re.search(
                r"(?<!\d)(19|20)\d{2}(?!\d)",
                " ".join(str(row.get(key) or "") for key in (
                    "original_name", "source_path",
                )),
            )
            return match.group(0) if match else ""
        return row.get(field)

    @classmethod
    def _rule_matches(cls, rule: dict, row: dict) -> bool:
        actual = cls._rule_value(str(rule.get("field") or ""), row)
        expected = rule.get("value")
        operator = str(rule.get("operator") or "")
        if operator == "in":
            values = expected if isinstance(expected, list) else str(expected).split(",")
            return str(actual).casefold() in {
                str(value).strip().casefold() for value in values
            }
        actual_text = str(actual or "")
        expected_text = str(expected or "")
        if operator == "equals":
            return actual_text.casefold() == expected_text.casefold()
        if operator == "contains":
            return expected_text.casefold() in actual_text.casefold()
        if operator == "prefix":
            return actual_text.casefold().startswith(expected_text.casefold())
        if operator == "regex":
            return bool(re.search(expected_text, actual_text, flags=re.IGNORECASE))
        return False

    def _event(self, version_id: str, event_type: str, actor: str,
               detail: dict, *, commit: bool = True) -> None:
        self.conn.execute(
            """INSERT INTO structure_events
               (version_id,event_type,actor,detail_json,created_at)
               VALUES (?,?,?,?,?)""",
            (version_id, event_type, actor, _json(detail), _now()),
        )
        if commit:
            self.conn.commit()
