"""把已确认结构安全地落实到飞书。

结构发布只创建/映射新的写入目标并切换路由，不搬动历史文件。改名、移动、合并和
拆分造成的历史内容调整统一生成独立的逐文件重定位计划，经预检和审批后另行执行。
"""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Callable

from ..feishu.writer import FeishuWriter
from .service import StructureConflict, StructureService

Progress = Callable[[int, int, str], None] | None


class StructureReconciler:
    def __init__(self, structures: StructureService, writer: FeishuWriter):
        self.structures = structures
        self.writer = writer

    def apply(self, version_id: str, *, plan_id: str = "",
              root_token: str = "", space_id: str = "",
              user_token: str = "", progress: Progress = None) -> dict:
        version = self.structures.get_version(version_id)
        if version["status"] not in ("approved", "applying", "failed"):
            raise StructureConflict("请先保存并最终确认结构版本")
        mode = version["mode"]
        if mode == "wiki" and not user_token:
            raise StructureConflict("Wiki 结构发布需要 user_access_token")

        snapshot = self.structures.latest_snapshot(mode)
        if plan_id:
            plan = self.structures.get_change_plan(plan_id)
            if plan["version_id"] != version_id:
                raise StructureConflict("结构计划与待发布版本不一致")
            if plan["status"] not in ("approved", "applying", "failed"):
                raise StructureConflict("结构计划尚未审批或已不可执行")
            if (
                plan.get("remote_snapshot_id")
                and snapshot
                and plan["remote_snapshot_id"] != snapshot["id"]
            ):
                raise StructureConflict(
                    "飞书目录快照已变化，请重新生成并审批结构计划"
                )
        else:
            # 兼容 CLI/既有调用：仍生成持久化计划，避免绕过审计。
            plan = self.structures.create_diff_plan(
                version_id, snapshot["id"] if snapshot else "",
                actor="system",
            )
            plan = self.structures.approve_change_plan(
                plan["id"], actor="system"
            )
            plan_id = plan["id"]
        blockers = [
            action for action in plan["actions"]
            if action["action_type"] == "CONFLICT"
        ]
        if blockers:
            labels = "、".join(
                f"{a['action_type']}:{a['display_name']}" for a in blockers[:8]
            )
            raise StructureConflict(f"差异计划仍有阻断项：{labels}")

        if mode == "drive" and not root_token:
            root_token = self.writer.create_folder(version["root_name"], "")
        if mode == "wiki" and not space_id:
            space_id = self.writer.create_wiki_space(
                version["root_name"], user_token=user_token
            )

        history_scope = plan.get("history_scope") or "unmigrated_only"
        merge_actions = [
            action for action in plan["actions"]
            if action["action_type"] == "MERGE"
        ]
        rename_actions = [
            action for action in plan["actions"]
            if action["action_type"] == "RENAME"
        ]
        self.structures.set_status(version_id, "applying")
        self.structures.set_change_plan_status(plan_id, "applying")
        actions_by_node: dict[str, list[dict]] = defaultdict(list)
        for action in plan["actions"]:
            if action["node_id"]:
                actions_by_node[action["node_id"]].append(action)
        ordered = self._topological(version["nodes"])
        total = len(ordered)
        current_actions: list[dict] = []
        try:
            for index, node in enumerate(ordered, 1):
                if progress:
                    progress(index - 1, total, f"同步目录：{node['display_name']}")
                fresh = self.structures.get_version(version_id)
                fresh_node = next(n for n in fresh["nodes"]
                                  if n["node_id"] == node["node_id"])
                parent_token = self._parent_token(
                    fresh, fresh_node, root_token if mode == "drive" else ""
                )
                binding = fresh_node.get("binding")
                actions = actions_by_node.get(node["node_id"], [])
                current_actions = [
                    action for action in actions
                    if action["action_type"] in {
                        "CREATE", "MAP", "RENAME", "MOVE", "NOOP",
                    }
                    and action["status"] != "completed"
                ]
                for action in current_actions:
                    self.structures.update_change_action(
                        plan_id, action["action_order"], "running"
                    )
                map_action = next(
                    (a for a in actions if a["action_type"] == "MAP"), None
                )
                move_action = next(
                    (a for a in actions if a["action_type"] == "MOVE"), None
                )
                rename_action = next(
                    (a for a in actions if a["action_type"] == "RENAME"), None
                )
                if map_action and (
                    not binding
                    or binding.get("remote_token") != map_action["remote_token"]
                ):
                    self.structures.bind_node(
                        version_id, node["node_id"], mode,
                        map_action["remote_token"],
                        parent_remote_token=map_action["source_parent_token"],
                    )
                    binding = {"remote_token": map_action["remote_token"]}
                if not binding:
                    if mode == "drive":
                        token = self.writer.create_folder(
                            node["display_name"], parent_token
                        )
                    else:
                        token = self.writer.create_wiki_node(
                            space_id, node["display_name"],
                            parent_node_token=parent_token,
                            user_token=user_token,
                        )["node_token"]
                    self.structures.bind_node(
                        version_id, node["node_id"], mode, token,
                        parent_remote_token=parent_token,
                    )
                    binding = {"remote_token": token}
                elif rename_action or move_action:
                    # 结构发布永远只切换后续写入路由。即便计划选择历史重定位，
                    # 也必须先发布结构，再生成独立逐文件计划，经审批后才能搬动。
                    replacement = self._logical_retarget(
                        version_id, fresh_node, binding["remote_token"],
                        parent_token, mode=mode, space_id=space_id,
                        user_token=user_token, history_scope=history_scope,
                    )
                    self.structures.bind_node(
                        version_id, node["node_id"], mode, replacement,
                        parent_remote_token=parent_token,
                    )
                    binding = {"remote_token": replacement}
                for action in current_actions:
                    self.structures.update_change_action(
                        plan_id, action["action_order"], "completed"
                    )
                current_actions = []
                if progress:
                    progress(index, total, f"目录已就绪：{node['display_name']}")

            moved = 0
            for offset, action in enumerate(merge_actions, 1):
                if action["status"] == "completed":
                    continue
                current_actions = [action]
                self.structures.update_change_action(
                    plan_id, action["action_order"], "running"
                )
                if progress:
                    progress(
                        total + offset - 1, total + len(merge_actions),
                        f"合并目录：{action['display_name']}",
                    )
                # 合并在此只改变稳定路由和治理模型；来源目录及其中内容保留。
                # 受影响的台账文件会在激活后进入独立重定位计划。
                self.structures.update_change_action(
                    plan_id, action["action_order"], "completed"
                )
                for retire in plan["actions"]:
                    if (
                        retire["action_type"] == "RETIRE"
                        and retire["remote_token"] == action["remote_token"]
                        and retire["status"] != "completed"
                    ):
                        self.structures.update_change_action(
                            plan_id, retire["action_order"],
                            "skipped",
                        )
                current_actions = []
            for transform in version.get("transformations") or []:
                if transform["transformation_type"] == "split":
                    # 规则拆分只影响后续写入；历史内容必须经影响预览后另行处理。
                    self.structures.complete_transformation(transform["id"])
                    for split_action in plan["actions"]:
                        if (
                            split_action["action_type"] == "SPLIT_RULE"
                            and split_action["before"].get("transformation_id")
                            == transform["id"]
                        ):
                            self.structures.update_change_action(
                                plan_id, split_action["action_order"], "completed"
                            )
                    continue
                if transform["transformation_type"] != "merge":
                    continue
                self.structures.complete_transformation(transform["id"])

            active = self.structures.activate(
                version_id, root_token=root_token, space_id=space_id
            )
            remote_nodes = []
            by_id = {n["node_id"]: n for n in active["nodes"]}
            for node in active["nodes"]:
                binding = node["binding"]
                parent = by_id.get(node.get("parent_node_id") or "")
                parent_binding = parent.get("binding") if parent else None
                remote_nodes.append({
                    "remote_token": binding["remote_token"],
                    "parent_token": (
                        parent_binding["remote_token"] if parent_binding
                        else root_token if mode == "drive" else ""
                    ),
                    "display_name": node["display_name"],
                    "node_type": "folder" if mode == "drive" else "wiki",
                    "has_children": True,
                })
            for action in merge_actions:
                remote_nodes.append({
                    "remote_token": action["remote_token"],
                    "parent_token": action["source_parent_token"],
                    "display_name": action["before"].get(
                        "display_name", action["display_name"].split(" → ", 1)[0]
                    ),
                    "node_type": (
                        "historical"
                    ),
                    "has_children": True,
                    "raw": {
                        "managed_status": "historical_preserved",
                        "delete": False,
                    },
                })
            historical_actions = {
                action["remote_token"]: action
                for action in [*rename_actions, *[
                    item for item in plan["actions"]
                    if item["action_type"] == "MOVE"
                ]]
                if action["remote_token"]
            }
            for action in historical_actions.values():
                remote_nodes.append({
                    "remote_token": action["remote_token"],
                    "parent_token": action["source_parent_token"],
                    "display_name": action["before"].get(
                        "display_name", action["display_name"]
                    ),
                    "node_type": "historical",
                    "has_children": True,
                    "raw": {
                        "managed_status": "historical_preserved",
                        "delete": False,
                    },
                })
            snapshot = self.structures.save_remote_snapshot(
                mode, remote_nodes, root_token=root_token, space_id=space_id
            )
            for action in plan["actions"]:
                if action["action_type"] == "REMOTE_ONLY":
                    self.structures.update_change_action(
                        plan_id, action["action_order"], "skipped"
                    )
            self.structures.set_change_plan_status(plan_id, "completed")
            relocation_plan = None
            if history_scope == "relocate_history":
                relocation_plan = self.structures.create_item_relocation_plan(
                    version_id, actor=plan.get("approved_by") or "system"
                )
            return {
                "version_id": version_id, "status": "active", "mode": mode,
                "plan_id": plan_id,
                "root_token": root_token, "space_id": space_id,
                "snapshot_id": snapshot["id"], "nodes": len(active["nodes"]),
                "merged_children": moved,
                "history_scope": history_scope,
                "relocation_plan_id": (
                    relocation_plan["id"] if relocation_plan else ""
                ),
                "relocation_candidates": (
                    relocation_plan["summary"]["total"]
                    if relocation_plan else 0
                ),
            }
        except Exception as exc:
            for action in current_actions:
                self.structures.update_change_action(
                    plan_id, action["action_order"], "failed", error=str(exc)
                )
            self.structures.set_change_plan_status(
                plan_id, "failed", error=str(exc)
            )
            self.structures.set_status(
                version_id, "failed", detail={"error": str(exc)}
            )
            raise

    def _logical_retarget(
        self, version_id: str, node: dict, source_token: str,
        parent_token: str, *, mode: str, space_id: str,
        user_token: str, history_scope: str,
    ) -> str:
        """创建后续写入目标，保留旧目录和其中全部历史内容。"""
        existing = self.structures.find_relocation(
            version_id, "logical_retarget", source_token,
            node_id=node["node_id"],
        )
        if existing:
            return existing["target_token"]
        if mode == "drive":
            target_token = self.writer.create_folder(
                node["display_name"], parent_token
            )
        else:
            target_token = self.writer.create_wiki_node(
                space_id, node["display_name"],
                parent_node_token=parent_token,
                user_token=user_token,
            )["node_token"]
        relocation = self.structures.begin_relocation(
            version_id, "logical_retarget", source_token, target_token,
            node_id=node["node_id"],
            detail={
                "history_scope": history_scope,
                "historical_content_moved": False,
                "new_writes_only": True,
            },
        )
        self.structures.complete_relocation(
            relocation["id"],
            detail={"historical_content_moved": False},
        )
        return target_token

    def _safe_rename_drive(self, version_id: str, node: dict,
                           source_token: str, parent_token: str) -> str:
        existing = self.structures.find_relocation(
            version_id, "rename", source_token, node_id=node["node_id"]
        )
        if existing:
            target_token = existing["target_token"]
            relocation = existing
        else:
            target_token = self.writer.create_folder(
                node["display_name"], parent_token
            )
            relocation = self.structures.begin_relocation(
                version_id, "rename", source_token, target_token,
                node_id=node["node_id"],
                detail={"old_name": node.get("aliases", [""])[-1]
                        if node.get("aliases") else "",
                        "new_name": node["display_name"]},
            )
        source_items = self.writer.list_drive_children(source_token)
        target_items = self.writer.list_drive_children(target_token)
        conflicts = self._name_conflicts(target_items, source_items, "drive")
        if conflicts:
            raise StructureConflict(
                f"重命名目录存在同名内容冲突：{', '.join(conflicts[:8])}"
            )
        moved = self._move_children(
            "drive", source_token, target_token, source_items
        )
        self.structures.complete_relocation(
            relocation["id"], detail={"moved_children": moved}
        )
        return target_token

    def _preflight_merges(self, mode: str, actions: list[dict], *,
                          space_id: str, user_token: str) -> dict[str, list[dict]]:
        """读取全部来源和目标，确认无同名内容后才允许开始任何合并移动。"""
        if not actions:
            return {}
        target_cache: dict[str, list[dict]] = {}
        source_cache: dict[str, list[dict]] = {}
        seen_by_target: dict[str, list[dict]] = {}
        conflicts: list[str] = []
        for action in actions:
            target = action["target_parent_token"]
            if target not in target_cache:
                target_cache[target] = self._list_children(
                    mode, target, space_id=space_id, user_token=user_token
                )
                seen_by_target[target] = list(target_cache[target])
            source = action["remote_token"]
            children = self._list_children(
                mode, source, space_id=space_id, user_token=user_token
            )
            source_cache[source] = children
            conflicts.extend(
                self._name_conflicts(seen_by_target[target], children, mode)
            )
            seen_by_target[target].extend(children)
        if conflicts:
            unique = list(dict.fromkeys(conflicts))
            raise StructureConflict(
                f"目录合并存在同名内容冲突，尚未移动任何内容：{', '.join(unique[:8])}"
            )
        return source_cache

    def _list_children(self, mode: str, token: str, *,
                       space_id: str = "", user_token: str = "") -> list[dict]:
        if mode == "drive":
            return self.writer.list_drive_children(token)
        return self.writer.list_wiki_children(
            space_id, token, user_token=user_token
        )

    @staticmethod
    def _name_conflicts(existing: list[dict], incoming: list[dict],
                        mode: str) -> list[str]:
        name_key = "name" if mode == "drive" else "title"
        type_key = "type" if mode == "drive" else "obj_type"
        seen = {
            (str(item.get(name_key) or "").strip().casefold(),
             str(item.get(type_key) or ""))
            for item in existing
        }
        return [
            str(item.get(name_key) or "")
            for item in incoming
            if (
                str(item.get(name_key) or "").strip().casefold(),
                str(item.get(type_key) or ""),
            ) in seen
        ]

    def _move_children(self, mode: str, source_token: str, target_token: str,
                       children: list[dict], *, space_id: str = "",
                       user_token: str = "") -> int:
        moved = 0
        for item in children:
            if mode == "drive":
                token = str(item.get("token") or "")
                if not token:
                    continue
                self.writer.move_file(
                    token, target_token, str(item.get("type") or "file")
                )
            else:
                token = str(item.get("node_token") or "")
                if not token:
                    continue
                self.writer.move_wiki_node(
                    space_id, token, target_token, user_token=user_token
                )
            moved += 1
        return moved

    @staticmethod
    def _topological(nodes: list[dict]) -> list[dict]:
        by_parent: dict[str, list[dict]] = defaultdict(list)
        indegree = {n["node_id"]: 0 for n in nodes}
        for node in nodes:
            parent = node.get("parent_node_id") or ""
            by_parent[parent].append(node)
            if parent in indegree:
                indegree[node["node_id"]] += 1
        queue = deque(sorted(
            (n for n in nodes if indegree[n["node_id"]] == 0),
            key=lambda n: (n["sort_order"], n["display_name"]),
        ))
        result = []
        while queue:
            node = queue.popleft()
            result.append(node)
            for child in sorted(
                by_parent[node["node_id"]],
                key=lambda n: (n["sort_order"], n["display_name"]),
            ):
                indegree[child["node_id"]] -= 1
                if indegree[child["node_id"]] == 0:
                    queue.append(child)
        if len(result) != len(nodes):
            raise StructureConflict("目录结构存在循环，无法发布")
        return result

    @staticmethod
    def _parent_token(version: dict, node: dict, root_token: str) -> str:
        parent_id = node.get("parent_node_id")
        if not parent_id:
            return root_token
        parent = next(
            (n for n in version["nodes"] if n["node_id"] == parent_id), None
        )
        binding = parent.get("binding") if parent else None
        if not binding:
            raise StructureConflict(f"父目录尚未绑定：{node['display_name']}")
        return binding["remote_token"]
