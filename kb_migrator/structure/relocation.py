"""历史知识条目的安全重定位。

先对所有选中动作执行目标目录同名预检，再开始任何远程移动。每个动作独立持久化，
中断后只重试未完成项；回滚仅作用于本计划确实移动且仍可定位原父目录的条目。
"""
from __future__ import annotations

from collections import defaultdict
import re
from typing import Callable

from .service import StructureConflict, StructureService

Progress = Callable[[int, int, str], None] | None


class ItemRelocationExecutor:
    def __init__(self, structures: StructureService, writer):
        self.structures = structures
        self.writer = writer

    def preflight(self, plan_id: str, *, space_id: str = "",
                  user_token: str = "") -> dict:
        plan = self.structures.get_item_relocation_plan(plan_id)
        if plan["status"] not in ("draft", "approved", "ready", "failed"):
            raise StructureConflict("当前重定位计划状态不能执行冲突预检")
        target_cache: dict[str, list[dict]] = {}
        permission_cache: dict[tuple[str, str], dict] = {}
        planned_names: dict[str, set[tuple[str, str]]] = defaultdict(set)
        conflicts = []
        for action in plan["actions"]:
            if not action["selected"] or action["status"] in (
                "completed", "rolled_back",
            ):
                continue
            target = action["target_parent_token"]
            permission_risk = self._permission_risk(
                plan, action, permission_cache,
                user_token=user_token,
            )
            if permission_risk:
                conflicts.append(action["stable_key"])
                self.structures.update_item_relocation_action(
                    plan_id, action["stable_key"], "conflict",
                    error=permission_risk,
                    detail={"preflight": "permission_expansion"},
                )
                continue
            if target not in target_cache:
                target_cache[target] = self._list_children(
                    plan["target_mode"], target,
                    space_id=space_id, user_token=user_token,
                )
            existing = target_cache[target]
            same_token = any(
                self._token(plan["target_mode"], item)
                == action["object_token"]
                for item in existing
            )
            if same_token or action["status"] == "already_moved":
                self.structures.update_item_relocation_action(
                    plan_id, action["stable_key"], "already_moved",
                    detail={"preflight": "object_already_in_target"},
                )
                continue
            signature = self._signature(
                plan["target_mode"], action["display_name"],
                action["object_type"],
            )
            exact_duplicate = next((
                item for item in existing
                if self._signature(
                    plan["target_mode"],
                    self._name(plan["target_mode"], item),
                    self._type(plan["target_mode"], item),
                ) == signature
                and self._same_known_hash(action, item)
            ), None)
            if exact_duplicate:
                self.structures.update_item_relocation_action(
                    plan_id, action["stable_key"], "already_moved",
                    detail={
                        "preflight": "exact_duplicate_in_target",
                        "duplicate_of_token": self._token(
                            plan["target_mode"], exact_duplicate
                        ),
                    },
                )
                continue
            same_name = any(
                self._signature(
                    plan["target_mode"],
                    self._name(plan["target_mode"], item),
                    self._type(plan["target_mode"], item),
                ) == signature
                for item in existing
            )
            near_duplicate = next((
                item for item in existing
                if self._looks_like_near_duplicate(action, item)
            ), None)
            if same_name or near_duplicate or signature in planned_names[target]:
                conflicts.append(action["stable_key"])
                self.structures.update_item_relocation_action(
                    plan_id, action["stable_key"], "conflict",
                    error=(
                        "目标目录存在疑似近似内容，需人工确认"
                        if near_duplicate and not same_name
                        else "目标目录存在同名同类型内容"
                    ),
                    detail={
                        "preflight": (
                            "near_duplicate_review"
                            if near_duplicate and not same_name
                            else "same_name_different_or_unknown_content"
                        ),
                        "conflicting_token": self._token(
                            plan["target_mode"], near_duplicate
                        ) if near_duplicate else "",
                    },
                )
                continue
            planned_names[target].add(signature)
            self.structures.update_item_relocation_action(
                plan_id, action["stable_key"], "ready",
                detail={"preflight": "ready"},
            )
        refreshed = self.structures.get_item_relocation_plan(plan_id)
        if refreshed["status"] in ("approved", "failed") and not conflicts:
            refreshed = self.structures.set_item_relocation_plan_status(
                plan_id, "ready"
            )
        else:
            refreshed = self.structures.set_item_relocation_plan_status(
                plan_id, refreshed["status"]
            )
        return {
            "plan": refreshed,
            "conflicts": conflicts,
            "ready": refreshed["summary"].get("ready", 0),
            "already_moved": refreshed["summary"].get("already_moved", 0),
        }

    def _permission_risk(
        self, plan: dict, action: dict,
        cache: dict[tuple[str, str], dict], *, user_token: str,
    ) -> str:
        """目标权限比来源更开放时阻断；真实客户端无法读取权限时也阻断。"""
        reader = getattr(self.writer, "get_public_permission", None)
        if not callable(reader):
            return ""  # 测试替身/兼容旧 writer；真实 FeishuWriter 总是具备此方法。
        obj_type = "wiki" if plan["target_mode"] == "wiki" else "folder"

        def read(token: str) -> dict:
            key = (token, obj_type)
            if key not in cache:
                try:
                    cache[key] = reader(
                        token, obj_type, user_token=user_token
                    )
                except Exception as exc:
                    raise StructureConflict(
                        f"无法读取目录权限，已阻止移动：{token}（{exc}）"
                    ) from exc
            return cache[key]

        source = read(action["source_parent_token"])
        target = read(action["target_parent_token"])
        source_level = self._permission_openness(source)
        target_level = self._permission_openness(target)
        if any(
            target_value > source_value
            for source_value, target_value in zip(source_level, target_level)
        ):
            return "目标目录权限比来源目录更开放，需先收紧权限或人工确认"
        return ""

    @staticmethod
    def _permission_openness(permission: dict) -> tuple[int, int, int, int]:
        """返回可比较的开放程度；元组任一维度增大都视作风险。"""
        link_rank = {
            "closed": 0,
            "tenant_readable": 1,
            "tenant_editable": 2,
            "anyone_readable": 3,
            "anyone_editable": 4,
        }
        share_rank = {
            "same_tenant": 0,
            "anyone": 2,
        }
        return (
            int(bool(permission.get("external_access"))),
            int(bool(permission.get("invite_external"))),
            link_rank.get(str(permission.get("link_share_entity") or ""), 1),
            share_rank.get(str(permission.get("share_entity") or ""), 1),
        )

    def execute(self, plan_id: str, *, space_id: str = "",
                user_token: str = "", progress: Progress = None) -> dict:
        plan = self.structures.get_item_relocation_plan(plan_id)
        if plan["status"] not in ("approved", "ready", "failed"):
            raise StructureConflict("请先审批历史文件重定位计划")
        checked = self.preflight(
            plan_id, space_id=space_id, user_token=user_token
        )
        if checked["conflicts"]:
            raise StructureConflict(
                f"存在 {len(checked['conflicts'])} 条预检冲突"
                "（同名冲突、近似内容或权限扩大），尚未移动任何文件"
            )
        plan = checked["plan"]
        actions = [
            action for action in plan["actions"]
            if action["selected"]
            and action["status"] in ("ready", "already_moved")
        ]
        self.structures.set_item_relocation_plan_status(plan_id, "running")
        completed = 0
        try:
            for index, action in enumerate(actions, 1):
                if progress:
                    progress(
                        index - 1, len(actions),
                        f"重定位 {action['display_name']}",
                    )
                try:
                    if action["status"] == "already_moved":
                        self._complete_assignment(
                            plan, action, remote_moved=False
                        )
                    else:
                        relocation = self.structures.begin_relocation(
                            plan["version_id"], "item_split",
                            action["object_token"],
                            action["target_parent_token"],
                            node_id=action["target_node_id"],
                            detail={
                                "plan_id": plan_id,
                                "stable_key": action["stable_key"],
                                "source_parent_token": (
                                    action["source_parent_token"] or ""
                                ),
                            },
                        )
                        self._move(
                            plan["target_mode"], action["object_token"],
                            action["target_parent_token"],
                            action["object_type"],
                            space_id=space_id, user_token=user_token,
                        )
                        self.structures.complete_relocation(
                            relocation["id"],
                            detail={"remote_moved": True},
                        )
                        self._complete_assignment(
                            plan, action, remote_moved=True
                        )
                except Exception as exc:
                    self.structures.update_item_relocation_action(
                        plan_id, action["stable_key"], "failed",
                        error=str(exc),
                    )
                    raise
                completed += 1
                if progress:
                    progress(
                        index, len(actions),
                        f"已重定位 {action['display_name']}",
                    )
            final = self.structures.set_item_relocation_plan_status(
                plan_id, "completed"
            )
            return {
                "plan_id": plan_id, "status": final["status"],
                "completed": completed,
                "selected": final["summary"].get("selected", 0),
            }
        except Exception as exc:
            self.structures.set_item_relocation_plan_status(
                plan_id, "failed", error=str(exc)
            )
            raise

    def rollback(self, plan_id: str, *, space_id: str = "",
                 user_token: str = "", progress: Progress = None) -> dict:
        plan = self.structures.get_item_relocation_plan(plan_id)
        if plan["status"] not in ("completed", "failed"):
            raise StructureConflict("只有已执行或部分失败的计划可以回滚")
        actions = [
            action for action in reversed(plan["actions"])
            if action["selected"]
            and action["status"] in ("completed", "rollback_failed")
            and action["detail"].get("remote_moved")
        ]
        unsupported = [
            action for action in actions if not action["source_parent_token"]
        ]
        if unsupported:
            raise StructureConflict(
                f"有 {len(unsupported)} 条缺少原父目录，无法安全回滚"
            )
        self._preflight_rollback(
            plan, actions, space_id=space_id, user_token=user_token
        )
        self.structures.set_item_relocation_plan_status(plan_id, "running")
        current = None
        try:
            for index, action in enumerate(actions, 1):
                current = action
                if progress:
                    progress(
                        index - 1, len(actions),
                        f"回滚 {action['display_name']}",
                    )
                source_children = self._list_children(
                    plan["target_mode"], action["source_parent_token"],
                    space_id=space_id, user_token=user_token,
                )
                if not any(
                    self._token(plan["target_mode"], item)
                    == action["object_token"]
                    for item in source_children
                ):
                    self._move(
                        plan["target_mode"], action["object_token"],
                        action["source_parent_token"], action["object_type"],
                        space_id=space_id, user_token=user_token,
                    )
                self.structures.ledger.assign_structure_target(
                    action["stable_key"], plan["version_id"],
                    action["source_node_id"],
                    source="split_rule_rollback",
                )
                self.structures.update_item_relocation_action(
                    plan_id, action["stable_key"], "rolled_back",
                    detail={"rolled_back": True},
                )
                if progress:
                    progress(
                        index, len(actions),
                        f"已回滚 {action['display_name']}",
                    )
            final = self.structures.set_item_relocation_plan_status(
                plan_id, "rolled_back"
            )
            return {
                "plan_id": plan_id, "status": final["status"],
                "rolled_back": len(actions),
            }
        except Exception as exc:
            if current:
                self.structures.update_item_relocation_action(
                    plan_id, current["stable_key"], "rollback_failed",
                    error=str(exc),
                )
            self.structures.set_item_relocation_plan_status(
                plan_id, "failed", error=str(exc)
            )
            raise

    def _complete_assignment(self, plan: dict, action: dict, *,
                             remote_moved: bool) -> None:
        effective_remote_moved = remote_moved
        if not remote_moved:
            relocation = self.structures.find_relocation(
                plan["version_id"], "item_split",
                action["object_token"], node_id=action["target_node_id"],
            )
            if relocation and relocation["status"] != "completed":
                effective_remote_moved = True
                self.structures.complete_relocation(
                    relocation["id"],
                    detail={
                        "remote_moved": True,
                        "recovered_after_unknown_response": True,
                    },
                )
        self.structures.ledger.assign_structure_target(
            action["stable_key"], plan["version_id"],
            action["target_node_id"], source="split_rule_relocation",
        )
        self.structures.update_item_relocation_action(
            plan["id"], action["stable_key"], "completed",
            detail={
                "remote_moved": effective_remote_moved,
                "rollback_supported": bool(
                    effective_remote_moved and action["source_parent_token"]
                ),
            },
        )

    def _preflight_rollback(self, plan: dict, actions: list[dict], *,
                            space_id: str, user_token: str) -> None:
        caches: dict[str, list[dict]] = {}
        conflicts = []
        for action in actions:
            parent = action["source_parent_token"]
            if parent not in caches:
                caches[parent] = self._list_children(
                    plan["target_mode"], parent,
                    space_id=space_id, user_token=user_token,
                )
            signature = (
                action["display_name"].strip().casefold(),
                self._normalized_type(
                    plan["target_mode"], action["object_type"]
                ),
            )
            for item in caches[parent]:
                if self._token(plan["target_mode"], item) == action["object_token"]:
                    continue
                remote = (
                    self._name(plan["target_mode"], item).strip().casefold(),
                    self._normalized_type(
                        plan["target_mode"],
                        self._type(plan["target_mode"], item),
                    ),
                )
                if remote == signature:
                    conflicts.append(action["display_name"])
                    break
        if conflicts:
            raise StructureConflict(
                "原目录出现同名内容，回滚前未移动任何文件："
                + "、".join(conflicts[:8])
            )

    def _list_children(self, mode: str, token: str, *,
                       space_id: str, user_token: str) -> list[dict]:
        if mode == "drive":
            return self.writer.list_drive_children(token)
        return self.writer.list_wiki_children(
            space_id, token, user_token=user_token
        )

    def _move(self, mode: str, token: str, parent: str, obj_type: str, *,
              space_id: str, user_token: str) -> None:
        if mode == "drive":
            self.writer.move_file(token, parent, obj_type)
        else:
            self.writer.move_wiki_node(
                space_id, token, parent, user_token=user_token
            )

    @staticmethod
    def _token(mode: str, item: dict) -> str:
        return str(
            item.get("token") if mode == "drive"
            else item.get("node_token") or item.get("wiki_token") or ""
        )

    @staticmethod
    def _name(mode: str, item: dict) -> str:
        return str(
            item.get("name") if mode == "drive"
            else item.get("title") or item.get("name") or ""
        )

    @classmethod
    def _signature(cls, mode: str, name: str, obj_type: str) -> tuple[str, str]:
        return (
            str(name or "").strip().casefold(),
            cls._normalized_type(mode, obj_type),
        )

    @staticmethod
    def _known_hash(item: dict) -> str:
        return str(
            item.get("content_sha256")
            or item.get("sha256")
            or item.get("file_sha256")
            or ""
        ).strip().casefold()

    @classmethod
    def _same_known_hash(cls, action: dict, remote: dict) -> bool:
        left = cls._known_hash(action.get("detail") or {})
        right = cls._known_hash(remote)
        return bool(left and right and left == right)

    @classmethod
    def _looks_like_near_duplicate(cls, action: dict, remote: dict) -> bool:
        """用名称主干和大小做保守候选识别；只阻断，不自动删除或覆盖。"""
        action_name = str(action.get("display_name") or "")
        remote_name = str(
            remote.get("name") or remote.get("title") or ""
        )
        normalize = lambda value: re.sub(  # noqa: E731
            r"(?:\s*[\(\（]?(?:副本|copy|\d+)[\)\）]?)?(?=\.[^.]+$|$)",
            "",
            value.strip().casefold(),
        )
        if not action_name or normalize(action_name) != normalize(remote_name):
            return False
        left_size = int((action.get("detail") or {}).get("size") or 0)
        right_size = int(remote.get("size") or remote.get("file_size") or 0)
        if not left_size or not right_size:
            return False
        return abs(left_size - right_size) / max(left_size, right_size) <= 0.05

    @staticmethod
    def _type(mode: str, item: dict) -> str:
        return str(
            item.get("type") if mode == "drive"
            else item.get("obj_type") or item.get("node_type") or "wiki"
        )

    @staticmethod
    def _normalized_type(mode: str, value: str) -> str:
        if mode == "wiki":
            return "wiki"
        return str(value or "file")
