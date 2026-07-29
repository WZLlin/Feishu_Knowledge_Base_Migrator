"""递归读取飞书云空间或 Wiki 的实际目录，供结构工作台生成只读快照。"""
from __future__ import annotations

from collections import deque

from ..feishu.writer import FeishuWriter


class FeishuStructureDiscovery:
    def __init__(self, writer: FeishuWriter, *, max_nodes: int = 10000):
        self.writer = writer
        self.max_nodes = max_nodes

    def drive(self, root_token: str, *, user_token: str = "") -> list[dict]:
        queue = deque([root_token])
        seen: set[str] = set()
        result: list[dict] = []
        by_token: dict[str, dict] = {}
        while queue:
            parent = queue.popleft()
            if parent in seen:
                continue
            seen.add(parent)
            children = self.writer.list_drive_children(
                parent, user_token=user_token
            )
            if parent in by_token:
                by_token[parent]["file_count"] = sum(
                    item.get("type") != "folder" for item in children
                )
                by_token[parent]["has_children"] = bool(children)
            for item in children:
                if item.get("type") != "folder":
                    continue
                token = str(item.get("token") or "")
                if not token:
                    continue
                node = {
                    "remote_token": token,
                    "parent_token": parent,
                    "display_name": item.get("name") or token,
                    "node_type": "folder",
                    "has_children": False,
                    "file_count": 0,
                    "remote_updated_at": (
                        item.get("modified_time")
                        or item.get("modified_at")
                        or item.get("updated_at")
                        or ""
                    ),
                    "raw": item,
                }
                result.append(node)
                by_token[token] = node
                queue.append(token)
                if len(result) >= self.max_nodes:
                    raise RuntimeError(f"远程目录节点超过安全上限 {self.max_nodes}")
        return result

    def wiki(self, space_id: str, *, root_node_token: str = "",
             user_token: str = "") -> list[dict]:
        queue = deque([root_node_token])
        seen: set[str] = set()
        result: list[dict] = []
        by_token: dict[str, dict] = {}
        while queue:
            parent = queue.popleft()
            if parent in seen:
                continue
            seen.add(parent)
            children = self.writer.list_wiki_children(
                space_id, parent, user_token=user_token
            )
            if parent in by_token:
                by_token[parent]["file_count"] = len(children)
                by_token[parent]["has_children"] = bool(children)
            for item in children:
                token = str(item.get("node_token") or "")
                if not token:
                    continue
                has_child = bool(item.get("has_child"))
                node = {
                    "remote_token": token,
                    "parent_token": parent,
                    "display_name": item.get("title") or token,
                    "node_type": item.get("obj_type") or "wiki",
                    "has_children": has_child,
                    "file_count": 0,
                    "remote_updated_at": (
                        item.get("updated_at")
                        or item.get("modified_time")
                        or ""
                    ),
                    "raw": item,
                }
                result.append(node)
                by_token[token] = node
                if has_child:
                    queue.append(token)
                if len(result) >= self.max_nodes:
                    raise RuntimeError(f"远程 Wiki 节点超过安全上限 {self.max_nodes}")
        return result
