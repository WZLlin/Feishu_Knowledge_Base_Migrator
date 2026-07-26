"""阶段1：在飞书建目标结构，产出并持久化「分类 -> 目标 token」映射(folder_map)。

两种目标形态：
- **drive_tree（默认，推荐先跑）**：在云空间建「根文件夹 + 每个分类一个子文件夹」。
  仅需 tenant_access_token（应用凭证），**无需 OAuth**，最快打通真实写入、看沉淀比例上升。
- **wiki_space**：建 Wiki 知识空间 + 每分类一个节点。建空间**必须 user_access_token(OAuth)**。
  文件仍先落云空间文件夹，再按需 move_docs_to_wiki 挂入（写入层已有该原语）。

产出持久化到 targets_file(JSON)：
    {mode, root_token, space_id, folder_map:{分类: token}, wiki_node_map:{分类: node_token}}
load 阶段读取该文件的 folder_map，决定每份文档上传到哪个云空间文件夹。

幂等：已存在的 targets_file 会被读入并跳过已创建项（按 folder_map 是否已有该分类判断），
避免重复建目录。删除该文件即可强制重建。
"""
from __future__ import annotations

import json
import os
from typing import Optional

from ..taxonomy import Taxonomy
from .writer import FeishuWriter


class FeishuBootstrapper:
    def __init__(self, writer: FeishuWriter, taxonomy: Taxonomy, targets_file: str):
        self.w = writer
        self.tx = taxonomy
        self.targets_file = targets_file

    # ── 持久化 ────────────────────────────────────────────

    def load_targets(self) -> dict:
        if os.path.exists(self.targets_file):
            with open(self.targets_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"mode": "", "root_token": "", "space_id": "",
                "folder_map": {}, "wiki_node_map": {}}

    def _save(self, targets: dict) -> None:
        os.makedirs(os.path.dirname(self.targets_file) or ".", exist_ok=True)
        with open(self.targets_file, "w", encoding="utf-8") as f:
            json.dump(targets, f, ensure_ascii=False, indent=2)

    # ── 云空间目录树（tenant token，无需 OAuth）──────────

    def bootstrap_drive_tree(self, root_name: str = "", root_parent_token: str = "") -> dict:
        """建「根文件夹 + 各分类子文件夹」。幂等：已在 folder_map 里的分类跳过。返回 targets。"""
        t = self.load_targets()
        t["mode"] = "drive"
        root_name = root_name or self.tx.space_name
        if not t.get("root_token"):
            t["root_token"] = self.w.create_folder(root_name, root_parent_token)
            self._save(t)
        fm: dict = t.setdefault("folder_map", {})
        for path in self.tx.all_folder_paths():
            if path in fm:
                continue
            fm[path] = self.w.create_folder(path, t["root_token"])
            self._save(t)   # 逐个落盘，中断可续跑
        return t

    # ── Wiki 知识空间（建空间需 user_access_token / OAuth）─

    def bootstrap_wiki_space(self, user_token: str, space_name: str = "",
                             description: str = "") -> dict:
        """建 Wiki 空间 + 每分类一个节点。建空间必须 user_token。幂等：已建则跳过。返回 targets。"""
        if not user_token:
            raise ValueError("建 Wiki 知识空间必须提供 user_access_token（见 OAuth 流程）")
        t = self.load_targets()
        t["mode"] = "wiki"
        space_name = space_name or self.tx.space_name
        if not t.get("space_id"):
            t["space_id"] = self.w.create_wiki_space(space_name, description, user_token)
            self._save(t)
        nm: dict = t.setdefault("wiki_node_map", {})
        for path in self.tx.all_folder_paths():
            if path in nm:
                continue
            node = self.w.create_wiki_node(t["space_id"], title=path, obj_type="docx",
                                           user_token=user_token)
            nm[path] = node["node_token"]
            self._save(t)
        return t

    # ── 便于人工核对 ──────────────────────────────────────

    def summary(self, targets: Optional[dict] = None) -> str:
        t = targets or self.load_targets()
        lines = [f"目标形态: {t.get('mode') or '(未初始化)'}"]
        if t.get("root_token"):
            lines.append(f"云空间根文件夹 token: {t['root_token']}")
        if t.get("space_id"):
            lines.append(f"Wiki 空间 space_id: {t['space_id']}")
        fm = t.get("folder_map") or {}
        for path in self.tx.all_folder_paths():
            token = fm.get(path) or (t.get("wiki_node_map") or {}).get(path) or "(缺)"
            lines.append(f"  {path:16s} -> {token}")
        return "\n".join(lines)
