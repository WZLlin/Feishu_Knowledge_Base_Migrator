"""来源主体到飞书 open_id 的本地映射。"""
from __future__ import annotations

import json
from pathlib import Path


def load_identity_map(path: str) -> dict[str, str]:
    """加载 ``{来源邮箱/用户ID: 飞书open_id}``；缺文件返回空映射。"""
    if not path or not Path(path).exists():
        return {}
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("身份映射必须是 JSON 对象 {来源主体: 飞书open_id}")
    return {str(k).strip().lower(): str(v).strip() for k, v in data.items()
            if str(k).strip() and str(v).strip()}


def resolve_identity(identity_map: dict[str, str], principal: str) -> str:
    return identity_map.get(principal.strip().lower(), "")
