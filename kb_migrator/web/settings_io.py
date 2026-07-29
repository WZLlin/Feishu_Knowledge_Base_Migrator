""".env 文件的读取 / 行级写入 / 脱敏，供 Web 控制台「配置」页使用。

原则：
- 凭证仍只落在 .env（不入代码、不入台账、不明文回显）；
- 写入是**行级 upsert**：保留原文件的注释与顺序，只更新/追加传入的键，不整文件重写；
- 只覆盖「用户确实填了新值」的键；脱敏占位（••••）回传时视为「不修改」；
- 写完调用 reload_settings() 让 get_settings 缓存失效、热生效。
"""
from __future__ import annotations

import os
from pathlib import Path

# 配置页可编辑的键 -> (分组, 是否敏感)。敏感项 GET 时脱敏。
FIELD_SPEC: dict[str, tuple[str, bool]] = {
    # 飞书
    "FEISHU_APP_ID": ("飞书", False),
    "FEISHU_APP_SECRET": ("飞书", True),
    "FEISHU_REDIRECT_URI": ("飞书", False),
    "KBM_FEISHU_OAUTH_SCOPE": ("飞书", False),
    # Claude
    "ANTHROPIC_API_KEY": ("Claude", True),
    "ANTHROPIC_BASE_URL": ("Claude", False),
    "KBM_CLAUDE_MODEL": ("Claude", False),
    "KBM_CONFIDENCE_THRESHOLD": ("Claude", False),
    "KBM_IDENTITY_MAP_FILE": ("治理", False),
    # 企业微信微盘
    "WECOM_CORP_ID": ("企业微信", False),
    "WECOM_WEDRIVE_SECRET": ("企业微信", True),
    # 企业微信群聊（会话存档）
    "WECOM_CHAT_ARCHIVE_SECRET": ("企业微信群聊", True),
    "WECOM_CHAT_PRIVATE_KEY_FILE": ("企业微信群聊", False),   # 只填路径，不粘贴私钥
    "WECOM_CHAT_SDK_LIB": ("企业微信群聊", False),            # 原生存档库路径（只填路径）
    # SharePoint
    "MS_TENANT_ID": ("SharePoint", False),
    "MS_CLIENT_ID": ("SharePoint", False),
    "MS_CLIENT_SECRET": ("SharePoint", True),
    "MS_SITE_FILTER": ("SharePoint", False),                 # 站点收窄关键字（可空=全部）
}

_MASK = "••••••"


def _env_path() -> str:
    return os.environ.get("KBM_ENV_FILE", ".env")


def read_env(path: str | None = None) -> dict[str, str]:
    """读 .env 为 dict（忽略注释/空行）。文件不存在返回空。"""
    path = path or _env_path()
    out: dict[str, str] = {}
    if not os.path.exists(path):
        return out
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        key, _, val = s.partition("=")
        # 去掉行内注释与首尾空白（值本身含 # 的场景少见，简单处理）
        val = val.split("#", 1)[0].strip() if " #" in val else val.strip()
        out[key.strip()] = val
    return out


def mask(value: str, sensitive: bool) -> str:
    """脱敏：敏感且已配置 -> ••••••+末2位；非敏感原样；未配置 -> 空。"""
    if not value:
        return ""
    if not sensitive:
        return value
    tail = value[-2:] if len(value) >= 2 else ""
    return f"{_MASK}{tail}"


def masked_settings(path: str | None = None) -> dict[str, dict]:
    """给前端配置页用：每个字段的分组、是否敏感、脱敏后的值、是否已配置。"""
    env = read_env(path)
    out: dict[str, dict] = {}
    for key, (group, sensitive) in FIELD_SPEC.items():
        val = env.get(key, "")
        out[key] = {
            "group": group,
            "sensitive": sensitive,
            "value": mask(val, sensitive),
            "configured": bool(val),
        }
    return out


def _is_placeholder(new_val: str) -> bool:
    """脱敏占位回传（•••• 开头）视为「未修改」，不覆盖已有值。"""
    return new_val.startswith(_MASK)


def write_env(updates: dict[str, str], path: str | None = None) -> list[str]:
    """行级 upsert。仅写入白名单内、非空、非脱敏占位的键。返回实际改动的键名。"""
    path = path or _env_path()
    changed: list[str] = []
    # 过滤：白名单 + 非 None + 去空白后非空 + 非脱敏占位
    clean: dict[str, str] = {}
    for k, v in updates.items():
        if k not in FIELD_SPEC or v is None:
            continue
        v = str(v).strip()
        if v == "" or _is_placeholder(v):
            continue
        clean[k] = v
    if not clean:
        return changed

    lines = Path(path).read_text(encoding="utf-8").splitlines() if os.path.exists(path) else []
    seen: set[str] = set()
    for i, line in enumerate(lines):
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        key = s.partition("=")[0].strip()
        if key in clean:
            lines[i] = f"{key}={clean[key]}"
            seen.add(key)
            changed.append(key)
    # 未在文件中出现的键 -> 追加
    for k, v in clean.items():
        if k not in seen:
            lines.append(f"{k}={v}")
            changed.append(k)

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return changed


def reload_settings():
    """清 get_settings 的 lru_cache 并返回新实例，使 .env 改动热生效。"""
    from ..config import get_settings

    get_settings.cache_clear()
    return get_settings()
