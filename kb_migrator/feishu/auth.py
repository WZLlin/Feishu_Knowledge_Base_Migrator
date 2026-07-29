"""飞书认证。

- tenant_access_token：应用身份，多数读写用它（自动缓存 + 提前刷新）。
- user_access_token：建知识空间等必须以用户身份调用；走 OAuth 授权码流程，
  此处提供换取/刷新逻辑，token 由 Web 控制台的回调保存后注入。
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Optional

import httpx

_BASE = "https://open.feishu.cn/open-apis"
_ACCOUNTS = "https://accounts.feishu.cn"


class TenantTokenProvider:
    """tenant_access_token 提供者，带内存缓存与提前 5 分钟刷新。"""

    def __init__(self, app_id: str, app_secret: str, client: httpx.Client | None = None):
        self.app_id = app_id
        self.app_secret = app_secret
        self._client = client or httpx.Client(timeout=30)
        self._token: Optional[str] = None
        self._expire_at: float = 0.0
        self._lock = threading.Lock()

    def token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._expire_at - 300:
                return self._token
            resp = self._client.post(
                f"{_BASE}/auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.app_secret},
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                raise RuntimeError(f"获取 tenant_access_token 失败: {data}")
            self._token = data["tenant_access_token"]
            self._expire_at = time.time() + int(data.get("expire", 7200))
            return self._token


def build_authorize_url(app_id: str, redirect_uri: str, scope: str = "", state: str = "") -> str:
    """构造用户授权 URL（获取 code）。"""
    from urllib.parse import urlencode

    params = {"client_id": app_id, "redirect_uri": redirect_uri, "response_type": "code"}
    if scope:
        params["scope"] = scope
    if state:
        params["state"] = state
    return f"{_ACCOUNTS}/open-apis/authen/v1/authorize?{urlencode(params)}"


def exchange_user_token(app_id: str, app_secret: str, code: str, redirect_uri: str,
                        client: httpx.Client | None = None) -> dict:
    """用授权码换取 user_access_token（含 refresh_token，如授予 offline_access）。"""
    client = client or httpx.Client(timeout=30)
    resp = client.post(
        f"{_ACCOUNTS}/oauth/v3/token",
        data={
            "grant_type": "authorization_code",
            "client_id": app_id,
            "client_secret": app_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        },
    )
    resp.raise_for_status()
    return resp.json()


def refresh_user_token(app_id: str, app_secret: str, refresh_token: str,
                       client: httpx.Client | None = None) -> dict:
    """用 refresh_token 换取新用户令牌；OAuth 授权需包含 offline_access。"""
    client = client or httpx.Client(timeout=30)
    resp = client.post(
        f"{_ACCOUNTS}/oauth/v3/token",
        data={
            "grant_type": "refresh_token",
            "client_id": app_id,
            "client_secret": app_secret,
            "refresh_token": refresh_token,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(
            f"刷新 user_access_token 失败：{data.get('error_description') or data['error']}"
        )
    return data


# ── user_access_token 本地持久化（供 CLI bootstrap --wiki 复用 OAuth 结果）──

def save_user_token(path: str, token_data: dict) -> None:
    """把 OAuth 换取的 token 落盘（含获取时间戳，便于判断过期）。文件应被 .gitignore。"""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    record = dict(token_data)
    record["obtained_at"] = time.time()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def load_user_token(path: str, app_id: str = "", app_secret: str = "",
                    client: httpx.Client | None = None) -> Optional[str]:
    """读取有效用户令牌；过期时有 refresh_token 则自动续期，否则返回 None。"""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    # 兼容 v3 返回结构：顶层字段或 data 包裹字段。
    token_data = data.get("data") or data
    token = token_data.get("access_token")
    expires_in = int(token_data.get("expires_in") or data.get("expires_in") or 0)
    obtained_at = float(data.get("obtained_at") or 0)
    expired = bool(
        token and expires_in and obtained_at
        and time.time() >= obtained_at + expires_in - 300
    )
    if token and not expired:
        return token
    refresh_token = (
        token_data.get("refresh_token") or data.get("refresh_token") or ""
    )
    if not (refresh_token and app_id and app_secret):
        return None
    try:
        refreshed = refresh_user_token(
            app_id, app_secret, refresh_token, client=client
        )
        # 部分服务只在首次授权返回 refresh_token，续期响应缺失时沿用旧值。
        refreshed.setdefault("refresh_token", refresh_token)
        save_user_token(path, refreshed)
        return (
            refreshed.get("access_token")
            or (refreshed.get("data") or {}).get("access_token")
        )
    except (httpx.HTTPError, RuntimeError, ValueError):
        return None
