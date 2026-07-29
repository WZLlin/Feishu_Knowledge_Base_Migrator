"""飞书用户 OAuth token 的过期判断与自动续期。"""
from __future__ import annotations

import json
import time

from kb_migrator.feishu.auth import load_user_token


class _Response:
    def __init__(self, payload: dict):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Client:
    def __init__(self, payload: dict):
        self.payload = payload
        self.calls = []

    def post(self, url: str, data: dict):
        self.calls.append((url, data))
        return _Response(self.payload)


def test_expired_user_token_without_refresh_token_is_rejected(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({
        "access_token": "expired",
        "expires_in": 3600,
        "obtained_at": time.time() - 7200,
    }), encoding="utf-8")

    assert load_user_token(str(path)) is None


def test_expired_user_token_is_refreshed_and_persisted(tmp_path):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({
        "access_token": "expired",
        "refresh_token": "refresh-old",
        "expires_in": 3600,
        "obtained_at": time.time() - 7200,
    }), encoding="utf-8")
    client = _Client({"access_token": "fresh", "expires_in": 7200})

    assert load_user_token(
        str(path), "app-id", "app-secret", client=client
    ) == "fresh"
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["access_token"] == "fresh"
    assert saved["refresh_token"] == "refresh-old"
    assert saved["obtained_at"] > time.time() - 10
    assert client.calls[0][1]["grant_type"] == "refresh_token"

