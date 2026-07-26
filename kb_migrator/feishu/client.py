"""飞书底层 HTTP 客户端：统一鉴权头、限流、错误分类与重试。

把飞书返回码归一为「可重试 / 不可重试」：
- HTTP 429 或返回码 99991400/1061045 等 -> RetryableError（尊重 Retry-After）；
- 其他非 0 业务码 -> FeishuAPIError（不可重试，交上层记 error_detail）。
"""
from __future__ import annotations

from typing import Any, Optional

import httpx

from ..utils.ratelimit import RateLimiterRegistry, RetryableError, with_retry
from .auth import TenantTokenProvider

_BASE = "https://open.feishu.cn/open-apis"

# 归类为可重试的飞书业务码（限频 / 资源争用）
_RETRYABLE_CODES = {99991400, 1061045, 1069923}


class FeishuAPIError(Exception):
    def __init__(self, code: int, msg: str, payload: Any = None):
        super().__init__(f"[{code}] {msg}")
        self.code = code
        self.msg = msg
        self.payload = payload


class FeishuClient:
    def __init__(self, app_id: str, app_secret: str, http: httpx.Client | None = None):
        self.http = http or httpx.Client(timeout=60)
        self.tenant = TenantTokenProvider(app_id, app_secret, self.http)
        self.limiter = RateLimiterRegistry()

    def _headers(self, user_token: str | None = None) -> dict[str, str]:
        token = user_token or self.tenant.token()
        return {"Authorization": f"Bearer {token}"}

    def call(
        self,
        method: str,
        path: str,
        *,
        bucket: str = "default",
        json: dict | None = None,
        params: dict | None = None,
        data: dict | None = None,
        files: dict | None = None,
        user_token: str | None = None,
        expect_json: bool = True,
    ) -> dict | httpx.Response:
        """带限流 + 重试的单次调用。path 以 / 开头，拼接到 open-apis 后。"""

        def _do() -> dict | httpx.Response:
            self.limiter.acquire(bucket)
            url = f"{_BASE}{path}"
            headers = self._headers(user_token)
            try:
                resp = self.http.request(
                    method, url, headers=headers, json=json,
                    params=params, data=data, files=files,
                )
            except httpx.TransportError as e:
                raise RetryableError(f"网络错误: {e}")
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                raise RetryableError("429 限频", retry_after=float(ra) if ra else None)
            if resp.status_code >= 500:
                raise RetryableError(f"服务端 {resp.status_code}")
            if not expect_json:
                resp.raise_for_status()
                return resp
            body = resp.json()
            code = body.get("code", 0)
            if code == 0:
                return body
            if code in _RETRYABLE_CODES:
                raise RetryableError(f"限频码 {code}: {body.get('msg')}")
            raise FeishuAPIError(code, body.get("msg", ""), body)

        return with_retry(_do)
