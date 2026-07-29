"""SharePoint 连接器（Microsoft Graph，client_credentials 应用权限）。

- 认证：Entra ID 应用权限 Sites.Read.All + Files.Read.All（需管理员同意）；
- 列举：/sites -> /sites/{id}/drives -> /drives/{id}/root/children 递归；
- 下载：/drives/{id}/items/{id}/content（302 到预签名 URL，httpx 默认跟随）；
- 限频：遵守 429 Retry-After；大批量优先 delta（此处提供全量递归 MVP）。
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Iterator, Optional

import httpx

from ..models import Permission, SourceItem, SourceType
from ..utils.ratelimit import RetryableError, with_retry
from .base import BaseConnector

_GRAPH = "https://graph.microsoft.com/v1.0"
_AUTH = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"


class SharePointConnector(BaseConnector):
    source_name = "sharepoint"

    def __init__(self, tenant_id: str, client_id: str, client_secret: str,
                 work_dir: str, site_filter: Optional[str] = None,
                 http: httpx.Client | None = None):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.work_dir = work_dir
        self.site_filter = site_filter        # 只迁指定站点(搜索关键字)，None=全部根站点
        self.http = http or httpx.Client(timeout=120, follow_redirects=True)
        self._token: Optional[str] = None

    # ── 认证 ──────────────────────────────────────────────

    def _access_token(self) -> str:
        if self._token:
            return self._token
        try:
            import msal

            app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}",
                client_credential=self.client_secret,
            )
            result = app.acquire_token_for_client(
                scopes=["https://graph.microsoft.com/.default"]
            )
        except ImportError:
            # 无 msal 时直接走 OAuth token 端点
            resp = self.http.post(
                _AUTH.format(tenant=self.tenant_id),
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": "https://graph.microsoft.com/.default",
                    "grant_type": "client_credentials",
                },
            )
            resp.raise_for_status()
            result = resp.json()
        if "access_token" not in result:
            raise RuntimeError(f"Graph 认证失败: {result.get('error_description', result)}")
        self._token = result["access_token"]
        return self._token

    def health_check(self) -> dict:
        """真实校验 Entra 凭证以及 Microsoft Graph 站点读取权限。"""
        try:
            token_resp = self.http.post(
                _AUTH.format(tenant=self.tenant_id),
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": "https://graph.microsoft.com/.default",
                    "grant_type": "client_credentials",
                },
            )
            if token_resp.status_code >= 400:
                try:
                    body = token_resp.json()
                    detail = str(body.get("error_description") or body.get("error") or "")
                except Exception:  # noqa: BLE001
                    detail = token_resp.text[:300]
                return {
                    "ready": False,
                    "status": "auth",
                    "label": "凭证无效",
                    "message": "SharePoint 应用鉴权失败，请检查 Tenant ID、Client ID 和 Client Secret。",
                    "detail": detail[:400],
                    "status_code": token_resp.status_code,
                }
            token = str(token_resp.json().get("access_token") or "")
            if not token:
                return {
                    "ready": False,
                    "status": "auth",
                    "label": "凭证无效",
                    "message": "Microsoft 登录接口未返回 access_token，请检查 SharePoint 应用凭证。",
                }

            graph_resp = self.http.get(
                f"{_GRAPH}/sites/root?$select=id,displayName",
                headers={"Authorization": f"Bearer {token}"},
            )
            if graph_resp.status_code == 403:
                return {
                    "ready": False,
                    "status": "permission",
                    "label": "权限不足",
                    "message": "SharePoint 已通过鉴权，但缺少 Sites.Read.All/Files.Read.All 管理员授权。",
                    "status_code": 403,
                }
            if graph_resp.status_code == 401:
                return {
                    "ready": False,
                    "status": "auth",
                    "label": "凭证无效",
                    "message": "Microsoft Graph 拒绝了访问令牌，请检查应用凭证与租户配置。",
                    "status_code": 401,
                }
            graph_resp.raise_for_status()
            site = graph_resp.json()
            return {
                "ready": True,
                "status": "ready",
                "label": "连接正常",
                "message": "SharePoint 凭证和 Microsoft Graph 站点读取权限均已通过校验。",
                "site": site.get("displayName") or site.get("id") or "",
            }
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            return {
                "ready": False,
                "status": "endpoint",
                "label": "访问地址异常",
                "message": "无法访问 Microsoft 登录或 Graph 接口，请检查网络、代理和防火墙。",
                "detail": str(exc)[:400],
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "ready": False,
                "status": "error",
                "label": "连接异常",
                "message": "SharePoint 连接校验失败，请检查应用配置和 Microsoft Graph 服务状态。",
                "detail": str(exc)[:400],
            }

    def _get(self, url: str, **kw) -> httpx.Response:
        def _do() -> httpx.Response:
            headers = {"Authorization": f"Bearer {self._access_token()}"}
            resp = self.http.get(url, headers=headers, **kw)
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                raise RetryableError("Graph 429", retry_after=float(ra) if ra else None)
            if resp.status_code >= 500:
                raise RetryableError(f"Graph {resp.status_code}")
            resp.raise_for_status()
            return resp
        return with_retry(_do)

    def _paged(self, url: str) -> Iterator[dict]:
        """跟随 @odata.nextLink 分页。"""
        while url:
            body = self._get(url).json()
            yield from body.get("value", [])
            url = body.get("@odata.nextLink", "")

    # ── 列举 ──────────────────────────────────────────────

    def _sites(self) -> Iterator[dict]:
        if self.site_filter:
            yield from self._paged(f"{_GRAPH}/sites?search={self.site_filter}")
        else:
            yield from self._paged(f"{_GRAPH}/sites?$filter=siteCollection/root ne null")

    def _walk_children(self, drive_id: str, item_id: str = "root") -> Iterator[dict]:
        url = f"{_GRAPH}/drives/{drive_id}/items/{item_id}/children"
        for it in self._paged(url):
            if "folder" in it:
                yield from self._walk_children(drive_id, it["id"])
            elif "file" in it:
                it["_drive_id"] = drive_id
                yield it

    def discover(self) -> Iterator[SourceItem]:
        for site in self._sites():
            site_id = site["id"]
            for drive in self._paged(f"{_GRAPH}/sites/{site_id}/drives"):
                for f in self._walk_children(drive["id"]):
                    yield self._to_item(f, site.get("webUrl", ""))

    def _to_item(self, f: dict, site_url: str) -> SourceItem:
        file_facet = f.get("file", {})
        return SourceItem(
            source_type=SourceType.SHAREPOINT,
            source_id=f"{f['_drive_id']}/{f['id']}",
            source_path=f.get("webUrl", site_url),
            original_name=f.get("name", ""),
            size=int(f.get("size", 0)),
            content_sha256=(file_facet.get("hashes", {}) or {}).get("sha256Hash", "") or None,
            author=(f.get("createdBy", {}).get("user", {}) or {}).get("displayName"),
            created_at=_parse_dt(f.get("createdDateTime")),
            modified_at=_parse_dt(f.get("lastModifiedDateTime")),
            raw_metadata={"drive_id": f["_drive_id"], "graph_id": f["id"],
                          "mime": file_facet.get("mimeType", ""), "etag": f.get("eTag", "")},
        )

    # ── 下载 + 权限 ───────────────────────────────────────

    def fetch(self, item: SourceItem) -> SourceItem:
        import os

        drive_id = item.raw_metadata["drive_id"]
        gid = item.raw_metadata["graph_id"]
        resp = self._get(f"{_GRAPH}/drives/{drive_id}/items/{gid}/content")
        os.makedirs(self.work_dir, exist_ok=True)
        local = os.path.join(self.work_dir, f"{gid}_{item.original_name}")
        with open(local, "wb") as fp:
            fp.write(resp.content)
        item.local_blob_path = local
        item.content_sha256 = hashlib.sha256(resp.content).hexdigest()
        item.permissions, collected = self._permissions(drive_id, gid)
        item.raw_metadata["permissions_collected"] = collected
        return item

    def _permissions(self, drive_id: str, gid: str) -> tuple[list[Permission], bool]:
        out: list[Permission] = []
        try:
            body = self._get(f"{_GRAPH}/drives/{drive_id}/items/{gid}/permissions").json()
        except Exception:
            # 失败时不能把已有权限快照误覆盖为空；由上层保留旧快照。
            return out, False
        for p in body.get("value", []):
            granted = p.get("grantedToV2", {}) or {}
            principal = ((granted.get("user") or {}).get("email")
                         or (granted.get("user") or {}).get("displayName")
                         or (granted.get("siteGroup") or {}).get("displayName"))
            if not principal:
                continue
            roles = p.get("roles", [])
            role = "edit" if any(r in ("write", "owner") for r in roles) else "view"
            out.append(Permission(principal=principal, role=role))
        return out, True


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
