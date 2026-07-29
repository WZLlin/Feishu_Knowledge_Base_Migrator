"""企业微信微盘（WeDrive）连接器。

要点（对齐设计边界）：
- 认证：gettoken(corpid+secret)，token 2h；服务器 IP 须在可信 IP 白名单；
- 列举：/cgi-bin/wedrive/file_list（file_type 1=夹 2=文件 3=文档…），递归文件夹；
- 下载：/cgi-bin/wedrive/file_download —— **仅普通文件**，两步式（拿 download_url
  + cookie 再 GET 取字节）；文件夹/原生微文档不支持（原生微文档无法批量导出）；
- 应用需被加入每个相关微盘空间成员方可列举。
"""
from __future__ import annotations

import hashlib
import os
import time
from typing import Iterator, Optional

import httpx

from ..models import SourceItem, SourceType
from ..utils.ratelimit import RetryableError, with_retry
from .base import BaseConnector

_QYAPI = "https://qyapi.weixin.qq.com/cgi-bin"

# file_type 语义
_FT_FOLDER = 1
_FT_FILE = 2          # 普通文件（可下载）
# 3=文档 4=表格 5=收集表 —— 原生智能文档，file_download 不支持，仅登记不下载


class WeDriveConnector(BaseConnector):
    source_name = "wedrive"

    def __init__(self, corp_id: str, secret: str, space_ids: list[str],
                 work_dir: str, http: httpx.Client | None = None):
        self.corp_id = corp_id
        self.secret = secret
        self.space_ids = space_ids       # 需迁移的微盘空间（应用须为其成员）
        self.work_dir = work_dir
        self.http = http or httpx.Client(timeout=120, follow_redirects=True)
        self._token: Optional[str] = None
        self._token_exp: float = 0.0

    def _access_token(self) -> str:
        if self._token and time.time() < self._token_exp - 300:
            return self._token
        resp = self.http.get(
            f"{_QYAPI}/gettoken",
            params={"corpid": self.corp_id, "corpsecret": self.secret},
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode", 0) != 0:
            raise RuntimeError(f"企业微信 gettoken 失败: {data}")
        self._token = data["access_token"]
        self._token_exp = time.time() + int(data.get("expires_in", 7200))
        return self._token

    def _post(self, path: str, payload: dict) -> dict:
        def _do() -> dict:
            resp = self.http.post(
                f"{_QYAPI}{path}",
                params={"access_token": self._access_token()}, json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
            errcode = data.get("errcode", 0)
            if errcode == 0:
                return data
            if errcode in (-1, 45009):     # 系统繁忙 / 频率限制
                raise RetryableError(f"企业微信可重试错误 {errcode}")
            if errcode == 42001:           # token 过期
                self._token = None
                raise RetryableError("access_token 过期，重取")
            raise RuntimeError(f"企业微信错误 {errcode}: {data.get('errmsg')}")
        return with_retry(_do)

    # ── 列举 ──────────────────────────────────────────────

    def _list_folder(self, space_id: str, father_id: str) -> Iterator[dict]:
        start = 0
        while True:
            data = self._post("/wedrive/file_list", {
                "spaceid": space_id, "fatherid": father_id,
                "sort_type": 1, "start": start, "limit": 1000,
            })
            items = data.get("file_list", {}).get("item", [])
            for it in items:
                it["_space_id"] = space_id
                yield it
                if it.get("file_type") == _FT_FOLDER:
                    yield from self._list_folder(space_id, it["fileid"])
            if not data.get("has_more"):
                break
            start = data.get("next_start", start + len(items))

    def discover(self) -> Iterator[SourceItem]:
        for space_id in self.space_ids:
            for it in self._list_folder(space_id, space_id):  # 根 fatherid=spaceid
                ftype = it.get("file_type")
                if ftype == _FT_FOLDER:
                    continue
                downloadable = ftype == _FT_FILE
                yield SourceItem(
                    source_type=SourceType.WEDRIVE,
                    source_id=it["fileid"],
                    source_path=it.get("url", f"wedrive://{it['_space_id']}/{it['fileid']}"),
                    original_name=it.get("file_name", it["fileid"]),
                    size=int(it.get("file_size", 0)),
                    content_sha256=it.get("sha") or None,
                    raw_metadata={
                        "space_id": it["_space_id"], "file_type": ftype,
                        "file_id": it["fileid"],
                        "downloadable": downloadable, "md5": it.get("md5", ""),
                    },
                )

    # ── 下载 ──────────────────────────────────────────────

    def fetch(self, item: SourceItem) -> SourceItem:
        if not item.raw_metadata.get("downloadable"):
            # 原生微文档：无法通过 API 导出，登记但不下载（编排器记 note）
            item.raw_metadata["skip_reason"] = "原生微文档无法批量导出（平台限制）"
            return item
        file_id = item.raw_metadata.get("file_id") or item.source_id.split("#rev", 1)[0]
        data = self._post("/wedrive/file_download", {"fileid": file_id})
        download_url = data["download_url"]
        cookie = {data.get("cookie_name", ""): data.get("cookie_value", "")}
        resp = self.http.get(download_url, cookies={k: v for k, v in cookie.items() if k})
        resp.raise_for_status()
        os.makedirs(self.work_dir, exist_ok=True)
        local = os.path.join(self.work_dir, f"{file_id}_{item.original_name}")
        with open(local, "wb") as fp:
            fp.write(resp.content)
        item.local_blob_path = local
        item.content_sha256 = hashlib.sha256(resp.content).hexdigest()
        return item
