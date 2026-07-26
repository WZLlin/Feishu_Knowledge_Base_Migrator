"""连接器基类。

约定：
- `discover()` 只做「列举 + 轻量元数据」，不下载正文（便于先盘点、后按优先级下载）；
- `fetch(item)` 负责把正文下载到本地缓存并回填 `local_blob_path` / `content_sha256`；
- 两段式设计支持大批量场景下「先全量盘点、再增量抽取」。
"""
from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

from ..models import SourceItem


def sha256_of_file(path: str | Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


class BaseConnector(ABC):
    source_name: str = "base"

    @abstractmethod
    def discover(self) -> Iterator[SourceItem]:
        """列举源系统内的条目（不下载正文）。"""
        raise NotImplementedError

    @abstractmethod
    def fetch(self, item: SourceItem) -> SourceItem:
        """下载条目正文到本地缓存，回填 local_blob_path 与 content_sha256。"""
        raise NotImplementedError
