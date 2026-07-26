r"""本地文件夹 / 文件共享连接器（MVP 主源）。

要点（对齐设计中「本地文件夹」连接器的关键限制）：
- UNC 路径 \\server\share 与本地路径统一处理；
- Windows 长路径用 \\?\ 前缀规避 MAX_PATH(260)；
- 跳过 Office 锁文件（~$ 前缀）与隐藏/临时文件；
- st_ctime 在 Windows 是创建时间，直接用；
- source_id 用「相对根目录的规范化相对路径」保证跨批次幂等稳定。
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from ..models import SourceItem, SourceType
from ..utils.naming import long_path
from .base import BaseConnector, sha256_of_file

# 需要迁移的常见知识类文档后缀
_ALLOWED_EXT = {
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".pdf", ".txt", ".md", ".csv", ".rtf",
}
# 跳过的文件名模式
_SKIP_PREFIX = ("~$", ".~", "._")


class LocalFolderConnector(BaseConnector):
    source_name = "local"

    def __init__(self, root: str, allowed_ext: set[str] | None = None):
        self.root = os.path.abspath(root)
        self.allowed_ext = allowed_ext or _ALLOWED_EXT

    def _should_skip(self, name: str) -> bool:
        if name.startswith(_SKIP_PREFIX):
            return True
        ext = os.path.splitext(name)[1].lower()
        return ext not in self.allowed_ext

    @staticmethod
    def _strip_long_prefix(path: str) -> str:
        r"""去掉 os.walk 在长路径场景引入的 \\?\ 或 \\?\UNC\ 前缀。"""
        if path.startswith("\\\\?\\UNC\\"):
            return "\\\\" + path[len("\\\\?\\UNC\\"):]
        if path.startswith("\\\\?\\"):
            return path[len("\\\\?\\"):]
        return path

    def _rel_id(self, abspath: str) -> str:
        """相对根目录的正斜杠相对路径，作为稳定 source_id。"""
        rel = os.path.relpath(self._strip_long_prefix(abspath), self.root)
        return rel.replace(os.sep, "/")

    def discover(self) -> Iterator[SourceItem]:
        for dirpath, _dirnames, filenames in os.walk(long_path(self.root)):
            for name in filenames:
                if self._should_skip(name):
                    continue
                abspath = self._strip_long_prefix(os.path.join(dirpath, name))
                try:
                    st = os.stat(long_path(abspath))
                except OSError:
                    # 锁定/无权限文件跳过（由编排器汇总告警）
                    continue
                yield SourceItem(
                    source_type=SourceType.LOCAL,
                    source_id=self._rel_id(abspath),
                    source_path=abspath,
                    original_name=name,
                    size=st.st_size,
                    created_at=datetime.fromtimestamp(st.st_ctime, tz=timezone.utc),
                    modified_at=datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
                    raw_metadata={"root": self.root},
                )

    def fetch(self, item: SourceItem) -> SourceItem:
        """本地文件无需下载：原地即缓存，计算 sha256 回填。"""
        src = long_path(item.source_path)
        item.local_blob_path = item.source_path
        item.content_sha256 = sha256_of_file(src)
        return item
