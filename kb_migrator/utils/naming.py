"""文件名归一与规范化。

规范：YYYY-MM-DD_<类型>_<简述标题>_v<版本>
- 保留中文（知识库以中文为主），只清理文件系统/飞书非法字符与多余空白；
- 保留原始文件名由调用方存入元数据，此处只产出规范名。
"""
from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Optional

# 飞书文件名与常见文件系统均不接受的字符
_ILLEGAL = r'[\\/:*?"<>|\r\n\t]'
# 全角空格等归一后统一压缩的空白
_WS = re.compile(r"\s+")


def slugify(text: str, max_len: int = 60) -> str:
    """清洗为安全的标题片段：NFC 归一、去非法字符、压缩空白、限长。

    保留中文与常见可读字符；不做转拼音（保持中文可读性）。
    """
    if not text:
        return "untitled"
    text = unicodedata.normalize("NFC", text)
    text = re.sub(_ILLEGAL, "", text)
    text = text.replace("　", " ")          # 全角空格 -> 半角
    text = _WS.sub(" ", text).strip(" ._-")
    text = text.replace(" ", "-")
    if not text:
        return "untitled"
    return text[:max_len].rstrip(" ._-") or "untitled"


def _coerce_date(value: Optional[str | date | datetime]) -> str:
    if value is None:
        return date.today().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    # 尽量归一到 YYYY-MM-DD；无法解析则退回今天
    m = re.search(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    return date.today().isoformat()


def canonical_name(
    *,
    doc_type: str,
    title: str,
    doc_date: Optional[str | date | datetime] = None,
    version: str = "1",
    ext: str = "",
) -> str:
    """按命名规范拼装规范文件名。ext 传入时补后缀（含点或不含点均可）。"""
    parts = [
        _coerce_date(doc_date),
        slugify(doc_type or "doc", max_len=20),
        slugify(title, max_len=60),
        f"v{version or '1'}",
    ]
    name = "_".join(parts)
    if ext:
        ext = ext if ext.startswith(".") else f".{ext}"
        name = f"{name}{ext}"
    return name


def long_path(path: str) -> str:
    r"""Windows 下为超过 MAX_PATH(260) 的绝对路径加 \\?\ 前缀，规避长路径限制。

    非 Windows 或已带前缀 / 相对路径 / UNC 直接原样返回。
    """
    import os

    if os.name != "nt":
        return path
    if path.startswith("\\\\?\\"):
        return path
    if not os.path.isabs(path):
        return path
    if path.startswith("\\\\"):  # UNC \\server\share
        return "\\\\?\\UNC\\" + path[2:]
    return "\\\\?\\" + path
