"""跨模块共享的数据模型与枚举。

`SourceItem` 是所有连接器统一输出的中间对象，管线对源系统无感知。
`Stage` 是台账里记录的处理阶段，支持断点续跑（只重跑未完成阶段）。
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    LOCAL = "local"
    SHAREPOINT = "sharepoint"
    WEDRIVE = "wedrive"
    WECOM_CHAT = "wecom_chat"


class Stage(str, Enum):
    """管线阶段。台账以此为断点续跑依据，顺序推进。"""
    DISCOVERED = "discovered"      # 已被连接器发现，尚未下载
    EXTRACTED = "extracted"        # 已下载 + 文本提取
    DEDUPED = "deduped"            # 已参与去重判定
    CLASSIFIED = "classified"      # 已 AI 分类/元数据抽取
    CONFIRMED = "confirmed"        # 人工确认通过（或高置信自动通过）
    LOADED = "loaded"             # 已写入飞书
    FAILED = "failed"             # 某阶段失败，error_detail 记录原因
    SKIPPED_DUPLICATE = "skipped_duplicate"  # 判为重复，不入库


class DedupVerdict(str, Enum):
    UNIQUE = "unique"
    EXACT_DUPLICATE = "exact_duplicate"       # SHA256 完全相同
    NEAR_DUPLICATE = "near_duplicate"         # MinHash 近似
    SEMANTIC_CANDIDATE = "semantic_candidate"  # 嵌入相似，需人工判定


class Permission(BaseModel):
    """源系统的一条权限记录，用于映射到飞书协作者。"""
    principal: str                 # 用户/组标识（邮箱、userid 等）
    role: str = "view"             # view / edit / full_access（已归一到飞书语义）


class SourceItem(BaseModel):
    """连接器统一输出的中间对象。"""
    source_type: SourceType
    source_id: str                 # 源系统内的稳定唯一 id（用于幂等）
    source_path: str               # 源路径或 URL（可读，回链用）
    original_name: str
    size: int = 0
    content_sha256: Optional[str] = None
    local_blob_path: Optional[str] = None   # 下载后的本地缓存路径
    author: Optional[str] = None
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    permissions: list[Permission] = Field(default_factory=list)
    raw_metadata: dict[str, Any] = Field(default_factory=dict)

    def stable_key(self) -> str:
        """跨批次幂等主键：source_type + source_id。"""
        return f"{self.source_type.value}:{self.source_id}"


class ClassificationResult(BaseModel):
    """Claude 结构化输出结果（分类 + 元数据）。"""
    category: str                  # 必须是 taxonomy 的合法 path 之一
    confidence: float = 0.0
    rationale: str = ""
    needs_human_review: bool = True
    title: str = ""
    doc_type: str = ""
    doc_date: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    summary: str = ""
    obsolete_flag: bool = False
