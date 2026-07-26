"""分类体系加载与查询。

单一事实来源是 config/taxonomy.yaml。提供：
- 合法分类路径枚举（供 Claude enum 约束与人工确认下拉）；
- 按分类查 owner/steward/复审节奏/保留期（治理元数据继承）。
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class Category:
    path: str
    doc_types: list[str]
    owner: str
    steward: str
    review_months: int
    retention_years: int


class Taxonomy:
    def __init__(self, data: dict):
        self.space_name: str = data.get("space_name", "组织知识库")
        self.triage_path: str = data.get("triage_path", "90 待整理")
        self.archive_path: str = data.get("archive_path", "99 归档")
        self.naming: dict = data.get("naming", {})
        self._categories: list[Category] = [
            Category(
                path=c["path"],
                doc_types=c.get("doc_types", []),
                owner=c.get("owner", ""),
                steward=c.get("steward", ""),
                review_months=int(c.get("review_months", 12)),
                retention_years=int(c.get("retention_years", 5)),
            )
            for c in data.get("categories", [])
        ]

    @classmethod
    def load(cls, path: str | Path) -> "Taxonomy":
        with open(path, "r", encoding="utf-8") as f:
            return cls(yaml.safe_load(f))

    @property
    def categories(self) -> list[Category]:
        return list(self._categories)

    def category_paths(self) -> list[str]:
        """AI 分类 enum 值。含 triage 作为「无法归类」兜底选项。"""
        return [c.path for c in self._categories] + [self.triage_path]

    def get(self, path: str) -> Category | None:
        for c in self._categories:
            if c.path == path:
                return c
        return None

    def all_folder_paths(self) -> list[str]:
        """建目录树时需要创建的全部一级目录（含 triage / archive）。"""
        return [c.path for c in self._categories] + [self.triage_path, self.archive_path]
