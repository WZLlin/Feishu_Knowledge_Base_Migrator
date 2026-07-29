"""知识生命周期日期计算。"""
from __future__ import annotations

import calendar
from datetime import date

from ..taxonomy import Category


def parse_doc_date(value: str | None) -> date:
    """解析文档日期；缺失或非法时以当天作为治理周期起点。"""
    if value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            pass
    return date.today()


def add_months(value: date, months: int) -> date:
    month = value.month - 1 + months
    year = value.year + month // 12
    month = month % 12 + 1
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def governance_fields(category: Category | None, doc_date: str | None) -> dict[str, str]:
    """由分类治理规则生成可落库的责任人与生命周期字段。"""
    if category is None:
        return {"owner": "", "steward": "", "review_due_at": "", "retention_due_at": ""}
    base = parse_doc_date(doc_date)
    return {
        "owner": category.owner,
        "steward": category.steward,
        "review_due_at": add_months(base, category.review_months).isoformat(),
        "retention_due_at": add_months(base, category.retention_years * 12).isoformat(),
    }
