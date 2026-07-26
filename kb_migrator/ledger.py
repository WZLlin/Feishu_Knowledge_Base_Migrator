"""迁移台账（SQLite）—— 幂等、追溯、断点恢复的唯一权威。

设计要点：
- `items` 表：文件级迁移记录，主键 `stable_key`(source_type:source_id)，保证跨批次幂等；
- `chat_migrations` 表：企业微信群聊迁移记录，主键 `wecom_chat_id`，含增量游标；
- 任一阶段可失败重试；`stage` 字段推进，已 LOADED 的条目重跑时跳过；
- 所有写操作即时提交，进程崩溃可断点续跑。

正式环境可将同样的 schema 迁到 PostgreSQL；此处用标准库 sqlite3 以零依赖跑通 MVP。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from .models import SourceItem, Stage

_SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    stable_key      TEXT PRIMARY KEY,
    source_type     TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    source_path     TEXT,
    original_name   TEXT,
    size            INTEGER DEFAULT 0,
    content_sha256  TEXT,
    local_blob_path TEXT,
    dedup_cluster_id TEXT,
    dedup_verdict   TEXT,
    stage           TEXT NOT NULL,
    category        TEXT,
    confidence      REAL,
    canonical_name  TEXT,
    feishu_token    TEXT,
    feishu_url      TEXT,
    wiki_node_token TEXT,
    metadata_json   TEXT,
    error_detail    TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_items_stage ON items(stage);
CREATE INDEX IF NOT EXISTS idx_items_sha ON items(content_sha256);
CREATE INDEX IF NOT EXISTS idx_items_cluster ON items(dedup_cluster_id);

CREATE TABLE IF NOT EXISTS chat_migrations (
    wecom_chat_id       TEXT PRIMARY KEY,
    chat_name_original  TEXT,
    migration_status    TEXT NOT NULL,
    feishu_space_id     TEXT,
    feishu_node_token   TEXT,   -- 「当前可写节点」指针，写满自动新开
    feishu_url          TEXT,
    last_message_seq    TEXT,   -- 增量游标
    member_snapshot     TEXT,   -- JSON: 群成员 userid 列表快照
    error_detail        TEXT,
    first_migrated_at   TEXT,
    last_synced_at      TEXT
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Ledger:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_SCHEMA)
        self.conn.commit()
        self._migrate()

    def _migrate(self) -> None:
        """对早于当前 schema 的既有 DB 做幂等列补齐（新建库已含这些列，跳过）。"""
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(items)")}
        for col, decl in (("wiki_node_token", "TEXT"),):
            if col not in cols:
                self.conn.execute(f"ALTER TABLE items ADD COLUMN {col} {decl}")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ── 文件级 items ──────────────────────────────────────

    def upsert_discovered(self, item: SourceItem) -> bool:
        """登记一个新发现的条目。已存在则不覆盖（幂等），返回是否为新增。"""
        key = item.stable_key()
        row = self.conn.execute(
            "SELECT stage FROM items WHERE stable_key=?", (key,)
        ).fetchone()
        if row is not None:
            return False
        now = _now()
        self.conn.execute(
            """INSERT INTO items
               (stable_key, source_type, source_id, source_path, original_name,
                size, content_sha256, local_blob_path, stage, metadata_json,
                created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                key, item.source_type.value, item.source_id, item.source_path,
                item.original_name, item.size, item.content_sha256,
                item.local_blob_path, Stage.DISCOVERED.value,
                json.dumps(item.raw_metadata, ensure_ascii=False, default=str),
                now, now,
            ),
        )
        self.conn.commit()
        return True

    def update(self, stable_key: str, **fields: Any) -> None:
        """更新任意字段并刷新 updated_at。Stage 类型自动转 value。"""
        if "stage" in fields and isinstance(fields["stage"], Stage):
            fields["stage"] = fields["stage"].value
        fields["updated_at"] = _now()
        cols = ", ".join(f"{k}=?" for k in fields)
        self.conn.execute(
            f"UPDATE items SET {cols} WHERE stable_key=?",
            (*fields.values(), stable_key),
        )
        self.conn.commit()

    def get(self, stable_key: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM items WHERE stable_key=?", (stable_key,)
        ).fetchone()

    def items_in_stage(self, stage: Stage) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM items WHERE stage=? ORDER BY created_at", (stage.value,)
        ).fetchall()

    def find_by_sha(self, sha256: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM items WHERE content_sha256=?", (sha256,)
        ).fetchall()

    def pending_review(self) -> list[sqlite3.Row]:
        """人工确认队列：已分类但需人工复核，或去重待判定的条目。"""
        return self.conn.execute(
            """SELECT * FROM items
               WHERE stage=? OR dedup_verdict=?
               ORDER BY updated_at DESC""",
            (Stage.CLASSIFIED.value, "semantic_candidate"),
        ).fetchall()

    def requeue_failed(self, *, error_prefix: str = "load: ",
                       target_stage: Stage = Stage.CONFIRMED) -> int:
        """把 FAILED 且 error_detail 以 error_prefix 开头的条目重排回 target_stage。

        FAILED 是扁平状态，靠 error_detail 前缀区分失败来源（"load: "/"fetch: "）。
        只回捞 load 阶段失败，避免把 fetch/ingest 失败误送写入器。清空 error_detail。
        返回重排条数。
        """
        cur = self.conn.execute(
            "SELECT COUNT(*) c FROM items WHERE stage=? AND error_detail LIKE ?",
            (Stage.FAILED.value, error_prefix + "%"),
        ).fetchone()
        n = cur["c"] if cur else 0
        if n:
            self.conn.execute(
                "UPDATE items SET stage=?, error_detail=NULL, updated_at=? "
                "WHERE stage=? AND error_detail LIKE ?",
                (target_stage.value, _now(), Stage.FAILED.value, error_prefix + "%"),
            )
            self.conn.commit()
        return n

    def stage_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT stage, COUNT(*) c FROM items GROUP BY stage"
        ).fetchall()
        return {r["stage"]: r["c"] for r in rows}

    # ── 群聊 chat_migrations ──────────────────────────────

    def get_chat(self, chat_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM chat_migrations WHERE wecom_chat_id=?", (chat_id,)
        ).fetchone()

    def upsert_chat(self, chat_id: str, **fields: Any) -> None:
        """群聊迁移记录 upsert。member_snapshot 传 list 自动序列化。"""
        if "member_snapshot" in fields and not isinstance(fields["member_snapshot"], str):
            fields["member_snapshot"] = json.dumps(
                fields["member_snapshot"], ensure_ascii=False
            )
        existing = self.get_chat(chat_id)
        if existing is None:
            fields.setdefault("migration_status", "pending")
            fields.setdefault("first_migrated_at", _now())
            cols = ["wecom_chat_id", *fields.keys()]
            vals = [chat_id, *fields.values()]
            placeholders = ",".join("?" * len(cols))
            self.conn.execute(
                f"INSERT INTO chat_migrations ({','.join(cols)}) VALUES ({placeholders})",
                vals,
            )
        else:
            fields["last_synced_at"] = _now()
            cols = ", ".join(f"{k}=?" for k in fields)
            self.conn.execute(
                f"UPDATE chat_migrations SET {cols} WHERE wecom_chat_id=?",
                (*fields.values(), chat_id),
            )
        self.conn.commit()

    def chat_is_completed(self, chat_id: str) -> bool:
        row = self.get_chat(chat_id)
        return row is not None and row["migration_status"] == "completed"
