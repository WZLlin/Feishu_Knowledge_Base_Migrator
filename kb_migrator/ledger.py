"""迁移台账（SQLite）—— 幂等、追溯、断点恢复的唯一权威。

设计要点：
- `items` 表：文件级迁移记录，主键 `stable_key`(source_type:source_id)，保证跨批次幂等；
- `chat_migrations` 表：企业微信群聊迁移记录，主键 `wecom_chat_id`，含增量游标；
- 任一阶段可失败重试；`stage` 字段推进，已 LOADED 的条目重跑时跳过；
- 所有写操作即时提交，进程崩溃可断点续跑。

正式环境可将同样的 schema 迁到 PostgreSQL；此处用标准库 sqlite3 以零依赖跑通 MVP。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from .models import Permission, SourceItem, SourceType, Stage

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

-- 来源发生变化时不直接覆盖已迁移记录；先进入人工/版本化同步队列。
CREATE TABLE IF NOT EXISTS source_changes (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    stable_key        TEXT NOT NULL,
    detected_at       TEXT NOT NULL,
    old_signature     TEXT,
    new_signature     TEXT NOT NULL,
    old_snapshot_json TEXT,
    new_snapshot_json TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',
    resolved_at       TEXT,
    resolution_note   TEXT
);
CREATE INDEX IF NOT EXISTS idx_source_changes_status ON source_changes(status, detected_at);

CREATE TABLE IF NOT EXISTS source_permissions (
    stable_key   TEXT NOT NULL,
    principal    TEXT NOT NULL,
    role         TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    PRIMARY KEY (stable_key, principal)
);
CREATE INDEX IF NOT EXISTS idx_source_permissions_principal ON source_permissions(principal);

CREATE TABLE IF NOT EXISTS permission_syncs (
    stable_key      TEXT NOT NULL,
    target_token    TEXT NOT NULL,
    target_type     TEXT NOT NULL,
    principal       TEXT NOT NULL,
    feishu_open_id  TEXT NOT NULL,
    perm            TEXT NOT NULL,
    status          TEXT NOT NULL,
    error_detail    TEXT,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (stable_key, target_token, target_type, principal, perm)
);

CREATE TABLE IF NOT EXISTS classification_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stable_key TEXT NOT NULL,
    suggested_category TEXT,
    confirmed_category TEXT NOT NULL,
    confidence REAL,
    confirmed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_classification_feedback_category ON classification_feedback(confirmed_category);

CREATE TABLE IF NOT EXISTS governance_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stable_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor TEXT,
    detail_json TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_governance_events_item ON governance_events(stable_key, created_at);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    stats_json TEXT,
    error_detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs(started_at);

CREATE TABLE IF NOT EXISTS item_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stable_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    detail_json TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_item_events_item ON item_events(stable_key, created_at);

-- 目录结构工作台：规划版本与飞书实际目录彻底分离。
CREATE TABLE IF NOT EXISTS structure_versions (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    mode               TEXT NOT NULL,
    status             TEXT NOT NULL,
    revision           INTEGER NOT NULL DEFAULT 1,
    base_version_id    TEXT,
    remote_snapshot_id TEXT,
    root_name          TEXT,
    created_by         TEXT,
    approved_by        TEXT,
    required_approvals INTEGER NOT NULL DEFAULT 1,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    approved_at        TEXT
);
CREATE INDEX IF NOT EXISTS idx_structure_versions_status
    ON structure_versions(status, updated_at);

CREATE TABLE IF NOT EXISTS structure_nodes (
    version_id       TEXT NOT NULL,
    node_id          TEXT NOT NULL,
    parent_node_id   TEXT,
    semantic_key     TEXT NOT NULL,
    display_name     TEXT NOT NULL,
    sort_order       INTEGER NOT NULL DEFAULT 0,
    node_kind        TEXT NOT NULL DEFAULT 'category',
    owner            TEXT,
    steward          TEXT,
    review_months    INTEGER,
    retention_years  INTEGER,
    status           TEXT NOT NULL DEFAULT 'active',
    aliases_json     TEXT,
    assignment_rule_json TEXT,
    PRIMARY KEY (version_id, node_id)
);
CREATE INDEX IF NOT EXISTS idx_structure_nodes_parent
    ON structure_nodes(version_id, parent_node_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_structure_nodes_semantic
    ON structure_nodes(version_id, semantic_key);

CREATE TABLE IF NOT EXISTS structure_bindings (
    version_id         TEXT NOT NULL,
    node_id            TEXT NOT NULL,
    target_mode        TEXT NOT NULL,
    remote_token       TEXT NOT NULL,
    parent_remote_token TEXT,
    binding_status     TEXT NOT NULL DEFAULT 'bound',
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    PRIMARY KEY (version_id, node_id, target_mode)
);
CREATE INDEX IF NOT EXISTS idx_structure_bindings_remote
    ON structure_bindings(target_mode, remote_token);

CREATE TABLE IF NOT EXISTS remote_structure_snapshots (
    id           TEXT PRIMARY KEY,
    target_mode  TEXT NOT NULL,
    root_token   TEXT,
    space_id     TEXT,
    status       TEXT NOT NULL,
    tree_hash    TEXT,
    node_count   INTEGER NOT NULL DEFAULT 0,
    error_detail TEXT,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS remote_structure_nodes (
    snapshot_id  TEXT NOT NULL,
    remote_token TEXT NOT NULL,
    parent_token TEXT,
    display_name TEXT NOT NULL,
    node_type    TEXT NOT NULL,
    has_children INTEGER NOT NULL DEFAULT 0,
    file_count    INTEGER NOT NULL DEFAULT 0,
    remote_updated_at TEXT,
    sort_order   INTEGER NOT NULL DEFAULT 0,
    raw_json     TEXT,
    PRIMARY KEY (snapshot_id, remote_token)
);
CREATE INDEX IF NOT EXISTS idx_remote_structure_nodes_parent
    ON remote_structure_nodes(snapshot_id, parent_token, sort_order);

CREATE TABLE IF NOT EXISTS remote_node_decisions (
    target_mode       TEXT NOT NULL,
    remote_token      TEXT NOT NULL,
    decision          TEXT NOT NULL,
    planned_node_id   TEXT,
    actor             TEXT,
    note              TEXT,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (target_mode, remote_token)
);

CREATE TABLE IF NOT EXISTS structure_change_plans (
    id                 TEXT PRIMARY KEY,
    version_id         TEXT NOT NULL,
    remote_snapshot_id TEXT,
    status             TEXT NOT NULL,
    revision           INTEGER NOT NULL DEFAULT 1,
    history_scope      TEXT NOT NULL DEFAULT 'unmigrated_only',
    created_by         TEXT,
    approved_by        TEXT,
    summary_json       TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT,
    approved_at        TEXT,
    applied_at         TEXT,
    error_detail       TEXT
);

CREATE TABLE IF NOT EXISTS structure_change_actions (
    plan_id             TEXT NOT NULL,
    action_order        INTEGER NOT NULL,
    action_type         TEXT NOT NULL,
    display_name        TEXT,
    node_id             TEXT,
    remote_token        TEXT,
    source_parent_token TEXT,
    target_parent_token TEXT,
    before_json         TEXT,
    after_json          TEXT,
    item_count          INTEGER NOT NULL DEFAULT 0,
    depends_on_json     TEXT,
    rollbackable        INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'pending',
    error_detail        TEXT,
    started_at          TEXT,
    executed_at         TEXT,
    PRIMARY KEY (plan_id, action_order)
);

CREATE TABLE IF NOT EXISTS item_target_assignments (
    stable_key          TEXT NOT NULL,
    structure_version_id TEXT NOT NULL,
    node_id             TEXT NOT NULL,
    assignment_source   TEXT NOT NULL,
    confidence          REAL,
    status              TEXT NOT NULL DEFAULT 'assigned',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (stable_key, structure_version_id)
);

CREATE TABLE IF NOT EXISTS structure_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id  TEXT,
    event_type  TEXT NOT NULL,
    actor       TEXT,
    detail_json TEXT,
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_structure_events_version
    ON structure_events(version_id, created_at);

CREATE TABLE IF NOT EXISTS structure_approvals (
    version_id  TEXT NOT NULL,
    actor       TEXT NOT NULL,
    decision    TEXT NOT NULL DEFAULT 'approved',
    comment     TEXT,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (version_id, actor)
);
CREATE INDEX IF NOT EXISTS idx_structure_approvals_version
    ON structure_approvals(version_id, created_at);

CREATE TABLE IF NOT EXISTS structure_transformations (
    id                   TEXT PRIMARY KEY,
    version_id           TEXT NOT NULL,
    transformation_type  TEXT NOT NULL,
    source_node_ids_json TEXT NOT NULL,
    target_node_id       TEXT NOT NULL,
    rule_json            TEXT,
    status               TEXT NOT NULL DEFAULT 'pending',
    error_detail         TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_structure_transformations_version
    ON structure_transformations(version_id, status);

CREATE TABLE IF NOT EXISTS structure_relocations (
    id             TEXT PRIMARY KEY,
    version_id     TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    node_id        TEXT,
    source_token   TEXT NOT NULL,
    target_token   TEXT NOT NULL,
    status         TEXT NOT NULL,
    detail_json    TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    UNIQUE(version_id, operation_type, source_token, target_token)
);
CREATE INDEX IF NOT EXISTS idx_structure_relocations_status
    ON structure_relocations(version_id, status);

CREATE TABLE IF NOT EXISTS item_relocation_plans (
    id           TEXT PRIMARY KEY,
    version_id   TEXT NOT NULL,
    target_mode  TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'draft',
    revision     INTEGER NOT NULL DEFAULT 1,
    created_by   TEXT,
    approved_by  TEXT,
    summary_json TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    approved_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_item_relocation_plans_version
    ON item_relocation_plans(version_id, created_at);

CREATE TABLE IF NOT EXISTS item_relocation_actions (
    plan_id             TEXT NOT NULL,
    action_order        INTEGER NOT NULL,
    stable_key          TEXT NOT NULL,
    source_node_id      TEXT,
    target_node_id      TEXT NOT NULL,
    source_parent_token TEXT,
    target_parent_token TEXT NOT NULL,
    object_token        TEXT NOT NULL,
    object_type         TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    selected            INTEGER NOT NULL DEFAULT 1,
    status              TEXT NOT NULL DEFAULT 'pending',
    error_detail        TEXT,
    detail_json         TEXT,
    executed_at         TEXT,
    PRIMARY KEY (plan_id, stable_key)
);
CREATE INDEX IF NOT EXISTS idx_item_relocation_actions_status
    ON item_relocation_actions(plan_id, selected, status);

CREATE TABLE IF NOT EXISTS chat_migrations (
    wecom_chat_id       TEXT PRIMARY KEY,
    chat_name_original  TEXT,
    migration_status    TEXT NOT NULL,
    feishu_space_id     TEXT,
    feishu_node_token   TEXT,   -- 「当前可写节点」指针，写满自动新开
    feishu_url          TEXT,
    last_message_seq    TEXT,   -- 增量游标
    member_snapshot     TEXT,   -- JSON: 群成员 userid 列表快照
    tag_status          TEXT,   -- 群名打标结果: renamed/notified/manual/未打标
    error_detail        TEXT,
    first_migrated_at   TEXT,
    last_synced_at      TEXT
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class DiscoveryResult:
    """一次盘点登记的结果。

    ``changed`` 仅表示源端版本发生变化，不会重置既有迁移阶段；这避免未确认前
    覆盖已上传到飞书的历史副本。
    """
    created: bool = False
    changed: bool = False


def _source_snapshot(item: SourceItem) -> dict[str, Any]:
    """用于盘点比较的稳定来源快照，不含本地下载缓存等运行期字段。"""
    metadata = item.raw_metadata or {}
    return {
        "source_path": item.source_path,
        "original_name": item.original_name,
        "size": item.size,
        "content_sha256": item.content_sha256 or "",
        "modified_at": item.modified_at.isoformat() if item.modified_at else "",
        # 各远端连接器若提供 etag/version，优先纳入；未知键不影响兼容性。
        "source_version": (metadata.get("etag") or metadata.get("eTag")
                           or metadata.get("version") or ""),
    }


def _source_signature(snapshot: dict[str, Any]) -> str:
    raw = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


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
        for col, decl in (("wiki_node_token", "TEXT"),
                          ("source_signature", "TEXT"),
                          ("source_snapshot_json", "TEXT"),
                          ("last_seen_at", "TEXT"),
                          ("failed_stage", "TEXT"),
                          ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
                          ("retryable", "INTEGER NOT NULL DEFAULT 1"),
                          ("last_error_at", "TEXT"),
                          ("owner", "TEXT"),
                          ("steward", "TEXT"),
                          ("review_due_at", "TEXT"),
                          ("retention_due_at", "TEXT"),
                          ("archived_at", "TEXT"),
                          ("archive_reason", "TEXT"),
                          ("source_missing_at", "TEXT"),
                          ("source_missing_resolution", "TEXT"),
                          ("source_missing_resolved_at", "TEXT"),
                          ("extraction_ok", "INTEGER"),
                          ("extraction_note", "TEXT"),
                          ("extracted_text_chars", "INTEGER"),
                          ("classifier_version", "TEXT"),
                          ("target_node_id", "TEXT"),
                          ("structure_version_id", "TEXT")):
            if col not in cols:
                self.conn.execute(f"ALTER TABLE items ADD COLUMN {col} {decl}")
        run_cols = {r["name"] for r in
                    self.conn.execute("PRAGMA table_info(pipeline_runs)")}
        if "structure_version_id" not in run_cols:
            self.conn.execute(
                "ALTER TABLE pipeline_runs ADD COLUMN structure_version_id TEXT"
            )
        version_cols = {
            r["name"] for r in
            self.conn.execute("PRAGMA table_info(structure_versions)")
        }
        if "required_approvals" not in version_cols:
            self.conn.execute(
                "ALTER TABLE structure_versions "
                "ADD COLUMN required_approvals INTEGER NOT NULL DEFAULT 1"
            )
        node_cols = {
            r["name"] for r in
            self.conn.execute("PRAGMA table_info(structure_nodes)")
        }
        if "assignment_rule_json" not in node_cols:
            self.conn.execute(
                "ALTER TABLE structure_nodes ADD COLUMN assignment_rule_json TEXT"
            )
        plan_cols = {
            r["name"] for r in
            self.conn.execute("PRAGMA table_info(structure_change_plans)")
        }
        for col, decl in (
            ("revision", "INTEGER NOT NULL DEFAULT 1"),
            (
                "history_scope",
                "TEXT NOT NULL DEFAULT 'unmigrated_only'",
            ),
            ("created_by", "TEXT"),
            ("approved_by", "TEXT"),
            ("updated_at", "TEXT"),
            ("applied_at", "TEXT"),
            ("error_detail", "TEXT"),
        ):
            if col not in plan_cols:
                self.conn.execute(
                    f"ALTER TABLE structure_change_plans ADD COLUMN {col} {decl}"
                )
        action_cols = {
            r["name"] for r in
            self.conn.execute("PRAGMA table_info(structure_change_actions)")
        }
        for col, decl in (
            ("display_name", "TEXT"),
            ("depends_on_json", "TEXT"),
            ("rollbackable", "INTEGER NOT NULL DEFAULT 0"),
            ("started_at", "TEXT"),
            ("executed_at", "TEXT"),
        ):
            if col not in action_cols:
                self.conn.execute(
                    f"ALTER TABLE structure_change_actions ADD COLUMN {col} {decl}"
                )
        remote_cols = {
            r["name"] for r in
            self.conn.execute("PRAGMA table_info(remote_structure_nodes)")
        }
        for col, decl in (
            ("file_count", "INTEGER NOT NULL DEFAULT 0"),
            ("remote_updated_at", "TEXT"),
        ):
            if col not in remote_cols:
                self.conn.execute(
                    f"ALTER TABLE remote_structure_nodes ADD COLUMN {col} {decl}"
                )
        chat_cols = {r["name"] for r in
                     self.conn.execute("PRAGMA table_info(chat_migrations)")}
        for col, decl in (("tag_status", "TEXT"),):
            if col not in chat_cols:
                self.conn.execute(f"ALTER TABLE chat_migrations ADD COLUMN {col} {decl}")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ── 文件级 items ──────────────────────────────────────

    def record_discovered(self, item: SourceItem) -> DiscoveryResult:
        """登记盘点结果，并检测同一来源对象的变化。

        新对象插入 ``items``。已存在且来源签名变化时，向 ``source_changes`` 写入
        审计事件，但不改动 items 的处理阶段或文件元数据，防止静默覆盖已入库版本。
        """
        key = item.stable_key()
        snapshot = _source_snapshot(item)
        signature = _source_signature(snapshot)
        row = self.conn.execute(
            "SELECT * FROM items WHERE stable_key=?", (key,)
        ).fetchone()
        now = _now()
        if row is not None:
            old_signature = row["source_signature"]
            # 兼容历史库：首次升级只建立基线，不把所有旧记录误报为变更。
            if not old_signature:
                self.conn.execute(
                    "UPDATE items SET source_signature=?, source_snapshot_json=?, "
                    "last_seen_at=?, updated_at=? "
                    "WHERE stable_key=?",
                    (signature, json.dumps(snapshot, ensure_ascii=False, default=str), now, now, key),
                )
                self.conn.commit()
                return DiscoveryResult()
            if old_signature != signature:
                old_snapshot = json.loads(row["source_snapshot_json"] or "{}")
                if not old_snapshot:
                    old_snapshot = {
                        "source_path": row["source_path"] or "",
                        "original_name": row["original_name"] or "",
                        "size": row["size"] or 0,
                        "content_sha256": row["content_sha256"] or "",
                    }
                self.conn.execute(
                    """INSERT INTO source_changes
                       (stable_key, detected_at, old_signature, new_signature,
                        old_snapshot_json, new_snapshot_json)
                       VALUES (?,?,?,?,?,?)""",
                    (key, now, old_signature, signature,
                     json.dumps(old_snapshot, ensure_ascii=False),
                     json.dumps(snapshot, ensure_ascii=False, default=str)),
                )
                self.conn.execute(
                    "UPDATE items SET source_signature=?, source_snapshot_json=?, "
                    "last_seen_at=?, updated_at=? "
                    "WHERE stable_key=?",
                    (signature, json.dumps(snapshot, ensure_ascii=False, default=str), now, now, key),
                )
                self.conn.commit()
                return DiscoveryResult(changed=True)
            self.conn.execute(
                "UPDATE items SET last_seen_at=?, updated_at=? WHERE stable_key=?",
                (now, now, key),
            )
            self.conn.commit()
            return DiscoveryResult()

        self.conn.execute(
            """INSERT INTO items
               (stable_key, source_type, source_id, source_path, original_name,
                size, content_sha256, local_blob_path, stage, metadata_json,
                source_signature, source_snapshot_json, last_seen_at, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                key, item.source_type.value, item.source_id, item.source_path,
                item.original_name, item.size, item.content_sha256,
                item.local_blob_path, Stage.DISCOVERED.value,
                json.dumps(item.raw_metadata, ensure_ascii=False, default=str),
                signature, json.dumps(snapshot, ensure_ascii=False, default=str), now, now, now,
            ),
        )
        self.conn.commit()
        return DiscoveryResult(created=True)

    def upsert_discovered(self, item: SourceItem) -> bool:
        """兼容旧调用：仅返回是否为新增，变化检测仍会被记录。"""
        return self.record_discovered(item).created

    def update(self, stable_key: str, **fields: Any) -> None:
        """更新任意字段并刷新 updated_at。Stage 类型自动转 value。"""
        before = self.get(stable_key)
        if "stage" in fields and isinstance(fields["stage"], Stage):
            fields["stage"] = fields["stage"].value
        fields["updated_at"] = _now()
        cols = ", ".join(f"{k}=?" for k in fields)
        self.conn.execute(
            f"UPDATE items SET {cols} WHERE stable_key=?",
            (*fields.values(), stable_key),
        )
        changes = {}
        if before:
            for name in ("stage", "failed_stage"):
                if name in fields and before[name] != fields[name]:
                    changes[name] = {"from": before[name], "to": fields[name]}
        if changes:
            self.conn.execute(
                "INSERT INTO item_events "
                "(stable_key,event_type,detail_json,created_at) VALUES (?,?,?,?)",
                (stable_key, "state_changed",
                 json.dumps(changes, ensure_ascii=False), _now()),
            )
        self.conn.commit()

    def start_pipeline_run(self, run_type: str,
                           structure_version_id: str = "") -> int:
        cur = self.conn.execute(
            """INSERT INTO pipeline_runs
               (run_type,status,started_at,structure_version_id)
               VALUES (?,?,?,?)""",
            (
                run_type, "running", _now(),
                structure_version_id or None,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_pipeline_run(self, run_id: int, *, stats: dict | None = None,
                            error: str = "") -> None:
        self.conn.execute(
            "UPDATE pipeline_runs SET status=?,completed_at=?,stats_json=?,"
            "error_detail=? WHERE id=?",
            ("failed" if error else "success", _now(),
             json.dumps(stats or {}, ensure_ascii=False, default=str),
             error or None, run_id),
        )
        self.conn.commit()

    def recent_pipeline_runs(self, limit: int = 50) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        ).fetchall()

    def get(self, stable_key: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM items WHERE stable_key=?", (stable_key,)
        ).fetchone()

    def items_in_stage(self, stage: Stage,
                       source_type: SourceType | str | None = None) -> list[sqlite3.Row]:
        """按阶段取条目；指定 source_type 可避免不同连接器混处理待办。"""
        if source_type is None:
            return self.conn.execute(
                "SELECT * FROM items WHERE stage=? ORDER BY created_at", (stage.value,)
            ).fetchall()
        value = source_type.value if isinstance(source_type, SourceType) else source_type
        return self.conn.execute(
            "SELECT * FROM items WHERE stage=? AND source_type=? ORDER BY created_at",
            (stage.value, value),
        ).fetchall()

    def pending_source_changes(self) -> list[sqlite3.Row]:
        """返回尚未处理的源端变化，供后续版本化同步/人工确认使用。"""
        return self.conn.execute(
            "SELECT * FROM source_changes WHERE status='pending' ORDER BY detected_at"
        ).fetchall()

    def materialize_source_change(self, change_id: int) -> str:
        """将已检测到的源端变化安全地派生为新的待迁移版本。

        原条目及其飞书副本不覆盖；新版本使用 ``#revN`` source_id，仍保留
        logical_source_id 以便后续人工执行替换或归档决策。
        """
        change = self.conn.execute("SELECT * FROM source_changes WHERE id=?", (change_id,)).fetchone()
        if not change or change["status"] != "pending":
            raise ValueError("变更不存在或已处理")
        base = self.get(change["stable_key"])
        if not base:
            raise ValueError("变更对应的原条目不存在")
        snap = json.loads(change["new_snapshot_json"])
        n = self.conn.execute("SELECT COUNT(*) c FROM items WHERE source_type=? AND source_id LIKE ?",
                              (base["source_type"], base["source_id"] + "#rev%")).fetchone()["c"] + 1
        source_id = f"{base['source_id']}#rev{n}"
        metadata = json.loads(base["metadata_json"] or "{}")
        metadata["logical_source_id"] = base["source_id"]
        metadata["source_change_id"] = change_id
        now = _now()
        key = f"{base['source_type']}:{source_id}"
        self.conn.execute("""INSERT INTO items
            (stable_key,source_type,source_id,source_path,original_name,size,content_sha256,stage,metadata_json,
             source_signature,source_snapshot_json,last_seen_at,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (key, base["source_type"], source_id, snap.get("source_path", base["source_path"]),
             snap.get("original_name", base["original_name"]), snap.get("size", base["size"]),
             snap.get("content_sha256") or None, Stage.DISCOVERED.value,
             json.dumps(metadata, ensure_ascii=False), change["new_signature"],
             json.dumps(snap, ensure_ascii=False), now, now, now))
        self.conn.execute("UPDATE source_changes SET status='materialized', resolved_at=?, resolution_note=? WHERE id=?",
                          (now, f"derived={key}", change_id))
        self.conn.commit()
        return key

    def mark_missing_local_sources(self, root: str, seen_keys: set[str],
                                   scan_started_at: str) -> int:
        """标记本次完整本地目录扫描中消失的源文件，不触碰飞书副本。"""
        rows = self.conn.execute(
            "SELECT stable_key FROM items WHERE source_type=? AND source_path LIKE ? "
            "AND last_seen_at<? AND source_missing_at IS NULL",
            (SourceType.LOCAL.value, root.rstrip("\\/") + "%", scan_started_at),
        ).fetchall()
        keys = [r["stable_key"] for r in rows if r["stable_key"] not in seen_keys]
        if keys:
            self.conn.executemany("UPDATE items SET source_missing_at=?, updated_at=? WHERE stable_key=?",
                                  [(_now(), _now(), key) for key in keys])
            self.conn.commit()
        return len(keys)

    def missing_source_items(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM items WHERE source_missing_at IS NOT NULL AND source_missing_resolution IS NULL ORDER BY source_missing_at DESC").fetchall()

    def resolve_missing_source(self, stable_key: str, resolution: str) -> None:
        if resolution not in ("keep", "archived"):
            raise ValueError("resolution 必须为 keep/archived")
        self.update(stable_key, source_missing_resolution=resolution,
                    source_missing_resolved_at=_now())

    def get_source_change(self, change_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM source_changes WHERE id=?", (change_id,)).fetchone()

    def complete_source_change(self, change_id: int, note: str) -> None:
        self.conn.execute("UPDATE source_changes SET status='completed', resolved_at=?, resolution_note=? WHERE id=?",
                          (_now(), note, change_id))
        self.conn.commit()

    def replace_source_permissions(self, stable_key: str,
                                   permissions: Iterable[Permission]) -> None:
        """以一次成功采集的结果原子替换某条目的来源权限快照。"""
        now = _now()
        self.conn.execute("DELETE FROM source_permissions WHERE stable_key=?", (stable_key,))
        self.conn.executemany(
            "INSERT INTO source_permissions (stable_key, principal, role, collected_at) "
            "VALUES (?,?,?,?)",
            [(stable_key, p.principal, p.role, now) for p in permissions],
        )
        self.conn.commit()

    def source_permissions(self, stable_key: str) -> list[sqlite3.Row]:
        """读取最近一次成功采集的来源权限快照。"""
        return self.conn.execute(
            "SELECT * FROM source_permissions WHERE stable_key=? ORDER BY principal",
            (stable_key,),
        ).fetchall()

    def permission_sync_succeeded(self, stable_key: str, target_token: str,
                                  target_type: str, principal: str, perm: str) -> bool:
        row = self.conn.execute(
            """SELECT status FROM permission_syncs WHERE stable_key=? AND target_token=?
               AND target_type=? AND principal=? AND perm=?""",
            (stable_key, target_token, target_type, principal, perm),
        ).fetchone()
        return bool(row and row["status"] == "succeeded")

    def record_permission_sync(self, stable_key: str, target_token: str, target_type: str,
                               principal: str, feishu_open_id: str, perm: str,
                               status: str, error_detail: str | None = None) -> None:
        self.conn.execute(
            """INSERT INTO permission_syncs
               (stable_key,target_token,target_type,principal,feishu_open_id,perm,status,error_detail,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(stable_key,target_token,target_type,principal,perm) DO UPDATE SET
               feishu_open_id=excluded.feishu_open_id,status=excluded.status,
               error_detail=excluded.error_detail,updated_at=excluded.updated_at""",
            (stable_key, target_token, target_type, principal, feishu_open_id,
             perm, status, error_detail, _now()),
        )
        self.conn.commit()

    def managed_permissions(self, stable_key: str, target_token: str,
                            target_type: str) -> list[sqlite3.Row]:
        """返回仍由本工具管理且上次同步成功的协作者。"""
        return self.conn.execute(
            "SELECT * FROM permission_syncs WHERE stable_key=? AND target_token=? "
            "AND target_type=? AND status='succeeded' ORDER BY principal",
            (stable_key, target_token, target_type),
        ).fetchall()

    def close_managed_permission(self, stable_key: str, target_token: str,
                                 target_type: str, principal: str,
                                 status: str) -> None:
        self.conn.execute(
            "UPDATE permission_syncs SET status=?, updated_at=? "
            "WHERE stable_key=? AND target_token=? AND target_type=? "
            "AND principal=? AND status='succeeded'",
            (status, _now(), stable_key, target_token, target_type, principal),
        )
        self.conn.commit()

    def find_by_sha(self, sha256: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM items WHERE content_sha256=?", (sha256,)
        ).fetchall()

    def items_for_chat(self, chat_id: str) -> list[sqlite3.Row]:
        """某群产出的所有条目（会话片段 chat_id:date + 群文件 chat_id:file:xxx）。

        source_id 以 'chat_id:' 前缀区分；冒号边界使 chat42 不会误匹配 chat420。
        """
        return self.conn.execute(
            "SELECT * FROM items WHERE source_type=? AND source_id LIKE ? "
            "ORDER BY created_at",
            (SourceType.WECOM_CHAT.value, chat_id + ":%"),
        ).fetchall()

    def pending_review(self) -> list[sqlite3.Row]:
        """人工确认队列：已分类但需人工复核，或去重待判定的条目。"""
        return self.conn.execute(
            """SELECT * FROM items
               WHERE stage=? OR dedup_verdict=?
               ORDER BY updated_at DESC""",
            (Stage.CLASSIFIED.value, "semantic_candidate"),
        ).fetchall()

    def mark_failed(self, stable_key: str, failed_stage: str, detail: str,
                    *, retryable: bool = True, preserve_stage: bool = False) -> None:
        """以结构化信息记录失败。

        默认进入 FAILED。对于 Wiki 挂载这类发生在“文件已成功入库”之后的附加动作，
        preserve_stage=True 可保留 LOADED 事实，同时仍进入统一失败清单。
        """
        prefix = f"{failed_stage}:"
        message = detail if detail.startswith(prefix) else f"{prefix} {detail}"
        fields = {
            "failed_stage": failed_stage, "error_detail": message,
            "retryable": int(retryable), "last_error_at": _now(),
            "retry_count": (self.get(stable_key)["retry_count"] or 0) + 1,
        }
        if not preserve_stage:
            fields["stage"] = Stage.FAILED.value
        self.update(stable_key, **fields)

    def failed_items(self, failed_stage: str | None = None) -> list[sqlite3.Row]:
        """返回失败项；可按原始阶段过滤。"""
        if failed_stage:
            return self.conn.execute(
                "SELECT * FROM items WHERE failed_stage=? ORDER BY last_error_at DESC",
                (failed_stage,),
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM items WHERE failed_stage IS NOT NULL OR stage=? "
            "ORDER BY last_error_at DESC",
            (Stage.FAILED.value,),
        ).fetchall()

    def requeue_failures(self, failed_stage: str, target_stage: Stage) -> int:
        """重排指定阶段的可重试失败项，并清理本次错误详情。"""
        cur = self.conn.execute(
            "SELECT COUNT(*) c FROM items WHERE failed_stage=? AND retryable=1",
            (failed_stage,),
        ).fetchone()
        n = cur["c"] if cur else 0
        if n:
            self.conn.execute(
                """UPDATE items SET stage=?, error_detail=NULL, failed_stage=NULL,
                   updated_at=? WHERE failed_stage=? AND retryable=1""",
                (target_stage.value, _now(), failed_stage),
            )
            self.conn.commit()
        return n

    def requeue_failure_keys(
        self,
        stable_keys: Iterable[str],
        stage_targets: dict[str, Stage],
    ) -> dict:
        """只重排用户选中的失败项，供治理页单条/批量重试。

        非失败、不可重试、阶段未知和不存在的记录不会被修改，并在 ``skipped``
        中返回原因。所有有效项在同一事务中更新，避免批量操作只完成一部分。
        """
        keys = list(dict.fromkeys(str(key).strip() for key in stable_keys if str(key).strip()))
        requeued: list[dict] = []
        skipped: list[dict] = []
        now = _now()
        try:
            self.conn.execute("BEGIN")
            for key in keys:
                row = self.get(key)
                if row is None:
                    skipped.append({"key": key, "reason": "not_found"})
                    continue
                failed_stage = str(row["failed_stage"] or "")
                if not failed_stage and row["error_detail"]:
                    failed_stage = str(row["error_detail"]).split(":", 1)[0].strip()
                if not failed_stage:
                    skipped.append({"key": key, "reason": "not_failed"})
                    continue
                if not bool(row["retryable"]):
                    skipped.append({"key": key, "reason": "not_retryable"})
                    continue
                target = stage_targets.get(failed_stage)
                if target is None:
                    skipped.append({
                        "key": key,
                        "reason": "unsupported_stage",
                        "failed_stage": failed_stage,
                    })
                    continue
                self.conn.execute(
                    """UPDATE items
                       SET stage=?, error_detail=NULL, failed_stage=NULL,
                           updated_at=?
                       WHERE stable_key=?""",
                    (target.value, now, key),
                )
                self.conn.execute(
                    """INSERT INTO item_events
                       (stable_key,event_type,detail_json,created_at)
                       VALUES (?,?,?,?)""",
                    (
                        key,
                        "failure_requeued",
                        json.dumps({
                            "failed_stage": failed_stage,
                            "target_stage": target.value,
                            "retry_count": row["retry_count"] or 0,
                        }, ensure_ascii=False),
                        now,
                    ),
                )
                requeued.append({
                    "key": key,
                    "failed_stage": failed_stage,
                    "target_stage": target.value,
                })
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return {"requeued": requeued, "skipped": skipped}

    def requeue_failed(self, *, error_prefix: str = "load: ",
                       target_stage: Stage = Stage.CONFIRMED) -> int:
        """把 FAILED 且 error_detail 以 error_prefix 开头的条目重排回 target_stage。

        FAILED 是扁平状态，靠 error_detail 前缀区分失败来源（"load: "/"fetch: "）。
        只回捞 load 阶段失败，避免把 fetch/ingest 失败误送写入器。清空 error_detail。
        返回重排条数。
        """
        failed_stage = error_prefix.rstrip(": ")
        # 旧数据库中尚无 failed_stage 的记录继续按 error_detail 前缀兼容回捞。
        cur = self.conn.execute(
            "SELECT COUNT(*) c FROM items WHERE stage=? AND retryable=1 "
            "AND (failed_stage=? OR (failed_stage IS NULL AND error_detail LIKE ?))",
            (Stage.FAILED.value, failed_stage, error_prefix + "%"),
        ).fetchone()
        n = cur["c"] if cur else 0
        if n:
            self.conn.execute(
                "UPDATE items SET stage=?, error_detail=NULL, failed_stage=NULL, updated_at=? "
                "WHERE stage=? AND retryable=1 AND (failed_stage=? OR "
                "(failed_stage IS NULL AND error_detail LIKE ?))",
                (target_stage.value, _now(), Stage.FAILED.value, failed_stage, error_prefix + "%"),
            )
            self.conn.commit()
        return n

    def stage_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT stage, COUNT(*) c FROM items GROUP BY stage"
        ).fetchall()
        return {r["stage"]: r["c"] for r in rows}

    def governance_items(self, *, as_of: str | None = None,
                         triage_path: str = "90 待整理") -> dict[str, list[sqlite3.Row]]:
        """返回治理巡检队列；日期均为 ISO 格式，SQLite 可安全字典序比较。"""
        today = as_of or _now()[:10]
        active = (Stage.CONFIRMED.value, Stage.LOADED.value)
        placeholders = ",".join("?" * len(active))
        common = f"stage IN ({placeholders}) AND archived_at IS NULL"
        due_review = self.conn.execute(
            f"SELECT * FROM items WHERE {common} AND review_due_at<>'' AND review_due_at<=? "
            "ORDER BY review_due_at, updated_at",
            (*active, today),
        ).fetchall()
        due_archive = self.conn.execute(
            f"SELECT * FROM items WHERE {common} AND retention_due_at<>'' AND retention_due_at<=? "
            "ORDER BY retention_due_at, updated_at",
            (*active, today),
        ).fetchall()
        unowned = self.conn.execute(
            f"SELECT * FROM items WHERE {common} AND COALESCE(owner,'')='' ORDER BY updated_at",
            active,
        ).fetchall()
        triage = self.conn.execute(
            "SELECT * FROM items WHERE category=? AND stage IN (?,?,?) AND archived_at IS NULL "
            "ORDER BY updated_at",
            (triage_path, Stage.CLASSIFIED.value, *active),
        ).fetchall()
        return {"review_due": due_review, "archive_due": due_archive,
                "unowned": unowned, "triage": triage}

    def mark_archived(self, stable_key: str, reason: str) -> None:
        """仅在远端移动成功后标记归档，保留原处理阶段与可追溯原因。"""
        self.update(stable_key, archived_at=_now(), archive_reason=reason,
                    failed_stage=None, error_detail=None)
        self.record_governance_event(
            stable_key, "archived", detail={"reason": reason},
        )

    def record_governance_event(self, stable_key: str, event_type: str,
                                *, actor: str = "", detail: dict | None = None) -> None:
        self.conn.execute(
            "INSERT INTO governance_events "
            "(stable_key,event_type,actor,detail_json,created_at) VALUES (?,?,?,?,?)",
            (stable_key, event_type, actor,
             json.dumps(detail or {}, ensure_ascii=False), _now()),
        )
        self.conn.commit()

    def assign_structure_target(self, stable_key: str, structure_version_id: str,
                                node_id: str, *, source: str = "category",
                                confidence: float | None = None) -> None:
        """固定单个迁移条目使用的结构版本和稳定目标节点。"""
        now = _now()
        self.conn.execute(
            """INSERT INTO item_target_assignments
               (stable_key,structure_version_id,node_id,assignment_source,
                confidence,status,created_at,updated_at)
               VALUES (?,?,?,?,?,'assigned',?,?)
               ON CONFLICT(stable_key,structure_version_id) DO UPDATE SET
                 node_id=excluded.node_id,
                 assignment_source=excluded.assignment_source,
                 confidence=excluded.confidence,
                 status='assigned',
                 updated_at=excluded.updated_at""",
            (stable_key, structure_version_id, node_id, source, confidence, now, now),
        )
        self.conn.execute(
            "UPDATE items SET target_node_id=?,structure_version_id=?,updated_at=? "
            "WHERE stable_key=?",
            (node_id, structure_version_id, now, stable_key),
        )
        self.conn.commit()

    def complete_review(self, stable_key: str, next_review_due_at: str,
                        *, actor: str = "") -> None:
        row = self.get(stable_key)
        if not row:
            raise KeyError(stable_key)
        previous = row["review_due_at"] or ""
        self.update(stable_key, review_due_at=next_review_due_at)
        self.record_governance_event(
            stable_key, "review_completed", actor=actor,
            detail={"previous_due_at": previous,
                    "next_review_due_at": next_review_due_at},
        )

    def governance_health(self, *, triage_path: str = "90 待整理",
                          as_of: str | None = None) -> dict:
        """计算可用于 CLI/Web 看板的知识健康度，不把重复/失败项混入分母。"""
        today = as_of or _now()[:10]
        rows = self.conn.execute(
            "SELECT owner,category,review_due_at,failed_stage FROM items "
            "WHERE stage IN (?,?) AND archived_at IS NULL",
            (Stage.CONFIRMED.value, Stage.LOADED.value),
        ).fetchall()
        total = len(rows)
        owned = sum(bool(r["owner"]) for r in rows)
        classified = sum(bool(r["category"] and r["category"] != triage_path) for r in rows)
        review_current = sum(
            not r["review_due_at"] or r["review_due_at"] > today for r in rows
        )
        healthy = sum(
            bool(r["owner"])
            and bool(r["category"] and r["category"] != triage_path)
            and (not r["review_due_at"] or r["review_due_at"] > today)
            and not r["failed_stage"]
            for r in rows
        )
        pct = lambda n: round(n / total * 100, 1) if total else 0.0
        return {
            "active": total,
            "owner_coverage": pct(owned),
            "classified_coverage": pct(classified),
            "review_current": pct(review_current),
            "healthy": pct(healthy),
        }

    def record_classification_feedback(self, stable_key: str, suggested: str | None,
                                       confirmed: str, confidence: float | None) -> None:
        self.conn.execute("INSERT INTO classification_feedback (stable_key,suggested_category,confirmed_category,confidence,confirmed_at) VALUES (?,?,?,?,?)",
                          (stable_key, suggested, confirmed, confidence, _now()))
        self.conn.commit()

    def classification_feedback_summary(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT confirmed_category, COUNT(*) AS confirmed_count, SUM(CASE WHEN suggested_category=confirmed_category THEN 1 ELSE 0 END) AS matches FROM classification_feedback GROUP BY confirmed_category ORDER BY confirmed_count DESC").fetchall()

    def classification_feedback_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT suggested_category,confirmed_category,confidence,confirmed_at "
            "FROM classification_feedback WHERE confidence IS NOT NULL "
            "ORDER BY confirmed_at"
        ).fetchall()

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
