"""带 SQLite 快照的后台任务运行器。"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

MAX_JOBS = 50
MAX_LOG_LINES = 500


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobCancelled(RuntimeError):
    pass


class JobContext:
    def __init__(self, job: "Job"):
        self._job = job

    def log(self, msg: str) -> None:
        self._job._append_log(msg)

    def progress(self, done: int, total: int) -> None:
        self.raise_if_cancelled()
        self._job._set_progress(done, total)

    def raise_if_cancelled(self) -> None:
        if self._job.cancel_requested:
            raise JobCancelled("任务已取消")


class Job:
    def __init__(self, job_id: str, job_type: str, persist: Callable[["Job"], None],
                 lock_key: str = ""):
        self.id, self.type, self.lock_key = job_id, job_type, lock_key
        self.status, self.log, self.progress_done, self.progress_total = "pending", [], 0, 0
        self.result: Any = None
        self.error: Optional[str] = None
        self.cancel_requested = False
        self.created_at, self.updated_at = _now(), _now()
        self._persist, self._lock = persist, threading.RLock()

    def _changed(self) -> None:
        self.updated_at = _now()
        self._persist(self)

    def _append_log(self, msg: str) -> None:
        with self._lock:
            self.log.append(msg)
            self.log = self.log[-MAX_LOG_LINES:]
            self._changed()

    def _set_progress(self, done: int, total: int) -> None:
        with self._lock:
            self.progress_done, self.progress_total = done, total
            self._changed()

    def to_dict(self) -> dict:
        with self._lock:
            return {"id": self.id, "type": self.type, "status": self.status,
                    "log": list(self.log), "progress": {"done": self.progress_done,
                    "total": self.progress_total}, "result": self.result, "error": self.error,
                    "cancel_requested": self.cancel_requested, "created_at": self.created_at,
                    "updated_at": self.updated_at}


class JobManager:
    def __init__(self, db_path: str = ":memory:", max_concurrent: int | None = None):
        self.db_path, self._jobs, self._lock = db_path, {}, threading.RLock()
        configured = max_concurrent if max_concurrent is not None else int(
            os.getenv("KBM_MAX_CONCURRENT_JOBS", "4")
        )
        self.max_concurrent = max(1, configured)
        self._memory_conn = sqlite3.connect(":memory:", check_same_thread=False) if db_path == ":memory:" else None
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self):
        return self._memory_conn or sqlite3.connect(self.db_path)

    @contextmanager
    def _db(self):
        c = self._conn()
        try:
            yield c
            c.commit()
        finally:
            if self._memory_conn is None:
                c.close()

    def _init_db(self) -> None:
        with self._db() as c:
            c.execute("CREATE TABLE IF NOT EXISTS web_jobs (id TEXT PRIMARY KEY, lock_key TEXT, status TEXT, updated_at TEXT, snapshot_json TEXT NOT NULL)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_web_jobs_lock ON web_jobs(lock_key, status)")
            rows = c.execute(
                "SELECT id, snapshot_json FROM web_jobs "
                "WHERE status IN ('pending','running')"
            ).fetchall()
            for job_id, raw in rows:
                try:
                    data = json.loads(raw)
                except (TypeError, ValueError):
                    data = {"id": job_id, "type": "unknown", "log": []}
                data["status"] = "interrupted"
                data["error"] = "服务重启，后台任务已中断；请按需重新发起"
                data["updated_at"] = _now()
                data.setdefault("log", []).append(
                    "服务重启：任务未完成，状态已恢复为 interrupted"
                )
                data["log"] = data["log"][-MAX_LOG_LINES:]
                c.execute(
                    "UPDATE web_jobs SET status=?, updated_at=?, snapshot_json=? WHERE id=?",
                    ("interrupted", data["updated_at"],
                     json.dumps(data, ensure_ascii=False, default=str), job_id),
                )

    def _persist(self, job: Job) -> None:
        data = job.to_dict()
        with self._lock:
            with self._db() as c:
                c.execute("INSERT INTO web_jobs VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET lock_key=excluded.lock_key,status=excluded.status,updated_at=excluded.updated_at,snapshot_json=excluded.snapshot_json",
                          (job.id, job.lock_key, job.status, job.updated_at, json.dumps(data, ensure_ascii=False, default=str)))

    def start(self, job_type: str, fn: Callable[[JobContext], Any], lock_key: str = "") -> str:
        with self._lock:
            with self._db() as c:
                active = c.execute(
                    "SELECT COUNT(*) FROM web_jobs "
                    "WHERE status IN ('pending','running')"
                ).fetchone()[0]
                if active >= self.max_concurrent:
                    raise RuntimeError(
                        f"并发任务已达上限 {self.max_concurrent}，请等待现有任务结束"
                    )
                if lock_key:
                    row = c.execute("SELECT id FROM web_jobs WHERE lock_key=? AND status IN ('pending','running')", (lock_key,)).fetchone()
                    if row:
                        raise RuntimeError(f"同一资源已有运行任务：{row[0]}")
            job = Job(uuid.uuid4().hex[:12], job_type, self._persist, lock_key)
            self._jobs[job.id] = job
            self._persist(job)
        def runner():
            job.status = "running"; job._changed(); ctx = JobContext(job)
            try:
                result = fn(ctx)
                with job._lock:
                    job.result = result
                    job.status = "cancelled" if job.cancel_requested else "success"
                    job._changed()
            except JobCancelled as e:
                with job._lock:
                    job.error = str(e)
                    job.status = "cancelled"
                    job.log.append("⏹ " + str(e))
                    job.log = job.log[-MAX_LOG_LINES:]
                    job._changed()
            except Exception as e:  # noqa: BLE001
                with job._lock:
                    job.error = f"{type(e).__name__}: {e}"
                    job.status = "error"
                    job.log.extend([
                        "❌ " + job.error,
                        traceback.format_exc().splitlines()[-1],
                    ])
                    job.log = job.log[-MAX_LOG_LINES:]
                    job._changed()
            finally:
                if job.status not in ("success", "cancelled", "error"):
                    job._changed()
        threading.Thread(target=runner, name=f"job-{job_type}-{job.id}", daemon=True).start()
        return job.id

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            live = self._jobs.get(job_id)
            if live:
                return live
            with self._db() as c:
                row = c.execute("SELECT snapshot_json FROM web_jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                return None
            data = json.loads(row[0])
            job = Job(data["id"], data["type"], self._persist, data.get("lock_key", ""))
            job.status, job.log = data["status"], data.get("log", [])
            job.progress_done, job.progress_total = data.get("progress", {}).get("done", 0), data.get("progress", {}).get("total", 0)
            job.result, job.error = data.get("result"), data.get("error")
            job.cancel_requested = data.get("cancel_requested", False)
            job.created_at, job.updated_at = data.get("created_at", job.created_at), data.get("updated_at", job.updated_at)
            self._jobs[job_id] = job
            return job

    def cancel(self, job_id: str) -> bool:
        job = self.get(job_id)
        if not job or job.status not in ("pending", "running"):
            return False
        job.cancel_requested = True; job._append_log("请求取消，当前安全检查点将停止任务")
        return True

    def list(self) -> list[dict]:
        with self._db() as c:
            rows = c.execute("SELECT snapshot_json FROM web_jobs ORDER BY updated_at DESC LIMIT ?", (MAX_JOBS,)).fetchall()
        out = [json.loads(r[0]) for r in rows]
        for item in out:
            item["log"] = item.get("log", [])[-1:]
        return out
