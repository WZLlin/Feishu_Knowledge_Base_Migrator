"""内存任务运行器：把长耗时操作（盘点/迁移/写飞书）放后台线程跑，前端轮询进度。

设计：
- Web 请求不能阻塞几分钟，故 start(type, fn) 立即返回 job_id，fn 在 daemon 线程执行；
- fn(ctx) 收到 JobContext，用 ctx.log(msg) 追加日志行、ctx.progress(done,total) 报进度；
- 所有读写经锁保护，前端 GET /api/jobs/{id} 拿到 status/log/progress/result；
- 仅保留最近 MAX_JOBS 条，避免内存无限增长；
- 每个 job 线程内部自建 Ledger/Orchestrator（SQLite 连接不可跨线程共享）。
"""
from __future__ import annotations

import threading
import traceback
import uuid
from typing import Any, Callable, Optional

MAX_JOBS = 50
MAX_LOG_LINES = 500


class JobContext:
    """传给任务函数的句柄，用于回报日志与进度（线程安全，经 Job 的锁）。"""

    def __init__(self, job: "Job"):
        self._job = job

    def log(self, msg: str) -> None:
        self._job._append_log(msg)

    def progress(self, done: int, total: int) -> None:
        self._job._set_progress(done, total)


class Job:
    def __init__(self, job_id: str, job_type: str):
        self.id = job_id
        self.type = job_type
        self.status = "pending"          # pending / running / success / error
        self.log: list[str] = []
        self.progress_done = 0
        self.progress_total = 0
        self.result: Any = None
        self.error: Optional[str] = None
        self._lock = threading.Lock()

    # ── 供 JobContext 回调（加锁）──
    def _append_log(self, msg: str) -> None:
        with self._lock:
            self.log.append(msg)
            if len(self.log) > MAX_LOG_LINES:
                del self.log[: len(self.log) - MAX_LOG_LINES]

    def _set_progress(self, done: int, total: int) -> None:
        with self._lock:
            self.progress_done = done
            self.progress_total = total

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "id": self.id,
                "type": self.type,
                "status": self.status,
                "log": list(self.log),
                "progress": {"done": self.progress_done, "total": self.progress_total},
                "result": self.result,
                "error": self.error,
            }


class JobManager:
    def __init__(self):
        self._jobs: dict[str, Job] = {}
        self._order: list[str] = []
        self._lock = threading.Lock()

    def start(self, job_type: str, fn: Callable[[JobContext], Any]) -> str:
        """登记并起后台线程跑 fn(ctx)。返回 job_id。"""
        job_id = uuid.uuid4().hex[:12]
        job = Job(job_id, job_type)
        with self._lock:
            self._jobs[job_id] = job
            self._order.append(job_id)
            self._evict_locked()

        def _runner():
            job.status = "running"
            ctx = JobContext(job)
            try:
                job.result = fn(ctx)
                job.status = "success"
            except Exception as e:  # noqa: BLE001 —— 任务失败不应崩线程，记录到 job
                job.error = f"{type(e).__name__}: {e}"
                job._append_log("❌ " + job.error)
                job._append_log(traceback.format_exc().splitlines()[-1])
                job.status = "error"

        threading.Thread(target=_runner, name=f"job-{job_type}-{job_id}",
                         daemon=True).start()
        return job_id

    def _evict_locked(self) -> None:
        while len(self._order) > MAX_JOBS:
            old = self._order.pop(0)
            self._jobs.pop(old, None)

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self) -> list[dict]:
        """按时间倒序返回 job 摘要（不含全量日志）。"""
        with self._lock:
            jobs = [self._jobs[j] for j in reversed(self._order) if j in self._jobs]
        out = []
        for j in jobs:
            d = j.to_dict()
            d["log"] = d["log"][-1:]      # 摘要只带最后一行
            out.append(d)
        return out
