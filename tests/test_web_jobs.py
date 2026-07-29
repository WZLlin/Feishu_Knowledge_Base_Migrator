"""JobManager 测试：后台线程跑任务、捕获日志/进度、成功与失败状态流转。"""
import json
import sqlite3
import time

from kb_migrator.web.jobs import JobManager


def _wait(mgr, job_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = mgr.get(job_id)
        if job and job.to_dict()["status"] in (
                "success", "error", "cancelled", "interrupted"):
            return job
        time.sleep(0.02)
    raise AssertionError("job 未在超时内结束")


def test_job_success_captures_log_and_progress():
    mgr = JobManager()

    def fn(ctx):
        ctx.log("开始")
        for i in range(1, 4):
            ctx.progress(i, 3)
        ctx.log("结束")
        return {"n": 3}

    jid = mgr.start("demo", fn)
    job = _wait(mgr, jid)
    d = job.to_dict()
    assert d["status"] == "success"
    assert d["result"] == {"n": 3}
    assert d["progress"] == {"done": 3, "total": 3}
    assert d["log"][0] == "开始" and d["log"][-1] == "结束"


def test_job_error_sets_status_and_message():
    mgr = JobManager()

    def boom(ctx):
        ctx.log("即将失败")
        raise ValueError("炸了")

    jid = mgr.start("boom", boom)
    job = _wait(mgr, jid)
    assert job.status == "error"
    assert "炸了" in job.error
    assert any("炸了" in ln for ln in job.log)


def test_list_returns_recent_first():
    mgr = JobManager()
    ids = [mgr.start("t", lambda ctx: None) for _ in range(3)]
    for i in ids:
        _wait(mgr, i)
    listed = mgr.list()
    assert len(listed) == 3
    # 倒序：最后启动的在最前
    assert listed[0]["id"] == ids[-1]


def test_job_snapshot_survives_new_manager(tmp_path):
    path = str(tmp_path / "jobs.db")
    mgr = JobManager(path)
    jid = mgr.start("persist", lambda ctx: {"ok": True})
    _wait(mgr, jid)
    restored = JobManager(path).get(jid)
    assert restored is not None
    assert restored.to_dict()["result"] == {"ok": True}


def test_lock_key_rejects_concurrent_job():
    mgr = JobManager()
    gate = __import__("threading").Event()
    jid = mgr.start("locked", lambda ctx: gate.wait(1), lock_key="same")
    try:
        try:
            mgr.start("locked", lambda ctx: None, lock_key="same")
            assert False, "应拒绝同一 lock_key 的并发任务"
        except RuntimeError:
            pass
    finally:
        gate.set()
    _wait(mgr, jid)


def test_restart_marks_unfinished_snapshot_interrupted(tmp_path):
    path = str(tmp_path / "jobs.db")
    JobManager(path)
    snapshot = {
        "id": "unfinished", "type": "scan", "status": "running",
        "log": ["started"], "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO web_jobs VALUES (?,?,?,?,?)",
            ("unfinished", "source:local", "running", snapshot["updated_at"],
             json.dumps(snapshot)),
        )
    restored = JobManager(path).get("unfinished")
    assert restored.status == "interrupted"
    assert "服务重启" in restored.error
    assert any("interrupted" in line for line in restored.log)


def test_global_concurrency_limit_rejects_extra_job():
    mgr = JobManager(max_concurrent=1)
    gate = __import__("threading").Event()
    jid = mgr.start("first", lambda ctx: gate.wait(1))
    try:
        try:
            mgr.start("second", lambda ctx: None)
            assert False, "应拒绝超过全局并发上限的任务"
        except RuntimeError as exc:
            assert "并发任务已达上限" in str(exc)
    finally:
        gate.set()
    _wait(mgr, jid)


def test_cancel_stops_job_at_safe_checkpoint():
    mgr = JobManager()
    entered = __import__("threading").Event()
    release = __import__("threading").Event()

    def work(ctx):
        entered.set()
        release.wait(1)
        ctx.raise_if_cancelled()
        return "should-not-complete"

    jid = mgr.start("cancel-me", work)
    assert entered.wait(1)
    assert mgr.cancel(jid) is True
    release.set()
    job = _wait(mgr, jid)
    assert job.status == "cancelled"
    assert job.result is None
