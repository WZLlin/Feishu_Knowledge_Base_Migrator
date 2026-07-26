"""JobManager 测试：后台线程跑任务、捕获日志/进度、成功与失败状态流转。"""
import time

from kb_migrator.web.jobs import JobManager


def _wait(mgr, job_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = mgr.get(job_id)
        if job and job.status in ("success", "error"):
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
