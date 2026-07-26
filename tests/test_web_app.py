"""Web 控制台端到端（离线）：TestClient 跑 scan-local job → 轮询至成功 → 台账新增。"""
import time

import pytest
from docx import Document
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    # 隔离到临时目录，避免动到真实 data/
    monkeypatch.setenv("KBM_LEDGER_DB", str(tmp_path / "ledger.db"))
    monkeypatch.setenv("KBM_WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setenv("KBM_TAXONOMY_FILE", "config/taxonomy.yaml")
    monkeypatch.setenv("KBM_FEISHU_TARGETS_FILE", str(tmp_path / "targets.json"))
    from kb_migrator.config import get_settings
    get_settings.cache_clear()
    from kb_migrator.web.app import app
    yield TestClient(app)
    get_settings.cache_clear()


def _poll(client, job_id, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        j = client.get(f"/api/jobs/{job_id}").json()
        if j["status"] in ("success", "error"):
            return j
        time.sleep(0.1)
    raise AssertionError("job 超时未结束")


def test_status_and_settings_endpoints(client):
    s = client.get("/api/status").json()
    assert s["total"] == 0                     # 新台账为空
    assert "sink_ratio" in s
    fields = client.get("/api/settings").json()["fields"]
    assert "FEISHU_APP_ID" in fields and "ANTHROPIC_API_KEY" in fields


def test_scan_local_job_end_to_end(client, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    d = Document()
    d.add_paragraph("远程办公管理制度")
    d.add_paragraph("本管理办法规定流程与审批。")
    d.save(str(src / "制度.docx"))

    r = client.post("/api/jobs/scan-local", json={"path": str(src)})
    assert r.status_code == 200
    job = _poll(client, r.json()["job_id"])
    assert job["status"] == "success", job
    assert job["result"]["discovered"] == 1
    assert job["result"]["extracted"] == 1

    # 台账已新增
    assert client.get("/api/status").json()["total"] == 1


def test_scan_local_rejects_bad_path(client):
    r = client.post("/api/jobs/scan-local", json={"path": "Z:/nonexistent-xyz"})
    assert r.status_code == 400


def test_retry_job_dry_run(client):
    # 空台账下 retry(dry-run) 应成功：重排 0 条、无真实写入
    r = client.post("/api/jobs/retry", json={"dry_run": True})
    assert r.status_code == 200
    job = _poll(client, r.json()["job_id"])
    assert job["status"] == "success", job
    assert job["result"]["requeued"] == 0
