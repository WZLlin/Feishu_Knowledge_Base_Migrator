""".env 读写 / 脱敏 / upsert 测试。用 KBM_ENV_FILE 指向临时文件隔离。"""
from kb_migrator.web import settings_io


def _use_tmp_env(tmp_path, monkeypatch, initial=""):
    p = tmp_path / ".env"
    p.write_text(initial, encoding="utf-8")
    monkeypatch.setenv("KBM_ENV_FILE", str(p))
    return p


def test_write_env_upserts_and_preserves_others(tmp_path, monkeypatch):
    p = _use_tmp_env(tmp_path, monkeypatch,
                     "# 注释\nKBM_WORK_DIR=./data/work\nFEISHU_APP_ID=old\n")
    changed = settings_io.write_env({"FEISHU_APP_ID": "cli_new", "FEISHU_APP_SECRET": "secret123"})
    assert set(changed) == {"FEISHU_APP_ID", "FEISHU_APP_SECRET"}
    env = settings_io.read_env()
    assert env["FEISHU_APP_ID"] == "cli_new"        # 更新
    assert env["FEISHU_APP_SECRET"] == "secret123"  # 追加
    assert env["KBM_WORK_DIR"] == "./data/work"     # 未动
    assert "# 注释" in p.read_text(encoding="utf-8")  # 注释保留


def test_write_env_skips_empty_placeholder_and_unknown(tmp_path, monkeypatch):
    _use_tmp_env(tmp_path, monkeypatch, "FEISHU_APP_SECRET=keep\n")
    changed = settings_io.write_env({
        "FEISHU_APP_SECRET": settings_io.mask("keep", True),  # 脱敏占位回传 -> 跳过
        "FEISHU_APP_ID": "",               # 空 -> 跳过
        "NOT_A_FIELD": "x",                # 非白名单 -> 跳过
    })
    assert changed == []
    assert settings_io.read_env()["FEISHU_APP_SECRET"] == "keep"


def test_masked_settings_hides_sensitive(tmp_path, monkeypatch):
    _use_tmp_env(tmp_path, monkeypatch, "FEISHU_APP_ID=app123\nFEISHU_APP_SECRET=supersecret\n")
    ms = settings_io.masked_settings()
    assert ms["FEISHU_APP_ID"]["value"] == "app123"        # 非敏感原样
    assert ms["FEISHU_APP_ID"]["configured"] is True
    assert ms["FEISHU_APP_SECRET"]["value"].startswith("••••")  # 敏感脱敏
    assert ms["FEISHU_APP_SECRET"]["configured"] is True
    assert ms["MS_CLIENT_SECRET"]["configured"] is False   # 未配置
