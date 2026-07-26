"""失败项重试：requeue_failed 选择性回捞 + retry_failed_loads 幂等护栏。

- 只回捞 error_detail 以 "load: " 开头的 FAILED，不误伤 fetch/ingest 失败；
- 已有 feishu_token 的条目重试时**不重传**（upload_file 非幂等），只重跑收紧。
"""
from kb_migrator.ledger import Ledger
from kb_migrator.models import SourceItem, SourceType, Stage
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy


def _mk(tmp_path):
    led = Ledger(str(tmp_path / "ledger.db"))
    tx = Taxonomy.load("config/taxonomy.yaml")
    orch = Orchestrator(led, tx, str(tmp_path / "work"))
    return led, orch


def _discover(led, sid, name):
    item = SourceItem(source_type=SourceType.LOCAL, source_id=sid,
                      source_path=f"/x/{name}", original_name=name)
    led.upsert_discovered(item)
    return item.stable_key()


def test_requeue_only_load_failures(tmp_path):
    led, _orch = _mk(tmp_path)
    ka = _discover(led, "a", "甲.docx")
    kb = _discover(led, "b", "乙.docx")
    led.update(ka, stage=Stage.FAILED.value, error_detail="load: 上传超时")
    led.update(kb, stage=Stage.FAILED.value, error_detail="fetch: 下载失败")

    n = led.requeue_failed()          # 默认前缀 "load: "
    assert n == 1
    assert led.get(ka)["stage"] == Stage.CONFIRMED.value
    assert led.get(ka)["error_detail"] is None
    assert led.get(kb)["stage"] == Stage.FAILED.value   # fetch 失败不动
    led.close()


def test_retry_failed_loads_dry_run(tmp_path):
    led, orch = _mk(tmp_path)
    ka = _discover(led, "a", "甲.docx")
    led.update(ka, stage=Stage.FAILED.value, error_detail="load: 上传超时")

    stats = orch.retry_failed_loads(writer=None, folder_map={})
    assert stats["requeued"] == 1
    assert stats["dry_run"] == 1      # writer=None -> dry-run 计数
    led.close()


class _FakeWriter:
    def __init__(self):
        self.uploads = 0
        self.locks = 0

    def upload_file(self, blob, folder, name):
        self.uploads += 1
        return "new_token"

    def lock_down_external(self, token, ftype):
        self.locks += 1


def test_retry_skips_reupload_when_token_exists(tmp_path):
    led, orch = _mk(tmp_path)
    ka = _discover(led, "a", "甲.docx")
    # 上次已上传成功（有 token），仅收紧步骤失败
    led.update(ka, stage=Stage.FAILED.value, error_detail="load: 收紧失败",
               feishu_token="tok_existing", local_blob_path=str(tmp_path / "甲.docx"),
               category="01 制度与流程", canonical_name="甲.docx")

    w = _FakeWriter()
    stats = orch.retry_failed_loads(writer=w, folder_map={"01 制度与流程": "fld"})
    assert stats["requeued"] == 1
    assert stats["loaded"] == 1
    assert w.uploads == 0             # 幂等护栏：不重传
    assert w.locks == 1               # 只重跑收紧
    assert led.get(ka)["stage"] == Stage.LOADED.value
    assert led.get(ka)["feishu_token"] == "tok_existing"
    led.close()
