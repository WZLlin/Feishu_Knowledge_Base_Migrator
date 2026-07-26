"""编排器端到端离线测试：本地文件夹 → 分类 → 人工确认 → load(dry-run)。"""
from docx import Document

from kb_migrator.connectors.local_folder import LocalFolderConnector
from kb_migrator.ledger import Ledger
from kb_migrator.models import Stage
from kb_migrator.pipeline.classify import Classifier
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy

TAX = "config/taxonomy.yaml"


def _mk(path, paras):
    d = Document()
    for p in paras:
        d.add_paragraph(p)
    d.save(path)


def test_full_pipeline_offline(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    _mk(src / "制度.docx", ["远程办公管理制度", "本管理办法规定流程与审批。"])
    _mk(src / "纪要.docx", ["项目会议纪要", "会议结论：按期上线。"])
    # 精确重复
    _mk(src / "制度副本.docx", ["远程办公管理制度", "本管理办法规定流程与审批。"])

    led = Ledger(str(tmp_path / "ledger.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "work"), confidence_threshold=0.85)

    ing = orch.ingest(LocalFolderConnector(str(src)))
    assert ing["discovered"] == 3
    assert ing["exact_dup"] == 1          # 副本被 SHA256 精确去重
    assert ing["extracted"] == 2

    orch.dedup_pass()
    clf = Classifier(tx, api_key="")       # 离线启发式
    cls = orch.classify_pass(clf)
    # 离线一律 needs_review，故都进人工队列，不自动确认
    assert cls["auto_confirmed"] == 0
    assert cls["to_review"] == 2

    # 人工确认一条
    keys = [r["stable_key"] for r in led.pending_review()]
    orch.confirm(keys[0], "01 制度与流程")

    # load dry-run
    load = orch.load_pass(writer=None, folder_map={})
    assert load["dry_run"] == 1
    led.close()


def test_resume_is_idempotent(tmp_path):
    """重复 ingest 不重复处理已完成条目（断点续跑）。"""
    src = tmp_path / "src"
    src.mkdir()
    _mk(src / "a.docx", ["制度内容", "流程说明"])
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"))
    conn = LocalFolderConnector(str(src))
    first = orch.ingest(conn)
    second = orch.ingest(conn)             # 二次运行
    assert first["discovered"] == 1
    assert second["discovered"] == 0       # 幂等：无新增
    led.close()
