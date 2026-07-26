"""阶段1 bootstrap + load 真实写入路径测试（用 Fake writer，不触网）。

验证：
- bootstrap_drive_tree 建根 + 各分类文件夹，folder_map 回填齐全并持久化；
- load_pass 用 folder_map 把 CONFIRMED 文档上传到对应分类文件夹、收紧对外分享、
  条目转 LOADED 并记 feishu_token；
- 幂等：二次 bootstrap 不重复建目录。
"""
import json

from docx import Document

from kb_migrator.connectors.local_folder import LocalFolderConnector
from kb_migrator.feishu.bootstrap import FeishuBootstrapper
from kb_migrator.ledger import Ledger
from kb_migrator.models import Stage
from kb_migrator.pipeline.classify import Classifier
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy

TAX = "config/taxonomy.yaml"


class FakeWriter:
    """记录调用并返回假 token，模拟 FeishuWriter 的最小接口。"""

    def __init__(self):
        self.folders = {}        # token -> (name, parent)
        self.uploads = []        # (local_path, folder_token, name) -> file_token
        self.locked = []         # (file_token, obj_type)
        self._n = 0

    def _tok(self, prefix):
        self._n += 1
        return f"{prefix}_{self._n}"

    def create_folder(self, name, parent_token=""):
        tok = self._tok("fld")
        self.folders[tok] = (name, parent_token)
        return tok

    def upload_file(self, local_path, parent_folder_token, file_name=None):
        tok = self._tok("file")
        self.uploads.append((local_path, parent_folder_token, file_name))
        self._last_file_token = tok
        return tok

    def lock_down_external(self, token, obj_type, sensitive=False):
        self.locked.append((token, obj_type))


def _mk(path, paras):
    d = Document()
    for p in paras:
        d.add_paragraph(p)
    d.save(path)


def test_bootstrap_and_load_realwrite(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    _mk(src / "制度.docx", ["远程办公管理制度", "本管理办法规定流程与审批。"])

    led = Ledger(str(tmp_path / "ledger.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "work"), confidence_threshold=0.85)

    orch.ingest(LocalFolderConnector(str(src)))
    orch.dedup_pass()
    orch.classify_pass(Classifier(tx, api_key=""))       # 离线 -> 进人工队列
    key = led.pending_review()[0]["stable_key"]
    orch.confirm(key, "01 制度与流程")

    # ── 阶段1 bootstrap（Fake writer）──
    fw = FakeWriter()
    targets_file = str(tmp_path / "targets.json")
    boot = FeishuBootstrapper(fw, tx, targets_file)
    t = boot.bootstrap_drive_tree(root_name="测试知识库")

    assert t["mode"] == "drive"
    assert t["root_token"]
    # 6 分类 + 待整理 + 归档 = 8
    assert len(t["folder_map"]) == 8
    assert "01 制度与流程" in t["folder_map"]
    assert "99 归档" in t["folder_map"]
    # 持久化落盘
    saved = json.load(open(targets_file, encoding="utf-8"))
    assert saved["folder_map"] == t["folder_map"]

    # ── 幂等：二次 bootstrap 不新增文件夹 ──
    folders_before = len(fw.folders)
    boot.bootstrap_drive_tree(root_name="测试知识库")
    assert len(fw.folders) == folders_before

    # ── load 真实写入（Fake writer）──
    load = orch.load_pass(fw, t["folder_map"])
    assert load["loaded"] == 1
    assert load["failed"] == 0

    # 文档上传到 01 制度与流程 对应文件夹
    assert len(fw.uploads) == 1
    _, folder_token, _ = fw.uploads[0]
    assert folder_token == t["folder_map"]["01 制度与流程"]
    # 收紧了对外分享
    assert len(fw.locked) == 1
    # 条目转 LOADED 且记录了 feishu_token
    rows = led.items_in_stage(Stage.LOADED)
    assert len(rows) == 1
    assert rows[0]["feishu_token"]
    led.close()


def test_load_routes_triage_when_category_folder_missing(tmp_path):
    """分类无对应文件夹时回退到 90 待整理 文件夹（不丢件）。"""
    src = tmp_path / "src"
    src.mkdir()
    _mk(src / "杂项.docx", ["一些无法归类的内容"])
    led = Ledger(str(tmp_path / "l.db"))
    tx = Taxonomy.load(TAX)
    orch = Orchestrator(led, tx, str(tmp_path / "w"), confidence_threshold=0.85)
    orch.ingest(LocalFolderConnector(str(src)))
    orch.dedup_pass()
    orch.classify_pass(Classifier(tx, api_key=""))
    key = led.pending_review()[0]["stable_key"]
    orch.confirm(key, "02 项目资料")

    fw = FakeWriter()
    # folder_map 故意只给 triage，不给 02
    folder_map = {tx.triage_path: "fld_triage"}
    load = orch.load_pass(fw, folder_map)
    assert load["loaded"] == 1
    assert fw.uploads[0][1] == "fld_triage"    # 回退到待整理
    led.close()
