from kb_migrator.utils.naming import canonical_name, slugify, long_path


def test_slugify_preserves_chinese_and_strips_illegal():
    assert slugify("远程办公/指引:草案") == "远程办公指引草案"
    assert slugify("  多个   空格 ") == "多个-空格"
    assert slugify("") == "untitled"


def test_canonical_name_normalizes_date_and_version():
    n = canonical_name(doc_type="policy", title="远程办公 指引",
                       doc_date="2026/3/1", version="2", ext="docx")
    assert n == "2026-03-01_policy_远程办公-指引_v2.docx"


def test_canonical_name_defaults_date_when_unparseable():
    n = canonical_name(doc_type="doc", title="无日期", doc_date="不是日期")
    # 退回今天，格式仍为 YYYY-MM-DD
    assert n.count("_") == 3 and n.split("_")[0].count("-") == 2


def test_long_path_noop_on_relative():
    assert long_path("relative/path.txt") == "relative/path.txt"
