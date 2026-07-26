import os

from kb_migrator.connectors.local_folder import LocalFolderConnector


def test_discover_skips_lockfiles_and_unsupported(tmp_path):
    (tmp_path / "a.docx").write_bytes(b"x")
    (tmp_path / "~$a.docx").write_bytes(b"lock")     # 锁文件应跳过
    (tmp_path / "note.txt").write_text("hi", encoding="utf-8")
    (tmp_path / "image.png").write_bytes(b"img")      # 非知识类后缀跳过
    conn = LocalFolderConnector(str(tmp_path))
    names = sorted(i.original_name for i in conn.discover())
    assert names == ["a.docx", "note.txt"]


def test_stable_source_id_is_relative(tmp_path):
    sub = tmp_path / "制度"
    sub.mkdir()
    (sub / "x.docx").write_bytes(b"x")
    conn = LocalFolderConnector(str(tmp_path))
    items = list(conn.discover())
    assert items[0].source_id == "制度/x.docx"     # 正斜杠相对路径


def test_fetch_computes_sha256(tmp_path):
    p = tmp_path / "note.txt"
    p.write_text("hello", encoding="utf-8")
    conn = LocalFolderConnector(str(tmp_path))
    item = next(conn.discover())
    item = conn.fetch(item)
    assert item.content_sha256 and len(item.content_sha256) == 64
    assert item.local_blob_path == item.source_path
