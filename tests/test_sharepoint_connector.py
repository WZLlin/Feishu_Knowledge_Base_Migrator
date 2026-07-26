"""SharePoint 连接器（离线）：桩掉 httpx.Client，验证分页/递归/下载/权限映射。

不触网：把连接器的 self.http 换成脚本化的假 client（按 URL 返回预置响应），
并直接注入 self._token 跳过 Entra 认证（无论环境是否装了 msal 都不触网）。
"""
from kb_migrator.connectors.sharepoint import SharePointConnector


class _Resp:
    def __init__(self, json_body=None, content=b""):
        self._json = json_body or {}
        self.content = content
        self.status_code = 200
        self.headers = {}

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


class _FakeHttp:
    """按 URL 关键片段返回预置 Graph 响应；记录调用过的 GET。"""
    def __init__(self):
        self.gets = []

    def get(self, url, headers=None, **kw):
        self.gets.append(url)
        if "/sites?" in url:
            return _Resp({"value": [{"id": "site1", "webUrl": "https://sp/site1"}]})
        if url.endswith("/sites/site1/drives"):
            return _Resp({"value": [{"id": "drive1"}]})
        if "/drives/drive1/items/root/children" in url:
            return _Resp({"value": [
                {"id": "folderA", "name": "A", "folder": {}},
                {"id": "f1", "name": "报告.docx", "size": 12,
                 "file": {"mimeType": "app/docx", "hashes": {"sha256Hash": "SRC"}},
                 "webUrl": "https://sp/f1",
                 "createdBy": {"user": {"displayName": "张三"}},
                 "createdDateTime": "2026-01-02T03:04:05Z",
                 "lastModifiedDateTime": "2026-01-03T03:04:05Z"},
            ]})
        if "/drives/drive1/items/folderA/children" in url:
            return _Resp({"value": [
                {"id": "f2", "name": "手册.pdf", "size": 34,
                 "file": {"mimeType": "app/pdf"}, "webUrl": "https://sp/f2"},
            ]})
        if url.endswith("/items/f1/content"):
            return _Resp(content=b"hello-bytes")
        if url.endswith("/items/f1/permissions"):
            return _Resp({"value": [
                {"roles": ["read"], "grantedToV2": {"user": {"email": "a@x.com"}}},
                {"roles": ["write"], "grantedToV2": {"user": {"email": "b@x.com"}}},
            ]})
        return _Resp({"value": []})


def _conn(work_dir):
    c = SharePointConnector("tenant", "cid", "secret", str(work_dir), http=_FakeHttp())
    c._token = "tok"          # 跳过 Entra 认证，避免任何网络
    return c


def test_discover_walks_sites_drives_and_recurses_folders(tmp_path):
    items = list(_conn(tmp_path).discover())
    # 根目录 f1 + 子文件夹 folderA 下的 f2；文件夹本身不产出条目
    assert {i.original_name for i in items} == {"报告.docx", "手册.pdf"}
    assert {i.source_id for i in items} == {"drive1/f1", "drive1/f2"}


def test_fetch_downloads_and_hashes_actual_bytes(tmp_path):
    conn = _conn(tmp_path)
    item = next(i for i in conn.discover() if i.source_id == "drive1/f1")
    item = conn.fetch(item)
    with open(item.local_blob_path, "rb") as fp:
        assert fp.read() == b"hello-bytes"
    # 按实际下载字节重算哈希，不信任源侧 sha256Hash
    import hashlib
    assert item.content_sha256 == hashlib.sha256(b"hello-bytes").hexdigest()


def test_fetch_maps_permissions_roles(tmp_path):
    conn = _conn(tmp_path)
    item = next(i for i in conn.discover() if i.source_id == "drive1/f1")
    item = conn.fetch(item)
    assert {p.principal: p.role for p in item.permissions} == {
        "a@x.com": "view", "b@x.com": "edit"}


def test_author_and_dates_parsed(tmp_path):
    item = next(i for i in _conn(tmp_path).discover() if i.source_id == "drive1/f1")
    assert item.author == "张三"
    assert item.created_at is not None and item.modified_at is not None
