"""群名打标连接器（离线）：桩掉 httpx，验证「尽力改名→降级发通知→记 manual」三级。

平台边界：应用只能改名/发消息到自己创建的服务群；对用户自建群两者都失败，降级为
manual（记录应打标的群名交人工），本测试覆盖三条路径 + 未配置凭证短路。
"""
from kb_migrator.connectors.wecom_group import WeComGroupConnector


class _Resp:
    def __init__(self, body):
        self._b = body

    def json(self):
        return self._b

    def raise_for_status(self):
        pass


class _FakeHttp:
    def __init__(self, update_body, send_body=None):
        self.posts = []
        self._update = update_body
        self._send = send_body or {"errcode": 0}

    def get(self, url, params=None, **kw):
        return _Resp({"errcode": 0, "access_token": "tok", "expires_in": 7200})

    def post(self, url, params=None, json=None, **kw):
        self.posts.append((url, json))
        if url.endswith("/appchat/update"):
            return _Resp(self._update)
        if url.endswith("/appchat/send"):
            return _Resp(self._send)
        return _Resp({"errcode": 0})


def test_rename_success():
    http = _FakeHttp(update_body={"errcode": 0})
    conn = WeComGroupConnector("corp", "secret", http=http)
    res = conn.tag_group("chatX", "上线群")
    assert res["tag_status"] == "renamed"
    assert "上线群[已备份]" in res["detail"]
    # 只调了改名，没走降级
    assert [u for u, _ in http.posts] == ["https://qyapi.weixin.qq.com/cgi-bin/appchat/update"]


def test_rename_fails_then_notify():
    # 非应用自建群改名失败，降级发通知成功
    http = _FakeHttp(update_body={"errcode": 86220, "errmsg": "not app chat"},
                     send_body={"errcode": 0})
    conn = WeComGroupConnector("corp", "secret", http=http)
    res = conn.tag_group("chatX", "上线群", feishu_url="https://feishu/x")
    assert res["tag_status"] == "notified"
    assert any(u.endswith("/appchat/send") for u, _ in http.posts)


def test_rename_and_notify_both_fail_manual():
    http = _FakeHttp(update_body={"errcode": 86220, "errmsg": "not app chat"},
                     send_body={"errcode": 86220, "errmsg": "not app chat"})
    conn = WeComGroupConnector("corp", "secret", http=http)
    res = conn.tag_group("chatX", "上线群", feishu_url="https://feishu/x")
    assert res["tag_status"] == "manual"
    assert "上线群[已备份]" in res["detail"]
    assert "https://feishu/x" in res["detail"]


def test_unconfigured_short_circuits_to_manual():
    http = _FakeHttp(update_body={"errcode": 0})
    conn = WeComGroupConnector("", "", http=http)     # 无凭证
    res = conn.tag_group("chatX", "上线群")
    assert res["tag_status"] == "manual"
    assert http.posts == []                            # 不触网
