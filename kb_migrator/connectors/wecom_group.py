"""企业微信「群名打标」连接器（appchat 应用群 API）。

用途：迁移完成后把原群改名为「原群名[已备份]」，并可发一条含飞书入口链接的通知。

**平台边界（诚实说明，务必理解）**：企业微信只允许应用改名/发消息到**该应用自己
创建的服务群（appchat）**；对用户在客户端自建的普通群聊，应用**既不能改名也不能发
消息**。因此本连接器按「尽力而为 + 降级」策略：
  1. 先尝试 appchat/update 改名；
  2. 改名失败（多为非应用自建群）则尝试 appchat/send 发通知消息；
  3. 两者都不行则返回 manual —— 记录「应打标的群名 + 飞书链接」交管理员/群主手动处理。

认证与微盘一致：gettoken(corpid+secret)，token 2h；服务器 IP 须在可信白名单。
http 可注入以便离线测试。
"""
from __future__ import annotations

import time
from typing import Optional

import httpx

_QYAPI = "https://qyapi.weixin.qq.com/cgi-bin"


class WeComGroupConnector:
    source_name = "wecom_group"

    def __init__(self, corp_id: str, app_secret: str,
                 http: httpx.Client | None = None):
        self.corp_id = corp_id
        self.secret = app_secret
        self.http = http or httpx.Client(timeout=60)
        self._token: Optional[str] = None
        self._token_exp: float = 0.0

    @property
    def available(self) -> bool:
        return bool(self.corp_id and self.secret)

    def _access_token(self) -> str:
        if self._token and time.time() < self._token_exp - 300:
            return self._token
        resp = self.http.get(
            f"{_QYAPI}/gettoken",
            params={"corpid": self.corp_id, "corpsecret": self.secret},
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode", 0) != 0:
            raise RuntimeError(f"企业微信 gettoken 失败: {data}")
        self._token = data["access_token"]
        self._token_exp = time.time() + int(data.get("expires_in", 7200))
        return self._token

    def _post(self, path: str, payload: dict) -> dict:
        resp = self.http.post(
            f"{_QYAPI}{path}",
            params={"access_token": self._access_token()}, json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def tag_group(self, chat_id: str, original_name: str,
                  feishu_url: str = "") -> dict:
        """尽力把群名改为「原群名[已备份]」，失败则降级发通知，再失败记 manual。

        返回 {"tag_status": renamed|notified|manual, "detail": ...}。
        未配置应用凭证时直接 manual（不触网）。
        """
        tagged_name = f"{original_name}[已备份]" if original_name else "[已备份]"
        if not self.available:
            return {"tag_status": "manual",
                    "detail": "未配置 WECOM_APP_SECRET，无法调用 appchat；请手动打标"}

        # 1) 尝试改名（仅对应用自建服务群有效）
        try:
            r = self._post("/appchat/update", {"chatid": chat_id, "name": tagged_name})
            if r.get("errcode", 0) == 0:
                return {"tag_status": "renamed", "detail": f"已改名为「{tagged_name}」"}
            rename_err = f"appchat/update errcode={r.get('errcode')} {r.get('errmsg','')}"
        except Exception as e:  # noqa: BLE001
            rename_err = f"appchat/update 异常 {e}"

        # 2) 降级：发一条含飞书入口的通知消息（同样仅应用自建群可发）
        text = f"本群知识已备份至飞书。{('入口：' + feishu_url) if feishu_url else ''}"
        try:
            r = self._post("/appchat/send", {
                "chatid": chat_id, "msgtype": "text", "text": {"content": text}})
            if r.get("errcode", 0) == 0:
                return {"tag_status": "notified",
                        "detail": f"改名不可用（{rename_err}）；已发备份通知消息"}
            send_err = f"appchat/send errcode={r.get('errcode')} {r.get('errmsg','')}"
        except Exception as e:  # noqa: BLE001
            send_err = f"appchat/send 异常 {e}"

        # 3) 两者都不行：非应用自建群的平台限制，记 manual 交人工
        return {
            "tag_status": "manual",
            "detail": (f"平台限制：应用无法改名/发消息到非自建群（{rename_err}；{send_err}）。"
                       f"请群主手动把群名改为「{tagged_name}」"
                       + (f"，飞书入口：{feishu_url}" if feishu_url else "")),
        }
