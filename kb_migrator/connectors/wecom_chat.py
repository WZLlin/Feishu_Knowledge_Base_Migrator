"""企业微信群聊「会话内容存档」连接器（Finance API）。

合规前提（须开发前拍板）：
- 企业已开通「会话内容存档」增值服务；
- 群内每位成员在客户端本人同意「聊天记录上传」（未同意者消息不入存档）；
- 消息在腾讯侧仅保留有限时长（历史约 6 个月）；
- 拉取到的消息为 RSA 加密，企业自托管私钥解密（私钥独立隔离）。

技术路径：
1. WeWorkFinanceSdk（原生库）GetChatData(seq, limit) 拉取密文批次；
2. 用企业 RSA 私钥解密每条的 encrypt_random_key 得到会话密钥；
3. SDK DecryptData(random_key, encrypt_chat_msg) 解出明文 JSON 消息；
4. 媒体消息(文件/图片)再调 SDK GetMediaData 换取二进制。

本模块把「SDK 绑定 + 解密」与「消息聚合成会话片段」解耦：
- SDK 部分在无原生库时不可用（online=False），但不影响聚合逻辑离线可测；
- 聚合器把逐条消息按自然日归并为「会话片段」（最小知识单元）。
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, Optional

# 系统/无正文类消息，聚合时剔除
_NON_CONTENT_TYPES = {"revoke", "agree", "disagree", "sphfeed"}


@dataclass
class ConversationSegment:
    """会话片段：一个群某个自然日的消息归并，作为最小知识单元。"""
    chat_id: str
    date: str                       # YYYY-MM-DD
    participants: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)     # "时间 发送人: 内容"
    files: list[dict] = field(default_factory=list)    # 附带文件消息(待素材下载)
    last_seq: int = 0

    def to_text(self) -> str:
        head = f"群聊记录 | {self.date} | 参与人: {', '.join(self.participants)}"
        return head + "\n" + "\n".join(self.lines)


def _msg_text(msg: dict) -> Optional[str]:
    """从一条明文消息提取可读文本。不同类型取不同字段。"""
    mtype = msg.get("msgtype", "")
    if mtype == "text":
        return msg.get("text", {}).get("content", "")
    if mtype == "link":
        link = msg.get("link", {})
        return f"[链接] {link.get('title','')} {link.get('link_url','')}"
    if mtype == "voice":
        return "[语音]"          # 如启用语音转文字，可在此填充
    if mtype in ("image", "video", "emotion"):
        return f"[{mtype}]"
    return None


def aggregate_messages(chat_id: str, messages: list[dict]) -> list[ConversationSegment]:
    """把一个群的逐条明文消息按自然日聚合为会话片段。

    messages 已按时间升序；每条含 msgtype/from/msgtime/seq 等字段。
    剔除系统/撤回类；文件类单列到 segment.files 供后续素材下载。
    """
    segments: dict[str, ConversationSegment] = {}
    for msg in messages:
        mtype = msg.get("msgtype", "")
        if mtype in _NON_CONTENT_TYPES:
            continue
        ts = int(msg.get("msgtime", 0)) / 1000
        day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        seg = segments.setdefault(day, ConversationSegment(chat_id=chat_id, date=day))
        sender = msg.get("from", "unknown")
        if sender not in seg.participants:
            seg.participants.append(sender)
        seg.last_seq = max(seg.last_seq, int(msg.get("seq", 0)))
        if mtype == "file":
            f = msg.get("file", {})
            seg.files.append({"sdkfileid": f.get("sdkfileid", ""),
                              "filename": f.get("filename", ""),
                              "filesize": f.get("filesize", 0)})
            seg.lines.append(f"[文件] {f.get('filename','')}")
            continue
        text = _msg_text(msg)
        if text:
            hhmm = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M")
            seg.lines.append(f"{hhmm} {sender}: {text}")
    return [segments[d] for d in sorted(segments)]


class ChatArchiveConnector:
    """会话存档 SDK 绑定 + 解密。无原生库时 online=False。"""

    source_name = "wecom_chat"

    def __init__(self, corp_id: str, archive_secret: str, private_key_pem: str,
                 sdk_lib_path: str = ""):
        self.corp_id = corp_id
        self.archive_secret = archive_secret
        self.private_key_pem = private_key_pem
        self._sdk = None
        self._online = False
        self._load_sdk(sdk_lib_path)

    @property
    def online(self) -> bool:
        return self._online

    def _load_sdk(self, sdk_lib_path: str) -> None:
        """加载 WeWorkFinanceSdk。缺库/缺配置时保持 online=False。"""
        if not (self.corp_id and self.archive_secret and self.private_key_pem):
            return
        try:
            # 官方提供 C 库；此处通过环境提供的 python 封装或 ctypes 加载。
            # 生产部署时替换为实际 SDK 初始化；缺失则维持离线。
            import WeWorkFinanceSdk  # type: ignore

            self._sdk = WeWorkFinanceSdk.init(self.corp_id, self.archive_secret)
            self._online = True
        except Exception:
            self._online = False

    def _rsa_decrypt_key(self, encrypt_random_key: str) -> bytes:
        """用企业 RSA 私钥解密 encrypt_random_key，得到会话 AES 密钥。"""
        from Crypto.Cipher import PKCS1_v1_5
        from Crypto.PublicKey import RSA

        key = RSA.import_key(self.private_key_pem)
        cipher = PKCS1_v1_5.new(key)
        return cipher.decrypt(base64.b64decode(encrypt_random_key), None)

    def fetch_messages(self, seq: int = 0, limit: int = 1000) -> tuple[list[dict], int]:
        """按 seq 游标增量拉取并解密一批消息，返回 (明文消息列表, 新游标)。

        需原生 SDK；离线时抛 RuntimeError（编排器降级为「仅迁群文件」）。
        """
        if not self._online:
            raise RuntimeError("会话存档 SDK 不可用：需开通存档并部署原生库")
        raw_batch = self._sdk.get_chat_data(seq, limit)   # SDK 返回密文批次
        out: list[dict] = []
        new_seq = seq
        for rec in raw_batch:
            aes_key = self._rsa_decrypt_key(rec["encrypt_random_key"])
            plain = self._sdk.decrypt_data(aes_key, rec["encrypt_chat_msg"])
            out.append(json.loads(plain))
            new_seq = max(new_seq, int(rec.get("seq", new_seq)))
        return out, new_seq
