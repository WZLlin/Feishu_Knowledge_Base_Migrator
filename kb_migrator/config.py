"""集中配置加载。

从环境变量 / .env 读取运行配置；凭证只从环境读取，绝不写死在代码里。
使用 pydantic-settings 做类型校验与默认值。
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # ── 通用 ──────────────────────────────────────────────
    ledger_db: str = Field(default="./data/ledger.db", alias="KBM_LEDGER_DB")
    jobs_db: str = Field(default="./data/web_jobs.db", alias="KBM_JOBS_DB")
    work_dir: str = Field(default="./data/work", alias="KBM_WORK_DIR")
    taxonomy_file: str = Field(default="./config/taxonomy.yaml", alias="KBM_TAXONOMY_FILE")
    confidence_threshold: float = Field(default=0.85, alias="KBM_CONFIDENCE_THRESHOLD")
    identity_map_file: str = Field(default="", alias="KBM_IDENTITY_MAP_FILE")

    # ── Claude ────────────────────────────────────────────
    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")
    # 中转/自建网关地址；为空则用 Anthropic 官方端点。可用 ANTHROPIC_BASE_URL 环境变量。
    anthropic_base_url: str = Field(default="", alias="ANTHROPIC_BASE_URL")
    # 网关鉴权方式：sk- 短格式中转常用 Bearer(auth_token)，官方 key 用 x-api-key。
    anthropic_auth_style: str = Field(default="auto", alias="KBM_CLAUDE_AUTH_STYLE")
    claude_model: str = Field(default="claude-sonnet-5", alias="KBM_CLAUDE_MODEL")
    claude_use_batch: bool = Field(default=True, alias="KBM_CLAUDE_USE_BATCH")

    # ── 飞书 ──────────────────────────────────────────────
    feishu_app_id: str = Field(default="", alias="FEISHU_APP_ID")
    feishu_app_secret: str = Field(default="", alias="FEISHU_APP_SECRET")
    feishu_redirect_uri: str = Field(
        default="http://localhost:8000/feishu/oauth/callback", alias="FEISHU_REDIRECT_URI"
    )
    feishu_wiki_space_id: str = Field(default="", alias="FEISHU_WIKI_SPACE_ID")
    # 阶段1 bootstrap 产出的「分类->token」映射持久化文件（load 阶段读取）
    feishu_targets_file: str = Field(
        default="./data/feishu_targets.json", alias="KBM_FEISHU_TARGETS_FILE"
    )
    # 建 Wiki 空间需 user_access_token；由 OAuth 回调获得后可临时注入此处或用 --user-token
    feishu_user_access_token: str = Field(default="", alias="FEISHU_USER_ACCESS_TOKEN")
    # OAuth 回调换取的 user token 落盘路径（bootstrap --wiki 会自动读取）；含凭证，勿提交
    feishu_user_token_file: str = Field(
        default="./data/feishu_user_token.json", alias="KBM_FEISHU_USER_TOKEN_FILE"
    )
    # 请求的用户授权 scope（建 Wiki 空间/写云文档所需，按应用实际授予调整）
    feishu_oauth_scope: str = Field(
        default="wiki:wiki drive:drive offline_access",
        alias="KBM_FEISHU_OAUTH_SCOPE",
    )

    # ── SharePoint / Graph ────────────────────────────────
    ms_tenant_id: str = Field(default="", alias="MS_TENANT_ID")
    ms_client_id: str = Field(default="", alias="MS_CLIENT_ID")
    ms_client_secret: str = Field(default="", alias="MS_CLIENT_SECRET")
    # 只迁指定站点（Graph /sites?search= 关键字收窄）；为空=全部根站点
    ms_site_filter: str = Field(default="", alias="MS_SITE_FILTER")

    # ── 企业微信 ──────────────────────────────────────────
    wecom_corp_id: str = Field(default="", alias="WECOM_CORP_ID")
    wecom_wedrive_secret: str = Field(default="", alias="WECOM_WEDRIVE_SECRET")
    wecom_chat_archive_secret: str = Field(default="", alias="WECOM_CHAT_ARCHIVE_SECRET")
    wecom_chat_private_key_file: str = Field(
        default="./secrets/wecom_chat_rsa.pem", alias="WECOM_CHAT_PRIVATE_KEY_FILE"
    )
    # 会话存档原生库(WeWorkFinanceSdk)路径；为空则群聊连接器 online=False（离线降级）
    wecom_chat_sdk_lib_path: str = Field(default="", alias="WECOM_CHAT_SDK_LIB")
    # 群名打标用的应用 secret（appchat 改名/发消息，仅对该 app 自建的服务群有效）
    wecom_app_secret: str = Field(default="", alias="WECOM_APP_SECRET")
    # 群成员→飞书协作者映射：JSON 文件路径 {wecom_userid: feishu_open_id}，人工维护
    wecom_feishu_user_map: str = Field(default="", alias="WECOM_FEISHU_USER_MAP")

    def ensure_dirs(self) -> None:
        """创建运行所需的本地目录（台账所在目录、工作缓存目录）。"""
        Path(self.ledger_db).parent.mkdir(parents=True, exist_ok=True)
        Path(self.work_dir).mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    return Settings()
