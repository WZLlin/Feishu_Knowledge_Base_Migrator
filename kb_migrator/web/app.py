"""统一 Web 控制台（FastAPI）——单页可视化，所有基本操作一页完成。

一页七个标签（前端 static/index.html）：
  概览 · 配置(填/存凭证) · 授权(飞书OAuth+连接测试) · 迁移(本地/微盘/群聊) ·
  确认队列 · 治理 · 目标结构(bootstrap/load)

后端只提供 JSON API + 持久化 JobManager 跑长任务；每个 job 线程内自建
Ledger/Orchestrator（SQLite 连接不跨线程）。任务快照落 SQLite，重启会收口未完成状态；
凭证仍只落 .env，GET 不回明文。

启动（务必绑定本机，避免暴露凭证操作面）：
  uvicorn kb_migrator.web.app:app --host 127.0.0.1 --port 8000
"""
from __future__ import annotations

import hashlib
import os
import secrets
import threading
import time
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from fastapi import Body, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from .. import __version__
from ..config import get_settings
from ..ledger import Ledger
from ..models import Stage
from ..pipeline.classify import Classifier
from ..pipeline.orchestrator import Orchestrator
from ..taxonomy import Taxonomy
from ..utils.identity import load_identity_map
from . import settings_io
from .jobs import JobContext, JobManager

app = FastAPI(title="kb-migrator 控制台")
JOBS = JobManager(get_settings().jobs_db)
_OAUTH_STATES: dict[str, float] = {}
_OAUTH_LOCK = threading.Lock()
_STATIC = Path(__file__).parent / "static"
API_PROTOCOL_VERSION = 1
API_VERSION = "2026-07-29"
SERVER_INSTANCE_ID = secrets.token_hex(6)
SERVER_STARTED_AT = datetime.now(timezone.utc).isoformat()
CAPABILITIES = (
    "durable_jobs",
    "governance_health",
    "oauth_state_validation",
    "structure_workbench",
    "structure_merge_split",
    "structure_multi_approval",
    "structure_reconciliation",
    "item_relocation",
    "item_relocation_rollback",
    "ai_health_check",
    "sharepoint_health_check",
    "selective_failure_retry",
)
REQUIRED_LEDGER_TABLES = frozenset({
    "items",
    "pipeline_runs",
    "structure_versions",
    "structure_nodes",
    "structure_bindings",
    "remote_structure_snapshots",
    "remote_node_decisions",
    "structure_change_plans",
    "item_target_assignments",
    "item_relocation_plans",
    "item_relocation_actions",
})


@app.middleware("http")
async def add_runtime_headers(request: Request, call_next):
    """禁止动态控制台被缓存，并暴露可快速定位版本漂移的响应头。"""
    response = await call_next(request)
    response.headers["X-KBM-API-Protocol"] = str(API_PROTOCOL_VERSION)
    response.headers["X-KBM-API-Version"] = API_VERSION
    response.headers["X-KBM-Instance-ID"] = SERVER_INSTANCE_ID
    if request.url.path == "/" or request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


# ── 运行期依赖（每次取新，配置热生效）───────────────────────

def S():
    return get_settings()


def TX() -> Taxonomy:
    return Taxonomy.load(S().taxonomy_file)


_AI_HEALTH_LOCK = threading.Lock()
_AI_HEALTH_CACHE: dict = {}
_SHAREPOINT_HEALTH_LOCK = threading.Lock()
_SHAREPOINT_HEALTH_CACHE: dict = {}


def _probe_ai_health(s) -> dict:
    if not s.anthropic_api_key:
        return {
            "ready": False,
            "status": "missing_key",
            "label": "未配置",
            "message": "未配置 ANTHROPIC_API_KEY，将使用离线启发式分类。",
        }
    classifier = Classifier(
        TX(),
        api_key=s.anthropic_api_key,
        model=s.claude_model,
        base_url=s.anthropic_base_url,
        auth_style=s.anthropic_auth_style,
        request_timeout=8.0,
        max_retries=0,
    )
    return classifier.health_check()


def _ai_health(*, force: bool = False) -> dict:
    """缓存 Claude 的真实可用性，避免概览刷新反复消耗额度。"""
    s = S()
    fingerprint = hashlib.sha256(
        "\0".join((
            s.anthropic_api_key,
            s.anthropic_base_url,
            s.anthropic_auth_style,
            s.claude_model,
        )).encode("utf-8")
    ).hexdigest()
    now = time.monotonic()
    with _AI_HEALTH_LOCK:
        if (
            not force
            and _AI_HEALTH_CACHE.get("fingerprint") == fingerprint
            and float(_AI_HEALTH_CACHE.get("expires_at", 0)) > now
        ):
            return dict(_AI_HEALTH_CACHE["result"], cached=True)

    result = _probe_ai_health(s)
    result["checked_at"] = datetime.now(timezone.utc).isoformat()
    # 成功结果稳定一些；失败结果缩短缓存，便于充值或修正地址后及时恢复。
    ttl = 120.0 if result.get("ready") else 30.0
    with _AI_HEALTH_LOCK:
        _AI_HEALTH_CACHE.update({
            "fingerprint": fingerprint,
            "expires_at": time.monotonic() + ttl,
            "result": dict(result),
        })
    return dict(result, cached=False)


def _probe_sharepoint_health(s) -> dict:
    missing = [
        name for name, value in (
            ("MS_TENANT_ID", s.ms_tenant_id),
            ("MS_CLIENT_ID", s.ms_client_id),
            ("MS_CLIENT_SECRET", s.ms_client_secret),
        )
        if not value
    ]
    if missing:
        return {
            "ready": False,
            "status": "missing_config",
            "label": "配置不完整",
            "message": f"缺少 SharePoint 配置：{', '.join(missing)}。",
            "missing": missing,
        }

    import httpx

    from ..connectors.sharepoint import SharePointConnector

    with httpx.Client(timeout=8.0, follow_redirects=True) as http:
        connector = SharePointConnector(
            s.ms_tenant_id,
            s.ms_client_id,
            s.ms_client_secret,
            s.work_dir,
            site_filter=s.ms_site_filter or None,
            http=http,
        )
        return connector.health_check()


def _sharepoint_health(*, force: bool = False) -> dict:
    """缓存 SharePoint 的真实鉴权与 Graph 权限状态。"""
    s = S()
    fingerprint = hashlib.sha256(
        "\0".join((
            s.ms_tenant_id,
            s.ms_client_id,
            s.ms_client_secret,
        )).encode("utf-8")
    ).hexdigest()
    now = time.monotonic()
    with _SHAREPOINT_HEALTH_LOCK:
        if (
            not force
            and _SHAREPOINT_HEALTH_CACHE.get("fingerprint") == fingerprint
            and float(_SHAREPOINT_HEALTH_CACHE.get("expires_at", 0)) > now
        ):
            return dict(_SHAREPOINT_HEALTH_CACHE["result"], cached=True)

    result = _probe_sharepoint_health(s)
    result["checked_at"] = datetime.now(timezone.utc).isoformat()
    ttl = 120.0 if result.get("ready") else 30.0
    with _SHAREPOINT_HEALTH_LOCK:
        _SHAREPOINT_HEALTH_CACHE.update({
            "fingerprint": fingerprint,
            "expires_at": time.monotonic() + ttl,
            "result": dict(result),
        })
    return dict(result, cached=False)


def _orch(s=None) -> tuple[Ledger, Orchestrator]:
    s = s or S()
    led = Ledger(s.ledger_db)
    return led, Orchestrator(led, TX(), s.work_dir, s.confidence_threshold)


def _structure(s=None):
    """每次请求使用独立 SQLite 连接，调用方负责关闭返回的 ledger。"""
    from ..structure import StructureService

    s = s or S()
    led = Ledger(s.ledger_db)
    return led, StructureService(led, TX(), s.feishu_targets_file)


def _active_structure_id(mode: str) -> str:
    """在 Web 任务提交时冻结当前目标版本，而不是在线程运行后再读取。"""
    led, structures = _structure()
    try:
        active = structures.active_version()
        return (
            active["id"]
            if active and (not mode or active["mode"] == mode)
            else ""
        )
    finally:
        led.close()


def _load_routing_context(structures, targets: dict, version_id: str):
    """解析 load/retry 的 Drive 上传目录及稳定结构节点。

    Drive 结构直接上传到各绑定目录；Wiki 结构先上传到云空间暂存根目录，但稳定
    ``node_id`` 仍按激活的 Wiki 结构分配，后续挂载不会丢失用户确认的目录路由。
    """
    legacy_folders = targets.get("folder_map") or {}
    if not version_id:
        return legacy_folders, {}, None, targets.get("mode") or "legacy"

    version = structures.get_version(version_id)
    mode = version["mode"]
    if mode == "drive":
        folders, node_ids = structures.routing_map(version_id, mode="drive")
        return folders or legacy_folders, node_ids, (
            lambda row: structures.resolve_item_target(
                row, version_id, mode="drive"
            )
        ), mode

    wiki_routes, node_ids = structures.routing_map(version_id, mode="wiki")
    staging_token = str(targets.get("root_token") or "")
    folders = {name: staging_token for name in wiki_routes}

    def resolve_wiki_staging_target(row):
        resolved = structures.resolve_item_target(
            row, version_id, mode="wiki"
        )
        if resolved:
            resolved = dict(resolved)
            resolved["remote_token"] = staging_token
        return resolved

    return folders, node_ids, resolve_wiki_staging_target, mode


def _mk_progress(ctx: JobContext):
    """把 orchestrator 的 progress(done,total,msg) 转成 job 的进度+日志。"""
    def cb(done: int, total: int, msg: str = ""):
        ctx.raise_if_cancelled()
        ctx.progress(done, total)
        if msg:
            ctx.log(msg)
    return cb


# ── 运行时元数据与就绪检查 ───────────────────────────────────────

def _runtime_readiness() -> tuple[bool, dict]:
    """检查控制台关键依赖；不触发任何飞书或第三方网络请求。"""
    s = S()
    checks: dict = {}
    failures: list[str] = []

    ledger_path = Path(s.ledger_db).resolve()
    led = None
    try:
        led = Ledger(s.ledger_db)  # 同时幂等执行现有数据库迁移
        quick_check = led.conn.execute("PRAGMA quick_check").fetchone()[0]
        tables = {
            row[0] for row in led.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        missing = sorted(REQUIRED_LEDGER_TABLES - tables)
        database_ok = quick_check == "ok" and not missing
        checks["ledger"] = {
            "ok": database_ok,
            "path": str(ledger_path),
            "quick_check": quick_check,
            "missing_tables": missing,
        }
        if not database_ok:
            failures.append("迁移台账结构不完整或 SQLite 完整性检查失败")
    except Exception as exc:
        checks["ledger"] = {
            "ok": False,
            "path": str(ledger_path),
            "error": f"{type(exc).__name__}: {exc}",
        }
        failures.append("迁移台账无法打开")
    finally:
        if led is not None:
            led.close()

    taxonomy_path = Path(s.taxonomy_file).resolve()
    try:
        taxonomy = TX()
        category_count = len(taxonomy.categories)
        taxonomy_ok = category_count > 0
        checks["taxonomy"] = {
            "ok": taxonomy_ok,
            "path": str(taxonomy_path),
            "categories": category_count,
        }
        if not taxonomy_ok:
            failures.append("分类配置未包含任何目录")
    except Exception as exc:
        checks["taxonomy"] = {
            "ok": False,
            "path": str(taxonomy_path),
            "error": f"{type(exc).__name__}: {exc}",
        }
        failures.append("分类配置无法读取")

    path_checks = {}
    for name, configured_path in (
        ("work_dir", s.work_dir),
        ("jobs_db_parent", str(Path(s.jobs_db).parent)),
        ("targets_parent", str(Path(s.feishu_targets_file).parent)),
    ):
        path = Path(configured_path).resolve()
        try:
            path.mkdir(parents=True, exist_ok=True)
            ok = path.is_dir() and os.access(path, os.R_OK | os.W_OK)
            path_checks[name] = {"ok": ok, "path": str(path)}
        except Exception as exc:
            ok = False
            path_checks[name] = {
                "ok": False,
                "path": str(path),
                "error": f"{type(exc).__name__}: {exc}",
            }
        if not ok:
            failures.append(f"运行路径不可读写：{name}")
    checks["paths"] = path_checks
    return not failures, {"checks": checks, "failures": failures}


@app.get("/api/meta")
def api_meta():
    """前端启动握手：版本、进程实例和能力清单。"""
    return {
        "app_version": __version__,
        "api_version": API_VERSION,
        "api_protocol": API_PROTOCOL_VERSION,
        "instance_id": SERVER_INSTANCE_ID,
        "started_at": SERVER_STARTED_AT,
        "capabilities": list(CAPABILITIES),
    }


@app.get("/api/health/ready")
def api_health_ready():
    ready, detail = _runtime_readiness()
    payload = {
        "ready": ready,
        "instance_id": SERVER_INSTANCE_ID,
        "api_protocol": API_PROTOCOL_VERSION,
        **detail,
    }
    if not ready:
        return JSONResponse(
            {"error": "控制台尚未就绪", **payload},
            status_code=503,
        )
    return payload


# ── 单页 ────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index():
    html = (_STATIC / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


# ── 概览 ────────────────────────────────────────────────────

@app.get("/api/status")
def api_status():
    s = S()
    ai_health = _ai_health()
    sharepoint_health = _sharepoint_health()
    led = Ledger(s.ledger_db)
    counts = led.stage_counts()
    governance = led.governance_items(triage_path=TX().triage_path)
    health = led.governance_health(triage_path=TX().triage_path)
    pipeline_runs = [dict(r) for r in led.recent_pipeline_runs(10)]
    led.close()
    total = sum(counts.values())
    loaded = counts.get(Stage.LOADED.value, 0)
    ratio = round(loaded / total * 100, 1) if total else 0.0

    ms = settings_io.masked_settings()
    configured = {k: v["configured"] for k, v in ms.items()}

    from ..feishu.bootstrap import FeishuBootstrapper
    from ..feishu.auth import load_user_token
    targets = FeishuBootstrapper(None, TX(), s.feishu_targets_file).load_targets()
    chat_private_key_ready = bool(
        s.wecom_chat_private_key_file
        and Path(s.wecom_chat_private_key_file).is_file()
    )
    chat_sdk_ready = bool(
        s.wecom_chat_sdk_lib_path
        and Path(s.wecom_chat_sdk_lib_path).is_file()
    )

    return {
        "counts": counts,
        "total": total,
        "loaded": loaded,
        "sink_ratio": ratio,
        "feishu_ready": configured.get("FEISHU_APP_ID") and configured.get("FEISHU_APP_SECRET"),
        "claude_ready": bool(ai_health.get("ready")),
        "claude_health": ai_health,
        "sharepoint_ready": bool(sharepoint_health.get("ready")),
        "sharepoint_health": sharepoint_health,
        "wecom_ready": configured.get("WECOM_CORP_ID") and configured.get("WECOM_WEDRIVE_SECRET"),
        "wecom_chat_ready": bool(
            configured.get("WECOM_CORP_ID")
            and configured.get("WECOM_CHAT_ARCHIVE_SECRET")
            and chat_private_key_ready
            and chat_sdk_ready
        ),
        "wecom_chat_checks": {
            "corp_id": bool(configured.get("WECOM_CORP_ID")),
            "archive_secret": bool(
                configured.get("WECOM_CHAT_ARCHIVE_SECRET")
            ),
            "private_key_file": chat_private_key_ready,
            "sdk_library": chat_sdk_ready,
        },
        "oauth_token": bool(load_user_token(
            s.feishu_user_token_file,
            s.feishu_app_id,
            s.feishu_app_secret,
        )),
        "targets_mode": targets.get("mode") or "",
        "targets_count": len(set((
            targets.get(
                "wiki_node_map" if targets.get("mode") == "wiki"
                else "folder_map"
            ) or {}
        ).values())),
        "jobs": JOBS.list(),
        "governance": {name: len(rows) for name, rows in governance.items()},
        "health": health,
        "pipeline_runs": pipeline_runs,
    }


@app.post("/api/ai/health")
def api_ai_health(payload: dict = Body(default={})):
    """主动重新检测 Claude，供概览按钮和运维验收使用。"""
    return _ai_health(force=bool((payload or {}).get("force", True)))


@app.post("/api/sharepoint/health")
def api_sharepoint_health(payload: dict = Body(default={})):
    """主动重新检测 SharePoint 凭证及 Graph 权限。"""
    return _sharepoint_health(force=bool((payload or {}).get("force", True)))


@app.get("/api/governance")
def api_governance():
    s = S()
    led = Ledger(s.ledger_db)
    queues = led.governance_items(triage_path=TX().triage_path)
    led.close()
    return {name: [{"key": r["stable_key"], "name": r["original_name"],
                    "category": r["category"], "owner": r["owner"],
                    "steward": r["steward"],
                    "review_due_at": r["review_due_at"],
                    "retention_due_at": r["retention_due_at"]} for r in rows]
            for name, rows in queues.items()}


@app.get("/api/failures")
def api_failures(stage: str = ""):
    led = Ledger(S().ledger_db)
    try:
        rows = led.failed_items(stage or None)
        return {"items": [{
            "key": r["stable_key"],
            "name": r["original_name"],
            "failed_stage": r["failed_stage"],
            "retryable": bool(r["retryable"]),
            "retry_count": r["retry_count"],
            "error": r["error_detail"],
            "last_error_at": r["last_error_at"],
        } for r in rows]}
    finally:
        led.close()


_FAILURE_RETRY_TARGETS = {
    "fetch": Stage.DISCOVERED,
    "extract": Stage.DISCOVERED,
    "dedup": Stage.EXTRACTED,
    "classify": Stage.DEDUPED,
    "load": Stage.CONFIRMED,
    "archive": Stage.LOADED,
    "wiki": Stage.LOADED,
}
_FAILURE_NEXT_ACTIONS = {
    "fetch": "前往迁移页重新执行对应数据接入与抽取",
    "extract": "前往迁移页重新执行对应数据接入与抽取",
    "dedup": "前往迁移页运行“去重与分类”",
    "classify": "前往迁移页运行“去重与分类”",
    "load": "前往迁移页执行“失败重试”",
    "archive": "前往迁移页执行“生命周期归档”",
    "wiki": "前往迁移页执行“Wiki 挂载”",
}


@app.post("/api/failures/retry")
def api_retry_failures(payload: dict = Body(...)):
    """重排用户选中的失败记录；同一接口支持单条和批量勾选。"""
    raw_keys = (payload or {}).get("keys") or []
    if isinstance(raw_keys, str):
        raw_keys = [raw_keys]
    keys = list(dict.fromkeys(str(key).strip() for key in raw_keys if str(key).strip()))
    if not keys:
        return JSONResponse({"error": "请至少选择一条失败记录"}, status_code=400)
    if len(keys) > 200:
        return JSONResponse({"error": "单次最多重试 200 条记录"}, status_code=400)

    led = Ledger(S().ledger_db)
    try:
        selected = [led.get(key) for key in keys]
        selected_stages = {
            str(row["failed_stage"] or "")
            for row in selected
            if row is not None
        }
        # 分类失败在重新入队前先校验真实 API，避免额度异常时记录从失败页消失。
        settings = S()
        if "classify" in selected_stages and settings.anthropic_api_key:
            health = _ai_health(force=True)
            if not health.get("ready"):
                return JSONResponse(
                    {
                        "error": f"Claude 分类不可用，未重排所选记录：{health['message']}",
                        "ai_health": health,
                    },
                    status_code=503,
                )

        result = led.requeue_failure_keys(keys, _FAILURE_RETRY_TARGETS)
        stages = list(dict.fromkeys(
            item["failed_stage"] for item in result["requeued"]
        ))
        result["requeued_count"] = len(result["requeued"])
        result["skipped_count"] = len(result["skipped"])
        result["next_actions"] = list(dict.fromkeys(
            _FAILURE_NEXT_ACTIONS[stage]
            for stage in stages
            if stage in _FAILURE_NEXT_ACTIONS
        ))
        return result
    finally:
        led.close()


@app.get("/api/insights")
def api_insights():
    led, orch = _orch()
    try:
        return {"feedback": [dict(r) for r in led.classification_feedback_summary()],
                "calibration": orch.classification_calibration(),
                "triage_topics": [{"term": term, "count": count}
                                  for term, count in orch.triage_topic_signals()],
                "triage_clusters": orch.triage_topic_clusters()}
    finally:
        led.close()


@app.post("/api/governance/review-complete")
def api_review_complete(payload: dict = Body(...)):
    key = str((payload or {}).get("key", "")).strip()
    actor = str((payload or {}).get("actor", "")).strip()
    if not key:
        return JSONResponse({"error": "缺少 key"}, status_code=400)
    led, orch = _orch()
    try:
        next_due = orch.complete_review(key, actor=actor)
        return {"key": key, "next_review_due_at": next_due}
    except KeyError:
        return JSONResponse({"error": f"条目不存在：{key}"}, status_code=404)
    finally:
        led.close()


# ── 配置 ────────────────────────────────────────────────────

@app.get("/api/settings")
def api_get_settings():
    return {"fields": settings_io.masked_settings()}


@app.post("/api/settings")
def api_post_settings(payload: dict = Body(...)):
    changed = settings_io.write_env(payload or {})
    settings_io.reload_settings()
    return {"changed": changed, "fields": settings_io.masked_settings()}


# ── 授权 / 连接测试 ─────────────────────────────────────────

@app.get("/api/oauth/feishu/login")
def oauth_login():
    from ..feishu.auth import build_authorize_url

    s = S()
    if not s.feishu_app_id:
        return JSONResponse({"error": "未配置 FEISHU_APP_ID"}, status_code=400)
    state = secrets.token_urlsafe(32)
    with _OAUTH_LOCK:
        now = time.time()
        for stale, expiry in list(_OAUTH_STATES.items()):
            if expiry < now:
                _OAUTH_STATES.pop(stale, None)
        _OAUTH_STATES[state] = now + 600
    url = build_authorize_url(s.feishu_app_id, s.feishu_redirect_uri,
                              scope=s.feishu_oauth_scope, state=state)
    response = RedirectResponse(url, status_code=302)
    response.set_cookie(
        "kbm_oauth_state", state, max_age=600, httponly=True, samesite="lax",
    )
    return response


@app.get("/feishu/oauth/callback", response_class=HTMLResponse)
def oauth_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    from ..feishu.auth import exchange_user_token, save_user_token

    def page(body: str, status_code: int = 200) -> HTMLResponse:
        response = HTMLResponse(body, status_code=status_code)
        response.delete_cookie("kbm_oauth_state")
        return response

    s = S()
    cookie_state = request.cookies.get("kbm_oauth_state", "")
    with _OAUTH_LOCK:
        expiry = _OAUTH_STATES.pop(state, 0)
    state_matches = bool(
        state and cookie_state and secrets.compare_digest(state, cookie_state)
    )
    if not state_matches or not expiry or expiry < time.time():
        return page(
            "<h3>授权失败：state 无效、会话不匹配或已过期</h3>"
            "<a href='/'>返回控制台</a>",
            status_code=400,
        )
    if error or not code:
        return page(
            f"<h3>授权失败：{escape(error or '无 code')}</h3>"
            "<a href='/'>返回控制台</a>"
        )
    data = exchange_user_token(s.feishu_app_id, s.feishu_app_secret, code, s.feishu_redirect_uri)
    token = data.get("access_token") or (data.get("data") or {}).get("access_token")
    if not token:
        return page(
            f"<h3>换取 token 失败：{escape(str(data))}</h3>"
            "<a href='/'>返回控制台</a>"
        )
    save_user_token(s.feishu_user_token_file, data)
    return page(
        "<h3>✅ 已获取 user_access_token 并保存</h3>"
        "<p>现在可在「目标结构」页建 Wiki 知识空间。</p><a href='/'>返回控制台</a>")


@app.post("/api/test/feishu")
def test_feishu():
    from ..feishu.client import FeishuClient

    s = S()
    if not (s.feishu_app_id and s.feishu_app_secret):
        return {"ok": False, "msg": "未配置飞书 App ID/Secret"}
    try:
        FeishuClient(s.feishu_app_id, s.feishu_app_secret).tenant.token()
        return {"ok": True, "msg": "飞书 tenant_access_token 获取成功"}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "msg": f"失败：{e}"}


@app.post("/api/test/wecom")
def test_wecom():
    from ..connectors.wedrive import WeDriveConnector

    s = S()
    if not (s.wecom_corp_id and s.wecom_wedrive_secret):
        return {"ok": False, "msg": "未配置企业微信 corp_id/微盘 secret"}
    try:
        conn = WeDriveConnector(s.wecom_corp_id, s.wecom_wedrive_secret, [], s.work_dir)
        conn._access_token()
        return {"ok": True, "msg": "企业微信 access_token 获取成功（注意服务器 IP 需在可信白名单）"}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "msg": f"失败：{e}（60020 多为 IP 未加白名单）"}


# ── 目标结构 ────────────────────────────────────────────────

@app.get("/api/targets")
def api_targets():
    from ..feishu.bootstrap import FeishuBootstrapper

    s = S()
    boot = FeishuBootstrapper(None, TX(), s.feishu_targets_file)
    t = boot.load_targets()
    led, structures = _structure(s)
    try:
        active = structures.active_version()
        return {
            "targets": t, "summary": boot.summary(t),
            "active_structure": active,
        }
    finally:
        led.close()


@app.get("/api/structures/active")
def api_structure_active():
    led, structures = _structure()
    try:
        return {"structure": structures.active_version()}
    finally:
        led.close()


@app.get("/api/structures")
def api_structure_versions(limit: int = 50):
    led, structures = _structure()
    try:
        return {"versions": structures.list_versions(limit)}
    finally:
        led.close()


@app.get("/api/structures/{version_id}/audit")
def api_structure_audit(version_id: str):
    led, structures = _structure()
    try:
        return structures.audit(version_id)
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    finally:
        led.close()


@app.get("/api/structures/{version_id}/health")
def api_structure_health(version_id: str):
    led, structures = _structure()
    try:
        return structures.health(version_id)
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    finally:
        led.close()


@app.post("/api/structures/drafts/{version_id}/generate")
def api_structure_generate(version_id: str):
    led, structures = _structure()
    try:
        return structures.suggest_structure(version_id)
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    finally:
        led.close()


@app.post("/api/structures/{source_version_id}/restore")
def api_structure_restore(
    source_version_id: str, payload: dict = Body(...)
):
    from ..structure import StructureConflict, StructureValidationError

    led, structures = _structure()
    try:
        version = structures.restore_version_to_draft(
            source_version_id,
            str((payload or {}).get("target_draft_id") or ""),
            int((payload or {}).get("revision") or 0),
            actor=str((payload or {}).get("actor") or ""),
        )
        return {
            "structure": version,
            "message": "历史版本已复制到当前草稿，飞书目录尚未发生变化",
        }
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    except StructureValidationError as exc:
        return JSONResponse(
            {"error": str(exc), "validation": exc.result}, status_code=422
        )
    finally:
        led.close()


@app.post("/api/structures/{version_id}/relocation-plans")
def api_item_relocation_plan_create(
    version_id: str, payload: dict = Body(default={})
):
    from ..structure import StructureConflict

    led, structures = _structure()
    try:
        plan = structures.create_item_relocation_plan(
            version_id, actor=str((payload or {}).get("actor") or "")
        )
        return {
            "plan": plan,
            "message": "历史重定位计划已生成，尚未移动任何飞书内容",
        }
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    finally:
        led.close()


@app.get("/api/structures/{version_id}/relocation-plans/latest")
def api_item_relocation_plan_latest(version_id: str):
    led, structures = _structure()
    try:
        structures.get_version(version_id)
        return {"plan": structures.latest_item_relocation_plan(version_id)}
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    finally:
        led.close()


@app.get("/api/relocation-plans/{plan_id}")
def api_item_relocation_plan_get(plan_id: str):
    led, structures = _structure()
    try:
        return {"plan": structures.get_item_relocation_plan(plan_id)}
    except KeyError:
        return JSONResponse({"error": "重定位计划不存在"}, status_code=404)
    finally:
        led.close()


@app.put("/api/relocation-plans/{plan_id}")
def api_item_relocation_plan_select(plan_id: str, payload: dict = Body(...)):
    from ..structure import StructureConflict

    led, structures = _structure()
    try:
        plan = structures.select_item_relocations(
            plan_id,
            int((payload or {}).get("revision") or 0),
            list((payload or {}).get("stable_keys") or []),
            actor=str((payload or {}).get("actor") or ""),
        )
        return {"plan": plan}
    except KeyError:
        return JSONResponse({"error": "重定位计划不存在"}, status_code=404)
    except (ValueError, StructureConflict) as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    finally:
        led.close()


@app.post("/api/relocation-plans/{plan_id}/approve")
def api_item_relocation_plan_approve(
    plan_id: str, payload: dict = Body(default={})
):
    from ..structure import StructureConflict

    led, structures = _structure()
    try:
        plan = structures.approve_item_relocation_plan(
            plan_id, actor=str((payload or {}).get("actor") or "")
        )
        return {
            "plan": plan,
            "message": "历史重定位计划已审批；执行前仍会检查同名、近似内容和权限扩大风险",
        }
    except KeyError:
        return JSONResponse({"error": "重定位计划不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    finally:
        led.close()


@app.post("/api/structures/drafts")
def api_structure_create_draft(payload: dict = Body(default={})):
    led, structures = _structure()
    try:
        draft = structures.ensure_draft(
            mode=str((payload or {}).get("mode") or ""),
            actor=str((payload or {}).get("actor") or ""),
        )
        return {"structure": draft}
    finally:
        led.close()


@app.get("/api/structures/drafts/{version_id}")
def api_structure_get_draft(version_id: str):
    led, structures = _structure()
    try:
        version = structures.get_version(version_id)
        if version["status"] not in ("draft", "reviewing"):
            return JSONResponse({"error": "该版本不是草稿或审批中版本"}, status_code=409)
        return {"structure": version}
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    finally:
        led.close()


@app.put("/api/structures/drafts/{version_id}")
def api_structure_save_draft(version_id: str, payload: dict = Body(...)):
    from ..structure import StructureConflict, StructureValidationError

    led, structures = _structure()
    try:
        version = structures.save_draft(
            version_id,
            int((payload or {}).get("revision") or 0),
            list((payload or {}).get("nodes") or []),
            name=str((payload or {}).get("name") or ""),
            root_name=str((payload or {}).get("root_name") or ""),
            actor=str((payload or {}).get("actor") or ""),
        )
        return {"structure": version}
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    except StructureValidationError as exc:
        return JSONResponse(
            {"error": str(exc), "validation": exc.result}, status_code=422
        )
    finally:
        led.close()


@app.post("/api/structures/drafts/{version_id}/validate")
def api_structure_validate(version_id: str):
    led, structures = _structure()
    try:
        return structures.validate_version(version_id)
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    finally:
        led.close()


@app.post("/api/structures/drafts/{version_id}/merge")
def api_structure_merge(version_id: str, payload: dict = Body(...)):
    from ..structure import StructureConflict, StructureValidationError

    led, structures = _structure()
    try:
        version = structures.merge_nodes(
            version_id,
            int((payload or {}).get("revision") or 0),
            str((payload or {}).get("target_node_id") or ""),
            list((payload or {}).get("source_node_ids") or []),
            actor=str((payload or {}).get("actor") or ""),
            policy_resolutions=dict(
                (payload or {}).get("policy_resolutions") or {}
            ),
        )
        return {
            "structure": version,
            "message": "目录已在草稿中合并；远程内容搬迁将在差异发布时执行",
        }
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    except (ValueError, StructureConflict) as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    except StructureValidationError as exc:
        return JSONResponse(
            {"error": str(exc), "validation": exc.result}, status_code=422
        )
    finally:
        led.close()


@app.post("/api/structures/drafts/{version_id}/split")
def api_structure_split(version_id: str, payload: dict = Body(...)):
    from ..structure import StructureConflict, StructureValidationError

    led, structures = _structure()
    try:
        version = structures.split_node(
            version_id,
            int((payload or {}).get("revision") or 0),
            str((payload or {}).get("source_node_id") or ""),
            list((payload or {}).get("children") or []),
            actor=str((payload or {}).get("actor") or ""),
        )
        return {
            "structure": version,
            "message": "拆分规则已保存；新内容将按规则路由，历史内容仅生成影响预览",
        }
    except KeyError:
        return JSONResponse({"error": "结构版本或节点不存在"}, status_code=404)
    except (ValueError, StructureConflict) as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    except StructureValidationError as exc:
        return JSONResponse(
            {"error": str(exc), "validation": exc.result}, status_code=422
        )
    finally:
        led.close()


@app.post("/api/structures/drafts/{version_id}/approve")
def api_structure_approve(version_id: str, payload: dict = Body(default={})):
    from ..structure import StructureConflict, StructureValidationError

    led, structures = _structure()
    try:
        current = structures.get_version(version_id)
        latest_plan = structures.latest_change_plan(version_id)
        latest_snapshot = structures.latest_snapshot(current["mode"])
        snapshot_matches = (
            (latest_plan or {}).get("remote_snapshot_id")
            == ((latest_snapshot or {}).get("id"))
        )
        if (
            current["status"] == "draft"
            and (
                not latest_plan
                or latest_plan["status"] != "preview"
                or int(latest_plan["revision"]) < 1
                or not snapshot_matches
            )
        ):
            return JSONResponse(
                {
                    "error": (
                        "请先基于最新飞书快照生成并检查当前草稿的差异计划"
                    )
                },
                status_code=409,
            )
        version = structures.approve(
            version_id,
            actor=str((payload or {}).get("actor") or ""),
            required_approvals=int(
                (payload or {}).get("required_approvals") or 1
            ),
            comment=str((payload or {}).get("comment") or ""),
        )
        waiting = max(
            0, int(version["required_approvals"]) - int(version["approval_count"])
        )
        return {
            "structure": version,
            "message": (
                f"审批已记录，还需 {waiting} 人确认"
                if waiting else
                "结构版本已确认；尚未对飞书执行创建、移动或删除操作"
            ),
        }
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    except StructureValidationError as exc:
        return JSONResponse(
            {"error": str(exc), "validation": exc.result}, status_code=422
        )
    finally:
        led.close()


@app.get("/api/remote-structures/latest")
def api_remote_structure_latest(mode: str = ""):
    led, structures = _structure()
    try:
        return {"snapshot": structures.latest_snapshot(mode)}
    finally:
        led.close()


@app.get("/api/remote-structures/snapshots/{snapshot_id}")
def api_remote_structure_snapshot(snapshot_id: str):
    led, structures = _structure()
    try:
        return {"snapshot": structures.get_snapshot(snapshot_id)}
    except KeyError:
        return JSONResponse({"error": "飞书目录快照不存在"}, status_code=404)
    finally:
        led.close()


@app.post("/api/remote-structures/refresh")
def api_remote_structure_refresh(payload: dict = Body(default={})):
    """抓取飞书实际结构；测试/导入场景可直接传 nodes，真实抓取不接受删除动作。"""
    from ..feishu.auth import load_user_token
    from ..feishu.bootstrap import FeishuBootstrapper
    from ..feishu.client import FeishuClient
    from ..feishu.writer import FeishuWriter
    from ..structure.discovery import FeishuStructureDiscovery

    s = S()
    data = payload or {}
    mode = str(data.get("mode") or "drive")
    targets = FeishuBootstrapper(None, TX(), s.feishu_targets_file).load_targets()
    root_token = str(data.get("root_token") or targets.get("root_token") or "")
    space_id = str(data.get("space_id") or targets.get("space_id") or "")
    supplied_nodes = data.get("nodes")
    if mode not in ("drive", "wiki"):
        return JSONResponse({"error": f"不支持的目标形态：{mode}"}, status_code=400)
    if supplied_nodes is None and not (s.feishu_app_id and s.feishu_app_secret):
        return JSONResponse({"error": "未配置飞书 App ID/Secret"}, status_code=400)
    if supplied_nodes is None and mode == "drive" and not root_token:
        return JSONResponse({"error": "缺少云空间根文件夹 token"}, status_code=400)
    if supplied_nodes is None and mode == "wiki" and not space_id:
        return JSONResponse({"error": "缺少 Wiki space_id"}, status_code=400)

    led, structures = _structure(s)
    try:
        nodes = supplied_nodes
        if nodes is None:
            # Drive 目录发现使用自动续期的 tenant token；过期的用户 OAuth
            # token 不应拖累本可由应用身份完成的只读刷新。Wiki 优先用户身份，
            # 没有可用用户 token 时由 FeishuClient 回退到 tenant token。
            user_token = ""
            if mode == "wiki":
                user_token = (
                    s.feishu_user_access_token
                    or load_user_token(
                        s.feishu_user_token_file,
                        s.feishu_app_id,
                        s.feishu_app_secret,
                    )
                    or ""
                )
            writer = FeishuWriter(
                FeishuClient(s.feishu_app_id, s.feishu_app_secret)
            )
            discovery = FeishuStructureDiscovery(writer)
            nodes = (
                discovery.wiki(space_id, user_token=user_token)
                if mode == "wiki"
                else discovery.drive(root_token, user_token=user_token)
            )
        snapshot = structures.save_remote_snapshot(
            mode, list(nodes or []), root_token=root_token, space_id=space_id
        )
        return {"snapshot": snapshot}
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"error": f"刷新飞书结构失败：{exc}"}, status_code=502)
    finally:
        led.close()


@app.post("/api/structures/drafts/{version_id}/map-remote")
def api_structure_map_remote(version_id: str, payload: dict = Body(...)):
    from ..structure import StructureConflict

    led, structures = _structure()
    try:
        version = structures.map_remote_node(
            version_id,
            int((payload or {}).get("revision") or 0),
            str((payload or {}).get("node_id") or ""),
            str((payload or {}).get("remote_token") or ""),
            actor=str((payload or {}).get("actor") or ""),
        )
        return {
            "structure": version,
            "message": "规划节点已绑定到飞书现有目录，尚未修改飞书内容",
        }
    except KeyError:
        return JSONResponse({"error": "规划节点或飞书节点不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    finally:
        led.close()


@app.post("/api/structures/drafts/{version_id}/adopt-remote")
def api_structure_adopt_remote(version_id: str, payload: dict = Body(...)):
    from ..structure import StructureConflict, StructureValidationError

    led, structures = _structure()
    try:
        version = structures.adopt_remote_node(
            version_id,
            int((payload or {}).get("revision") or 0),
            str((payload or {}).get("remote_token") or ""),
            parent_node_id=str((payload or {}).get("parent_node_id") or ""),
            actor=str((payload or {}).get("actor") or ""),
        )
        return {
            "structure": version,
            "message": "飞书节点已采纳到规划草稿，尚未修改飞书内容",
        }
    except KeyError:
        return JSONResponse({"error": "规划父节点或飞书节点不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    except StructureValidationError as exc:
        return JSONResponse(
            {"error": str(exc), "validation": exc.result}, status_code=422
        )
    finally:
        led.close()


@app.post("/api/remote-structures/decisions")
def api_remote_structure_decision(payload: dict = Body(...)):
    led, structures = _structure()
    try:
        decision = structures.set_remote_decision(
            str((payload or {}).get("mode") or ""),
            str((payload or {}).get("remote_token") or ""),
            str((payload or {}).get("decision") or ""),
            planned_node_id=str(
                (payload or {}).get("planned_node_id") or ""
            ),
            actor=str((payload or {}).get("actor") or ""),
            note=str((payload or {}).get("note") or ""),
        )
        return {"decision": decision}
    except KeyError:
        return JSONResponse({"error": "飞书节点不存在于最新快照"}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    finally:
        led.close()


@app.post("/api/structures/drafts/{version_id}/diff")
def api_structure_diff(version_id: str, payload: dict = Body(default={})):
    led, structures = _structure()
    try:
        return {
            "plan": structures.create_diff_plan(
                version_id,
                str((payload or {}).get("snapshot_id") or ""),
                actor=str((payload or {}).get("actor") or ""),
                history_scope=str(
                    (payload or {}).get("history_scope")
                    or "unmigrated_only"
                ),
            )
        }
    except KeyError as exc:
        return JSONResponse({"error": f"结构版本或快照不存在：{exc}"}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    finally:
        led.close()


@app.get("/api/structures/{version_id}/plans/latest")
def api_structure_plan_latest(version_id: str):
    led, structures = _structure()
    try:
        structures.get_version(version_id)
        return {"plan": structures.latest_change_plan(version_id)}
    except KeyError:
        return JSONResponse({"error": "结构版本不存在"}, status_code=404)
    finally:
        led.close()


@app.get("/api/structure-plans/{plan_id}")
@app.get("/api/structure-plans/{plan_id}/status")
def api_structure_plan_get(plan_id: str):
    led, structures = _structure()
    try:
        return {"plan": structures.get_change_plan(plan_id)}
    except KeyError:
        return JSONResponse({"error": "结构计划不存在"}, status_code=404)
    finally:
        led.close()


@app.get("/api/structure-plans/{plan_id}/impact")
def api_structure_plan_impact(plan_id: str):
    led, structures = _structure()
    try:
        plan = structures.get_change_plan(plan_id)
        return {
            "plan_id": plan_id,
            "status": plan["status"],
            "history_scope": plan["history_scope"],
            "summary": plan["summary"],
            "actions": plan["actions"],
        }
    except KeyError:
        return JSONResponse({"error": "结构计划不存在"}, status_code=404)
    finally:
        led.close()


@app.put("/api/structure-plans/{plan_id}")
def api_structure_plan_update(plan_id: str, payload: dict = Body(...)):
    from ..structure import StructureConflict

    led, structures = _structure()
    try:
        plan = structures.update_change_plan(
            plan_id,
            int((payload or {}).get("revision") or 0),
            history_scope=str(
                (payload or {}).get("history_scope") or "unmigrated_only"
            ),
            actor=str((payload or {}).get("actor") or ""),
        )
        return {"plan": plan}
    except KeyError:
        return JSONResponse({"error": "结构计划不存在"}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    finally:
        led.close()


@app.post("/api/structure-plans/{plan_id}/approve")
def api_structure_plan_approve(
    plan_id: str, payload: dict = Body(default={})
):
    from ..structure import StructureConflict

    led, structures = _structure()
    try:
        return {
            "plan": structures.approve_change_plan(
                plan_id, actor=str((payload or {}).get("actor") or "")
            ),
            "message": "结构发布计划已审批，尚未修改飞书",
        }
    except KeyError:
        return JSONResponse({"error": "结构计划不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    finally:
        led.close()


@app.post("/api/structure-plans/{plan_id}/cancel")
def api_structure_plan_cancel(
    plan_id: str, payload: dict = Body(default={})
):
    from ..structure import StructureConflict

    led, structures = _structure()
    try:
        return {
            "plan": structures.cancel_change_plan(
                plan_id, actor=str((payload or {}).get("actor") or "")
            )
        }
    except KeyError:
        return JSONResponse({"error": "结构计划不存在"}, status_code=404)
    except StructureConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    finally:
        led.close()


# ── 人工确认队列 ────────────────────────────────────────────

@app.get("/api/review")
def api_review():
    s = S()
    led = Ledger(s.ledger_db)
    rows = led.pending_review()
    led.close()
    items = [{
        "key": r["stable_key"], "name": r["original_name"], "category": r["category"],
        "confidence": r["confidence"], "note": r["error_detail"],
    } for r in rows]
    return {"items": items, "categories": TX().category_paths()}


@app.post("/api/confirm")
def api_confirm(payload: dict = Body(...)):
    led, orch = _orch()
    try:
        orch.confirm(payload.get("key", ""), payload.get("category", ""),
                     payload.get("name"))
        return {"ok": True}
    except KeyError:
        return JSONResponse({"error": "条目不存在"}, status_code=404)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    finally:
        led.close()


@app.post("/api/reject")
def api_reject(payload: dict = Body(...)):
    led, orch = _orch()
    try:
        orch.reject_as_duplicate(payload.get("key", ""))
        return {"ok": True}
    except KeyError:
        return JSONResponse({"error": "条目不存在"}, status_code=404)
    finally:
        led.close()


# ── 迁移 / 长任务 ───────────────────────────────────────────

@app.get("/api/jobs")
def api_jobs():
    return {"jobs": JOBS.list()}


@app.get("/api/jobs/{job_id}")
def api_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "job 不存在"}, status_code=404)
    return job.to_dict()


@app.post("/api/jobs/{job_id}/cancel")
def api_cancel_job(job_id: str):
    if not JOBS.cancel(job_id):
        return JSONResponse({"error": "任务不存在或已结束"}, status_code=409)
    return {"ok": True}


@app.post("/api/jobs/scan-local")
def job_scan_local(payload: dict = Body(...)):
    from ..connectors.local_folder import LocalFolderConnector

    root = (payload or {}).get("path", "").strip()
    if not root or not os.path.isdir(root):
        return JSONResponse({"error": f"路径无效: {root}"}, status_code=400)

    def fn(ctx: JobContext):
        s = S()
        led, orch = _orch(s)
        try:
            stats = orch.ingest(LocalFolderConnector(root), progress=_mk_progress(ctx))
            ctx.log(f"✅ 盘点+抽取完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("scan-local", fn, lock_key=f"local:{os.path.normcase(os.path.abspath(root))}")}


@app.post("/api/jobs/sharepoint")
def job_sharepoint(payload: dict = Body(...)):
    from ..connectors.sharepoint import SharePointConnector

    site = (payload or {}).get("site", "").strip()

    def fn(ctx: JobContext):
        s = S()
        if not (s.ms_tenant_id and s.ms_client_id and s.ms_client_secret):
            raise RuntimeError("未配置 SharePoint 凭证（MS_TENANT_ID/MS_CLIENT_ID/MS_CLIENT_SECRET）")
        led, orch = _orch(s)
        try:
            site_filter = site or s.ms_site_filter or None
            ctx.log(f"SharePoint 盘点（站点={site_filter or '全部根站点'}）…")
            conn = SharePointConnector(s.ms_tenant_id, s.ms_client_id, s.ms_client_secret,
                                       s.work_dir, site_filter=site_filter)
            stats = orch.ingest(conn, progress=_mk_progress(ctx))
            ctx.log(f"✅ SharePoint 盘点+抽取完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("sharepoint", fn, lock_key=f"sharepoint:{site or 'all'}")}


@app.post("/api/jobs/wedrive")
def job_wedrive(payload: dict = Body(...)):
    from ..connectors.wedrive import WeDriveConnector

    space_ids = [x.strip() for x in (payload or {}).get("space_ids", "").split(",") if x.strip()]
    if not space_ids:
        return JSONResponse({"error": "请填写至少一个微盘空间 ID（逗号分隔）"}, status_code=400)

    def fn(ctx: JobContext):
        s = S()
        if not (s.wecom_corp_id and s.wecom_wedrive_secret):
            raise RuntimeError("未配置企业微信 corp_id/微盘 secret")
        led, orch = _orch(s)
        try:
            conn = WeDriveConnector(s.wecom_corp_id, s.wecom_wedrive_secret, space_ids, s.work_dir)
            stats = orch.ingest(conn, progress=_mk_progress(ctx))
            ctx.log(f"✅ 微盘盘点+抽取完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("wedrive", fn, lock_key="wedrive:" + ",".join(sorted(space_ids)))}


@app.post("/api/jobs/wecom-chat")
def job_wecom_chat(payload: dict = Body(...)):
    """群聊会话存档：给了 chat_id 且 SDK 就绪则真正迁移，否则仅做就绪性检测。

    未就绪（无原生 SDK / 无 RSA 私钥）时不报错，返回 {ready: False}，
    降级为「仅迁群文件」——用微盘连接器迁群文件即可。
    """
    chat_id = (payload or {}).get("chat_id", "").strip()
    chat_name = (payload or {}).get("name", "").strip()

    def fn(ctx: JobContext):
        from ..connectors.wecom_chat import ChatArchiveConnector

        s = S()
        ctx.log("检测会话内容存档就绪性…")
        pem = ""
        if s.wecom_chat_private_key_file and os.path.exists(s.wecom_chat_private_key_file):
            pem = Path(s.wecom_chat_private_key_file).read_text(encoding="utf-8")
        else:
            ctx.log(f"⚠️ 未找到 RSA 私钥文件：{s.wecom_chat_private_key_file}")
        conn = ChatArchiveConnector(s.wecom_corp_id, s.wecom_chat_archive_secret, pem,
                                    sdk_lib_path=s.wecom_chat_sdk_lib_path)
        if not conn.online:
            ctx.log("未就绪：需开通会话内容存档 + 部署原生 WeWorkFinanceSdk + 配置 RSA 私钥")
            ctx.log("（未就绪时降级为「仅迁群文件」——用微盘连接器迁群文件即可）")
            return {"ready": False}
        ctx.log("✅ 会话存档 SDK 就绪")
        if not chat_id:
            ctx.log("未指定 chat_id：仅做就绪性检测。填入群聊 ID 即可触发迁移。")
            return {"ready": True}
        led, orch = _orch(s)
        try:
            ctx.log(f"开始迁移群聊 {chat_id}（按天聚合会话片段进标准管线）…")
            stats = orch.ingest_chat(conn, chat_id, chat_name=chat_name,
                                     progress=_mk_progress(ctx))
            ctx.log(f"✅ 群聊迁移完成：{stats}（随后跑「去重+分类」→确认→写飞书/挂 Wiki）")
            return {"ready": True, **stats}
        finally:
            led.close()

    return {"job_id": JOBS.start("wecom-chat", fn, lock_key=f"wecom-chat:{chat_id or 'readiness'}")}


@app.post("/api/jobs/pipeline")
def job_pipeline(payload: dict = Body(...)):
    """跑近似去重 + AI 分类（承接 scan 之后）。"""
    settings = S()
    ai_health = _ai_health(force=True)
    if settings.anthropic_api_key and not ai_health.get("ready"):
        return JSONResponse(
            {
                "error": f"Claude 分类不可用：{ai_health['message']}",
                "ai_health": ai_health,
            },
            status_code=503,
        )

    def fn(ctx: JobContext):
        s = S()
        led, orch = _orch(s)
        try:
            if ai_health.get("ready"):
                ctx.log(f"✅ Claude 前置校验通过：{ai_health.get('label', '连接正常')}")
            else:
                ctx.log("⚠️ 未配置 Claude API Key，本次使用离线启发式分类")
            ctx.log("① 近似去重…")
            d = orch.dedup_pass(progress=_mk_progress(ctx))
            ctx.log(f"去重结果：{d}")
            clf = Classifier(TX(), api_key=s.anthropic_api_key, model=s.claude_model,
                             base_url=s.anthropic_base_url, auth_style=s.anthropic_auth_style)
            ctx.log(f"② AI 分类（online={clf.online}）…")
            c = orch.classify_pass(clf, progress=_mk_progress(ctx))
            ctx.log(f"分类结果：{c}")
            return {"dedup": d, "classify": c}
        finally:
            led.close()

    return {"job_id": JOBS.start("pipeline", fn, lock_key="pipeline:dedup-classify")}


@app.post("/api/jobs/semantic")
def job_semantic(payload: dict = Body(...)):
    """语义去重（第三层）：标疑似重复进人工队列，不自动删。缺重依赖时自动跳过。"""
    threshold = float((payload or {}).get("threshold", 0.90))

    def fn(ctx: JobContext):
        s = S()
        led, orch = _orch(s)
        try:
            ctx.log("语义去重（cos≥%.2f）…" % threshold)
            stats = orch.semantic_pass(progress=_mk_progress(ctx), cos_threshold=threshold)
            if not stats.get("available"):
                ctx.log("⚠️ 语义去重不可用：需安装 sentence-transformers + faiss-cpu，已跳过")
            ctx.log(f"✅ 语义去重完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("semantic", fn, lock_key="pipeline:semantic")}


@app.post("/api/jobs/govern-chat")
def job_govern_chat(payload: dict = Body(...)):
    """群聊治理：成员→协作者映射 + 群名打标（dry_run=True 仅预览）。"""
    chat_id = (payload or {}).get("chat_id", "").strip()
    feishu_url = (payload or {}).get("url", "").strip()
    dry_run = bool((payload or {}).get("dry_run", True))
    if not chat_id:
        return JSONResponse({"error": "请填写群聊 ID（chat_id）"}, status_code=400)

    def fn(ctx: JobContext):
        import json as _json

        from ..connectors.wecom_group import WeComGroupConnector
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter

        s = S()
        user_map = {}
        if s.wecom_feishu_user_map and os.path.exists(s.wecom_feishu_user_map):
            user_map = _json.loads(Path(s.wecom_feishu_user_map).read_text(encoding="utf-8"))
        else:
            ctx.log(f"⚠️ 未找到成员映射文件：{s.wecom_feishu_user_map or '(未配置 WECOM_FEISHU_USER_MAP)'}")
        led, orch = _orch(s)
        try:
            writer = None
            group_conn = None
            if dry_run:
                ctx.log(f"[dry-run] 仅预览治理动作，不真实写入。映射条目={len(user_map)}")
            else:
                if not (s.feishu_app_id and s.feishu_app_secret):
                    raise RuntimeError("未配置飞书 App ID/Secret")
                writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
                group_conn = WeComGroupConnector(s.wecom_corp_id, s.wecom_app_secret)
            ctx.log("① 成员→协作者映射…")
            c = orch.map_chat_collaborators(writer, chat_id, user_map,
                                            progress=_mk_progress(ctx))
            ctx.log(f"协作者映射：{c}")
            ctx.log("② 群名打标…")
            t = orch.tag_chat_group(group_conn, chat_id, feishu_url=feishu_url,
                                    progress=_mk_progress(ctx))
            ctx.log(f"群名打标：{t}")
            return {"collaborators": c, "tag": t}
        finally:
            led.close()

    return {
        "job_id": JOBS.start(
            "govern-chat", fn, lock_key="feishu:write"
        )
    }


@app.post("/api/jobs/bootstrap")
def job_bootstrap(payload: dict = Body(...)):
    mode = (payload or {}).get("mode", "drive")

    def fn(ctx: JobContext):
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.auth import load_user_token
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter

        s = S()
        if not (s.feishu_app_id and s.feishu_app_secret):
            raise RuntimeError("未配置飞书 App ID/Secret")
        writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
        boot = FeishuBootstrapper(writer, TX(), s.feishu_targets_file)
        if mode == "wiki":
            token = s.feishu_user_access_token or load_user_token(
                s.feishu_user_token_file,
                s.feishu_app_id,
                s.feishu_app_secret,
            )
            if not token:
                raise RuntimeError("建 Wiki 空间需先完成飞书 OAuth（授权页）")
            ctx.log("建 Wiki 知识空间 + 分类节点…")
            t = boot.bootstrap_wiki_space(token)
        else:
            ctx.log("建云空间文件夹树 + 分类子文件夹…")
            t = boot.bootstrap_drive_tree()
        ctx.log("✅ 完成，映射已持久化")
        ctx.log(boot.summary(t))
        return {"mode": t.get("mode"), "folder_map": t.get("folder_map")}

    return {
        "job_id": JOBS.start(
            "bootstrap", fn, lock_key="feishu:write"
        )
    }


@app.post("/api/jobs/structure-apply")
def job_structure_apply(payload: dict = Body(...)):
    """预览或执行已确认的版本化目录计划；REMOTE_ONLY 永不自动删除。"""
    data = payload or {}
    version_id = str(data.get("version_id") or "")
    plan_id = str(data.get("plan_id") or "")
    dry_run = bool(data.get("dry_run", True))
    if not version_id and not plan_id:
        return JSONResponse(
            {"error": "缺少结构版本 ID 或结构计划 ID"}, status_code=400
        )

    def fn(ctx: JobContext):
        from ..feishu.auth import load_user_token
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter
        from ..structure import StructureService
        from ..structure.reconcile import StructureReconciler

        s = S()
        led = Ledger(s.ledger_db)
        try:
            structures = StructureService(led, TX(), s.feishu_targets_file)
            nonlocal version_id, plan_id
            if plan_id:
                selected_plan = structures.get_change_plan(plan_id)
                if version_id and selected_plan["version_id"] != version_id:
                    raise RuntimeError("结构计划与结构版本不一致")
                version_id = selected_plan["version_id"]
            version = structures.get_version(version_id)
            snapshot = structures.latest_snapshot(version["mode"])
            if dry_run:
                plan = (
                    structures.get_change_plan(plan_id)
                    if plan_id else
                    structures.create_diff_plan(
                        version_id, snapshot["id"] if snapshot else ""
                    )
                )
                ctx.log(
                    f"[dry-run] 节点={plan['summary']['planned_nodes']} "
                    f"影响文件={plan['summary']['affected_items']} "
                    f"冲突={plan['summary']['blocking_conflicts']}"
                )
                for action, count in sorted(plan["summary"]["counts"].items()):
                    ctx.log(f"{action}: {count}")
                return plan["summary"]

            if not plan_id:
                latest = structures.latest_change_plan(version_id)
                if not latest or latest["status"] not in (
                    "approved", "failed",
                ):
                    raise RuntimeError("请先生成并审批结构发布计划")
                plan_id = latest["id"]
            if not (s.feishu_app_id and s.feishu_app_secret):
                raise RuntimeError("未配置飞书 App ID/Secret")
            targets = FeishuBootstrapper(
                None, TX(), s.feishu_targets_file
            ).load_targets()
            user_token = ""
            if version["mode"] == "wiki":
                user_token = (
                    s.feishu_user_access_token
                    or load_user_token(
                        s.feishu_user_token_file,
                        s.feishu_app_id,
                        s.feishu_app_secret,
                    )
                )
                if not user_token:
                    raise RuntimeError("发布 Wiki 结构需要先完成飞书 OAuth")
            writer = FeishuWriter(
                FeishuClient(s.feishu_app_id, s.feishu_app_secret)
            )
            result = StructureReconciler(structures, writer).apply(
                version_id,
                plan_id=plan_id,
                root_token=str(targets.get("root_token") or ""),
                space_id=str(targets.get("space_id") or ""),
                user_token=user_token,
                progress=_mk_progress(ctx),
            )
            ctx.log(f"✅ 结构版本已激活：{version_id}")
            return result
        finally:
            led.close()

    return {
        "job_id": JOBS.start(
            "structure-apply", fn, lock_key="feishu:write"
        )
    }


@app.post("/api/structure-plans/{plan_id}/apply")
def api_structure_plan_apply(
    plan_id: str, payload: dict = Body(default={})
):
    led, structures = _structure()
    try:
        plan = structures.get_change_plan(plan_id)
    except KeyError:
        return JSONResponse({"error": "结构计划不存在"}, status_code=404)
    finally:
        led.close()
    return job_structure_apply({
        "plan_id": plan_id,
        "version_id": plan["version_id"],
        "dry_run": bool((payload or {}).get("dry_run", True)),
    })


@app.post("/api/jobs/relocation-apply")
def job_item_relocation_apply(payload: dict = Body(...)):
    """预检、执行或回滚已审批的历史文件重定位计划。"""
    data = payload or {}
    plan_id = str(data.get("plan_id") or "")
    dry_run = bool(data.get("dry_run", True))
    rollback = bool(data.get("rollback", False))
    if not plan_id:
        return JSONResponse({"error": "缺少重定位计划 ID"}, status_code=400)
    if dry_run and rollback:
        return JSONResponse(
            {"error": "回滚不支持 dry-run 组合；请先预览计划审计记录"},
            status_code=400,
        )

    def fn(ctx: JobContext):
        from ..feishu.auth import load_user_token
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter
        from ..structure import ItemRelocationExecutor, StructureService

        s = S()
        led = Ledger(s.ledger_db)
        try:
            structures = StructureService(led, TX(), s.feishu_targets_file)
            plan = structures.get_item_relocation_plan(plan_id)
            if dry_run and not (s.feishu_app_id and s.feishu_app_secret):
                ctx.log(
                    "[dry-run] 未配置飞书应用凭证，仅返回本地候选；"
                    "执行前仍会完成远程同名冲突预检。"
                )
                return {
                    **plan["summary"], "plan_id": plan_id,
                    "remote_preflight": False,
                }
            if not (s.feishu_app_id and s.feishu_app_secret):
                raise RuntimeError("未配置飞书 App ID/Secret")
            targets = FeishuBootstrapper(
                None, TX(), s.feishu_targets_file
            ).load_targets()
            user_token = ""
            if plan["target_mode"] == "wiki":
                user_token = (
                    s.feishu_user_access_token
                    or load_user_token(
                        s.feishu_user_token_file,
                        s.feishu_app_id,
                        s.feishu_app_secret,
                    )
                )
                if not user_token:
                    raise RuntimeError("Wiki 重定位需要先完成飞书 OAuth")
            writer = FeishuWriter(
                FeishuClient(s.feishu_app_id, s.feishu_app_secret)
            )
            executor = ItemRelocationExecutor(structures, writer)
            if dry_run:
                result = executor.preflight(
                    plan_id,
                    space_id=str(targets.get("space_id") or ""),
                    user_token=user_token,
                )
                ctx.log(
                    f"[dry-run] 就绪={result['ready']} "
                    f"已在目标={result['already_moved']} "
                    f"冲突={len(result['conflicts'])}"
                )
                return {
                    **result["plan"]["summary"],
                    "plan_id": plan_id, "remote_preflight": True,
                }
            if rollback:
                ctx.log("开始按审计记录回滚本计划已移动内容…")
                result = executor.rollback(
                    plan_id,
                    space_id=str(targets.get("space_id") or ""),
                    user_token=user_token,
                    progress=_mk_progress(ctx),
                )
            else:
                ctx.log("执行全量冲突预检，通过后才开始移动…")
                result = executor.execute(
                    plan_id,
                    space_id=str(targets.get("space_id") or ""),
                    user_token=user_token,
                    progress=_mk_progress(ctx),
                )
            ctx.log(f"✅ 历史重定位任务完成：{result}")
            return result
        finally:
            led.close()

    action = "rollback" if rollback else "apply"
    return {
        "job_id": JOBS.start(
            f"item-relocation-{action}", fn,
            lock_key="feishu:write",
        )
    }


@app.post("/api/jobs/load")
def job_load(payload: dict = Body(...)):
    dry_run = bool((payload or {}).get("dry_run", True))
    frozen_structure_version_id = _active_structure_id("")

    def fn(ctx: JobContext):
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter
        from ..structure import StructureService

        s = S()
        led, orch = _orch(s)
        try:
            bootstrap = FeishuBootstrapper(
                None, TX(), s.feishu_targets_file
            )
            targets = bootstrap.load_targets()
            structures = StructureService(led, TX(), s.feishu_targets_file)
            structure_version_id = frozen_structure_version_id
            writer = None
            if not dry_run:
                if not (s.feishu_app_id and s.feishu_app_secret):
                    raise RuntimeError("未配置飞书 App ID/Secret")
                writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
                if structure_version_id:
                    version = structures.get_version(structure_version_id)
                    if (
                        version["mode"] == "wiki"
                        and not targets.get("root_token")
                        and led.items_in_stage(Stage.CONFIRMED)
                    ):
                        bootstrap = FeishuBootstrapper(
                            writer, TX(), s.feishu_targets_file
                        )
                        targets = bootstrap.ensure_staging_root(
                            f"{version['root_name']} · Wiki 文件暂存"
                        )
                        ctx.log("已自动创建 Wiki 写入暂存目录")
            folder_map, target_node_map, target_resolver, target_mode = (
                _load_routing_context(
                    structures, targets, structure_version_id
                )
            )
            if not folder_map and led.items_in_stage(Stage.CONFIRMED):
                raise RuntimeError(
                    "当前没有可用的写入路由：请先在「结构工作台」发布并激活目录结构"
                )
            staging = (
                "待真实写入时自动创建"
                if target_mode == "wiki" and not targets.get("root_token")
                else "已就绪"
            )
            ctx.log(
                f"[{'dry-run' if dry_run else 'preflight'}] "
                f"目标形态={target_mode} 分类数={len(folder_map)} "
                f"结构版本={structure_version_id or 'legacy'} 暂存目录={staging}"
            )
            stats = orch.load_pass(
                writer, folder_map, progress=_mk_progress(ctx),
                identity_map=load_identity_map(s.identity_map_file),
                structure_version_id=structure_version_id,
                target_node_map=target_node_map,
                target_resolver=target_resolver,
            )
            ctx.log(f"✅ load 完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("load", fn, lock_key="feishu:write")}


@app.post("/api/jobs/retry")
def job_retry(payload: dict = Body(...)):
    """重试写飞书失败的条目（load 阶段 FAILED）。镜像 job_load。"""
    dry_run = bool((payload or {}).get("dry_run", True))
    frozen_structure_version_id = _active_structure_id("")

    def fn(ctx: JobContext):
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter
        from ..structure import StructureService

        s = S()
        led, orch = _orch(s)
        try:
            bootstrap = FeishuBootstrapper(
                None, TX(), s.feishu_targets_file
            )
            targets = bootstrap.load_targets()
            structures = StructureService(led, TX(), s.feishu_targets_file)
            structure_version_id = frozen_structure_version_id
            writer = None
            if not dry_run:
                if not (s.feishu_app_id and s.feishu_app_secret):
                    raise RuntimeError("未配置飞书 App ID/Secret")
                writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
                if structure_version_id:
                    version = structures.get_version(structure_version_id)
                    if (
                        version["mode"] == "wiki"
                        and not targets.get("root_token")
                        and led.failed_items("load")
                    ):
                        bootstrap = FeishuBootstrapper(
                            writer, TX(), s.feishu_targets_file
                        )
                        targets = bootstrap.ensure_staging_root(
                            f"{version['root_name']} · Wiki 文件暂存"
                        )
                        ctx.log("已自动创建 Wiki 写入暂存目录")
            folder_map, target_node_map, target_resolver, target_mode = (
                _load_routing_context(
                    structures, targets, structure_version_id
                )
            )
            if not folder_map and led.failed_items("load"):
                raise RuntimeError(
                    "当前没有可用的写入路由：请先在「结构工作台」发布并激活目录结构"
                )
            ctx.log(
                f"[{'dry-run' if dry_run else 'preflight'}] "
                f"{'仅预览失败项重排，不真实写入。' if dry_run else ''}"
                f"目标形态={target_mode} 分类数={len(folder_map)} "
                f"结构版本={structure_version_id or 'legacy'}"
            )
            stats = orch.retry_failed_loads(
                writer, folder_map, progress=_mk_progress(ctx),
                identity_map=load_identity_map(s.identity_map_file),
                structure_version_id=structure_version_id,
                target_node_map=target_node_map,
                target_resolver=(
                    lambda row: {
                        **(
                            structures.resolve_retry_target(
                                row, structure_version_id, mode=target_mode
                            ) or {}
                        ),
                        **(
                            {"remote_token": str(targets.get("root_token") or "")}
                            if target_mode == "wiki" else {}
                        ),
                    }
                ) if structure_version_id else target_resolver,
            )
            ctx.log(f"✅ retry 完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("retry", fn, lock_key="feishu:write")}


@app.post("/api/jobs/archive")
def job_archive(payload: dict = Body(...)):
    """归档到期 Drive 文件或 Wiki 节点；dry_run 缺省 True。"""
    dry_run = bool((payload or {}).get("dry_run", True))
    reason = str((payload or {}).get("reason", "retention_due"))
    frozen_structure_version_id = _active_structure_id("")

    def fn(ctx: JobContext):
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.auth import load_user_token
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter
        from ..structure import StructureService

        s = S()
        led, orch = _orch(s)
        try:
            targets = FeishuBootstrapper(None, TX(), s.feishu_targets_file).load_targets()
            archive_token = (targets.get("folder_map") or {}).get(TX().archive_path, "")
            wiki_archive = (targets.get("wiki_node_map") or {}).get(TX().archive_path, "")
            wiki_space = targets.get("space_id") or ""
            if frozen_structure_version_id:
                structures = StructureService(
                    led, TX(), s.feishu_targets_file
                )
                frozen = structures.get_version(frozen_structure_version_id)
                archive_node = next((
                    node for node in frozen["nodes"]
                    if node["node_kind"] == "archive"
                ), None)
                archive_binding = (
                    (archive_node or {}).get("binding") or {}
                )
                if frozen["mode"] == "drive":
                    archive_token = (
                        archive_binding.get("remote_token")
                        or archive_token
                    )
                else:
                    wiki_archive = (
                        archive_binding.get("remote_token")
                        or wiki_archive
                    )
            user_token = ""
            writer = None
            if dry_run:
                ctx.log("[dry-run] 仅预览保留期到期的归档候选，不移动文件。")
            else:
                if not (archive_token or (wiki_space and wiki_archive)):
                    raise RuntimeError("缺少 99 归档目标：请先 bootstrap 云空间或 Wiki 目录树")
                if not (s.feishu_app_id and s.feishu_app_secret):
                    raise RuntimeError("未配置飞书 App ID/Secret")
                if wiki_space and wiki_archive:
                    user_token = (s.feishu_user_access_token
                                  or load_user_token(
                                      s.feishu_user_token_file,
                                      s.feishu_app_id,
                                      s.feishu_app_secret,
                                  ))
                    if not user_token:
                        raise RuntimeError("归档 Wiki 节点需要 user_access_token，请先完成 OAuth")
                writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
            stats = orch.archive_due_items(writer, archive_token, commit=not dry_run,
                                            reason=reason, progress=_mk_progress(ctx),
                                            wiki_space_id=wiki_space,
                                            wiki_archive_node=wiki_archive,
                                            user_token=user_token,
                                            structure_version_id=(
                                                frozen_structure_version_id
                                            ))
            ctx.log(f"✅ archive 完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("archive", fn, lock_key="feishu:write")}


@app.post("/api/jobs/push-to-wiki")
def job_push_to_wiki(payload: dict = Body(...)):
    """把已上传云文件挂进 Wiki 分类节点（需先 bootstrap --wiki）。镜像 job_load。"""
    dry_run = bool((payload or {}).get("dry_run", True))
    frozen_structure_version_id = _active_structure_id("wiki")

    def fn(ctx: JobContext):
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.auth import load_user_token
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter
        from ..structure import StructureService

        s = S()
        led, orch = _orch(s)
        try:
            targets = FeishuBootstrapper(None, TX(), s.feishu_targets_file).load_targets()
            node_map = targets.get("wiki_node_map") or {}
            structures = StructureService(led, TX(), s.feishu_targets_file)
            structure_version_id = frozen_structure_version_id
            if structure_version_id:
                structure_nodes, _ = structures.routing_map(
                    structure_version_id, mode="wiki"
                )
                if structure_nodes:
                    node_map = structure_nodes
            writer = None
            user_token = ""
            if dry_run:
                ctx.log(f"[dry-run] space_id={targets.get('space_id') or '未初始化'} "
                        f"wiki 节点分类数={len(node_map)}，不真实挂入")
            else:
                if not (targets.get("space_id") and node_map):
                    raise RuntimeError("缺少 Wiki 空间/节点映射：请先在「目标结构」页 bootstrap（Wiki 模式）")
                if not (s.feishu_app_id and s.feishu_app_secret):
                    raise RuntimeError("未配置飞书 App ID/Secret")
                user_token = s.feishu_user_access_token or load_user_token(
                    s.feishu_user_token_file,
                    s.feishu_app_id,
                    s.feishu_app_secret,
                )
                if not user_token:
                    raise RuntimeError("挂入用户所属 Wiki 空间需先完成飞书 OAuth（授权页）")
                writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
            stats = orch.move_loaded_to_wiki(
                writer, targets, user_token=user_token, progress=_mk_progress(ctx),
                identity_map=load_identity_map(s.identity_map_file),
                structure_version_id=structure_version_id,
                target_resolver=(
                    lambda row: structures.resolve_item_target(
                        row, structure_version_id, mode="wiki"
                    )
                ) if structure_version_id else None,
            )
            ctx.log(f"✅ push-to-wiki 完成：{stats}")
            return stats
        finally:
            led.close()

    return {
        "job_id": JOBS.start(
            "push-to-wiki", fn, lock_key="feishu:write"
        )
    }
