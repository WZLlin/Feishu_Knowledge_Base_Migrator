"""统一 Web 控制台（FastAPI）——单页可视化，所有基本操作一页完成。

一页六个标签（前端 static/index.html）：
  概览 · 配置(填/存凭证) · 授权(飞书OAuth+连接测试) · 迁移(本地/微盘/群聊) ·
  确认队列 · 目标结构(bootstrap/load)

后端只提供 JSON API + 一个内存 JobManager 跑长任务；每个 job 线程内自建
Ledger/Orchestrator（SQLite 连接不跨线程）。凭证仍只落 .env，GET 不回明文。

启动（务必绑定本机，避免暴露凭证操作面）：
  uvicorn kb_migrator.web.app:app --host 127.0.0.1 --port 8000
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import Body, FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from ..config import get_settings
from ..ledger import Ledger
from ..models import Stage
from ..pipeline.classify import Classifier
from ..pipeline.orchestrator import Orchestrator
from ..taxonomy import Taxonomy
from . import settings_io
from .jobs import JobContext, JobManager

app = FastAPI(title="kb-migrator 控制台")
JOBS = JobManager()
_STATIC = Path(__file__).parent / "static"


# ── 运行期依赖（每次取新，配置热生效）───────────────────────

def S():
    return get_settings()


def TX() -> Taxonomy:
    return Taxonomy.load(S().taxonomy_file)


def _orch(s=None) -> tuple[Ledger, Orchestrator]:
    s = s or S()
    led = Ledger(s.ledger_db)
    return led, Orchestrator(led, TX(), s.work_dir, s.confidence_threshold)


def _mk_progress(ctx: JobContext):
    """把 orchestrator 的 progress(done,total,msg) 转成 job 的进度+日志。"""
    def cb(done: int, total: int, msg: str = ""):
        ctx.progress(done, total)
        if msg:
            ctx.log(msg)
    return cb


# ── 单页 ────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index():
    html = (_STATIC / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


# ── 概览 ────────────────────────────────────────────────────

@app.get("/api/status")
def api_status():
    s = S()
    led = Ledger(s.ledger_db)
    counts = led.stage_counts()
    led.close()
    total = sum(counts.values())
    loaded = counts.get(Stage.LOADED.value, 0)
    ratio = round(loaded / total * 100, 1) if total else 0.0

    ms = settings_io.masked_settings()
    configured = {k: v["configured"] for k, v in ms.items()}

    from ..feishu.bootstrap import FeishuBootstrapper
    from ..feishu.auth import load_user_token
    targets = FeishuBootstrapper(None, TX(), s.feishu_targets_file).load_targets()

    return {
        "counts": counts,
        "total": total,
        "loaded": loaded,
        "sink_ratio": ratio,
        "feishu_ready": configured.get("FEISHU_APP_ID") and configured.get("FEISHU_APP_SECRET"),
        "claude_ready": configured.get("ANTHROPIC_API_KEY"),
        "wecom_ready": configured.get("WECOM_CORP_ID") and configured.get("WECOM_WEDRIVE_SECRET"),
        "oauth_token": bool(load_user_token(s.feishu_user_token_file)),
        "targets_mode": targets.get("mode") or "",
        "targets_count": len(targets.get("folder_map") or {}),
        "jobs": JOBS.list(),
    }


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
    url = build_authorize_url(s.feishu_app_id, s.feishu_redirect_uri,
                              scope=s.feishu_oauth_scope, state="kbm")
    return RedirectResponse(url, status_code=302)


@app.get("/feishu/oauth/callback", response_class=HTMLResponse)
def oauth_callback(code: str = "", state: str = "", error: str = ""):
    from ..feishu.auth import exchange_user_token, save_user_token

    s = S()
    if error or not code:
        return HTMLResponse(f"<h3>授权失败：{error or '无 code'}</h3><a href='/'>返回控制台</a>")
    data = exchange_user_token(s.feishu_app_id, s.feishu_app_secret, code, s.feishu_redirect_uri)
    token = data.get("access_token") or (data.get("data") or {}).get("access_token")
    if not token:
        return HTMLResponse(f"<h3>换取 token 失败：{data}</h3><a href='/'>返回控制台</a>")
    save_user_token(s.feishu_user_token_file, data)
    return HTMLResponse(
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
    return {"targets": t, "summary": boot.summary(t)}


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
    orch.confirm(payload["key"], payload["category"], payload.get("name"))
    led.close()
    return {"ok": True}


@app.post("/api/reject")
def api_reject(payload: dict = Body(...)):
    led, orch = _orch()
    orch.reject_as_duplicate(payload["key"])
    led.close()
    return {"ok": True}


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

    return {"job_id": JOBS.start("scan-local", fn)}


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

    return {"job_id": JOBS.start("sharepoint", fn)}


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

    return {"job_id": JOBS.start("wedrive", fn)}


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

    return {"job_id": JOBS.start("wecom-chat", fn)}


@app.post("/api/jobs/pipeline")
def job_pipeline(payload: dict = Body(...)):
    """跑近似去重 + AI 分类（承接 scan 之后）。"""

    def fn(ctx: JobContext):
        s = S()
        led, orch = _orch(s)
        try:
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

    return {"job_id": JOBS.start("pipeline", fn)}


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

    return {"job_id": JOBS.start("semantic", fn)}


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

    return {"job_id": JOBS.start("govern-chat", fn)}


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
            token = s.feishu_user_access_token or load_user_token(s.feishu_user_token_file)
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

    return {"job_id": JOBS.start("bootstrap", fn)}


@app.post("/api/jobs/load")
def job_load(payload: dict = Body(...)):
    dry_run = bool((payload or {}).get("dry_run", False))

    def fn(ctx: JobContext):
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter

        s = S()
        led, orch = _orch(s)
        try:
            targets = FeishuBootstrapper(None, TX(), s.feishu_targets_file).load_targets()
            folder_map = targets.get("folder_map") or {}
            writer = None
            if dry_run:
                ctx.log(f"[dry-run] 目标形态={targets.get('mode') or '未初始化'} 分类数={len(folder_map)}")
            else:
                if not folder_map:
                    raise RuntimeError("folder_map 为空：请先在「目标结构」页 bootstrap")
                if not (s.feishu_app_id and s.feishu_app_secret):
                    raise RuntimeError("未配置飞书 App ID/Secret")
                writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
            stats = orch.load_pass(writer, folder_map, progress=_mk_progress(ctx))
            ctx.log(f"✅ load 完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("load", fn)}


@app.post("/api/jobs/retry")
def job_retry(payload: dict = Body(...)):
    """重试写飞书失败的条目（load 阶段 FAILED）。镜像 job_load。"""
    dry_run = bool((payload or {}).get("dry_run", False))

    def fn(ctx: JobContext):
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter

        s = S()
        led, orch = _orch(s)
        try:
            targets = FeishuBootstrapper(None, TX(), s.feishu_targets_file).load_targets()
            folder_map = targets.get("folder_map") or {}
            writer = None
            if dry_run:
                ctx.log(f"[dry-run] 仅重排失败项回 CONFIRMED，不真实写入。分类数={len(folder_map)}")
            else:
                if not folder_map:
                    raise RuntimeError("folder_map 为空：请先在「目标结构」页 bootstrap")
                if not (s.feishu_app_id and s.feishu_app_secret):
                    raise RuntimeError("未配置飞书 App ID/Secret")
                writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
            stats = orch.retry_failed_loads(writer, folder_map, progress=_mk_progress(ctx))
            ctx.log(f"✅ retry 完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("retry", fn)}


@app.post("/api/jobs/push-to-wiki")
def job_push_to_wiki(payload: dict = Body(...)):
    """把已上传云文件挂进 Wiki 分类节点（需先 bootstrap --wiki）。镜像 job_load。"""
    dry_run = bool((payload or {}).get("dry_run", False))

    def fn(ctx: JobContext):
        from ..feishu.bootstrap import FeishuBootstrapper
        from ..feishu.auth import load_user_token
        from ..feishu.client import FeishuClient
        from ..feishu.writer import FeishuWriter

        s = S()
        led, orch = _orch(s)
        try:
            targets = FeishuBootstrapper(None, TX(), s.feishu_targets_file).load_targets()
            node_map = targets.get("wiki_node_map") or {}
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
                user_token = s.feishu_user_access_token or load_user_token(s.feishu_user_token_file)
                if not user_token:
                    raise RuntimeError("挂入用户所属 Wiki 空间需先完成飞书 OAuth（授权页）")
                writer = FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))
            stats = orch.move_loaded_to_wiki(writer, targets, user_token=user_token,
                                             progress=_mk_progress(ctx))
            ctx.log(f"✅ push-to-wiki 完成：{stats}")
            return stats
        finally:
            led.close()

    return {"job_id": JOBS.start("push-to-wiki", fn)}
