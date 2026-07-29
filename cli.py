#!/usr/bin/env python
"""kb-migrator 命令行入口。

典型流程（本地文件夹 → 飞书）：
  python cli.py scan-local  D:/知识资料      # 盘点 + 抽取 + 精确去重
  python cli.py dedup                        # 近似去重
  python cli.py classify                     # AI 分类（无 API Key 走离线启发式）
  python cli.py stats                        # 查看各阶段条目数
  python cli.py review                       # 查看人工确认队列
  python cli.py confirm <key> "01 制度与流程"  # 人工确认归类
  python cli.py load --dry-run               # 预览写飞书计划（不真实上传）
  python cli.py load                         # 真实写飞书（默认即 commit，会上传！）
"""
from __future__ import annotations

import argparse
import sys

# Windows 控制台默认 GBK，会把中文分类/标题显示成乱码；强制 stdout/stderr 走 UTF-8。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass

from kb_migrator.config import get_settings
from kb_migrator.ledger import Ledger
from kb_migrator.models import Stage
from kb_migrator.pipeline.classify import Classifier
from kb_migrator.pipeline.orchestrator import Orchestrator
from kb_migrator.taxonomy import Taxonomy
from kb_migrator.utils.identity import load_identity_map


def _bootstrap():
    s = get_settings()
    s.ensure_dirs()
    tx = Taxonomy.load(s.taxonomy_file)
    led = Ledger(s.ledger_db)
    orch = Orchestrator(led, tx, s.work_dir, s.confidence_threshold)
    return s, tx, led, orch


def cmd_scan_local(args):
    from kb_migrator.connectors.local_folder import LocalFolderConnector

    _s, _tx, led, orch = _bootstrap()
    conn = LocalFolderConnector(args.root)
    stats = orch.ingest(conn)
    print("ingest:", stats)
    led.close()


def cmd_scan_sharepoint(args):
    """盘点+抽取 SharePoint（MS Graph，client_credentials 应用权限）。"""
    from kb_migrator.connectors.sharepoint import SharePointConnector

    s, _tx, led, orch = _bootstrap()
    if not (s.ms_tenant_id and s.ms_client_id and s.ms_client_secret):
        raise SystemExit(
            "缺少 MS_TENANT_ID / MS_CLIENT_ID / MS_CLIENT_SECRET，请在 .env 配置 "
            "Entra ID 应用凭证（应用权限 Sites.Read.All + Files.Read.All，需管理员同意）。")
    site_filter = args.site or s.ms_site_filter or None
    conn = SharePointConnector(s.ms_tenant_id, s.ms_client_id, s.ms_client_secret,
                               s.work_dir, site_filter=site_filter)
    print(f"scan-sharepoint (site_filter={site_filter or '全部根站点'}):",
          orch.ingest(conn))
    led.close()


def cmd_scan_wedrive(args):
    """盘点+抽取 企业微信微盘（仅普通文件可下载；服务器 IP 须在可信白名单）。"""
    from kb_migrator.connectors.wedrive import WeDriveConnector

    s, _tx, led, orch = _bootstrap()
    if not (s.wecom_corp_id and s.wecom_wedrive_secret):
        raise SystemExit("缺少 WECOM_CORP_ID / WECOM_WEDRIVE_SECRET，请在 .env 配置。")
    space_ids = [x.strip() for x in args.space_ids.split(",") if x.strip()]
    if not space_ids:
        raise SystemExit("请提供至少一个微盘空间 ID（逗号分隔）。")
    conn = WeDriveConnector(s.wecom_corp_id, s.wecom_wedrive_secret, space_ids, s.work_dir)
    print("scan-wedrive:", orch.ingest(conn))
    led.close()


def cmd_migrate_chat(args):
    """群聊会话存档迁移：按自然日聚合会话片段，进标准管线（需原生存档 SDK + RSA 私钥）。"""
    import os

    from kb_migrator.connectors.wecom_chat import ChatArchiveConnector

    s, _tx, led, orch = _bootstrap()
    pem = ""
    if s.wecom_chat_private_key_file and os.path.exists(s.wecom_chat_private_key_file):
        with open(s.wecom_chat_private_key_file, "r", encoding="utf-8") as f:
            pem = f.read()
    conn = ChatArchiveConnector(s.wecom_corp_id, s.wecom_chat_archive_secret, pem,
                                sdk_lib_path=s.wecom_chat_sdk_lib_path)
    if not conn.online:
        print("会话存档未就绪：需开通「会话内容存档」+ 部署原生 WeWorkFinanceSdk"
              "（WECOM_CHAT_SDK_LIB）+ 配置 RSA 私钥（WECOM_CHAT_PRIVATE_KEY_FILE）。")
        print("未就绪时降级为「仅迁群文件」——用 `scan-wedrive` 迁群文件即可。")
        led.close()
        return
    print("migrate-chat:", orch.ingest_chat(conn, args.chat_id, chat_name=args.name,
                                            limit=args.limit))
    led.close()


def _load_user_map(path):
    """读 {wecom_userid: feishu_open_id} 映射 JSON；缺文件返回空表（不阻断）。"""
    import json
    import os

    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def cmd_govern_chat(args):
    """群聊治理：成员→协作者映射 + 群名打标（默认 dry-run，--commit 才真写）。"""
    s, _tx, led, orch = _bootstrap()
    user_map = _load_user_map(s.wecom_feishu_user_map)
    writer = None
    group_conn = None
    if args.commit:
        writer = _feishu_writer(s)
        from kb_migrator.connectors.wecom_group import WeComGroupConnector
        group_conn = WeComGroupConnector(s.wecom_corp_id, s.wecom_app_secret)
    else:
        print(f"[dry-run] 预览治理动作，不真实写入（加 --commit 才写）。"
              f"映射表条目={len(user_map)}")
    if not user_map:
        print("⚠️ 成员→飞书映射为空：请在 WECOM_FEISHU_USER_MAP 指向的 JSON 里维护 "
              "{企业微信userid: 飞书open_id}；未映射成员会进人工清单。")
    print("collaborators:", orch.map_chat_collaborators(writer, args.chat_id, user_map))
    print("tag-group:", orch.tag_chat_group(group_conn, args.chat_id, feishu_url=args.url))
    led.close()


def cmd_dedup(args):
    _s, _tx, led, orch = _bootstrap()
    print("dedup:", orch.dedup_pass(threshold=args.threshold))
    led.close()


def cmd_semantic(args):
    """语义去重（第三层）：标疑似重复进人工队列，不自动删。缺重依赖时自动跳过。"""
    _s, _tx, led, orch = _bootstrap()
    stats = orch.semantic_pass(cos_threshold=args.threshold)
    if not stats.get("available"):
        print("语义去重不可用：需安装 sentence-transformers + faiss-cpu（可选重依赖），已跳过。")
    print("semantic:", stats)
    led.close()


def cmd_classify(args):
    s, tx, led, orch = _bootstrap()
    clf = Classifier(tx, api_key=s.anthropic_api_key, model=s.claude_model,
                     base_url=s.anthropic_base_url, auth_style=s.anthropic_auth_style)
    print(f"classifier online={clf.online}")
    print("classify:", orch.classify_pass(clf))
    led.close()


def cmd_stats(args):
    _s, _tx, led, _orch = _bootstrap()
    counts = led.stage_counts()
    total = sum(counts.values())
    loaded = counts.get(Stage.LOADED.value, 0)
    print(f"台账条目总数: {total}")
    print(f"沉淀比例: {loaded / total * 100:.1f}%" if total else "沉淀比例: 0.0%")
    for stage in Stage:
        if stage.value in counts:
            print(f"  {stage.value:20s} {counts[stage.value]}")
    led.close()


def cmd_runs(args):
    """查看最近管线执行批次及最终统计。"""
    _s, _tx, led, _orch = _bootstrap()
    rows = led.recent_pipeline_runs(args.limit)
    print(f"管线批次: {len(rows)} 条")
    for row in rows:
        print(f"  [{row['id']}] {row['run_type']} {row['status']} "
              f"{row['started_at']} stats={row['stats_json'] or '{}'} "
              f"error={row['error_detail'] or ''}")
    led.close()


def cmd_source_changes(args):
    """查看源端发生变化、尚未进入版本化同步的审计队列。"""
    _s, _tx, led, _orch = _bootstrap()
    rows = led.pending_source_changes()
    print(f"源端变更待处理: {len(rows)} 条")
    for row in rows[: args.limit]:
        print(f"  [{row['id']}] {row['stable_key']} detected={row['detected_at']}")
    led.close()


def cmd_apply_source_changes(args):
    _s, _tx, led, _orch = _bootstrap()
    rows = led.pending_source_changes()[:args.limit]
    for row in rows:
        print(f"[{row['id']}] -> {led.materialize_source_change(row['id'])}")
    print(f"已派生 {len(rows)} 个新版本；请运行对应来源 scan 命令下载与抽取。")
    led.close()


def cmd_missing_sources(args):
    _s, _tx, led, _orch = _bootstrap()
    rows = led.missing_source_items()
    print(f"源端已删除待处理: {len(rows)} 条")
    for row in rows[:args.limit]:
        print(f"  [{row['stable_key']}] {row['original_name']} missing={row['source_missing_at']}")
    led.close()


def _archive_context(s, tx):
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper
    targets = FeishuBootstrapper(None, tx, s.feishu_targets_file).load_targets()
    return (targets.get("folder_map") or {}).get(tx.archive_path, "")


def cmd_resolve_missing(args):
    s, tx, led, orch = _bootstrap()
    folder = _archive_context(s, tx)
    if args.action == "archive" and args.commit and not folder:
        raise SystemExit("缺少 99 归档 folder token")
    writer = _feishu_writer(s) if args.action == "archive" and args.commit else None
    print(orch.resolve_missing_source(args.key, args.action, writer, folder, args.commit))
    led.close()


def cmd_finalize_source_change(args):
    s, tx, led, orch = _bootstrap()
    folder = _archive_context(s, tx)
    if args.commit and not folder:
        raise SystemExit("缺少 99 归档 folder token")
    writer = _feishu_writer(s) if args.commit else None
    print(orch.finalize_source_change(args.change_id, writer, folder, commit=args.commit))
    led.close()


def cmd_review(args):
    _s, _tx, led, _orch = _bootstrap()
    rows = led.pending_review()
    print(f"人工确认队列: {len(rows)} 条")
    for r in rows[: args.limit]:
        print(f"  [{r['stable_key']}] 建议={r['category']} conf={r['confidence']} "
              f"名={r['original_name']} note={r['error_detail'] or ''}")
    led.close()


def cmd_failures(args):
    """查看结构化失败队列及其可重试性。"""
    _s, _tx, led, _orch = _bootstrap()
    rows = led.failed_items(args.stage or None)
    print(f"失败项: {len(rows)} 条")
    for row in rows[:args.limit]:
        print(f"  [{row['stable_key']}] stage={row['failed_stage'] or '?'} "
              f"retryable={bool(row['retryable'])} retries={row['retry_count']} "
              f"error={row['error_detail'] or ''}")
    led.close()


def cmd_permissions(args):
    """查看某条目最近一次成功采集的来源权限快照。"""
    _s, _tx, led, _orch = _bootstrap()
    rows = led.source_permissions(args.key)
    print(f"来源权限: {len(rows)} 条")
    for row in rows:
        print(f"  {row['principal']} -> {row['role']}（采集于 {row['collected_at']}）")
    led.close()


def cmd_governance(args):
    """输出复审、归档、待整理与无责任人治理队列。"""
    s, tx, led, _orch = _bootstrap()
    queues = led.governance_items(triage_path=tx.triage_path)
    for name, rows in queues.items():
        print(f"{name}: {len(rows)}")
        for row in rows[:args.limit]:
            due = row["review_due_at"] if name == "review_due" else row["retention_due_at"]
            print(f"  [{row['stable_key']}] {row['original_name']} {due or ''}")
    print("health:", led.governance_health(triage_path=tx.triage_path))
    led.close()


def cmd_insights(args):
    _s, _tx, led, orch = _bootstrap()
    print("分类反馈:")
    for row in led.classification_feedback_summary():
        print(f"  {row['confirmed_category']}: {row['confirmed_count']} 确认，命中 {row['matches'] or 0}")
    print("待整理主题信号:")
    for term, count in orch.triage_topic_signals(args.limit):
        print(f"  {term}: {count}")
    calibration = orch.classification_calibration()
    print("分类阈值建议:", calibration["recommended_threshold"],
          f"(反馈 {calibration['feedback_samples']} 条，"
          f"可自动采用={calibration['auto_applicable']})")
    print("待整理文档簇:")
    for cluster in orch.triage_topic_clusters(limit=args.limit):
        names = "、".join(item["name"] for item in cluster["items"][:5])
        print(f"  簇{cluster['cluster']} ({cluster['size']}): "
              f"{'/'.join(cluster['terms'][:5])} — {names}")
    led.close()


def cmd_complete_review(args):
    """完成一次内容复审并按 taxonomy 周期计算下次到期日。"""
    _s, _tx, led, orch = _bootstrap()
    try:
        next_due = orch.complete_review(args.key, actor=args.actor)
    except KeyError:
        raise SystemExit(f"条目不存在：{args.key}")
    finally:
        led.close()
    print(f"复审已完成：{args.key}，下次复审 {next_due}")


def cmd_archive(args):
    """将保留期到期的 Drive 文件或 Wiki 节点移入 taxonomy 的归档目录。"""
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper
    from kb_migrator.feishu.auth import load_user_token

    s, tx, led, orch = _bootstrap()
    targets = FeishuBootstrapper(None, tx, s.feishu_targets_file).load_targets()
    archive_folder = (targets.get("folder_map") or {}).get(tx.archive_path, "")
    wiki_archive = (targets.get("wiki_node_map") or {}).get(tx.archive_path, "")
    wiki_space = targets.get("space_id") or ""
    if args.commit and not (archive_folder or (wiki_space and wiki_archive)):
        raise SystemExit("缺少 99 归档目标：请先 bootstrap 云空间或 Wiki 目录树。")
    user_token = ""
    if args.commit and wiki_space and wiki_archive:
        user_token = (s.feishu_user_access_token
                      or load_user_token(s.feishu_user_token_file))
        if not user_token:
            raise SystemExit("归档 Wiki 节点需要 user_access_token，请先完成 OAuth。")
    writer = _feishu_writer(s) if args.commit else None
    if not args.commit:
        print("[dry-run] 仅预览归档候选；加 --commit 才移动 Drive/Wiki 条目。")
    print("archive:", orch.archive_due_items(writer, archive_folder, commit=args.commit,
                                               reason=args.reason,
                                               wiki_space_id=wiki_space,
                                               wiki_archive_node=wiki_archive,
                                               user_token=user_token))
    led.close()


def cmd_confirm(args):
    _s, _tx, led, orch = _bootstrap()
    orch.confirm(args.key, args.category, args.name)
    print(f"已确认 {args.key} -> {args.category}")
    led.close()


def _feishu_writer(s):
    from kb_migrator.feishu.client import FeishuClient
    from kb_migrator.feishu.writer import FeishuWriter

    if not (s.feishu_app_id and s.feishu_app_secret):
        raise SystemExit("缺少 FEISHU_APP_ID / FEISHU_APP_SECRET，请在 .env 配置飞书应用凭证。")
    return FeishuWriter(FeishuClient(s.feishu_app_id, s.feishu_app_secret))


def cmd_bootstrap(args):
    """阶段1：在飞书建目标结构并回填 分类->token 映射。"""
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper

    s, tx, led, _orch = _bootstrap()
    boot = FeishuBootstrapper(_feishu_writer(s), tx, s.feishu_targets_file)
    if args.wiki:
        from kb_migrator.feishu.auth import load_user_token

        user_token = (args.user_token or s.feishu_user_access_token
                      or load_user_token(s.feishu_user_token_file))
        if not user_token:
            raise SystemExit(
                "建 Wiki 空间需 user_access_token：先启动 Web 控制台并访问 "
                "/feishu/oauth/login 完成授权（自动落盘），或用 --user-token <token>、"
                "在 .env 设 FEISHU_USER_ACCESS_TOKEN。")
        t = boot.bootstrap_wiki_space(user_token, space_name=args.name)
    else:
        t = boot.bootstrap_drive_tree(root_name=args.name)
    print("bootstrap 完成，目标映射已持久化到:", s.feishu_targets_file)
    print(boot.summary(t))
    led.close()


def cmd_targets(args):
    """查看当前已回填的 分类->token 映射。"""
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper

    s, tx, led, _orch = _bootstrap()
    boot = FeishuBootstrapper(None, tx, s.feishu_targets_file)   # 只读，无需 writer
    print(boot.summary())
    led.close()


def cmd_load(args):
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper
    from kb_migrator.structure import StructureService

    s, tx, led, orch = _bootstrap()
    # 读取阶段1 bootstrap 产出的 folder_map（分类 -> 云空间文件夹 token）
    boot = FeishuBootstrapper(None, tx, s.feishu_targets_file)   # 只读，无需 writer
    targets = boot.load_targets()
    folder_map = targets.get("folder_map") or {}
    structures = StructureService(led, tx, s.feishu_targets_file)
    active = structures.active_version()
    structure_version_id = active["id"] if active and active["mode"] == "drive" else ""
    target_node_map = {}
    if structure_version_id:
        structure_folders, target_node_map = structures.routing_map(
            structure_version_id, mode="drive"
        )
        if structure_folders:
            folder_map = structure_folders
    writer = None
    if args.commit:
        if not folder_map:
            raise SystemExit(
                "folder_map 为空：请先运行 `python cli.py bootstrap` 建目录树并回填映射。")
        writer = _feishu_writer(s)
    else:
        print(f"[dry-run] 默认预览，不真实上传（加 --commit 才写飞书）。"
              f"目标形态={targets.get('mode') or '未初始化'} folder_map 分类数={len(folder_map)}")
    identity_map = load_identity_map(s.identity_map_file)
    print("load:", orch.load_pass(
        writer, folder_map, identity_map=identity_map,
        structure_version_id=structure_version_id,
        target_node_map=target_node_map,
        target_resolver=(
            lambda row: structures.resolve_item_target(
                row, structure_version_id, mode="drive"
            )
        ) if structure_version_id else None,
    ))
    led.close()


def cmd_retry_failed(args):
    """重试写飞书失败的条目（error_detail 以 'load: ' 开头的 FAILED）。"""
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper
    from kb_migrator.structure import StructureService

    s, tx, led, orch = _bootstrap()
    targets = {
        "fetch": Stage.DISCOVERED,
        "extract": Stage.DISCOVERED,
        "dedup": Stage.EXTRACTED,
        "classify": Stage.DEDUPED,
        "archive": Stage.LOADED,
        "wiki": Stage.LOADED,
    }
    if args.stage in targets:
        n = led.requeue_failures(args.stage, targets[args.stage])
        print(f"已重排 {n} 条 {args.stage} 失败项 -> {targets[args.stage].value}。")
        led.close()
        return
    boot = FeishuBootstrapper(None, tx, s.feishu_targets_file)   # 只读，无需 writer
    targets = boot.load_targets()
    folder_map = targets.get("folder_map") or {}
    structures = StructureService(led, tx, s.feishu_targets_file)
    active = structures.active_version()
    structure_version_id = active["id"] if active and active["mode"] == "drive" else ""
    target_node_map = {}
    if structure_version_id:
        structure_folders, target_node_map = structures.routing_map(
            structure_version_id, mode="drive"
        )
        if structure_folders:
            folder_map = structure_folders
    writer = None
    if args.commit:
        if not folder_map:
            raise SystemExit(
                "folder_map 为空：请先运行 `python cli.py bootstrap` 建目录树并回填映射。")
        writer = _feishu_writer(s)
    else:
        print(f"[dry-run] 默认预览，仅重排失败项回 CONFIRMED、不真实写入（加 --commit 才写飞书）。"
              f"folder_map 分类数={len(folder_map)}")
    identity_map = load_identity_map(s.identity_map_file)
    print("retry-failed:", orch.retry_failed_loads(
        writer, folder_map, identity_map=identity_map,
        structure_version_id=structure_version_id,
        target_node_map=target_node_map,
        target_resolver=(
            lambda row: structures.resolve_item_target(
                row, structure_version_id, mode="drive"
            )
        ) if structure_version_id else None,
    ))
    led.close()


def cmd_push_to_wiki(args):
    """阶段5：把已上传云空间文件挂进 Wiki 分类节点（需先 bootstrap --wiki）。"""
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper
    from kb_migrator.structure import StructureService

    s, tx, led, orch = _bootstrap()
    boot = FeishuBootstrapper(None, tx, s.feishu_targets_file)   # 只读，无需 writer
    targets = boot.load_targets()
    node_map = targets.get("wiki_node_map") or {}
    structures = StructureService(led, tx, s.feishu_targets_file)
    active = structures.active_version()
    structure_version_id = active["id"] if active and active["mode"] == "wiki" else ""
    if structure_version_id:
        structure_nodes, _ = structures.routing_map(
            structure_version_id, mode="wiki"
        )
        if structure_nodes:
            node_map = structure_nodes
    writer = None
    user_token = ""
    if args.commit:
        from kb_migrator.feishu.auth import load_user_token

        if not (targets.get("space_id") and node_map):
            raise SystemExit(
                "缺少 Wiki 空间/节点映射：请先运行 `python cli.py bootstrap --wiki` "
                "（需先完成飞书 OAuth 拿 user_access_token）。")
        user_token = (s.feishu_user_access_token
                      or load_user_token(s.feishu_user_token_file))
        if not user_token:
            raise SystemExit(
                "挂入用户所属 Wiki 空间需 user_access_token：先启动控制台访问 "
                "/feishu/oauth/login 完成授权（自动落盘）。")
        writer = _feishu_writer(s)
    else:
        print(f"[dry-run] 默认预览，不真实挂入 Wiki（加 --commit 才写）。"
              f"space_id={targets.get('space_id') or '未初始化'} "
              f"wiki 节点分类数={len(node_map)}")
    print("push-to-wiki:", orch.move_loaded_to_wiki(
        writer, targets, user_token=user_token,
        identity_map=load_identity_map(s.identity_map_file),
        structure_version_id=structure_version_id,
        target_resolver=(
            lambda row: structures.resolve_item_target(
                row, structure_version_id, mode="wiki"
            )
        ) if structure_version_id else None,
    ))
    led.close()


def main(argv=None):
    p = argparse.ArgumentParser(prog="kb-migrator", description="组织知识飞书迁移与治理工具")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("scan-local", help="盘点+抽取本地文件夹")
    sp.add_argument("root")
    sp.set_defaults(func=cmd_scan_local)

    sp = sub.add_parser("scan-sharepoint", help="盘点+抽取 SharePoint(MS Graph)")
    sp.add_argument("--site", default="", help="站点收窄关键字，缺省用 MS_SITE_FILTER 或全部根站点")
    sp.set_defaults(func=cmd_scan_sharepoint)

    sp = sub.add_parser("scan-wedrive", help="盘点+抽取 企业微信微盘")
    sp.add_argument("space_ids", help="微盘空间 ID（逗号分隔，应用须为其成员）")
    sp.set_defaults(func=cmd_scan_wedrive)

    sp = sub.add_parser("migrate-chat", help="群聊会话存档迁移(需存档 SDK + RSA 私钥)")
    sp.add_argument("chat_id", help="群聊 ID（wecom_chat_id）")
    sp.add_argument("--name", default="", help="群名（回写台账，便于人工核对）")
    sp.add_argument("--limit", type=int, default=1000, help="每批拉取消息数上限")
    sp.set_defaults(func=cmd_migrate_chat)

    sp = sub.add_parser("dedup", help="近似去重")
    sp.add_argument("--threshold", type=float, default=0.75)
    sp.set_defaults(func=cmd_dedup)

    sp = sub.add_parser("semantic", help="语义去重(第三层，标疑似重复进人工队列；需可选重依赖)")
    sp.add_argument("--threshold", type=float, default=0.90, help="余弦相似度阈值")
    sp.set_defaults(func=cmd_semantic)

    sp = sub.add_parser("govern-chat",
                        help="群聊治理：成员→协作者映射 + 群名打标（默认 dry-run，--commit 才真写）")
    sp.add_argument("chat_id", help="群聊 ID（wecom_chat_id）")
    sp.add_argument("--url", default="", help="飞书入口链接（打标通知/人工清单用）")
    sp.add_argument("--commit", action="store_true", help="真实写入（不加则仅预览）")
    sp.add_argument("--dry-run", action="store_true", help="（默认行为）仅预览，不写入")
    sp.set_defaults(func=cmd_govern_chat)

    sp = sub.add_parser("classify", help="AI 分类")
    sp.set_defaults(func=cmd_classify)

    sp = sub.add_parser("stats", help="台账统计")
    sp.set_defaults(func=cmd_stats)

    sp = sub.add_parser("runs", help="查看最近管线执行批次与统计")
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_runs)

    sp = sub.add_parser("source-changes", help="查看待处理的源端变更审计队列")
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_source_changes)

    sp = sub.add_parser("apply-source-changes", help="将待处理来源变更派生为新迁移版本")
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_apply_source_changes)

    sp = sub.add_parser("missing-sources", help="查看本地源端已删除、待人工处置的条目")
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_missing_sources)

    sp = sub.add_parser("resolve-missing", help="保留或归档源端已删除条目")
    sp.add_argument("key")
    sp.add_argument("--action", choices=["keep", "archive"], required=True)
    sp.add_argument("--commit", action="store_true")
    sp.set_defaults(func=cmd_resolve_missing)

    sp = sub.add_parser("finalize-source-change", help="新版本写入后归档旧版本")
    sp.add_argument("change_id", type=int)
    sp.add_argument("--commit", action="store_true")
    sp.set_defaults(func=cmd_finalize_source_change)

    sp = sub.add_parser("review", help="人工确认队列")
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_review)

    sp = sub.add_parser("failures", help="查看失败队列及可重试性")
    sp.add_argument(
        "--stage",
        choices=["fetch", "extract", "dedup", "classify", "load", "wiki", "archive"],
        default="",
    )
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_failures)

    sp = sub.add_parser("permissions", help="查看条目的来源权限快照")
    sp.add_argument("key", help="stable_key，例如 sharepoint:drive/item")
    sp.set_defaults(func=cmd_permissions)

    sp = sub.add_parser("governance", help="查看复审/归档/待整理/责任人治理队列")
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_governance)

    sp = sub.add_parser("complete-review", help="记录复审完成并顺延下次复审日期")
    sp.add_argument("key")
    sp.add_argument("--actor", default="", help="复审人标识（写审计事件）")
    sp.set_defaults(func=cmd_complete_review)

    sp = sub.add_parser("insights", help="查看分类反馈与待整理主题信号")
    sp.add_argument("--limit", type=int, default=20)
    sp.set_defaults(func=cmd_insights)

    sp = sub.add_parser("archive", help="归档保留期到期的云空间文件（默认 dry-run）")
    sp.add_argument("--commit", action="store_true", help="真实移动至 99 归档")
    sp.add_argument("--reason", default="retention_due", help="归档原因审计文本")
    sp.set_defaults(func=cmd_archive)

    sp = sub.add_parser("confirm", help="人工确认归类")
    sp.add_argument("key")
    sp.add_argument("category")
    sp.add_argument("--name", default=None)
    sp.set_defaults(func=cmd_confirm)

    sp = sub.add_parser("bootstrap", help="阶段1：在飞书建目录树/知识空间并回填映射")
    sp.add_argument("--wiki", action="store_true",
                    help="建 Wiki 知识空间(需 user_access_token)；缺省建云空间文件夹树")
    sp.add_argument("--name", default="", help="根文件夹/空间名，缺省用 taxonomy.space_name")
    sp.add_argument("--user-token", default="", help="user_access_token（--wiki 时必需）")
    sp.set_defaults(func=cmd_bootstrap)

    sp = sub.add_parser("targets", help="查看已回填的 分类->token 映射")
    sp.set_defaults(func=cmd_targets)

    sp = sub.add_parser("load", help="写飞书（默认 dry-run 预览，--commit 才真实上传）")
    sp.add_argument("--commit", action="store_true", help="真实写飞书（不加则仅预览）")
    sp.add_argument("--dry-run", action="store_true", help="（默认行为）仅预览，不写入")
    sp.set_defaults(func=cmd_load)

    sp = sub.add_parser("retry-failed", help="重试写飞书失败的条目（默认 dry-run，--commit 才真实写）")
    sp.add_argument(
        "--stage",
        choices=["load", "fetch", "extract", "dedup", "classify", "archive", "wiki"],
        default="load",
                    help="失败来源；非 load 阶段仅重排到对应处理阶段")
    sp.add_argument("--commit", action="store_true", help="真实重试写飞书（不加则仅预览）")
    sp.add_argument("--dry-run", action="store_true", help="（默认行为）仅预览，不写入")
    sp.set_defaults(func=cmd_retry_failed)

    sp = sub.add_parser("push-to-wiki",
                        help="把已上传云文件挂进 Wiki 节点（默认 dry-run，--commit 才真写；需先 bootstrap --wiki）")
    sp.add_argument("--commit", action="store_true", help="真实挂入 Wiki（不加则仅预览）")
    sp.add_argument("--dry-run", action="store_true", help="（默认行为）仅预览，不写入")
    sp.set_defaults(func=cmd_push_to_wiki)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
