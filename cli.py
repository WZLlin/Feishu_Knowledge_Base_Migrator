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


def cmd_dedup(args):
    _s, _tx, led, orch = _bootstrap()
    print("dedup:", orch.dedup_pass(threshold=args.threshold))
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
    print(f"台账条目总数: {total}")
    for stage in Stage:
        if stage.value in counts:
            print(f"  {stage.value:20s} {counts[stage.value]}")
    led.close()


def cmd_review(args):
    _s, _tx, led, _orch = _bootstrap()
    rows = led.pending_review()
    print(f"人工确认队列: {len(rows)} 条")
    for r in rows[: args.limit]:
        print(f"  [{r['stable_key']}] 建议={r['category']} conf={r['confidence']} "
              f"名={r['original_name']} note={r['error_detail'] or ''}")
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

    s, tx, led, orch = _bootstrap()
    # 读取阶段1 bootstrap 产出的 folder_map（分类 -> 云空间文件夹 token）
    boot = FeishuBootstrapper(None, tx, s.feishu_targets_file)   # 只读，无需 writer
    targets = boot.load_targets()
    folder_map = targets.get("folder_map") or {}
    writer = None
    if args.commit:
        if not folder_map:
            raise SystemExit(
                "folder_map 为空：请先运行 `python cli.py bootstrap` 建目录树并回填映射。")
        writer = _feishu_writer(s)
    else:
        print(f"[dry-run] 默认预览，不真实上传（加 --commit 才写飞书）。"
              f"目标形态={targets.get('mode') or '未初始化'} folder_map 分类数={len(folder_map)}")
    print("load:", orch.load_pass(writer, folder_map))
    led.close()


def cmd_retry_failed(args):
    """重试写飞书失败的条目（error_detail 以 'load: ' 开头的 FAILED）。"""
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper

    s, tx, led, orch = _bootstrap()
    boot = FeishuBootstrapper(None, tx, s.feishu_targets_file)   # 只读，无需 writer
    targets = boot.load_targets()
    folder_map = targets.get("folder_map") or {}
    writer = None
    if args.commit:
        if not folder_map:
            raise SystemExit(
                "folder_map 为空：请先运行 `python cli.py bootstrap` 建目录树并回填映射。")
        writer = _feishu_writer(s)
    else:
        print(f"[dry-run] 默认预览，仅重排失败项回 CONFIRMED、不真实写入（加 --commit 才写飞书）。"
              f"folder_map 分类数={len(folder_map)}")
    print("retry-failed:", orch.retry_failed_loads(writer, folder_map))
    led.close()


def cmd_push_to_wiki(args):
    """阶段5：把已上传云空间文件挂进 Wiki 分类节点（需先 bootstrap --wiki）。"""
    from kb_migrator.feishu.bootstrap import FeishuBootstrapper

    s, tx, led, orch = _bootstrap()
    boot = FeishuBootstrapper(None, tx, s.feishu_targets_file)   # 只读，无需 writer
    targets = boot.load_targets()
    node_map = targets.get("wiki_node_map") or {}
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
    print("push-to-wiki:", orch.move_loaded_to_wiki(writer, targets, user_token=user_token))
    led.close()


def main(argv=None):
    p = argparse.ArgumentParser(prog="kb-migrator", description="组织知识飞书迁移与治理工具")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("scan-local", help="盘点+抽取本地文件夹")
    sp.add_argument("root")
    sp.set_defaults(func=cmd_scan_local)

    sp = sub.add_parser("dedup", help="近似去重")
    sp.add_argument("--threshold", type=float, default=0.75)
    sp.set_defaults(func=cmd_dedup)

    sp = sub.add_parser("classify", help="AI 分类")
    sp.set_defaults(func=cmd_classify)

    sp = sub.add_parser("stats", help="台账统计")
    sp.set_defaults(func=cmd_stats)

    sp = sub.add_parser("review", help="人工确认队列")
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_review)

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
