#!/usr/bin/env python
"""kb-migrator Web 控制台 一键起停管理器（跨平台）。

用法：
    python console.py start      # 启动（后台分离运行，自动开浏览器）
    python console.py stop       # 关闭
    python console.py restart    # 重启
    python console.py status     # 查看运行状态 + /api/status

设计要点：
    - 用 sys.executable -m uvicorn 启动，拿到的 PID 就是真正的服务进程
      （不像 shell 里的 python 启动器会派生子进程，PID 更可靠）。
    - 服务以“分离进程”跑：关掉当前终端窗口不影响它继续运行。
    - 绑定 127.0.0.1（本机单人运维工具，勿暴露公网）。
    - 关闭优先按 PID，psutil 可用时再按监听端口兜底扫一遍，杀干净不留孤儿。
"""
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

# Windows 控制台默认 GBK，会把中文显示成乱码；与 cli.py 一致强制 stdout/stderr 走 UTF-8。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass

HOST = "127.0.0.1"
PORT = 8000
APP = "kb_migrator.web.app:app"
URL = f"http://{HOST}:{PORT}"

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
PID_FILE = DATA / "webconsole.pid"
LOG_FILE = DATA / "uvicorn.log"

try:
    import psutil  # 可选：用于按端口兜底查杀，缺失不影响主流程
except Exception:  # pragma: no cover - psutil 未装时降级
    psutil = None


def _port_open() -> bool:
    """端口是否已被监听（服务是否在跑）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((HOST, PORT)) == 0


def _pid_alive(pid: int) -> bool:
    if psutil is not None:
        return psutil.pid_exists(pid)
    try:
        os.kill(pid, 0)  # 信号 0：仅探测存活，不真正发信号
        return True
    except (OSError, ProcessLookupError):
        return False


def _read_pid() -> int | None:
    try:
        return int(PID_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def _pids_on_port() -> set[int]:
    """psutil 可用时，返回监听 PORT 的进程 PID 集合（兜底用）。"""
    if psutil is None:
        return set()
    found: set[int] = set()
    try:
        for c in psutil.net_connections(kind="inet"):
            if c.laddr and c.laddr.port == PORT and c.status == psutil.CONN_LISTEN and c.pid:
                found.add(c.pid)
    except (psutil.AccessDenied, PermissionError):
        pass
    return found


def start(open_browser: bool = True) -> int:
    if _port_open():
        print(f"[kb-migrator] 控制台已在运行：{URL}")
        if open_browser:
            webbrowser.open(URL)
        return 0

    DATA.mkdir(parents=True, exist_ok=True)
    log = open(LOG_FILE, "a", encoding="utf-8")

    cmd = [sys.executable, "-m", "uvicorn", APP, "--host", HOST, "--port", str(PORT)]
    kwargs: dict = dict(cwd=str(ROOT), stdout=log, stderr=subprocess.STDOUT,
                        stdin=subprocess.DEVNULL)
    if os.name == "nt":
        # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP：脱离当前控制台，关窗口不受影响
        kwargs["creationflags"] = 0x00000008 | 0x00000200
    else:
        kwargs["start_new_session"] = True  # posix：脱离进程组

    proc = subprocess.Popen(cmd, **kwargs)
    PID_FILE.write_text(str(proc.pid))
    print(f"[kb-migrator] 启动中… PID {proc.pid}  日志 {LOG_FILE}")

    for _ in range(30):  # 等端口就绪，最多 ~15s
        time.sleep(0.5)
        if _port_open():
            print(f"[kb-migrator] 已就绪：{URL}")
            if open_browser:
                webbrowser.open(URL)
            return 0
        if proc.poll() is not None:
            print(f"[kb-migrator] 启动失败（进程已退出，退出码 {proc.returncode}）。查看日志：{LOG_FILE}")
            return 1
    print(f"[kb-migrator] 等待超时，请查看日志：{LOG_FILE}")
    return 1


def _kill(pid: int) -> bool:
    if psutil is not None:
        try:
            p = psutil.Process(pid)
            for child in p.children(recursive=True):
                child.terminate()
            p.terminate()
            _, alive = psutil.wait_procs([p], timeout=3)
            for p in alive:
                p.kill()
            return True
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            print(f"[kb-migrator] 无权限结束 PID {pid}（试试管理员）")
            return False
    try:
        os.kill(pid, 15)  # SIGTERM
        return True
    except (OSError, ProcessLookupError):
        return False


def stop() -> int:
    targets: set[int] = set()
    pid = _read_pid()
    if pid and _pid_alive(pid):
        targets.add(pid)
    targets |= _pids_on_port()  # psutil 兜底：按监听端口查漏

    stopped = [p for p in targets if _kill(p)]
    PID_FILE.unlink(missing_ok=True)

    if stopped:
        print("[kb-migrator] 已停止控制台进程：" + ", ".join(map(str, sorted(stopped))))
    elif _port_open():
        print(f"[kb-migrator] 端口 {PORT} 仍被占用但未能定位进程（可能是权限问题，试试管理员）。")
        return 1
    else:
        print(f"[kb-migrator] 未发现运行中的控制台（端口 {PORT} 无监听）。")
    return 0


def status() -> int:
    running = _port_open()
    pid = _read_pid()
    print(f"[kb-migrator] 运行中：{'是' if running else '否'}"
          f"  地址 {URL}  PID文件 {pid or '(无)'}")
    if running:
        try:
            import json
            import urllib.request
            with urllib.request.urlopen(f"{URL}/api/status", timeout=3) as r:
                data = json.load(r)
            print("  台账：total=%s  loaded=%s  feishu_ready=%s  claude_ready=%s  targets=%s"
                  % (data.get("total"), data.get("loaded"), data.get("feishu_ready"),
                     data.get("claude_ready"), data.get("targets_count")))
        except Exception as e:
            print(f"  （/api/status 读取失败：{e}）")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="kb-migrator Web 控制台 一键起停")
    ap.add_argument("action", choices=["start", "stop", "restart", "status"])
    ap.add_argument("--no-browser", action="store_true", help="启动时不自动开浏览器")
    args = ap.parse_args()

    if args.action == "start":
        return start(open_browser=not args.no_browser)
    if args.action == "stop":
        return stop()
    if args.action == "restart":
        stop()
        time.sleep(1.0)
        return start(open_browser=not args.no_browser)
    if args.action == "status":
        return status()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
