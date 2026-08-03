#!/usr/bin/env python3
"""
Model Gateway 更新服务 (monitor + auto-updater)
================================================
功能:
  1. Tailscale 内网穿透健康检查（启动更新前必须在线）
  2. 从 GitHub/Gitee 拉取最新代码
  3. 检测是否有新提交
  4. 有新代码 → 停止旧服务 → 同步 → 启动新服务
  5. 服务存活监控（健康检查 + 自动重启，作为更新失败的兜底）
  6. 更新过程中 Hermes 无法连接 → 通过内网穿透地址恢复

Hermes 调用流程:
  - 发起更新前，先请求 /inner-address 获取当前内网地址
  - 确认 Tailscale 在线后，触发 /update 启动更新
  - 更新期间 Hermes 断开，通过内网地址重新配置
"""

import time
import subprocess
import sys
import os
import signal
import logging
import socket
import json
import urllib.request
from datetime import datetime
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────────────────────
SERVICE_NAME = "model-gateway.service"
REPO_DIR = Path(__file__).parent
CHECK_URL = "http://127.0.0.1:8650/health"
CHECK_INTERVAL = 10          # 检测间隔（秒）
RESTART_COOLDOWN = 30        # 重启冷却期（秒）
FAIL_THRESHOLD = 3           # 连续失败次数阈值
REQUEST_TIMEOUT = 5          # HTTP 请求超时（秒）
TAILSCALE_CHECK_INTERVAL = 60 # Tailscale 状态检查间隔
GIT_PULL_INTERVAL = 300      # 代码拉取间隔（秒），5分钟

# 内网穿透地址（Tailscale 连接后自动获取）
TAILSCALE_IFACE = "tailscale0"
INNER_IP = None  # 运行时自动检测

# ── 日志配置 ──────────────────────────────────────────────────────────
LOG_FILE = REPO_DIR / "updater.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("updater")

# ── 状态变量 ──────────────────────────────────────────────────────────
consecutive_fails = 0
last_restart_time = 0
running = True
last_git_check = 0
is_updating = False
update_progress = ""

# ── 网页仪表盘 HTML ──────────────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>守护面板 · 模型网关</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:"PingFang SC","Microsoft YaHei",system-ui,sans-serif;background:#0d1420;color:#e8edf5;min-height:100vh;display:flex;align-items:center;justify-content:center;background-image:radial-gradient(900px 500px at 8% -10%,rgba(45,212,191,.06),transparent 60%),radial-gradient(rgba(255,255,255,.028) 1px,transparent 1px);background-size:auto,26px 26px;background-attachment:fixed}
.card{background:#161f2e;border:1px solid #243044;border-radius:12px;padding:32px;width:520px;max-width:calc(100vw - 32px);box-shadow:0 20px 60px rgba(0,0,0,.5);animation:rise .35s ease both}
@keyframes rise{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:none}}
.header{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid #243044}
.header .mark{font-family:ui-monospace,"SF Mono",Consolas,monospace;font-weight:700;font-size:13px;color:#f5a524;border:1px solid rgba(245,165,36,.4);padding:3px 8px;border-radius:5px;background:rgba(245,165,36,.12);letter-spacing:1px}
.header h1{font-size:18px;font-weight:700;letter-spacing:.5px}
.header h1 small{font-size:12px;color:#8b98ac;font-weight:400;margin-left:6px}
.clock{font-family:ui-monospace,"SF Mono",Consolas,monospace;font-size:12px;color:#8b98ac}
.row{display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid rgba(36,48,68,.5)}
.row:last-child{border-bottom:none}
.label{font-size:13px;color:#8b98ac}
.value{font-family:ui-monospace,"SF Mono",Consolas,monospace;font-size:13px;font-weight:500}
.ok{color:#2dd4bf}
.err{color:#f87171}
.neutral{color:#e8edf5}
.upd{color:#f5a524}
.actions{display:flex;gap:10px;margin-top:22px;flex-wrap:wrap}
.btn{flex:1;padding:11px 0;border-radius:8px;border:1px solid transparent;cursor:pointer;font-size:14px;font-weight:600;font-family:inherit;transition:all .16s ease;text-align:center;min-width:100px}
.btn:active{transform:scale(.96)}
.btn-amber{background:#f5a524;color:#1a1206}
.btn-amber:hover{background:#ffb63d;box-shadow:0 0 0 3px rgba(245,165,36,.2)}
.btn-green{background:#2dd4bf;color:#0d1420}
.btn-green:hover{background:#5eead4;box-shadow:0 0 0 3px rgba(45,212,191,.2)}
.btn-red{background:transparent;color:#f87171;border-color:rgba(248,113,113,.35)}
.btn-red:hover{background:rgba(248,113,113,.12)}
.btn-ghost{background:transparent;color:#8b98ac;border-color:#33445e}
.btn-ghost:hover{color:#e8edf5;border-color:#8b98ac}
.btn:disabled{opacity:.5;cursor:not-allowed;transform:none!important}
.progress{margin-top:14px;padding:10px 14px;background:rgba(245,165,36,.1);border:1px solid rgba(245,165,36,.3);border-radius:8px;font-size:13px;color:#f5a524;display:none;animation:rise .25s ease both}
.progress.show{display:block}
.toast{position:fixed;right:20px;top:20px;z-index:100;display:flex;flex-direction:column;gap:8px}
.toast-item{background:#1b2637;border:1px solid #33445e;border-left:3px solid #2dd4bf;color:#e8edf5;padding:10px 16px;border-radius:7px;font-size:13px;box-shadow:0 10px 30px rgba(0,0,0,.4);animation:slidein .25s cubic-bezier(.22,1,.36,1)}
.toast-item.err{border-left-color:#f87171}
@keyframes slidein{from{opacity:0;transform:translateX(30px)}}
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div><span class="mark">GW</span><h1>守护面板<small>updater &middot; 8651</small></h1></div>
    <span class="clock" id="clock">__TS__</span>
  </div>
  <div>
    <div class="row"><span class="label">服务状态</span><span class="value __SC__" id="svc-status">__SV__</span></div>
    <div class="row"><span class="label">API 健康</span><span class="value __AC__" id="api-status">__AV__</span></div>
    <div class="row"><span class="label">当前版本</span><span class="value neutral" id="cur-ver">__CU__</span></div>
    <div class="row"><span class="label">最新版本</span><span class="value upd" id="lat-ver">__LA__</span></div>
    <div class="row"><span class="label">更新状态</span><span class="value neutral" id="upd-text">__UT__</span></div>
    <div class="row"><span class="label">Tailscale</span><span class="value neutral" id="ts-ip">__TI__</span></div>
  </div>
  <div class="progress" id="progress">__UP__</div>
  <div class="actions">
    <button class="btn btn-amber" onclick="doAction('update')" id="btn-update">&#x1f504; 立即更新</button>
    <button class="btn btn-green" onclick="doAction('restart')" id="btn-restart">&#x1f501; 重启网关</button>
    <button class="btn btn-red" onclick="doAction('stop')" id="btn-stop">&#x23f9; 停止网关</button>
    <button class="btn btn-ghost" onclick="doAction('start')" id="btn-start">&#x25b6; 启动网关</button>
  </div>
</div>
<div class="toast" id="toast"></div>
<script>
var pollTimer = null;
function toast(msg,isErr){var d=document.createElement('div');d.className='toast-item'+(isErr?' err':'');d.textContent=msg;document.getElementById('toast').appendChild(d);setTimeout(function(){d.style.opacity='0';d.style.transition='opacity .3s';setTimeout(function(){d.remove()},300)},2600)}
function showProgress(t){var p=document.getElementById('progress');p.textContent=t;p.classList.add('show')}
function hideProgress(){document.getElementById('progress').classList.remove('show')}
function setBusy(b){document.querySelectorAll('.actions .btn').forEach(function(x){x.disabled=b});if(b)startPoll();else stopPoll()}
function startPoll(){if(pollTimer)return;pollTimer=setInterval(fetchStatus,2000)}
function stopPoll(){if(pollTimer){clearInterval(pollTimer);pollTimer=null}}
async function fetchStatus(){try{var r=await fetch('/status');if(!r.ok)return;var d=await r.json();document.getElementById('svc-status').textContent=d.service_active?'运行中':'已停止';document.getElementById('svc-status').className='value '+(d.service_active?'ok':'err');document.getElementById('api-status').textContent=d.api_healthy?'正常':'异常';document.getElementById('api-status').className='value '+(d.api_healthy?'ok':'err');document.getElementById('cur-ver').textContent=d.git.current;document.getElementById('lat-ver').textContent=d.git.latest;document.getElementById('upd-text').textContent=d.git.has_update?'&#x1f504; 有可用更新':'&#x2705; 已是最新';document.getElementById('clock').textContent=d.timestamp.slice(0,19).replace('T',' ');if(d.is_updating){showProgress(d.update_progress||'更新中...');setBusy(true)}else{hideProgress();setBusy(false)}}catch(e){}}
async function doAction(action){if(action==='stop'&&!confirm('确定要停止网关服务吗？'))return;setBusy(true);showProgress('正在执行 '+action+' ...');try{var r=await fetch('/action/'+action);var d=await r.json();if(d.ok){toast('&#x2705; '+action+' 成功');showProgress(action+' 执行成功，正在等待服务就绪...');setTimeout(fetchStatus,1500)}else{toast('&#x274c; '+action+' 失败',true);hideProgress();setBusy(false)}}catch(e){toast('&#x274c; 请求失败: '+e,true);hideProgress();setBusy(false)}}
</script>
</body>
</html>"""

def render_dashboard(svc_class, svc_text, api_class, api_text, current, latest, update_text, tailscale_ip, update_progress, timestamp):
    h = DASHBOARD_HTML
    for old, new in [
        ("__SC__", svc_class), ("__SV__", svc_text),
        ("__AC__", api_class), ("__AV__", api_text),
        ("__CU__", current), ("__LA__", latest),
        ("__UT__", update_text), ("__TI__", tailscale_ip),
        ("__UP__", update_progress or ""), ("__TS__", timestamp),
    ]:
        h = h.replace(old, new)
    return h

# ── 内网穿透 ──────────────────────────────────────────────────────────
def get_tailscale_ip() -> str | None:
    """获取 Tailscale 内网 IP"""
    global INNER_IP
    try:
        import subprocess
        result = subprocess.run(
            ["tailscale", "ip", "-4"],
            capture_output=True, text=True, timeout=5
        )
        ip = result.stdout.strip()
        if ip and not ip.startswith("Error"):
            INNER_IP = ip
            return ip
    except Exception:
        pass
    return None


def is_tailscale_online() -> bool:
    """检查 Tailscale 是否在线"""
    try:
        result = subprocess.run(
            ["tailscale", "status", "--self", "--json"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            return False
        data = json.loads(result.stdout)
        self_info = data.get("Self", {})
        return self_info.get("Online", False)
    except Exception:
        return False


def get_inner_address() -> dict:
    """获取完整的内网地址信息（供 Hermes 调用）"""
    ip = get_tailscale_ip()
    online = is_tailscale_online()
    return {
        "online": online,
        "ip": ip,
        "dns": f"ubunnt22-04.tail4ce105.ts.net" if online else None,
        "address": f"{ip}:8650" if ip else None,
        "lan_address": f"192.168.68.223:8650",
        "timestamp": datetime.now().isoformat(),
    }


# ── 服务存活检测 ──────────────────────────────────────────────────────
def is_port_open() -> bool:
    """检测 8650 端口是否有进程在监听"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(("127.0.0.1", 8650))
        sock.close()
        return result == 0
    except Exception:
        return False


def is_service_active() -> bool:
    """通过 systemctl 检查服务状态"""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() == "active"
    except Exception:
        return False


def check_api_healthy() -> bool:
    """通过 HTTP 请求验证 API 是否响应"""
    try:
        req = urllib.request.Request(CHECK_URL)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read())
            return data.get("status") == "ok"
    except Exception:
        return False


# ── Git 更新 ──────────────────────────────────────────────────────────
def get_latest_commit(remote: str = "origin") -> str | None:
    """获取远程最新 commit hash（按提交日期选择最新分支）"""
    try:
        # 先检查 main 分支
        result = subprocess.run(
            ["git", "ls-remote", remote, "HEAD", "refs/heads/main", "refs/heads/master"],
            capture_output=True, text=True, timeout=30,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0 and result.stdout.strip():
            best_commit = None
            best_time = 0
            for line in result.stdout.strip().split("\n"):
                parts = line.strip().split()
                if len(parts) >= 2:
                    commit = parts[0]
                    ref = parts[1]
                    # 获取该 commit 的时间戳
                    time_result = subprocess.run(
                        ["git", "log", "-1", "--format=%ct", commit],
                        capture_output=True, text=True, timeout=5,
                        cwd=str(REPO_DIR),
                    )
                    if time_result.returncode == 0:
                        try:
                            ts = int(time_result.stdout.strip())
                            if ts > best_time:
                                best_time = ts
                                best_commit = commit
                        except ValueError:
                            pass
            return best_commit
    except Exception as e:
        log.error(f"获取远程 commit 失败: {e}")
    return None


def get_current_commit() -> str | None:
    """获取当前本地 commit hash"""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


def get_default_branch(remote: str = "origin") -> str:
    """获取远程仓库的默认分支名（main 或 master）"""
    try:
        result = subprocess.run(
            ["git", "remote", "show", remote],
            capture_output=True, text=True, timeout=15,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                line = line.strip()
                if line.startswith("HEAD branch:"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    # fallback: 检查 main/master 哪个存在
    for branch in ["main", "master"]:
        result = subprocess.run(
            ["git", "ls-remote", "--heads", remote, f"refs/heads/{branch}"],
            capture_output=True, text=True, timeout=10,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0 and result.stdout.strip():
            return branch
    return "main"  # 默认 fallback


def git_pull(remote: str = "origin", branch: str = "") -> tuple[bool, str, str]:
    """执行 git pull，返回 (成功, 输出信息, 实际使用的分支)"""
    if not branch:
        branch = get_default_branch(remote)
    try:
        # fetch
        result = subprocess.run(
            ["git", "fetch", remote, branch],
            capture_output=True, text=True, timeout=30,
            cwd=str(REPO_DIR),
        )
        if result.returncode != 0:
            # fallback 到另一个分支名
            alt_branch = "master" if branch == "main" else "main"
            result = subprocess.run(
                ["git", "fetch", remote, alt_branch],
                capture_output=True, text=True, timeout=30,
                cwd=str(REPO_DIR),
            )
            if result.returncode != 0:
                return False, f"fetch failed: {result.stderr}", branch
            branch = alt_branch

        # pull
        result = subprocess.run(
            ["git", "pull", remote, branch],
            capture_output=True, text=True, timeout=60,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0:
            return True, result.stdout.strip(), branch
        else:
            return False, result.stderr.strip(), branch
    except Exception as e:
        return False, str(e), branch


def has_updates_available() -> tuple[bool, str, str]:
    """检查是否有可用更新（基于提交日期选择最新分支）"""
    current = get_current_commit()
    latest_github = get_latest_commit("github")
    latest_gitee = get_latest_commit("origin")

    if not current:
        return False, "unknown", "unknown"

    # 任一远程有新代码即视为有更新
    if latest_github and latest_github != current:
        return True, current[:8], latest_github[:8]
    if latest_gitee and latest_gitee != current:
        return True, current[:8], latest_gitee[:8]

    return False, current[:8], current[:8]


def pick_latest_remote() -> tuple[str, str]:
    """选择提交日期最新的远程，优先选择 github"""
    # 先 fetch 两个远程，确保本地有最新 refs
    for r in ["github", "origin"]:
        try:
            subprocess.run(
                ["git", "fetch", r, "--quiet"],
                capture_output=True, text=True, timeout=10,
                cwd=str(REPO_DIR),
            )
        except Exception:
            pass

    refs = {
        "github": "github/main",
        "origin": "origin/master",
    }

    # 获取每个远程的最新 commit 和时间戳
    candidates = []
    for remote, ref in refs.items():
        try:
            result = subprocess.run(
                ["git", "rev-parse", ref],
                capture_output=True, text=True, timeout=5,
                cwd=str(REPO_DIR),
            )
            if result.returncode != 0:
                continue
            commit = result.stdout.strip()
            if not commit:
                continue

            time_result = subprocess.run(
                ["git", "log", "-1", "--format=%ct", commit],
                capture_output=True, text=True, timeout=5,
                cwd=str(REPO_DIR),
            )
            if time_result.returncode != 0:
                continue
            ts = int(time_result.stdout.strip())
            candidates.append((remote, commit, ts))
        except Exception:
            continue

    if not candidates:
        return "", ""
    if len(candidates) == 1:
        return candidates[0][0], candidates[0][1]

    # 排序，按时间戳降序（最新的在前）
    candidates.sort(key=lambda x: x[2], reverse=True)
    best = candidates[0]
    second = candidates[1]

    # 时间差在 1 小时内，优先选择 github
    if abs(best[2] - second[2]) < 3600:
        # 从中选 github
        for c in candidates:
            if c[0] == "github":
                return c[0], c[1]

    # 时间差超过 1 小时，选最新的
    return best[0], best[1]


# ── 服务管理 ──────────────────────────────────────────────────────────
def stop_service() -> bool:
    """停止 model-gateway 服务"""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "stop", SERVICE_NAME],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            log.info("✅ 服务已停止")
            return True
        else:
            log.error(f"❌ 停止服务失败: {result.stderr}")
            return False
    except Exception as e:
        log.error(f"❌ 停止服务异常: {e}")
        return False


def start_service() -> bool:
    """启动 model-gateway 服务"""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "start", SERVICE_NAME],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            log.info("✅ 服务启动命令已发出")
            # 等待启动
            time.sleep(5)
            if is_service_active() and check_api_healthy():
                log.info("✅ 服务启动成功")
                return True
            else:
                log.error("❌ 服务启动后未就绪")
                return False
        else:
            log.error(f"❌ 启动服务失败: {result.stderr}")
            return False
    except Exception as e:
        log.error(f"❌ 启动服务异常: {e}")
        return False


def restart_service() -> bool:
    """重启 model-gateway 服务"""
    global last_restart_time
    now = time.time()
    if now - last_restart_time < RESTART_COOLDOWN:
        log.warning(f"冷却期内（还需 {RESTART_COOLDOWN - int(now - last_restart_time)} 秒）")
        return False

    log.warning(">>> 正在重启 model-gateway.service ...")
    try:
        subprocess.run(
            ["systemctl", "--user", "restart", SERVICE_NAME],
            capture_output=True, text=True, timeout=30,
        )
        last_restart_time = time.time()
        time.sleep(5)
        if is_service_active() and check_api_healthy():
            log.info("✅ 服务重启成功")
            return True
        else:
            log.error("❌ 服务重启后仍未就绪")
            return False
    except Exception as e:
        log.error(f"❌ 重启命令执行失败: {e}")
        return False


# ── 核心更新流程 ──────────────────────────────────────────────────────
def perform_update() -> bool:
    """执行完整更新流程"""
    global is_updating, update_progress

    is_updating = True
    update_progress = "开始更新..."

    try:
        # 1. 检查 Tailscale
        update_progress = "检查内网穿透状态..."
        if not is_tailscale_online():
            log.error("❌ Tailscale 离线，无法开始更新")
            return False

        inner = get_inner_address()
        log.info(f"🌐 内网地址: {inner['address']} / {inner['dns']}")

        # 2. 检查是否有更新
        update_progress = "检查代码更新..."
        has_update, current, latest = has_updates_available()
        if not has_update:
            log.info("✅ 当前已是最新版本")
            return True

        log.info(f"📦 发现新版本: {current} → {latest}")

        # 3. 选择提交日期最新的远程
        update_progress = "选择最新代码源..."
        remote, commit = pick_latest_remote()
        if not remote:
            log.error("❌ 无法获取远程代码")
            return False
        log.info(f"📥 选择远程: {remote} (commit: {commit[:8]})")

        # 4. 停止服务
        update_progress = "停止旧服务..."
        if not stop_service():
            log.error("❌ 无法停止服务，中止更新")
            return False

        # 等待端口释放
        time.sleep(2)

        # 5. 拉取代码
        update_progress = "拉取最新代码..."
        success, output, branch = git_pull(remote)
        if not success:
            log.error(f"❌ Git pull 失败: {output}")
            # 尝试备用远程
            alt_remote = "github" if remote == "origin" else "origin"
            log.info(f"尝试备用远程 {alt_remote}...")
            success, output, branch = git_pull(alt_remote)
            if not success:
                log.error(f"❌ 备用也失败: {output}")
                update_progress = "代码拉取失败，尝试恢复旧版本..."
                start_service()
                return False

        log.info(f"✅ 代码更新成功 [{remote}/{branch}]: {output}")

        # 6. 更新依赖
        update_progress = "更新 Python 依赖..."
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q"],
                capture_output=True, text=True, timeout=120,
                cwd=str(REPO_DIR),
            )
        except Exception as e:
            log.warning(f"依赖更新警告: {e}")

        # 7. 启动新服务
        update_progress = "启动新服务..."
        if start_service():
            log.info("✅ 更新完成！新服务已启动")
            # 验证 API
            time.sleep(2)
            if check_api_healthy():
                log.info("✅ API 验证通过")
                return True
            else:
                log.error("⚠️ API 验证失败，但服务正在运行")
                return True  # 仍然算成功，服务可能还在初始化
        else:
            log.error("❌ 新服务启动失败")
            return False

    finally:
        is_updating = False
        update_progress = ""


# ── HTTP API（供 Hermes 调用）─────────────────────────────────────────
def handle_api_request(conn):
    """处理 HTTP API 请求"""
    import threading as _t
    try:
        request = conn.recv(4096).decode()
        lines = request.split("\r\n")
        if not lines:
            return
        request_line = lines[0]
        parts = request_line.split()
        if len(parts) < 2:
            return
        method, path = parts[0], parts[1]

        # 路由
        response_body = ""
        status_code = 200
        content_type = "application/json"

        if path == "/status" or path == "/":
            # 完整状态报告（带超时保护）
            inner = {}
            git_status = {"current": "unknown", "latest": "unknown", "has_update": False}
            svc_active = api_healthy = False

            def _get_status():
                nonlocal inner, svc_active, api_healthy, git_status
                inner = get_inner_address()
                svc_active = is_service_active()
                api_healthy = check_api_healthy()
                _, cur, lat = has_updates_available()
                git_status = {
                    "current": cur,
                    "latest": lat,
                    "has_update": cur != lat if cur != "unknown" else False,
                }

            t = _t.Thread(target=_get_status, daemon=True)
            t.start()
            t.join(timeout=15)

            response_body = json.dumps({
                "service": "updater",
                "inner_address": inner,
                "service_active": svc_active,
                "api_healthy": api_healthy,
                "is_updating": is_updating,
                "update_progress": update_progress,
                "git": git_status,
                "timestamp": datetime.now().isoformat(),
            }, ensure_ascii=False, indent=2)

        elif path == "/inner-address":
            response_body = json.dumps(get_inner_address(), ensure_ascii=False, indent=2)

        elif path == "/update":
            # 触发更新
            if is_updating:
                response_body = json.dumps({"status": "already_updating", "progress": update_progress})
            else:
                # 在后台执行更新
                import threading
                thread = threading.Thread(target=perform_update, daemon=True)
                thread.start()
                response_body = json.dumps({"status": "update_started", "inner_address": get_inner_address()})

        elif path == "/check":
            # 仅检查是否有更新（带超时保护）
            has_update = False
            current = "unknown"
            latest = "unknown"
            def _check():
                nonlocal has_update, current, latest
                h, c, l = has_updates_available()
                has_update, current, latest = h, c, l
            t = _t.Thread(target=_check, daemon=True)
            t.start()
            t.join(timeout=15)
            response_body = json.dumps({
                "has_update": has_update,
                "current": current,
                "latest": latest,
            })

        elif path == "/health":
            response_body = json.dumps({"status": "ok", "updater": True})

        elif path == "/action/stop":
            ok = stop_service()
            response_body = json.dumps({"ok": ok, "action": "stop"})

        elif path == "/action/update":
            if is_updating:
                response_body = json.dumps({"ok": False, "action": "update", "reason": "already_updating", "progress": update_progress})
            else:
                import threading
                thread = threading.Thread(target=perform_update, daemon=True)
                thread.start()
                response_body = json.dumps({"ok": True, "action": "update", "status": "update_started"})

        elif path == "/action/start":
            ok = start_service()
            response_body = json.dumps({"ok": ok, "action": "start"})

        elif path == "/action/restart":
            ok = restart_service()
            response_body = json.dumps({"ok": ok, "action": "restart"})

        elif path == "/dashboard":
            status_code = 200
            content_type = "text/html; charset=utf-8"
            inner = get_inner_address()
            _, current, latest = has_updates_available()
            svc_active = is_service_active()
            api_healthy = check_api_healthy()
            has_update = current != latest if current != "unknown" else False
            response_body = render_dashboard(
                svc_class="ok" if svc_active else "err",
                svc_text="运行中" if svc_active else "已停止",
                api_class="ok" if api_healthy else "err",
                api_text="正常" if api_healthy else "异常",
                current=current,
                latest=latest,
                update_text="🔄 有可用更新" if has_update else "✅ 已是最新",
                tailscale_ip=inner.get("ip", "—"),
                update_progress=update_progress or "",
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

        else:
            status_code = 404
            response_body = json.dumps({"error": "not_found", "path": path})

        # 发送响应
        response = f"HTTP/1.1 {status_code} OK\r\nContent-Type: {content_type}\r\nContent-Length: {len(response_body.encode())}\r\nConnection: close\r\n\r\n{response_body}"
        conn.sendall(response.encode())

    except Exception as e:
        log.error(f"API 请求处理异常: {e}")
    finally:
        conn.close()


def start_api_server():
    """启动本地 API 服务器（端口 8651）"""
    import threading

    def api_loop():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", 8651))
        server.listen(5)
        server.settimeout(1.0)
        log.info("📡 API 服务器启动 (127.0.0.1:8651)")

        while running:
            try:
                conn, _ = server.accept()
                # 每个连接一个线程，避免 git SSH 阻塞 API
                import threading
                threading.Thread(target=handle_api_request, args=(conn,), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                log.error(f"API 服务器错误: {e}")

        server.close()

    thread = threading.Thread(target=api_loop, daemon=True)
    thread.start()
    return thread


# ── 主循环 ─────────────────────────────────────────────────────────────
def health_check() -> dict:
    """执行完整健康检查"""
    port_ok = is_port_open()
    service_ok = is_service_active()
    api_ok = check_api_healthy() if port_ok else False
    tailscale_ok = is_tailscale_online()
    return {
        "port": port_ok,
        "service": service_ok,
        "api": api_ok,
        "tailscale": tailscale_ok,
        "healthy": port_ok and service_ok and api_ok,
    }


def main():
    global consecutive_fails, running

    log.info("=" * 60)
    log.info("Model Gateway 更新服务启动")
    log.info(f"检测间隔: {CHECK_INTERVAL}s | 失败阈值: {FAIL_THRESHOLD} | 冷却期: {RESTART_COOLDOWN}s")
    log.info(f"Git检查间隔: {GIT_PULL_INTERVAL}s | 监控目标: {CHECK_URL}")
    log.info("=" * 60)

    # 启动 API 服务器
    api_thread = start_api_server()

    # 注册信号处理
    def handle_signal(signum, frame):
        global running
        log.info(f"收到信号 {signum}，退出...")
        running = False

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # 初始 Tailscale 检查
    inner = get_inner_address()
    if inner["online"]:
        log.info(f"🌐 Tailscale 在线: {inner['address']}")
    else:
        log.warning("⚠️ Tailscale 离线 — 更新功能将不可用，仅执行监控")

    last_git_pull_check = 0

    while running:
        status = health_check()

        if is_updating:
            # 更新模式：等待更新完成，不执行监控重启
            log.info(f"🔄 更新中... {update_progress}")
            time.sleep(CHECK_INTERVAL)
            continue

        if status["healthy"]:
            if consecutive_fails > 0:
                log.info(f"服务恢复正常（之前连续失败 {consecutive_fails} 次）")
            consecutive_fails = 0

            # 定期 Git 检查（自动更新模式）
            now = time.time()
            if now - last_git_pull_check > GIT_PULL_INTERVAL:
                last_git_pull_check = now
                has_update, cur, lat = has_updates_available()
                if has_update:
                    log.info(f"📦 发现新版本 {cur} → {lat}，自动更新...")
                    # 检查 Tailscale
                    if is_tailscale_online():
                        perform_update()
                    else:
                        log.warning("⚠️ Tailscale 离线，跳过自动更新")
                else:
                    log.debug(f"✅ 当前已是最新版本 ({cur})")
        else:
            consecutive_fails += 1
            details = []
            if not status["port"]:
                details.append("端口不通")
            if not status["service"]:
                details.append("systemd 服务未运行")
            if not status["api"]:
                details.append("API 无响应")
            if not status["tailscale"]:
                details.append("Tailscale 离线")
            log.warning(f"检测失败 ({consecutive_fails}/{FAIL_THRESHOLD}): {', '.join(details)}")

            if consecutive_fails >= FAIL_THRESHOLD:
                log.error(f"连续失败 {FAIL_THRESHOLD} 次，触发重启")
                if restart_service():
                    consecutive_fails = 0
                else:
                    consecutive_fails = FAIL_THRESHOLD - 1

        time.sleep(CHECK_INTERVAL)

    log.info("更新服务已退出")


if __name__ == "__main__":
    main()
