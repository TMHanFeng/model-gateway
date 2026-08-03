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
import re
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
selected_source = "auto"  # "auto" / "github" / "gitee" / "gitee_first"
version_cache = {}
VERSION_CACHE_TTL = 60

# ── 全局版本缓存（后台线程写，前端秒读） ──
_global_version_cache = {
    "gitee": "—", "gitee_date": None,
    "github": "—", "github_date": None,
}

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
.sel{flex:1;padding:9px 10px;border-radius:8px;background:#0d1420;color:#e8edf5;border:1px solid #33445e;font-size:13px;font-family:ui-monospace,"SF Mono",Consolas,monospace;outline:none;max-width:280px;min-width:0}
.dim{color:#8b98ac;font-size:11px;font-weight:400}
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
    <div class="row"><span class="label">Gitee 版本</span><span class="value neutral" id="gitee-ver">加载中...</span></div>
    <div class="row"><span class="label">GitHub 版本</span><span class="value neutral" id="github-ver">加载中...</span></div>
    <div class="row"><span class="label">最新版本</span><span class="value upd" id="lat-ver">加载中...</span></div>
    <div class="row"><span class="label">更新源</span><select class="sel" id="src-select" onchange="selectSource()"><option value="auto">自动（GitHub 优先）</option><option value="github">GitHub</option><option value="gitee">Gitee</option><option value="gitee_first">Gitee 优先</option></select></div>
    <div class="row"><span class="label">可用版本</span><select class="sel" id="ver-select"><option value="">加载中...</option></select></div>
    <div class="row"><span class="label">更新状态</span><span class="value neutral" id="upd-text">加载中...</span></div>
    <div class="row"><span class="label">Tailscale</span><span class="value neutral" id="ts-ip">__TI__</span></div>
  </div>
  <div class="progress" id="progress">__UP__</div>
  <div class="actions">
    <button class="btn btn-amber" onclick="doAction('update')" id="btn-update">&#x1f504; 立即更新</button>
    <button class="btn btn-amber" onclick="doRollback()" id="btn-rollback">&#x2b07; 更新/回滚到所选版本</button>
    <button class="btn btn-green" onclick="doAction('restart')" id="btn-restart">&#x1f501; 重启网关</button>
    <button class="btn btn-red" onclick="doAction('stop')" id="btn-stop">&#x23f9; 停止网关</button>
    <button class="btn btn-ghost" onclick="doAction('start')" id="btn-start">&#x25b6; 启动网关</button>
    <button class="btn btn-ghost" onclick="refreshVersions()" id="btn-refresh" title="重新查询远程版本">&#x1f504; 刷新版本</button>
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
async function fetchStatus(){try{var r=await fetch('/status');if(!r.ok)return;var d=await r.json();document.getElementById('svc-status').textContent=d.service_active?'运行中':'已停止';document.getElementById('svc-status').className='value '+(d.service_active?'ok':'err');document.getElementById('api-status').textContent=d.api_healthy?'正常':'异常';document.getElementById('api-status').className='value '+(d.api_healthy?'ok':'err');document.getElementById('cur-ver').textContent=d.git.current+(d.git.commit?' · '+d.git.commit:'');document.getElementById('clock').textContent=d.timestamp.slice(0,19).replace('T',' ');var srcEl=document.getElementById('src-select');if(d.git.source&&srcEl.value!==d.git.source)srcEl.value=d.git.source;if(d.is_updating){showProgress(d.update_progress||'更新中...');setBusy(true)}else{hideProgress();setBusy(false)}}catch(e){}}
async function loadVersions(){try{var btn=document.getElementById('btn-refresh');btn.textContent='⏳ 查询中...';btn.disabled=true;var r=await fetch('/remote-versions');var d=await r.json();var giteeV=(d.gitee&&d.gitee!=='unknown'?d.gitee:'—');var giteeD=d.gitee_date?String(d.gitee_date).slice(0,16):'';document.getElementById('gitee-ver').textContent=giteeV+(giteeD?' · '+giteeD:'');var ghV=(d.github&&d.github!=='unknown'?d.github:'—');var ghD=d.github_date?String(d.github_date).slice(0,16):'';document.getElementById('github-ver').textContent=ghV+(ghD?' · '+ghD:'');document.getElementById('lat-ver').textContent=d.latest||'—';document.getElementById('upd-text').textContent=d.has_update?'🔄 有可用更新':'✅ 已是最新';var sel=document.getElementById('ver-select');if(d.tags&&d.tags.length>0){sel.innerHTML='';d.tags.forEach(function(v){var o=document.createElement('option');o.value=v.tag;o.textContent=v.tag+' ['+v.src+']'+(v.date?' · '+String(v.date).slice(0,16):'');sel.appendChild(o)})}else{sel.innerHTML='<option value="">暂无</option>'}var srcEl=document.getElementById('src-select');if(d.source&&srcEl.value!==d.source)srcEl.value=d.source}catch(e){document.getElementById('upd-text').textContent='⚠ 查询失败，可点击刷新重试'}finally{var btn=document.getElementById('btn-refresh');if(btn){btn.textContent='🔄 刷新版本';btn.disabled=false}}}
function refreshVersions(){loadVersions()}
async function doAction(action){if(action==='stop'&&!confirm('确定要停止网关服务吗？'))return;setBusy(true);showProgress('正在执行 '+action+' ...');try{var r=await fetch('/action/'+action);var d=await r.json();if(d.ok){toast('&#x2705; '+action+' 成功');showProgress(action+' 执行成功，正在等待服务就绪...');setTimeout(fetchStatus,1500)}else{toast('&#x274c; '+action+' 失败',true);hideProgress();setBusy(false)}}catch(e){toast('&#x274c; 请求失败: '+e,true);hideProgress();setBusy(false)}}
function verKey(t){var m=String(t).match(/[vV]?([0-9]+(?:[.][0-9]+)*)/);if(!m)return 0;var a=m[1].split('.').map(function(n){return parseInt(n,10)||0});for(var i=0;i<3;i++){if(!a[i])a[i]=0}return a[0]*10000+a[1]*100+a[2]}
function mergeVersions(d){var map={};(d.gitee||[]).forEach(function(v){map[v.tag]=v});(d.github||[]).forEach(function(v){if(map[v.tag]){map[v.tag].src='github/gitee'}else{map[v.tag]={tag:v.tag,date:v.date,src:'github'}}});return Object.keys(map).map(function(k){return map[k]}).sort(function(a,b){return verKey(b.tag)-verKey(a.tag)})}
 async function selectSource(){var s=document.getElementById('src-select').value;try{var r=await fetch('/select-source',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source:s})});var d=await r.json();toast(d.ok?'&#x2705; 已切换更新源: '+s:'&#x274c; 切换更新源失败',!d.ok)}catch(e){toast('&#x274c; 请求失败: '+e,true)}}
async function doRollback(){var sel=document.getElementById('ver-select');var tag=sel.value;if(!tag){toast('&#x274c; 请先选择版本',true);return}if(!confirm('确定要更新/回滚到版本 '+tag+' 吗？'))return;setBusy(true);showProgress('正在切换 '+tag+' ...');try{var r=await fetch('/rollback',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({tag:tag})});var d=await r.json();if(d.ok){toast('&#x2705; 已开始切换 '+tag);showProgress('切换任务已提交，等待服务就绪...');setTimeout(fetchStatus,1500)}else{toast('&#x274c; '+(d.error||'切换失败'),true);hideProgress();setBusy(false)}}catch(e){toast('&#x274c; 请求失败: '+e,true);hideProgress();setBusy(false)}}
// 页面加载后：立即查询远程版本（异步，不阻塞）
loadVersions();
</script>
</body>
</html>"""

def render_dashboard(svc_class, svc_text, api_class, api_text, current, latest="加载中", update_text="加载中", tailscale_ip="—", update_progress="", timestamp="", gitee="—", github="—", gitee_date=None, github_date=None, source="auto"):
    h = DASHBOARD_HTML
    gitee_text = gitee if gitee and gitee != "—" else "—"
    if gitee_date:
        gitee_text = f'{gitee_text}<small class="dim"> · {gitee_date}</small>'
    github_text = github if github and github != "—" else "—"
    if github_date:
        github_text = f'{github_text}<small class="dim"> · {github_date}</small>'
    for old, new in [
        ("__SC__", svc_class), ("__SV__", svc_text),
        ("__AC__", api_class), ("__AV__", api_text),
        ("__CU__", current), ("__GVR__", gitee_text), ("__HVR__", github_text), ("__LA__", latest),
        ("__UT__", update_text), ("__TI__", tailscale_ip),
        ("__UP__", update_progress or ""), ("__TS__", timestamp),
        ("__SRC__", source),
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


def parse_version(tag: str):
    """解析 v1.0.1 之类的版本号为可比较元组，失败返回 None"""
    try:
        m = re.match(r"^\s*[vV]?(\d+(?:\.\d+)*)", str(tag))
        if not m:
            return None
        return tuple(int(n) for n in m.group(1).split("."))
    except Exception:
        return None


def get_current_version() -> str:
    """获取本地当前版本：优先最近的 tag，无 tag 则回退短 commit hash"""
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            capture_output=True, text=True, timeout=5,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    commit = get_current_commit()
    return commit[:8] if commit else "unknown"


def get_current_commit_short() -> str:
    """获取当前 HEAD 的短 commit hash（如 aa0413e），失败返回空串"""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    commit = get_current_commit()
    return commit[:7] if commit else ""


def get_remote_latest_tag(remote: str, timeout: int = 30) -> str | None:
    """获取远程最高的语义化版本 tag，远程不存在/不可达时返回 None。
    timeout: 超时秒数（默认30，GitHub可设120）"""
    try:
        result = subprocess.run(
            ["git", "ls-remote", "--tags", remote],
            capture_output=True, text=True, timeout=timeout,
            cwd=str(REPO_DIR),
        )
        if result.returncode != 0:
            return None
        best_tag, best_ver = None, None
        fallback_tags = []
        for line in result.stdout.strip().split("\n"):
            parts = line.strip().split()
            if len(parts) < 2 or "^{}" in parts[1]:
                continue
            tag = parts[1].rsplit("/", 1)[-1]
            fallback_tags.append(tag)
            ver = parse_version(tag)
            if ver is None:
                continue
            if best_ver is None or ver > best_ver:
                best_ver, best_tag = ver, tag
        if best_tag:
            return best_tag
        return max(fallback_tags) if fallback_tags else None
    except Exception as e:
        log.error(f"获取远程 {remote} tag 失败: {e}")
        return None


def get_remote_versions(github_timeout: int = 120, gitee_timeout: int = 30) -> dict:
    """获取 gitee(origin) / github 两个远程的最新版本 tag。
    GitHub 默认120秒超时，Gitee默认30秒"""
    return {
        "gitee": get_remote_latest_tag("origin", timeout=gitee_timeout),
        "github": get_remote_latest_tag("github", timeout=github_timeout),
    }


def pick_latest_version(versions: dict) -> tuple[str, str]:
    """选出有效最新版本（github 优先），返回 (远程名, 版本号)，都拿不到返回 ("", "")"""
    if versions.get("github"):
        return "github", versions["github"]
    if versions.get("gitee"):
        return "origin", versions["gitee"]
    return "", ""


def get_remote_branch_commit(remote: str) -> str | None:
    """获取远程默认分支的最新 commit hash（需先 fetch）"""
    for branch in ["main", "master"]:
        try:
            result = subprocess.run(
                ["git", "rev-parse", f"{remote}/{branch}"],
                capture_output=True, text=True, timeout=5,
                cwd=str(REPO_DIR),
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except Exception:
            pass
    return get_latest_commit(remote)


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


def has_updates_available(versions: dict | None = None) -> tuple[bool, str, str]:
    """检查是否有可用更新（基于版本号判断，github 优先；无 tag 时回退 commit 比较）"""
    if versions is None:
        versions = get_remote_versions()
    current_ver = get_current_version()
    _, latest = pick_latest_version(versions)

    if latest:
        cur_parsed = parse_version(current_ver)
        lat_parsed = parse_version(latest)
        if cur_parsed is not None and lat_parsed is not None:
            return lat_parsed > cur_parsed, current_ver, latest
        return latest != current_ver, current_ver, latest

    # 两个远程都拿不到版本 tag，回退 commit 比较
    current = get_current_commit()
    if not current:
        return False, "unknown", "unknown"

    # 任一远程有新代码即视为有更新
    latest_github = get_latest_commit("github")
    if latest_github and latest_github != current:
        return True, current[:8], latest_github[:8]
    latest_gitee = get_latest_commit("origin")
    if latest_gitee and latest_gitee != current:
        return True, current[:8], latest_gitee[:8]

    return False, current[:8], current[:8]


def pick_latest_remote() -> tuple[str, str]:
    """选择更新源远程（版本号优先 github），返回 (远程名, commit hash)"""
    # 先 fetch 两个远程（含 tags），确保本地有最新 refs
    for r in ["github", "origin"]:
        try:
            subprocess.run(
                ["git", "fetch", r, "--quiet", "--tags"],
                capture_output=True, text=True, timeout=30,
                cwd=str(REPO_DIR),
            )
        except Exception:
            pass

    source, _version = pick_latest_version(get_remote_versions())
    if source:
        return source, get_remote_branch_commit(source) or ""

    # 两边都拿不到版本 tag，回退 commit 时间戳比较
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


def get_commit_date(commit: str) -> str | None:
    """获取 commit 的最后提交日期（ISO 格式），失败返回 None"""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cd", "--date=iso", commit],
            capture_output=True, text=True, timeout=5,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return None


def get_remote_tags_with_dates(remote: str) -> list:
    """获取某远程全部 v* 版本 tag 及对应提交日期，按版本降序返回 [(tag, date), ...]"""
    try:
        result = subprocess.run(
            ["git", "ls-remote", "--tags", remote],
            capture_output=True, text=True, timeout=20,
            cwd=str(REPO_DIR),
        )
        if result.returncode != 0:
            return []
        commits = {}
        for line in result.stdout.strip().split("\n"):
            parts = line.strip().split()
            if len(parts) < 2 or not parts[1].startswith("refs/tags/"):
                continue
            sha, ref = parts[0], parts[1]
            tag = ref[len("refs/tags/"):]
            if tag.endswith("^{}"):
                commits[tag[:-3]] = sha
            else:
                commits.setdefault(tag, sha)
        if not commits:
            return []
        subprocess.run(
            ["git", "fetch", remote, "--tags", "--quiet"],
            capture_output=True, text=True, timeout=30,
            cwd=str(REPO_DIR),
        )
        out = []
        for tag, sha in commits.items():
            if parse_version(tag) is None:
                continue
            date = get_commit_date(sha)
            if date is None:
                subprocess.run(
                    ["git", "fetch", remote, "tag", tag, "--force", "--quiet"],
                    capture_output=True, text=True, timeout=20,
                    cwd=str(REPO_DIR),
                )
                date = get_commit_date(sha)
            out.append((tag, date))
        out.sort(key=lambda x: parse_version(x[0]) or (0,), reverse=True)
        return out
    except Exception as e:
        log.error(f"获取远程 {remote} 版本列表失败: {e}")
        return []


def get_cached_tags_with_dates(remote: str) -> list:
    """带缓存地获取远程版本列表（避免轮询时频繁 git fetch）"""
    now = time.time()
    cached = version_cache.get(remote)
    if cached and now - cached[0] < VERSION_CACHE_TTL:
        return cached[1]
    tags = get_remote_tags_with_dates(remote)
    version_cache[remote] = (now, tags)
    return tags


def find_tag_remote(tag: str) -> str | None:
    """在 gitee/github 远程中查找包含该 tag 的远程名，找不到返回 None"""
    for remote in ("github", "origin"):
        try:
            result = subprocess.run(
                ["git", "ls-remote", "--tags", remote, f"refs/tags/{tag}"],
                capture_output=True, text=True, timeout=15,
                cwd=str(REPO_DIR),
            )
            if result.returncode == 0 and result.stdout.strip():
                return remote
        except Exception:
            pass
    return None


def fetch_tag(remote: str, tag: str) -> tuple[bool, str]:
    """拉取指定远程的 tag，返回 (成功, 错误信息)"""
    try:
        result = subprocess.run(
            ["git", "fetch", remote, "tag", tag, "--force", "--quiet"],
            capture_output=True, text=True, timeout=30,
            cwd=str(REPO_DIR),
        )
        if result.returncode == 0:
            return True, ""
        return False, result.stderr.strip()
    except Exception as e:
        return False, str(e)


def resolve_update_source(source: str | None = None) -> tuple[str, str]:
    """根据更新源设置解析要使用的远程，返回 (远程名, commit hash)"""
    source = source or selected_source
    if source == "github":
        return "github", get_remote_branch_commit("github") or ""
    if source == "gitee":
        return "origin", get_remote_branch_commit("origin") or ""
    if source == "gitee_first":
        versions = get_remote_versions()
        if versions.get("gitee"):
            return "origin", get_remote_branch_commit("origin") or ""
        if versions.get("github"):
            return "github", get_remote_branch_commit("github") or ""
        return "", ""
    return pick_latest_remote()


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

        # 3. 选择更新源远程（auto 时版本号优先 github）
        update_progress = "选择最新代码源..."
        remote, commit = resolve_update_source()
        if not remote:
            log.error("❌ 无法获取远程代码")
            return False
        log.info(f"📥 选择远程: {remote} (commit: {commit[:8] or 'unknown'})")

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


def perform_rollback(tag: str) -> tuple[bool, str]:
    """切换到指定版本：检出 tag 代码、装依赖、重启；失败时恢复最新并启动"""
    global is_updating, update_progress
    is_updating = True
    update_progress = f"开始切换到 {tag} ..."
    try:
        update_progress = "检查内网穿透状态..."
        if not is_tailscale_online():
            log.error("❌ Tailscale 离线，无法切换版本")
            return False, "Tailscale 离线，无法执行切换"

        remote = find_tag_remote(tag)
        if not remote:
            log.error(f"❌ 未在任何远程找到版本 {tag}")
            return False, f"未在任何远程找到版本 {tag}"

        update_progress = f"拉取版本 {tag} ..."
        ok, err = fetch_tag(remote, tag)
        if not ok:
            log.error(f"❌ 拉取 {tag} 失败: {err}")
            return False, f"拉取 {tag} 失败: {err}"

        update_progress = "停止服务..."
        if not stop_service():
            log.error("❌ 停止服务失败，中止切换")
            return False, "停止服务失败，中止切换"
        time.sleep(2)

        branch = get_default_branch(remote)
        update_progress = f"检出 {tag} 代码..."
        checkout = subprocess.run(
            ["git", "checkout", "-B", branch, tag],
            capture_output=True, text=True, timeout=30,
            cwd=str(REPO_DIR),
        )
        if checkout.returncode != 0:
            update_progress = "检出失败，恢复最新代码..."
            git_pull(remote, branch)
            start_service()
            log.error(f"❌ 检出 {tag} 失败: {checkout.stderr}")
            return False, f"检出 {tag} 失败: {checkout.stderr.strip()}"
        log.info(f"✅ 已检出 {tag} ({remote})")

        update_progress = "更新 Python 依赖..."
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q"],
                capture_output=True, text=True, timeout=120,
                cwd=str(REPO_DIR),
            )
        except Exception as e:
            log.warning(f"依赖更新警告: {e}")

        update_progress = "启动服务..."
        if start_service():
            time.sleep(2)
            if check_api_healthy():
                log.info(f"✅ 已切换到 {tag}，API 健康")
                return True, f"已切换到 {tag}"
            log.warning(f"⚠️ 已切换到 {tag}，但 API 未就绪")
            return True, f"已切换到 {tag}（API 未就绪）"

        update_progress = "启动失败，恢复最新版本..."
        log.error("❌ 切换后服务启动失败，尝试恢复最新代码")
        git_pull(remote, branch)
        start_service()
        return False, f"切换 {tag} 后启动失败，已尝试恢复最新版本"
    except Exception as e:
        log.error(f"❌ 切换版本异常: {e}")
        return False, f"切换版本异常: {e}"
    finally:
        is_updating = False
        update_progress = ""


# ── HTTP API（供 Hermes 调用）─────────────────────────────────────────
def parse_request(conn):
    """读取 HTTP 请求，解析出 (method, path, body)，失败返回 (None, "", "")"""
    try:
        raw = conn.recv(65536).decode()
        header_end = raw.find("\r\n\r\n")
        if header_end == -1:
            return None, "", ""
        head = raw[:header_end]
        body = raw[header_end + 4:]
        lines = head.split("\r\n")
        parts = lines[0].split()
        if len(parts) < 2:
            return None, "", ""
        method, path = parts[0], parts[1].split("?")[0]
        for line in lines[1:]:
            if line.lower().startswith("content-length:"):
                try:
                    clen = int(line.split(":", 1)[1].strip())
                    while len(body.encode("utf-8")) < clen:
                        chunk = conn.recv(65536).decode()
                        if not chunk:
                            break
                        body += chunk
                except Exception:
                    pass
                break
        return method, path, body
    except Exception:
        return None, "", ""


def handle_api_request(conn):
    """处理 HTTP API 请求"""
    global selected_source
    import threading as _t
    try:
        method, path, body = parse_request(conn)
        if not path:
            return

        # 路由
        response_body = ""
        status_code = 200
        content_type = "application/json"

        if path == "/status" or path == "/":
            # 快速状态报告（不执行 git 查询，避免阻塞）
            inner = get_inner_address()
            svc_active = is_service_active()
            api_healthy = check_api_healthy()
            cur_ver = get_current_version()
            commit = get_current_commit_short()
            git_status = {
                "current": cur_ver,
                "latest": "请刷新版本",
                "has_update": False,
                "commit": commit,
                "gitee_version": "加载中",
                "github_version": "加载中",
                "source": selected_source,
            }
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
            versions = {}
            def _check():
                nonlocal has_update, current, latest, versions
                versions = get_remote_versions()
                h, c, l = has_updates_available(versions)
                has_update, current, latest = h, c, l
            t = _t.Thread(target=_check, daemon=True)
            t.start()
            t.join(timeout=15)
            gitee_tags = get_cached_tags_with_dates("origin")
            github_tags = get_cached_tags_with_dates("github")
            response_body = json.dumps({
                "has_update": has_update,
                "current": current,
                "latest": latest,
                "gitee_version": versions.get("gitee") or "unknown",
                "github_version": versions.get("github") or "unknown",
                "gitee_date": gitee_tags[0][1] if gitee_tags else None,
                "github_date": github_tags[0][1] if github_tags else None,
                "source": selected_source,
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

        elif method == "POST" and path == "/select-source":
            src = "auto"
            try:
                payload = json.loads(body) if body else {}
                src = str(payload.get("source") or "auto").strip()
            except Exception:
                pass
            if src not in ("auto", "github", "gitee", "gitee_first"):
                src = "auto"
            selected_source = src
            log.info(f"📡 更新源已切换为: {src}")
            response_body = json.dumps({"ok": True, "source": src})

        elif method == "POST" and path == "/rollback":
            tag = ""
            try:
                payload = json.loads(body) if body else {}
                tag = str(payload.get("tag") or "").strip()
            except Exception:
                pass
            if not tag:
                response_body = json.dumps({"ok": False, "error": "缺少 tag 参数"})
            elif is_updating:
                response_body = json.dumps({"ok": False, "error": "已有更新任务进行中", "progress": update_progress})
            else:
                import threading
                thread = threading.Thread(target=perform_rollback, args=(tag,), daemon=True)
                thread.start()
                response_body = json.dumps({"ok": True, "status": "rollback_started", "tag": tag})

        elif path == "/versions":
            result = {"github": [], "gitee": [], "current": get_current_version()}
            def _versions():
                result["github"] = get_remote_tags_with_dates("github")
                result["gitee"] = get_remote_tags_with_dates("origin")
            t = _t.Thread(target=_versions, daemon=True)
            t.start()
            t.join(timeout=40)
            response_body = json.dumps(result, ensure_ascii=False)

        elif path == "/remote-versions":
            import threading as _t2
            rv = {"gitee": "—", "github": "—", "gitee_date": None, "github_date": None, "latest": "", "has_update": False, "tags": [], "source": selected_source}
            def _gitee():
                try:
                    t = get_remote_latest_tag("origin", timeout=30)
                    if t:
                        _global_version_cache["gitee"] = t
                        gts = get_cached_tags_with_dates("origin")
                        _global_version_cache["gitee_date"] = gts[0][1] if gts else None
                except Exception as e:
                    log.error(f"Gitee版本查询失败: {e}")
            def _github():
                try:
                    t = get_remote_latest_tag("github", timeout=120)
                    if t:
                        _global_version_cache["github"] = t
                        hts = get_cached_tags_with_dates("github")
                        _global_version_cache["github_date"] = hts[0][1] if hts else None
                except Exception as e:
                    log.error(f"GitHub版本查询失败: {e}")
            def _tags():
                try:
                    g = get_cached_tags_with_dates("origin")
                    h = get_cached_tags_with_dates("github")
                    rv["gitee_tags"] = [{"tag": t, "date": dt, "src": "gitee"} for t, dt in g]
                    rv["github_tags"] = [{"tag": t, "date": dt, "src": "github"} for t, dt in h]
                    rv["tags"] = rv["gitee_tags"] + rv["github_tags"]
                    cur = get_current_version()
                    for t, _ in g + h:
                        if t.startswith("v") and t > cur:
                            rv["has_update"] = True
                            rv["latest"] = t
                except Exception:
                    pass
            # 启动后台线程（不影响上次的缓存读取）
            t_g = _t2.Thread(target=_gitee, daemon=True); t_g.start()
            t_h = _t2.Thread(target=_github, daemon=True); t_h.start()
            t_t = _t2.Thread(target=_tags, daemon=True); t_t.start()
            t_g.join(timeout=35)
            t_t.join(timeout=8)
            # 读缓存：第一次加载可能暂无，之后都秒读
            rv["gitee"] = _global_version_cache["gitee"]
            rv["gitee_date"] = _global_version_cache["gitee_date"]
            rv["github"] = _global_version_cache["github"]
            rv["github_date"] = _global_version_cache["github_date"]
            response_body = json.dumps(rv, ensure_ascii=False)

        elif path in ("/updater", "/dashboard"):
            status_code = 200
            content_type = "text/html; charset=utf-8"
            inner = get_inner_address()
            cur_ver = get_current_version()
            commit = get_current_commit_short()
            current_display = f"{cur_ver} · {commit}" if commit else cur_ver
            svc_active = is_service_active()
            api_healthy = check_api_healthy()
            response_body = render_dashboard(
                svc_class="ok" if svc_active else "err",
                svc_text="运行中" if svc_active else "已停止",
                api_class="ok" if api_healthy else "err",
                api_text="正常" if api_healthy else "异常",
                current=current_display,
                tailscale_ip=inner.get("ip", "—"),
                update_progress=update_progress or "",
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                source=selected_source,
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
