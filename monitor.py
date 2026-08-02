#!/usr/bin/env python3
"""
Model Gateway 监控程序
- 每 10 秒检测一次服务是否存活
- 若服务不可达则自动重启
- 若自身异常退出，systemd 会重启它
"""

import time
import subprocess
import sys
import os
import signal
import logging
from datetime import datetime
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────────────────────
SERVICE_NAME = "model-gateway.service"
CHECK_URL = "http://127.0.0.1:8650/v1/models"
API_KEY = "123456"
CHECK_INTERVAL = 10          # 检测间隔（秒）
RESTART_COOLDOWN = 30        # 重启冷却期（秒），防止疯狂重启
FAIL_THRESHOLD = 3           # 连续失败次数阈值，超过才重启
REQUEST_TIMEOUT = 5          # HTTP 请求超时（秒）

# ── 日志配置 ──────────────────────────────────────────────────────────
LOG_FILE = Path(__file__).parent / "monitor.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("monitor")

# ── 状态变量 ──────────────────────────────────────────────────────────
consecutive_fails = 0
last_restart_time = 0
running = True


def is_port_open() -> bool:
    """检测 8650 端口是否有进程在监听"""
    try:
        import socket
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
    """通过 HTTP 请求验证 API 是否真的能响应"""
    try:
        import urllib.request
        req = urllib.request.Request(
            CHECK_URL,
            headers={"Authorization": f"Bearer {API_KEY}"},
        )
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.status == 200
    except Exception:
        return False


def restart_service() -> bool:
    """执行服务重启"""
    global last_restart_time
    now = time.time()
    if now - last_restart_time < RESTART_COOLDOWN:
        log.warning(f"冷却期内（还需 {RESTART_COOLDOWN - int(now - last_restart_time)} 秒），跳过重启")
        return False

    log.warning(">>> 正在重启 model-gateway.service ...")
    try:
        subprocess.run(
            ["systemctl", "--user", "restart", SERVICE_NAME],
            capture_output=True, text=True, timeout=30,
        )
        last_restart_time = time.time()
        # 等几秒让服务完全启动
        time.sleep(3)
        if is_service_active() and check_api_healthy():
            log.info("✅ 服务重启成功")
            return True
        else:
            log.error("❌ 服务重启后仍未就绪")
            return False
    except Exception as e:
        log.error(f"❌ 重启命令执行失败: {e}")
        return False


def health_check() -> dict:
    """执行完整健康检查，返回各指标状态"""
    port_ok = is_port_open()
    service_ok = is_service_active()
    api_ok = check_api_healthy() if port_ok else False
    return {
        "port": port_ok,
        "service": service_ok,
        "api": api_ok,
        "healthy": port_ok and service_ok and api_ok,
    }


def main():
    global consecutive_fails, running

    log.info("=" * 60)
    log.info("Model Gateway 监控程序启动")
    log.info(f"检测间隔: {CHECK_INTERVAL}s | 失败阈值: {FAIL_THRESHOLD} | 冷却期: {RESTART_COOLDOWN}s")
    log.info(f"监控目标: {CHECK_URL}")
    log.info("=" * 60)

    # 注册信号处理，优雅退出
    def handle_signal(signum, frame):
        global running
        log.info(f"收到信号 {signum}，监控程序退出")
        running = False

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    while running:
        status = health_check()

        if status["healthy"]:
            if consecutive_fails > 0:
                log.info(f"服务恢复正常（之前连续失败 {consecutive_fails} 次）")
            consecutive_fails = 0
        else:
            consecutive_fails += 1
            details = []
            if not status["port"]:
                details.append("端口不通")
            if not status["service"]:
                details.append("systemd 服务未运行")
            if not status["api"]:
                details.append("API 无响应")
            log.warning(f"检测失败 ({consecutive_fails}/{FAIL_THRESHOLD}): {', '.join(details)}")

            if consecutive_fails >= FAIL_THRESHOLD:
                log.error(f"连续失败 {FAIL_THRESHOLD} 次，触发重启")
                if restart_service():
                    consecutive_fails = 0
                else:
                    # 重启失败，重置计数器，下次继续尝试
                    consecutive_fails = FAIL_THRESHOLD - 1

        time.sleep(CHECK_INTERVAL)

    log.info("监控程序已退出")


if __name__ == "__main__":
    main()
