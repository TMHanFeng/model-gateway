# -*- coding: utf-8 -*-
"""统一日志配置：控制台精简 + 文件详细，双输出，按天轮转保留 30 天。

- 控制台: 短格式（仅 时间+级别+消息），INFO 及以上
- 文件:   完整格式（毫秒时间戳+模块名+消息），DEBUG 及以上 —— 详细内容全部落盘
- 格式:
    控制台  15:05:47 | INFO  | 消息
    文件    2026-08-26 15:05:47.971 | INFO    | uvicorn.access    | 127.0.0.1:53924 - "GET / HTTP/1.1" 404
"""
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "gateway.log")

CONSOLE_FMT = "[%(asctime)s] %(levelname)-5s | %(message)s"
CONSOLE_DATEFMT = "%H:%M:%S"
FILE_FMT = "%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)-18s | %(message)s"
FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"

# 控制台颜色（ANSI）：INFO 绿 / 警告黄 / 错误红 / 调试灰。访问日志按状态码着色
_COLORS = {
    logging.DEBUG: "\033[90m",
    logging.INFO: "\033[32m",
    logging.WARNING: "\033[33m",
    logging.ERROR: "\033[31m",
    logging.CRITICAL: "\033[1;31m",
}
_GREEN = "\033[32m"
_RESET = "\033[0m"

import re
_STATUS_RE = re.compile(r'" (\d{3})')
# 匹配行首 "[HH:MM:SS] " 之后的级别标签段（如 INFO/WARNING/ERROR/DEBUG）
_LEVEL_RE = re.compile(r'^(\[\d{2}:\d{2}:\d{2}\]\s+)(\S+)(\s*\|)')


class _ColorFormatter(logging.Formatter):
    """控制台着色规则：
    - INFO（banner、提示、访问日志等普通信息）→ 绿色
    - 访问日志按 HTTP 状态码着色：4xx 黄、5xx 红（覆盖绿色）
    - WARNING 黄 / ERROR 红 / DEBUG 灰
    非终端输出时不加色码。
    """

    def __init__(self, fmt=None, datefmt=None, colored: bool = True):
        super().__init__(fmt, datefmt)
        self.colored = colored

    def format(self, record):
        # 先由 logging 渲染出完整消息，再做局部着色：只染"级别标签"和"HTTP 状态码"，
        # 时间与消息内容保持白色（INFO 绿 / WARNING 黄 / ERROR 红 / DEBUG 灰）
        msg = super().format(record)
        if not self.colored:
            return msg
        color = _COLORS.get(record.levelno)
        if color:
            m = _LEVEL_RE.match(msg)  # 匹配行首 [时间] 后的级别标签段
            if m:
                s, e = m.span(2)
                msg = msg[:s] + color + msg[s:e] + _RESET + msg[e:]
        if record.name == "uvicorn.access":
            sm = _STATUS_RE.search(msg)
            if sm:
                code = int(sm.group(1))
                if code >= 500:
                    sc = _COLORS[logging.ERROR]
                elif code >= 400:
                    sc = _COLORS[logging.WARNING]
                else:
                    sc = _GREEN
                s, e = sm.span(1)
                msg = msg[:s] + sc + msg[s:e] + _RESET + msg[e:]
        return msg


def _enable_windows_ansi():
    """Windows 控制台启用 ANSI 转义（VT 模式），使彩色日志在 PowerShell/Terminal 生效。"""
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
        except Exception:
            pass


def setup_logging(console_level: int = logging.INFO, file_level: int = logging.DEBUG, console_color: bool = True) -> str:
    """配置 root logger（清除已有 handler 后重建），返回日志文件路径。"""
    os.makedirs(LOG_DIR, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(min(console_level, file_level))
    for h in list(root.handlers):
        root.removeHandler(h)

    _enable_windows_ansi()
    # 终端直接输出时用彩色；重定向到文件/管道时自动降级为无颜色
    use_color = console_color and bool(getattr(sys.stdout, "isatty", lambda: False)())

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(console_level)
    console.setFormatter(_ColorFormatter(CONSOLE_FMT, datefmt=CONSOLE_DATEFMT, colored=use_color))
    root.addHandler(console)

    file_h = TimedRotatingFileHandler(LOG_FILE, when="midnight", backupCount=30, encoding="utf-8")
    file_h.setLevel(file_level)
    file_h.setFormatter(logging.Formatter(FILE_FMT, datefmt=FILE_DATEFMT))  # 文件永远纯文本，无颜色码
    root.addHandler(file_h)

    # 让第三方库保持安静，避免刷屏（aiosqlite 每次 SQL 都打 DEBUG，文件也会被刷）
    for noisy in ("httpx", "httpcore", "apscheduler", "urllib3", "aiosqlite", "aiosqlite.cursor", "aiosqlite.connection"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return LOG_FILE