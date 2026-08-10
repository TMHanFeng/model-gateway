import aiosqlite
import asyncio
import time
import json
from pathlib import Path

DB_PATH = Path(__file__).parent / "gateway.db"


def _bj_today() -> str:
    """今日日期（YYYY-MM-DD，北京时间自然日）。
    与 keyauth._today 一致（不 import keyauth 避免循环依赖，独立实现）。"""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()

# Persistent connection + lock: avoids the per-query connect/close overhead that
# dominated request latency (every _check_available call used to open 1-4 new
# connections). WAL + NORMAL synchronous keep writes fast and readers unblocked.
_conn: aiosqlite.Connection | None = None
_lock = asyncio.Lock()


async def _get_conn() -> aiosqlite.Connection:
    global _conn
    if _conn is None:
        _conn = await aiosqlite.connect(str(DB_PATH))
        _conn.row_factory = aiosqlite.Row
        await _conn.execute("PRAGMA journal_mode=WAL")
        await _conn.execute("PRAGMA synchronous=NORMAL")
    return _conn


async def close_db():
    global _conn
    if _conn is not None:
        await _conn.close()
        _conn = None


async def init_db():
    async with _lock:
        db = await _get_conn()
        await db.execute("""
            CREATE TABLE IF NOT EXISTS token_usage (
                model_name TEXT PRIMARY KEY,
                used_tokens INTEGER DEFAULT 0,
                last_reset_date TEXT DEFAULT ''
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS request_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                timestamp REAL NOT NULL,
                tokens INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_request_log_model_ts
            ON request_log (model_name, timestamp)
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS one_time_state (
                model_name TEXT PRIMARY KEY,
                used_tokens INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                expired INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS decision_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                pool_name TEXT,
                requested TEXT,
                selected TEXT,
                estimated_tokens INTEGER,
                steps TEXT,
                caller TEXT
            )
        """)
        # 老库迁移：decision_log 补充 caller 列
        cols = [r[1] for r in await (await db.execute("PRAGMA table_info(decision_log)")).fetchall()]
        if "caller" not in cols:
            await db.execute("ALTER TABLE decision_log ADD COLUMN caller TEXT")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rolling5h_state (
                model_name TEXT PRIMARY KEY,
                used_amount INTEGER DEFAULT 0,
                window_start REAL NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                secret TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL DEFAULT 'user',
                allowed_pools TEXT DEFAULT '[]',
                token_type TEXT DEFAULT '',
                billing_mode TEXT DEFAULT 'token',
                limit_amount INTEGER DEFAULT 0,
                enabled INTEGER DEFAULT 1,
                created_at REAL NOT NULL,
                expire_seconds INTEGER DEFAULT 0,
                rotated_at REAL,
                previous_secret TEXT,
                previous_secret_expires REAL
            )
        """)
        # 老库迁移：api_keys 补充 expire_seconds / rotated_at / 宽限期列
        kcols = [r[1] for r in await (await db.execute("PRAGMA table_info(api_keys)")).fetchall()]
        if "expire_seconds" not in kcols:
            await db.execute("ALTER TABLE api_keys ADD COLUMN expire_seconds INTEGER DEFAULT 0")
        if "rotated_at" not in kcols:
            await db.execute("ALTER TABLE api_keys ADD COLUMN rotated_at REAL")
        if "previous_secret" not in kcols:
            await db.execute("ALTER TABLE api_keys ADD COLUMN previous_secret TEXT")
        if "previous_secret_expires" not in kcols:
            await db.execute("ALTER TABLE api_keys ADD COLUMN previous_secret_expires REAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS key_rotation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_id INTEGER NOT NULL,
                old_prefix TEXT,
                new_prefix TEXT,
                ts REAL NOT NULL,
                note TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS api_key_usage (
                key_id INTEGER PRIMARY KEY,
                used_amount INTEGER DEFAULT 0,
                window_start REAL,
                created_at REAL,
                expired INTEGER DEFAULT 0,
                last_reset_date TEXT DEFAULT ''
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'user',
                created_at REAL NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS api_key_hourly_usage (
                key_id INTEGER NOT NULL,
                hour_key TEXT NOT NULL,
                used_amount INTEGER DEFAULT 0,
                PRIMARY KEY (key_id, hour_key)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS model_daily_stats (
                model_name TEXT NOT NULL,
                date TEXT NOT NULL,
                request_count INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                PRIMARY KEY (model_name, date)
            )
        """)
        await db.commit()


async def get_daily_usage(model_name: str) -> int:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT used_tokens FROM token_usage WHERE model_name = ?",
            (model_name,),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


async def add_daily_usage(model_name: str, tokens: int):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            """INSERT INTO token_usage (model_name, used_tokens, last_reset_date)
               VALUES (?, ?, date('now'))
               ON CONFLICT(model_name) DO UPDATE SET used_tokens = used_tokens + ?""",
            (model_name, tokens, tokens),
        )
        await db.commit()


async def reset_daily_usage(model_name: str):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "UPDATE token_usage SET used_tokens = 0, last_reset_date = date('now') WHERE model_name = ?",
            (model_name,),
        )
        await db.commit()


async def reset_all_daily():
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "UPDATE token_usage SET used_tokens = 0, last_reset_date = date('now')"
        )
        await db.commit()


async def log_request(model_name: str, tokens: int):
    now = time.time()
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "INSERT INTO request_log (model_name, timestamp, tokens) VALUES (?, ?, ?)",
            (model_name, now, tokens),
        )
        await db.execute(
            "DELETE FROM request_log WHERE timestamp < ?", (now - 60,)
        )
        await db.commit()


async def get_rpm(model_name: str) -> int:
    cutoff = time.time() - 60
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT COUNT(*) FROM request_log WHERE model_name = ? AND timestamp > ?",
            (model_name, cutoff),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


async def get_tpm(model_name: str) -> int:
    cutoff = time.time() - 60
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT COALESCE(SUM(tokens), 0) FROM request_log WHERE model_name = ? AND timestamp > ?",
            (model_name, cutoff),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


# ── 模型调用统计（请求次数 / token 用量，与计费量 token_usage 分离）────

async def add_model_call(model_name: str, tokens: int):
    """记录一次模型调用：当日 request_count+1、total_tokens+tokens。
    日期用北京时间自然日（_bj_today），与配额刷新（scheduler Asia/Shanghai）对齐；
    tokens 为该次调用实际 token 数，可为 0。"""
    async with _lock:
        db = await _get_conn()
        await db.execute(
            """INSERT INTO model_daily_stats (model_name, date, request_count, total_tokens)
               VALUES (?, ?, 1, ?)
               ON CONFLICT(model_name, date) DO UPDATE SET
                   request_count = request_count + 1,
                   total_tokens = total_tokens + excluded.total_tokens""",
            (model_name, _bj_today(), tokens),
        )
        await db.commit()


async def get_model_daily_stats(model_name: str, date_str: str | None = None) -> dict:
    """返回某模型指定日期（默认今天，北京时间自然日 _bj_today）的
    {"request_count": n, "total_tokens": n}，无记录时返回 {"request_count": 0, "total_tokens": 0}。"""
    if date_str is None:
        date_str = _bj_today()
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT request_count, total_tokens FROM model_daily_stats WHERE model_name = ? AND date = ?",
            (model_name, date_str),
        )
        row = await cursor.fetchone()
    if not row:
        return {"request_count": 0, "total_tokens": 0}
    return {"request_count": row[0], "total_tokens": row[1]}


async def init_one_time(model_name: str):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            """INSERT OR IGNORE INTO one_time_state (model_name, used_tokens, created_at, expired)
               VALUES (?, 0, ?, 0)""",
            (model_name, time.time()),
        )
        await db.commit()


async def get_one_time_state(model_name: str) -> dict | None:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT used_tokens, created_at, expired FROM one_time_state WHERE model_name = ?",
            (model_name,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {"used_tokens": row[0], "created_at": row[1], "expired": row[2]}


async def add_one_time_usage(model_name: str, tokens: int):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "UPDATE one_time_state SET used_tokens = used_tokens + ? WHERE model_name = ?",
            (tokens, model_name),
        )
        await db.commit()


async def expire_one_time(model_name: str):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "UPDATE one_time_state SET expired = 1 WHERE model_name = ?",
            (model_name,),
        )
        await db.commit()


async def init_5h_state(model_name: str):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            """INSERT OR IGNORE INTO rolling5h_state (model_name, used_amount, window_start)
               VALUES (?, 0, ?)""",
            (model_name, time.time()),
        )
        await db.commit()


async def get_5h_state(model_name: str) -> dict | None:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT used_amount, window_start FROM rolling5h_state WHERE model_name = ?",
            (model_name,),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {"used_amount": row[0], "window_start": row[1]}


async def add_5h_usage(model_name: str, amount: int):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "UPDATE rolling5h_state SET used_amount = used_amount + ? WHERE model_name = ?",
            (amount, model_name),
        )
        await db.commit()


async def reset_5h_window(model_name: str):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            """INSERT INTO rolling5h_state (model_name, used_amount, window_start)
               VALUES (?, 0, ?)
               ON CONFLICT(model_name) DO UPDATE SET used_amount = 0, window_start = excluded.window_start""",
            (model_name, time.time()),
        )
        await db.commit()


async def log_decision(pool_name: str, requested: str | None, selected: str | None, estimated: int, steps: list, caller: str = ""):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "INSERT INTO decision_log (ts, pool_name, requested, selected, estimated_tokens, steps, caller) VALUES (?,?,?,?,?,?,?)",
            (time.time(), pool_name, requested or "", selected or "", estimated, json.dumps(steps, ensure_ascii=False), caller),
        )
        await db.execute(
            "DELETE FROM decision_log WHERE id NOT IN (SELECT id FROM decision_log ORDER BY id DESC LIMIT 500)"
        )
        await db.commit()


async def get_decisions(pool_name: str | None = None, limit: int = 100) -> list[dict]:
    async with _lock:
        db = await _get_conn()
        if pool_name:
            cursor = await db.execute(
                "SELECT * FROM decision_log WHERE pool_name = ? ORDER BY id DESC LIMIT ?",
                (pool_name, limit),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM decision_log ORDER BY id DESC LIMIT ?", (limit,)
            )
        rows = await cursor.fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["steps"] = json.loads(d["steps"])
            except Exception:
                d["steps"] = []
            out.append(d)
        return out


# ── API Key 管理 ──────────────────────────────────────────────────────

async def create_api_key(name: str, secret: str, ktype: str, allowed_pools: list,
                         token_type: str, billing_mode: str, limit_amount: int,
                         expire_seconds: int = 0) -> int:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            """INSERT INTO api_keys (name, secret, type, allowed_pools, token_type, billing_mode, limit_amount, expire_seconds, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, secret, ktype, json.dumps(allowed_pools, ensure_ascii=False),
             token_type, billing_mode, limit_amount, expire_seconds, time.time()),
        )
        await db.commit()
        return cursor.lastrowid


async def list_api_keys() -> list[dict]:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT * FROM api_keys ORDER BY id DESC"
        )
        rows = await cursor.fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["allowed_pools"] = json.loads(d["allowed_pools"])
            except Exception:
                d["allowed_pools"] = []
            out.append(d)
        return out


async def get_api_key_by_secret(secret: str) -> dict | None:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT * FROM api_keys WHERE secret = ?", (secret,)
        )
        r = await cursor.fetchone()
        if not r:
            return None
        d = dict(r)
        try:
            d["allowed_pools"] = json.loads(d["allowed_pools"])
        except Exception:
            d["allowed_pools"] = []
        return d


async def get_api_key_by_secret_or_previous(secret: str) -> dict | None:
    """认证查找：优先匹配当前 secret，其次匹配宽限期内的 previous_secret。
    返回记录与 get_api_key_by_secret 同构（allowed_pools 已解析）；均不匹配返回 None。"""
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT * FROM api_keys WHERE secret = ?", (secret,)
        )
        r = await cursor.fetchone()
        if not r:
            cursor = await db.execute(
                """SELECT * FROM api_keys
                   WHERE previous_secret = ? AND previous_secret_expires IS NOT NULL
                     AND previous_secret_expires > ?""",
                (secret, time.time()),
            )
            r = await cursor.fetchone()
        if not r:
            return None
        d = dict(r)
        try:
            d["allowed_pools"] = json.loads(d["allowed_pools"])
        except Exception:
            d["allowed_pools"] = []
        return d


async def get_api_key_by_id(key_id: int) -> dict | None:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT * FROM api_keys WHERE id = ?", (key_id,)
        )
        r = await cursor.fetchone()
        if not r:
            return None
        d = dict(r)
        try:
            d["allowed_pools"] = json.loads(d["allowed_pools"])
        except Exception:
            d["allowed_pools"] = []
        return d


async def rotate_key_secret(key_id: int, old_secret: str, new_secret: str, grace_seconds: int) -> bool:
    """到期自动轮换：把 secret 换成 new_secret 并记录轮换时间，旧 secret 转入宽限期。
    previous_secret 保留旧值，previous_secret_expires = now + grace_seconds：
    宽限期内旧 secret 仍可认证（客户端无感过渡），到期后彻底失效。
    保留 api_key_usage 用量累计（按 key_id 继续累计，跨轮换延续限额周期，不删除）。
    CAS（WHERE secret = ?）：并发两个请求都判定过期时，仅第一个真正轮换成功（返回 True）；
    第二个因 secret 已被替换而影响 0 行（返回 False），避免旧 secret 宽限期被二次覆盖丢失。"""
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            """UPDATE api_keys
               SET secret = ?, rotated_at = ?, previous_secret = secret,
                   previous_secret_expires = ?
               WHERE id = ? AND secret = ?""",
            (new_secret, time.time(), time.time() + grace_seconds, key_id, old_secret),
        )
        await db.commit()
        return cursor.rowcount == 1


async def log_key_rotation(key_id: int, old_prefix: str, new_prefix: str, note: str = "") -> None:
    """记录密钥轮换审计日志（只存 secret 前 8 字符前缀，绝不含明文）。"""
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "INSERT INTO key_rotation_log (key_id, old_prefix, new_prefix, ts, note) VALUES (?, ?, ?, ?, ?)",
            (key_id, old_prefix, new_prefix, time.time(), note),
        )
        await db.commit()


async def get_key_rotations(key_id: int, limit: int = 20) -> list[dict]:
    """查询某密钥的轮换记录（按时间倒序，新到旧）。"""
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT * FROM key_rotation_log WHERE key_id = ? ORDER BY ts DESC LIMIT ?",
            (key_id, limit),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def update_api_key(key_id: int, fields: dict):
    allowed = {"name", "type", "allowed_pools", "token_type", "billing_mode", "limit_amount", "enabled",
               "expire_seconds", "rotated_at"}
    sets = []
    vals = []
    for k in allowed:
        if k in fields:
            v = fields[k]
            if k == "allowed_pools":
                v = json.dumps(v, ensure_ascii=False)
            sets.append(f"{k} = ?")
            vals.append(v)
    if not sets:
        return
    vals.append(key_id)
    async with _lock:
        db = await _get_conn()
        await db.execute(
            f"UPDATE api_keys SET {', '.join(sets)} WHERE id = ?", vals
        )
        await db.commit()


async def delete_api_key(key_id: int):
    async with _lock:
        db = await _get_conn()
        await db.execute("DELETE FROM api_keys WHERE id = ?", (key_id,))
        await db.execute("DELETE FROM api_key_usage WHERE key_id = ?", (key_id,))
        await db.execute("DELETE FROM api_key_hourly_usage WHERE key_id = ?", (key_id,))
        await db.execute("DELETE FROM key_rotation_log WHERE key_id = ?", (key_id,))
        await db.commit()


# ── API Key 用量（daily / rolling_5h / one_time 语义，与模型一致）──────

async def get_key_usage(key_id: int) -> dict | None:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT * FROM api_key_usage WHERE key_id = ?", (key_id,)
        )
        r = await cursor.fetchone()
        if not r:
            return None
        return dict(r)


async def init_key_usage(key_id: int):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            """INSERT OR IGNORE INTO api_key_usage (key_id, used_amount, last_reset_date)
               VALUES (?, 0, ?)""",
            (key_id, ""),
        )
        await db.commit()


async def add_key_usage(key_id: int, amount: int):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "UPDATE api_key_usage SET used_amount = used_amount + ? WHERE key_id = ?",
            (amount, key_id),
        )
        await db.commit()


async def reset_key_usage(key_id: int):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    async with _lock:
        db = await _get_conn()
        await db.execute(
            """INSERT INTO api_key_usage (key_id, used_amount, window_start, created_at, expired, last_reset_date)
               VALUES (?, 0, ?, ?, 0, ?)
               ON CONFLICT(key_id) DO UPDATE SET
                   used_amount = 0, window_start = excluded.window_start,
                   created_at = excluded.created_at, expired = 0, last_reset_date = excluded.last_reset_date""",
            (key_id, time.time(), time.time(), today),
        )
        await db.commit()


async def expire_key_usage(key_id: int):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "UPDATE api_key_usage SET expired = 1 WHERE key_id = ?", (key_id,)
        )
        await db.commit()


# ── 用户（预留：未来普通用户账号体系）──────────────────────────────────

async def create_user(username: str, password_hash: str, role: str = "user") -> int:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
            (username, password_hash, role, time.time()),
        )
        await db.commit()
        return cursor.lastrowid


async def list_users() -> list[dict]:
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute("SELECT id, username, role, created_at FROM users ORDER BY id")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def delete_user(user_id: int):
    async with _lock:
        db = await _get_conn()
        await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        await db.commit()


# ── API Key 用量按小时记录（1h 粒度，可查询任意日期）──────────────────

async def add_hourly_usage(key_id: int, hour_key: str, amount: int):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            """INSERT INTO api_key_hourly_usage (key_id, hour_key, used_amount)
               VALUES (?, ?, ?)
               ON CONFLICT(key_id, hour_key) DO UPDATE SET used_amount = used_amount + excluded.used_amount""",
            (key_id, hour_key, amount),
        )
        await db.commit()


async def get_hourly_usage(key_id: int, date_str: str) -> dict[str, int]:
    """返回某日期(YYYY-MM-DD)的 24 小时用量 { 'HH': amount }，无记录的小时返回 0"""
    prefix = date_str + "-"
    async with _lock:
        db = await _get_conn()
        cursor = await db.execute(
            "SELECT hour_key, used_amount FROM api_key_hourly_usage WHERE key_id = ? AND hour_key LIKE ?",
            (key_id, prefix + "%"),
        )
        rows = await cursor.fetchall()
    out = {}
    for r in rows:
        hour = str(r["hour_key"]).split("-")[-1]
        out[hour] = r["used_amount"]
    return out
