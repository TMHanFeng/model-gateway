import aiosqlite
import asyncio
import time
import json
from pathlib import Path

DB_PATH = Path(__file__).parent / "gateway.db"

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
                steps TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rolling5h_state (
                model_name TEXT PRIMARY KEY,
                used_amount INTEGER DEFAULT 0,
                window_start REAL NOT NULL
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


async def log_decision(pool_name: str, requested: str | None, selected: str | None, estimated: int, steps: list):
    async with _lock:
        db = await _get_conn()
        await db.execute(
            "INSERT INTO decision_log (ts, pool_name, requested, selected, estimated_tokens, steps) VALUES (?,?,?,?,?,?)",
            (time.time(), pool_name, requested or "", selected or "", estimated, json.dumps(steps, ensure_ascii=False)),
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
