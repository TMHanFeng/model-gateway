"""API Key 认证与用量管理。

- 管理员 Key（type=admin）：无任何限制，可访问所有模型池、不计量用量。
- 用户 Key（type=user）：仅能访问被授权的模型池，并按设置的限额计量
  （token_type: daily / rolling_5h / one_time；billing_mode: token / request）。
- 服务器密钥（config.json server.api_key）即管理员账号，拥有最高权限。

用法语义与模型级用量一致：
  daily      按北京自然日刷新（日期变化时懒重置）
  rolling_5h 首次调用开窗，满 5 小时自动刷新
  one_time   到期或用完即永久失效
"""
import secrets
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import database as db

ROLLING_5H_SECONDS = 5 * 3600

KEY_PREFIX = "mg-"


def generate_key() -> str:
    """生成新的 API Key（mg- 前缀 + 32 位十六进制）"""
    return KEY_PREFIX + secrets.token_hex(16)


def _today() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()


def is_admin_key(key: dict) -> bool:
    return key.get("type") == "admin"


def _has_limit(key: dict) -> bool:
    return key.get("token_type") in ("daily", "rolling_5h", "one_time") and key.get("limit_amount", 0) > 0


def parse_allowed_pools(key: dict) -> list[str]:
    pools = key.get("allowed_pools") or []
    if isinstance(pools, str):
        import json
        try:
            pools = json.loads(pools)
        except Exception:
            pools = []
    return [str(p) for p in pools]


def is_pool_allowed(key: dict, pool_name: str) -> bool:
    """用户 Key 的池授权校验；管理员 Key 始终允许"""
    if is_admin_key(key):
        return True
    return pool_name in parse_allowed_pools(key)


async def key_usage_available(key: dict) -> tuple[bool, str]:
    """检查用户 Key 的用量限额是否仍有剩余。返回 (可用, 原因)"""
    if is_admin_key(key) or not _has_limit(key):
        return True, ""
    key_id = key["id"]
    limit = int(key.get("limit_amount", 0))
    now = time.time()
    state = await db.get_key_usage(key_id)

    if key.get("token_type") == "daily":
        if state is None or state.get("last_reset_date") != _today():
            return True, ""
        if state.get("used_amount", 0) >= limit:
            return False, "daily_exhausted"
        return True, ""
    if key.get("token_type") == "rolling_5h":
        if state is None:
            return True, ""
        ws = state.get("window_start")
        if ws is None or (now - ws) >= ROLLING_5H_SECONDS:
            return True, ""
        if state.get("used_amount", 0) >= limit:
            return False, "rolling5h_exhausted"
        return True, ""
    if key.get("token_type") == "one_time":
        if state is None:
            return True, ""
        if state.get("expired"):
            return False, "one_time_expired"
        if state.get("used_amount", 0) >= limit:
            return False, "one_time_expired"
        return True, ""
    return True, ""


async def charge_key_usage(key: dict, amount: int):
    """按用户 Key 的限额语义计量用量（管理员 Key / 无限额直接忽略）"""
    if is_admin_key(key) or not _has_limit(key):
        return
    key_id = key["id"]
    now = time.time()
    state = await db.get_key_usage(key_id)
    if state is None:
        await db.init_key_usage(key_id)
        state = await db.get_key_usage(key_id)

    if key.get("token_type") == "daily":
        if state.get("last_reset_date") != _today():
            await db.reset_key_usage(key_id)
        await db.add_key_usage(key_id, amount)
    elif key.get("token_type") == "rolling_5h":
        ws = state.get("window_start")
        if ws is None or (now - ws) >= ROLLING_5H_SECONDS:
            await db.reset_key_usage(key_id)
        await db.add_key_usage(key_id, amount)
    elif key.get("token_type") == "one_time":
        if not state.get("expired"):
            await db.add_key_usage(key_id, amount)
            s = await db.get_key_usage(key_id)
            if int(key.get("limit_amount", 0)) > 0 and s.get("used_amount", 0) >= int(key.get("limit_amount", 0)):
                await db.expire_key_usage(key_id)


async def key_usage_summary(key: dict) -> dict:
    """供管理后台展示：当前已用量 / 限额 / 类型 / 是否超限"""
    if is_admin_key(key) or not _has_limit(key):
        return {"used": 0, "limit": 0, "token_type": "", "billing_mode": "token", "exhausted": False}
    state = await db.get_key_usage(key["id"])
    used = state.get("used_amount", 0) if state else 0
    ok, _ = await key_usage_available(key)
    return {
        "used": used,
        "limit": int(key.get("limit_amount", 0)),
        "token_type": key.get("token_type", ""),
        "billing_mode": key.get("billing_mode", "token"),
        "exhausted": not ok,
    }
