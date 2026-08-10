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


def _hour_key(ts: float | None = None) -> str:
    """当前北京时间的小时桶键，如 2026-08-05-14"""
    dt = datetime.now(ZoneInfo("Asia/Shanghai"))
    if ts is not None:
        dt = datetime.fromtimestamp(ts, ZoneInfo("Asia/Shanghai"))
    return dt.strftime("%Y-%m-%d-%H")


def is_admin_key(key: dict) -> bool:
    return key.get("type") == "admin"


async def maybe_rotate_expired(key: dict) -> dict | None:
    """到期自动轮换密钥 secret：同一 key id 生成新 secret，旧 secret 进入宽限期仍可认证。

    - expire_seconds <= 0 表示永不过期，返回 None
    - 基准时间 base 取 rotated_at（轮换后重新计时），无轮换时退回 created_at
    - 未到期返回 None；已到期则轮换并返回轮换后的新记录（含新 secret）
    - 轮换后旧 secret 进入宽限期（expire_seconds 的 20%，下限 1h、上限 24h）
      仍可认证，实现客户端无感过渡；宽限期后旧 secret 彻底失效
    - 不依赖锁：并发下两请求同时判定过期时，rotate_key_secret 的 CAS
      （WHERE secret = ?）保证仅第一个真正轮换成功，第二个影响 0 行直接返回
      当前记录，旧 secret 宽限期不被二次覆盖丢失
    - 只写审计日志前缀（前 8 字符），绝不含明文 secret
    """
    expire_seconds = key.get("expire_seconds", 0)
    if not expire_seconds or expire_seconds <= 0:
        return None
    base = key.get("rotated_at") or key.get("created_at") or 0
    if base <= 0:
        return None
    if time.time() - base < expire_seconds:
        return None
    old_secret = key["secret"]
    new_secret = generate_key()
    grace = max(3600, min(int(expire_seconds * 0.2), 86400))
    rotated = await db.rotate_key_secret(key["id"], old_secret, new_secret, grace)
    if rotated:
        await db.log_key_rotation(key["id"], old_prefix=old_secret[:8], new_prefix=new_secret[:8], note="到期自动轮换")
    return await db.get_api_key_by_id(key["id"])


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
    """按用户 Key 的限额语义计量用量（管理员 Key / 无限额直接忽略），并写入小时桶记录。"""
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

    # 1h 粒度用量记录（供历史查询）
    await db.add_hourly_usage(key_id, _hour_key(), amount)


async def key_usage_summary(key: dict) -> dict:
    """供管理后台展示：当前窗口的有效已用量 / 限额 / 类型 / 是否超限。

    按限额语义归一化（后台逻辑，非仅前端显示）：
    - daily：日期变化即新周期，旧用量不显示（返回 0）
    - rolling_5h：5h 窗口过期即新周期，旧用量不显示（返回 0）
    - one_time：已失效时显示最终用量并标记 exhausted
    """
    if is_admin_key(key) or not _has_limit(key):
        return {"used": 0, "limit": 0, "token_type": "", "billing_mode": "token", "exhausted": False}
    state = await db.get_key_usage(key["id"])
    ttype = key.get("token_type", "")
    used = state.get("used_amount", 0) if state else 0
    now = time.time()
    window_reset = False
    if ttype == "daily":
        # 日期已变化 → 新的一天，用量归零展示
        if state is None or state.get("last_reset_date") != _today():
            used = 0
            window_reset = True
    elif ttype == "rolling_5h":
        # 窗口已过期 → 新窗口，用量归零展示
        ws = state.get("window_start") if state else None
        if ws is None or (now - ws) >= ROLLING_5H_SECONDS:
            used = 0
            window_reset = True
    elif ttype == "one_time":
        # 一次性：不归零（展示最终用量，失效标记由 exhausted 表达）
        pass
    ok, _ = await key_usage_available(key)
    return {
        "used": used,
        "limit": int(key.get("limit_amount", 0)),
        "token_type": ttype,
        "billing_mode": key.get("billing_mode", "token"),
        "exhausted": not ok,
        "window_reset": window_reset,
    }
