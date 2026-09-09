from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pathlib import Path
from shutil import copyfile
import logging
import database as db
from pool import load_config

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def db_maintenance():
    """低频 DB 维护（每 60s）：决策日志/校准样本批量裁剪 + RPM/TPM 内存滑窗清扫。
    v2.11.40 起裁剪从每笔写入的逐笔 DELETE 收敛至此（有界性不变，热路径少 2 条语句）。"""
    try:
        await db.trim_decision_log()
        await db.trim_call_metrics()
        db.sweep_req_windows()
    except Exception:
        logger.exception("[DB维护] 裁剪/清扫失败")


async def db_wal_checkpoint():
    """定期 WAL 截断（每 10 分钟）：持续读者使被动 checkpoint 长期完不成、WAL 单调膨胀
    （实测 4.2MB > 库本体 1.4MB），空闲时显式 TRUNCATE 回收。"""
    try:
        await db.wal_checkpoint()
    except Exception:
        logger.exception("[DB维护] WAL checkpoint 失败")


async def refresh_model(model_id: str):
    # 先同步 config 的 refresh_time 到 DB（防热加载后 DB 滞后）
    try:
        cfg = load_config()
        for m in cfg.get("models", []):
            if m.get("id") == model_id:
                rt = m.get("refresh_time", "")
                if rt:
                    await db.sync_model_refresh_time(model_id, rt)
                break
    except Exception:
        pass
    await db.reset_daily_usage(model_id)
    # 配额预检有 5s TTL 缓存：刷新后立即失效，避免刚重置的模型在缓存窗口内仍被判"用量已尽"
    try:
        from main import pool
        pool._invalidate_quota_cache(model_id)
    except Exception:
        pass


async def grant_gift_model(model_id: str):
    """余额返还制到账补账：balance = min(cap, balance + min(昨日自然日用量, cap))。

    具体补账逻辑在 db.get_gift_balance（惰性、逐日、可自愈）；此处仅定时触发，
    即便错过时点（网关重启/停机），下次预检读余额时也会按历史补齐。"""
    try:
        cfg = load_config()
        m = next((x for x in cfg.get("models", []) if x.get("id") == model_id), None)
        if not m:
            return
        cap = int(m.get("daily_token_limit", 0) or 0)
        rt = m.get("refresh_time", "")
        gc = int(m.get("gift_grant_cap", 0) or 0) or 5_000_000  # 每日返还上限（默认 500 万）
        balance = await db.get_gift_balance(model_id, cap, rt, gc)
        logger.info(f"[赠还补账] {model_id} 当前余额 {balance}（每日返还上限 {gc}）")
    except Exception:
        logger.exception(f"[赠还补账] {model_id} 失败")
    try:
        from main import pool
        pool._invalidate_quota_cache(model_id)
    except Exception:
        pass


async def sync_all_refresh_times(cfg: dict | None = None):
    """一次性把 config 中所有带 refresh_time 的模型同步到 token_usage 表。
    main.py 启动时调用，保证每个模型行都有正确的 refresh_time。"""
    if cfg is None:
        cfg = load_config()
    for m in cfg.get("models", []):
        if m.get("token_type", "daily") == "daily":
            rt = m.get("refresh_time", "")
            if rt:
                await db.sync_model_refresh_time(m["id"], rt)


CONFIG_PATH = Path(__file__).parent / "config.json"
BACKUP_PATH = Path(__file__).parent / "config.json.bak"


def backup_config():
    """每日备份 config.json -> config.json.bak（覆盖旧备份）。"""
    try:
        if CONFIG_PATH.exists():
            copyfile(CONFIG_PATH, BACKUP_PATH)
            logger.info(f"[BACKUP] config.json 已备份至 config.json.bak")
    except Exception as e:
        logger.error(f"[BACKUP-FAIL] 备份失败: {e}")


def _add_jobs():
    config = load_config()
    for m in config.get("models", []):
        if m.get("token_type", "daily") not in ("daily", "gift"):
            continue
        model_id = m["id"]
        if m.get("token_type") == "gift" and int(m.get("daily_token_limit", 0) or 0) > 0:
            # 余额返还制：refresh_time = 到账补账时刻（非清零重置）
            refresh_time = m.get("refresh_time", "")
            if not refresh_time:
                continue
            hour, minute = refresh_time.split(":")
            scheduler.add_job(
                grant_gift_model,
                CronTrigger(hour=int(hour), minute=int(minute), timezone=m.get("timezone", "UTC")),
                args=[model_id],
                id=f"gift_{model_id.replace('/', '_')}",
                replace_existing=True,
            )
            continue
        refresh_time = m.get("refresh_time", "")
        if not refresh_time:
            continue
        hour, minute = refresh_time.split(":")
        trigger = CronTrigger(
            hour=int(hour),
            minute=int(minute),
            timezone=m.get("timezone", "UTC"),
        )
        job_id = f"refresh_{model_id.replace('/', '_')}"
        scheduler.add_job(refresh_model, trigger, args=[model_id], id=job_id, replace_existing=True)

    scheduler.add_job(
        backup_config,
        CronTrigger(hour=14, minute=0, timezone="Asia/Shanghai"),
        id="daily_config_backup",
        replace_existing=True,
    )
    scheduler.add_job(
        db_maintenance,
        IntervalTrigger(seconds=60),
        id="db_maintenance",
        replace_existing=True,
    )
    scheduler.add_job(
        db_wal_checkpoint,
        IntervalTrigger(seconds=600),
        id="db_wal_checkpoint",
        replace_existing=True,
    )


def start_scheduler():
    _add_jobs()
    scheduler.start()


def restart_scheduler():
    scheduler.remove_all_jobs()
    _add_jobs()
