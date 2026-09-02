from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path
from shutil import copyfile
import logging
import database as db
from pool import load_config

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


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
        if m.get("token_type", "daily") != "daily":
            continue
        refresh_time = m.get("refresh_time", "")
        if not refresh_time:
            continue
        model_id = m["id"]
        timezone = m.get("timezone", "UTC")
        hour, minute = refresh_time.split(":")
        trigger = CronTrigger(
            hour=int(hour),
            minute=int(minute),
            timezone=timezone,
        )
        job_id = f"refresh_{model_id.replace('/', '_')}"
        scheduler.add_job(refresh_model, trigger, args=[model_id], id=job_id, replace_existing=True)

    scheduler.add_job(
        backup_config,
        CronTrigger(hour=14, minute=0, timezone="Asia/Shanghai"),
        id="daily_config_backup",
        replace_existing=True,
    )


def start_scheduler():
    _add_jobs()
    scheduler.start()


def restart_scheduler():
    scheduler.remove_all_jobs()
    _add_jobs()
