from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import database as db
from pool import load_config

scheduler = AsyncIOScheduler()


async def refresh_model(model_id: str):
    await db.reset_daily_usage(model_id)


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


def start_scheduler():
    _add_jobs()
    scheduler.start()


def restart_scheduler():
    scheduler.remove_all_jobs()
    _add_jobs()
