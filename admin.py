from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import HTMLResponse
from pathlib import Path
from pool import load_config, save_config
from scheduler import restart_scheduler

router = APIRouter(prefix="/admin")

FRONTEND_PATH = Path(__file__).parent / "static" / "index.html"


def verify_admin(request: Request):
    config = load_config()
    expected = config.get("server", {}).get("api_key", "")
    if not expected:
        return
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        auth = auth[7:]
    if auth != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


@router.get("/", response_class=HTMLResponse)
async def admin_page():
    return FRONTEND_PATH.read_text(encoding="utf-8")


@router.get("/models")
async def get_models(_=Depends(verify_admin)):
    config = load_config()
    return {"models": config.get("models", [])}


@router.post("/models")
async def add_model(request: Request, _=Depends(verify_admin)):
    body = await request.json()
    required = ["id", "name", "provider", "base_url", "api_key"]
    for field in required:
        if not body.get(field):
            raise HTTPException(status_code=400, detail=f"Missing field: {field}")

    config = load_config()
    models = config.get("models", [])
    for m in models:
        if m["id"] == body["id"]:
            raise HTTPException(status_code=409, detail=f"Model id '{body['id']}' already exists")

    entry = {
        "id": body["id"],
        "name": body["name"],
        "provider": body["provider"],
        "base_url": body["base_url"],
        "api_key": body["api_key"],
        "daily_token_limit": body.get("daily_token_limit", 0),
        "rpm_limit": body.get("rpm_limit", 0),
        "tpm_limit": body.get("tpm_limit", 0),
        "token_type": body.get("token_type", "daily"),
        "refresh_time": body.get("refresh_time", ""),
        "timezone": "Asia/Shanghai",
        "context_window": body.get("context_window", 0),
        "max_concurrency": body.get("max_concurrency", 10),
        "billing_mode": body.get("billing_mode", "token"),
        "is_free": bool(body.get("is_free", False)),
        "modality": body.get("modality", "text"),
    }
    if body.get("token_type") == "one_time":
        entry["max_tokens"] = body.get("max_tokens", 0)
        entry["ttl_seconds"] = body.get("ttl_seconds", 0)

    models.append(entry)
    config["models"] = models
    save_config(config)
    restart_scheduler()
    return {"ok": True, "model": entry}


@router.put("/models/{model_id:path}")
async def update_model(model_id: str, request: Request, _=Depends(verify_admin)):
    body = await request.json()
    config = load_config()
    models = config.get("models", [])

    idx = None
    for i, m in enumerate(models):
        if m["id"] == model_id:
            idx = i
            break
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Model '{model_id}' not found")

    for key, value in body.items():
        if key == "id":
            continue
        if key == "timezone":
            continue
        if key == "is_free":
            value = bool(value)
        models[idx][key] = value
    models[idx]["timezone"] = "Asia/Shanghai"

    config["models"] = models
    save_config(config)
    restart_scheduler()
    return {"ok": True, "model": models[idx]}


@router.delete("/models/{model_id:path}")
async def delete_model(model_id: str, _=Depends(verify_admin)):
    config = load_config()
    models = config.get("models", [])
    new_models = [m for m in models if m["id"] != model_id]
    if len(new_models) == len(models):
        raise HTTPException(status_code=404, detail=f"Model '{model_id}' not found")

    config["models"] = new_models
    for pool_name, pool_cfg in config.get("pools", {}).items():
        pool_cfg["model_ids"] = [mid for mid in pool_cfg.get("model_ids", []) if mid != model_id]

    save_config(config)
    restart_scheduler()
    return {"ok": True}


@router.get("/pools")
async def get_pools(_=Depends(verify_admin)):
    config = load_config()
    return {"pools": config.get("pools", {})}


@router.post("/pools")
async def create_pool(request: Request, _=Depends(verify_admin)):
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Missing pool name")

    config = load_config()
    pools = config.setdefault("pools", {})
    if name in pools:
        raise HTTPException(status_code=409, detail=f"Pool '{name}' already exists")

    pools[name] = {
        "strategy": body.get("strategy", "sequential"),
        "model_ids": body.get("model_ids", []),
        "auto_order": bool(body.get("auto_order", False)),
    }
    save_config(config)
    restart_scheduler()
    return {"ok": True, "pool": pools[name]}


@router.delete("/pools/{pool_name}")
async def delete_pool(pool_name: str, _=Depends(verify_admin)):
    if pool_name == "auto":
        raise HTTPException(status_code=400, detail="Auto pool cannot be deleted")

    config = load_config()
    pools = config.get("pools", {})
    if pool_name not in pools:
        raise HTTPException(status_code=404, detail=f"Pool '{pool_name}' not found")

    del pools[pool_name]
    save_config(config)
    restart_scheduler()
    return {"ok": True}


@router.put("/pools/{pool_name}")
async def update_pool(pool_name: str, request: Request, _=Depends(verify_admin)):
    body = await request.json()
    model_ids = body.get("model_ids")
    if model_ids is None:
        raise HTTPException(status_code=400, detail="Missing model_ids")

    config = load_config()
    pools = config.setdefault("pools", {})
    if pool_name not in pools:
        pools[pool_name] = {}
    pools[pool_name]["model_ids"] = model_ids
    if "strategy" in body:
        pools[pool_name]["strategy"] = body["strategy"]
    if "auto_order" in body:
        pools[pool_name]["auto_order"] = bool(body["auto_order"])

    save_config(config)
    restart_scheduler()
    return {"ok": True, "pool": pools[pool_name]}


@router.post("/reload")
async def reload_pool(request: Request, _=Depends(verify_admin)):
    from main import pool
    pool.reload()
    restart_scheduler()
    return {"ok": True}


@router.get("/decisions")
async def get_decisions(pool: str | None = None, limit: int = 100, _=Depends(verify_admin)):
    import database as db
    rows = await db.get_decisions(pool_name=pool, limit=min(limit, 500))
    return {"decisions": rows}
