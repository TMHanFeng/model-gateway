from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import HTMLResponse
from pathlib import Path
from pool import load_config, save_config
from scheduler import restart_scheduler
from providers.openai_provider import OpenAIProvider
from providers.anthropic_provider import AnthropicProvider

router = APIRouter(prefix="/admin")

FRONTEND_PATH = Path(__file__).parent / "static" / "index.html"


def _sync_pool():
    """Reload in-memory pool after config changes so /v1/models stays current."""
    from main import pool
    pool.reload()


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
    pid = (body.get("provider_id") or "").strip()

    config = load_config()
    providers = config.get("providers", [])

    if pid:
        # provider-based model: protocol/base_url/api_key inherited from provider
        if not body.get("id") or not body.get("name"):
            raise HTTPException(status_code=400, detail="Missing field: id/name")
        if not any(p["id"] == pid for p in providers):
            raise HTTPException(status_code=400, detail=f"Provider '{pid}' not found")
    else:
        # legacy inline model: full connection info required
        for field in ["id", "name", "provider", "base_url", "api_key"]:
            if not body.get(field):
                raise HTTPException(status_code=400, detail=f"Missing field: {field}")

    models = config.get("models", [])
    raw_id = (body.get("id") or "").strip()
    if not raw_id:
        raise HTTPException(status_code=400, detail="Missing field: id")
    if "/" in raw_id or "\\" in raw_id:
        raise HTTPException(status_code=400, detail="模型ID中不能包含 / 或 \\ 字符")

    # When adding under a provider, prefix the model ID with provider name
    final_id = f"{pid}/{raw_id}" if pid else raw_id
    for m in models:
        if m["id"] == final_id:
            raise HTTPException(status_code=409, detail=f"模型ID '{final_id}' 已存在")

    entry = {
        "id": final_id,
        "name": body["name"],
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
    if pid:
        entry["provider_id"] = pid
    else:
        entry["provider"] = body["provider"]
        entry["base_url"] = body["base_url"]
        entry["api_key"] = body["api_key"]
    # one-time fields: save whenever any is provided, and auto-switch to one_time
    max_tokens_raw = body.get("max_tokens", 0)
    ttl_raw = body.get("ttl_seconds", 0)
    exp_raw = body.get("expire_date", "")
    has_once_fields = (
        max_tokens_raw not in (None, "", 0)
        or ttl_raw not in (None, "", 0)
        or bool(exp_raw)
    )
    if body.get("token_type") == "one_time" or has_once_fields:
        entry["token_type"] = "one_time"
        entry["max_tokens"] = int(max_tokens_raw or 0)
        entry["ttl_seconds"] = int(ttl_raw or 0)
        if exp_raw:
            entry["expire_date"] = exp_raw

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

    pid = body.get("provider_id")
    if pid and not any(p["id"] == pid for p in config.get("providers", [])):
        raise HTTPException(status_code=400, detail=f"Provider '{pid}' not found")

    for key, value in body.items():
        if key == "id":
            continue
        if key == "timezone":
            continue
        if key == "is_free":
            value = bool(value)
        models[idx][key] = value
    models[idx]["timezone"] = "Asia/Shanghai"

    # provider-based and inline connection info are mutually exclusive
    if models[idx].get("provider_id"):
        for k in ("provider", "base_url", "api_key"):
            models[idx].pop(k, None)

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


@router.get("/providers")
async def get_providers(_=Depends(verify_admin)):
    config = load_config()
    return {"providers": config.get("providers", [])}


@router.post("/providers")
async def add_provider(request: Request, _=Depends(verify_admin)):
    body = await request.json()
    for field in ["id", "name", "protocol", "base_url", "api_key"]:
        if not body.get(field):
            raise HTTPException(status_code=400, detail=f"Missing field: {field}")
    if body["protocol"] not in ("openai", "anthropic"):
        raise HTTPException(status_code=400, detail="protocol must be 'openai' or 'anthropic'")

    config = load_config()
    providers = config.setdefault("providers", [])
    for p in providers:
        if p["id"] == body["id"]:
            raise HTTPException(status_code=409, detail=f"Provider id '{body['id']}' already exists")

    entry = {
        "id": body["id"],
        "name": body["name"],
        "protocol": body["protocol"],
        "base_url": body["base_url"],
        "api_key": body["api_key"],
    }
    providers.append(entry)
    config["providers"] = providers
    save_config(config)
    restart_scheduler()
    return {"ok": True, "provider": entry}


@router.put("/providers/{provider_id:path}")
async def update_provider(provider_id: str, request: Request, _=Depends(verify_admin)):
    body = await request.json()
    config = load_config()
    providers = config.get("providers", [])
    idx = None
    for i, p in enumerate(providers):
        if p["id"] == provider_id:
            idx = i
            break
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Provider '{provider_id}' not found")

    if "protocol" in body and body["protocol"] not in ("openai", "anthropic"):
        raise HTTPException(status_code=400, detail="protocol must be 'openai' or 'anthropic'")
    for key, value in body.items():
        if key == "id":
            continue
        providers[idx][key] = value

    config["providers"] = providers
    save_config(config)
    restart_scheduler()
    return {"ok": True, "provider": providers[idx]}


@router.put("/providers/reorder")
async def reorder_providers(request: Request, _=Depends(verify_admin)):
    """Reorder providers. Body: {provider_ids: ['id1', 'id2', ...]}"""
    body = await request.json()
    new_order = body.get("provider_ids", [])
    if not isinstance(new_order, list):
        raise HTTPException(status_code=400, detail="provider_ids must be a list")

    config = load_config()
    providers = config.get("providers", [])
    existing_ids = {p["id"] for p in providers}
    if set(new_order) != existing_ids:
        raise HTTPException(status_code=400, detail="provider_ids must contain exactly all existing provider IDs")

    # Rebuild providers list in new order
    by_id = {p["id"]: p for p in providers}
    config["providers"] = [by_id[pid] for pid in new_order]
    save_config(config)
    restart_scheduler()
    return {"ok": True}


@router.delete("/providers/{provider_id:path}")
async def delete_provider(provider_id: str, _=Depends(verify_admin)):
    config = load_config()
    providers = config.get("providers", [])
    new_providers = [p for p in providers if p["id"] != provider_id]
    if len(new_providers) == len(providers):
        raise HTTPException(status_code=404, detail=f"Provider '{provider_id}' not found")

    refs = [m["id"] for m in config.get("models", []) if m.get("provider_id") == provider_id]
    if refs:
        raise HTTPException(status_code=400, detail=f"供应商下仍有模型：{', '.join(refs)}，请先删除或迁移这些模型")

    config["providers"] = new_providers
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
    if "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="池名称不能包含 / 或 \\ 字符")

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
    _sync_pool()
    return {"ok": True, "pool": pools[name]}


@router.delete("/pools/{pool_name:path}")
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
    _sync_pool()
    return {"ok": True}


@router.put("/pools/{pool_name:path}")
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
    _sync_pool()
    return {"ok": True, "pool": pools[pool_name]}


@router.post("/pools/{pool_name:path}/rename")
async def rename_pool(pool_name: str, request: Request, _=Depends(verify_admin)):
    body = await request.json()
    new_name = (body.get("new_name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Missing new_name")
    if "/" in new_name or "\\" in new_name:
        raise HTTPException(status_code=400, detail="池名称不能包含 / 或 \\ 字符")
    if new_name == pool_name:
        return {"ok": True, "pool_name": pool_name}

    config = load_config()
    pools = config.get("pools", {})
    if pool_name not in pools:
        raise HTTPException(status_code=404, detail=f"Pool '{pool_name}' not found")
    if new_name in pools:
        raise HTTPException(status_code=409, detail=f"Pool '{new_name}' already exists")

    # Move pool data to new key
    pools[new_name] = pools.pop(pool_name)

    # Migrate pool:old_name references in all other pools
    old_ref = f"pool:{pool_name}"
    new_ref = f"pool:{new_name}"
    for pname, pcfg in pools.items():
        ids = pcfg.get("model_ids", [])
        if old_ref in ids:
            pcfg["model_ids"] = [new_ref if x == old_ref else x for x in ids]

    save_config(config)
    restart_scheduler()
    _sync_pool()
    return {"ok": True, "pool_name": new_name}


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


@router.post("/test_model")
async def test_model(request: Request, _=Depends(verify_admin)):
    """Lightweight connectivity test using the form's current values (not saved)."""
    body = await request.json()
    base_url = (body.get("base_url") or "").strip()
    api_key = (body.get("api_key") or "").strip()
    protocol = body.get("protocol") or body.get("provider") or "openai"
    model_name = (body.get("model_name") or body.get("name") or "").strip()
    if not base_url or not api_key or not model_name:
        raise HTTPException(400, "缺少 base_url / api_key / model_name")
    if protocol not in ("openai", "anthropic"):
        raise HTTPException(400, "protocol 必须为 openai 或 anthropic")

    if protocol == "anthropic":
        provider = AnthropicProvider(base_url, api_key)
    else:
        provider = OpenAIProvider(base_url, api_key)
    import database as db
    try:
        result = await provider.speedtest(model_name)
        # Record usage for stats even on test calls
        if result.get("status") == "ok" and result.get("tokens", 0) > 0:
            test_id = f"__test__{model_name}"
            await db.add_daily_usage(test_id, result["tokens"])
        return {"ok": True, "result": result}
    finally:
        await provider.close()
