from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import HTMLResponse
from pathlib import Path
import subprocess
from pool import load_config, save_config
from scheduler import restart_scheduler
from providers.openai_provider import OpenAIProvider
from providers.anthropic_provider import AnthropicProvider

router = APIRouter(prefix="/admin")

FRONTEND_PATH = Path(__file__).parent / "static" / "index.html"
REPO_DIR = Path(__file__).parent


def _read_git_version() -> tuple[str, str]:
    version = ""
    commit = ""
    try:
        r = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            capture_output=True, text=True, timeout=5, cwd=str(REPO_DIR),
        )
        if r.returncode == 0:
            version = r.stdout.strip()
    except Exception:
        pass
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, cwd=str(REPO_DIR),
        )
        if r.returncode == 0:
            commit = r.stdout.strip()
    except Exception:
        pass
    return version, commit


# 进程启动时缓存一次，反映"真正运行的代码"版本（避免 git pull 后磁盘已更新、
# 但进程仍是旧代码时误报新版本）
GATEWAY_VERSION, GATEWAY_COMMIT = _read_git_version()


def get_gateway_version() -> str:
    """获取网关版本（进程启动时缓存的版本 + commit），失败回退 unknown"""
    if GATEWAY_VERSION and GATEWAY_COMMIT:
        return f"{GATEWAY_VERSION} · {GATEWAY_COMMIT}"
    return GATEWAY_VERSION or GATEWAY_COMMIT or "unknown"


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
    html = FRONTEND_PATH.read_text(encoding="utf-8")
    return html.replace("__GATEWAY_VERSION__", get_gateway_version())


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
        "max_concurrency": body.get("max_concurrency", 0),
        "billing_mode": body.get("billing_mode", "token"),
        "is_free": bool(body.get("is_free", True)),
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


@router.put("/models/reorder")
async def reorder_models(request: Request, _=Depends(verify_admin)):
    """Reorder models. Body: {model_ids: [...]} must contain exactly all existing model IDs."""
    body = await request.json()
    new_order = body.get("model_ids", [])
    if not isinstance(new_order, list):
        raise HTTPException(status_code=400, detail="model_ids must be a list")

    config = load_config()
    models = config.get("models", [])
    existing_ids = {m["id"] for m in models}
    if set(new_order) != existing_ids:
        missing = existing_ids - set(new_order)
        extra = set(new_order) - existing_ids
        raise HTTPException(
            status_code=400,
            detail=f"model_ids must contain exactly all existing model IDs. Missing: {sorted(missing)}, Extra: {sorted(extra)}",
        )

    by_id = {m["id"]: m for m in models}
    config["models"] = [by_id[mid] for mid in new_order]
    save_config(config)
    restart_scheduler()
    _sync_pool()
    return {"ok": True}

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

    # Also clear any single_override that points to the deleted model
    if "single_override" in config:
        config["single_override"] = {
            k: v for k, v in config["single_override"].items()
            if v != model_id
        }

    save_config(config)
    restart_scheduler()
    _sync_pool()
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
        "proxy_url": body.get("proxy_url", ""),
    }
    providers.append(entry)
    config["providers"] = providers
    save_config(config)
    restart_scheduler()
    _sync_pool()
    return {"ok": True, "provider": entry}


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
    _sync_pool()
    return {"ok": True, "provider": providers[idx]}


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
    from main import pool
    # Return from in-memory pool.pools (includes auto-created 兜底池).
    # pool_order 保留 Python dict 的插入顺序：JS 的 Object.keys 会把纯数字池名
    # （如 "123"）按整数键规则提到最前，打破预期顺序，故显式下发有序数组。
    return {"pools": pool.pools, "pool_order": list(pool.pools.keys())}


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
    if body.get("owner_key_id"):
        raise HTTPException(status_code=400, detail="专属模型池请在密钥编辑界面创建")

    pools[name] = {
        "strategy": body.get("strategy", "sequential"),
        "model_ids": body.get("model_ids", []),
        "auto_order": bool(body.get("auto_order", False)),
        "load_balance": bool(body.get("load_balance", False)),
    }
    # 互斥：auto_order 与 load_balance 不可同时开启
    if pools[name]["load_balance"]:
        pools[name]["auto_order"] = False
    if body.get("fallback_pool"):
        pools[name]["fallback_pool"] = body["fallback_pool"]
    save_config(config)
    restart_scheduler()
    _sync_pool()
    return {"ok": True, "pool": pools[name]}


@router.delete("/pools/{pool_name}")
async def delete_pool(pool_name: str, _=Depends(verify_admin)):
    if pool_name in ("auto", "兜底池"):
        raise HTTPException(status_code=400, detail=f"Pool '{pool_name}' cannot be deleted")

    config = load_config()
    pools = config.get("pools", {})
    if pool_name not in pools:
        raise HTTPException(status_code=404, detail=f"Pool '{pool_name}' not found")
    if pools[pool_name].get("owner_key_id"):
        raise HTTPException(status_code=400, detail="专属模型池请在密钥编辑界面删除")

    del pools[pool_name]
    save_config(config)
    restart_scheduler()
    _sync_pool()
    return {"ok": True}


@router.put("/pools/reorder")
async def reorder_pools(request: Request, _=Depends(verify_admin)):
    """Reorder pools (body: {pool_names: ["pool1", "pool2", ...]}). auto always first."""
    body = await request.json()
    pool_names = body.get("pool_names", [])
    if not isinstance(pool_names, list):
        raise HTTPException(status_code=400, detail="pool_names must be a list")

    config = load_config()
    pools = config.get("pools", {})

    # 防御：pool_names 必须包含全部现有池（除 auto/兜底池），防止不完整列表误删池
    existing_others = {n for n in pools if n not in ("auto", "兜底池")}
    provided_others = {n for n in pool_names if n in pools and n not in ("auto", "兜底池")}
    missing = existing_others - provided_others
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"pool_names 不完整，缺少 {len(missing)} 个池: {sorted(missing)}；拒绝重排以防误删",
        )

    # auto must always be first; 兜底池 goes last
    ordered = ["auto"]
    for name in pool_names:
        if name not in ("auto", "兜底池") and name in pools:
            ordered.append(name)
    ordered.append("兜底池")

    # Rewrite pools dict in new order
    new_pools = {}
    for name in ordered:
        if name in pools:
            new_pools[name] = pools[name]
    config["pools"] = new_pools
    save_config(config)
    _sync_pool()
    return {"ok": True, "pools": list(new_pools.keys())}


@router.put("/pools/fallback_targets")
async def set_fallback_targets(request: Request, _=Depends(verify_admin)):
    """设置兜底池为哪些池兜底。Body: {pool_names: [...]} — 这些池的 fallback_pool 将指向兜底池；
    未列出的非 auto 池若当前指向兜底池则清空。auto 池强制兜底（不可取消）。"""
    body = await request.json()
    pool_names = body.get("pool_names", [])
    if not isinstance(pool_names, list):
        raise HTTPException(status_code=400, detail="pool_names must be a list")

    config = load_config()
    pools = config.get("pools", {})
    valid = {n for n in pool_names if n in pools and n != "auto" and n != "兜底池"}
    if set(pool_names) - valid - {"auto", "兜底池"}:
        missing = sorted(set(pool_names) - valid - {"auto", "兜底池"})
        raise HTTPException(status_code=400, detail=f"未知池名: {missing}")

    changed = False
    for pname in pools:
        if pname == "auto":
            # auto 池强制兜底
            if pools[pname].get("fallback_pool") != "兜底池":
                pools[pname]["fallback_pool"] = "兜底池"
                changed = True
        elif pname == "兜底池":
            continue
        else:
            target = "兜底池" if pname in valid else None
            cur = pools[pname].get("fallback_pool")
            if target is None and cur == "兜底池":
                pools[pname]["fallback_pool"] = None
                changed = True
            elif target == "兜底池" and cur != "兜底池":
                pools[pname]["fallback_pool"] = "兜底池"
                changed = True

    if changed:
        save_config(config)
        _sync_pool()
    return {"ok": True, "targets": sorted(valid)}


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
    if "load_balance" in body:
        pools[pool_name]["load_balance"] = bool(body["load_balance"])
    # 互斥：auto_order 与 load_balance 不可同时开启（后开者优先生效，另一者自动关闭）
    if pools[pool_name].get("load_balance"):
        pools[pool_name]["auto_order"] = False
    elif pools[pool_name].get("auto_order"):
        pools[pool_name]["load_balance"] = False
    if "slow_latency_threshold" in body:
        try:
            pools[pool_name]["slow_latency_threshold"] = int(body["slow_latency_threshold"])
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="slow_latency_threshold 必须为数字（0=不限）")

    save_config(config)
    restart_scheduler()
    _sync_pool()
    return {"ok": True, "pool": pools[pool_name]}


@router.post("/pools/{pool_name}/rename")
async def rename_pool(pool_name: str, request: Request, _=Depends(verify_admin)):
    if pool_name in ("auto", "兜底池"):
        raise HTTPException(status_code=400, detail=f"Pool '{pool_name}' cannot be renamed")
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
    if pools[pool_name].get("owner_key_id"):
        raise HTTPException(status_code=400, detail="专属模型池不允许重命名")
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


@router.post("/pools/{pool_name}/single_override")
async def set_single_override(pool_name: str, request: Request, _=Depends(verify_admin)):
    """Lock a pool to use only one model. Body: {model_id: "..."}"""
    body = await request.json()
    model_id = (body.get("model_id") or "").strip()
    if not model_id:
        raise HTTPException(status_code=400, detail="Missing model_id")

    from main import pool
    if pool_name not in pool.pools:
        raise HTTPException(status_code=404, detail=f"Pool '{pool_name}' not found")
    if model_id not in pool.registry:
        raise HTTPException(status_code=400, detail=f"Model '{model_id}' not found in registry")

    # Verify model is reachable from this pool (directly or via sub-pools)
    reachable = pool._collect_pool_models(pool_name)
    if model_id not in reachable:
        raise HTTPException(status_code=400, detail=f"Model '{model_id}' is not reachable from pool '{pool_name}'")

    pool.set_single_override(pool_name, model_id)
    return {"ok": True, "pool_name": pool_name, "model_id": model_id}


@router.delete("/pools/{pool_name}/single_override")
async def clear_single_override(pool_name: str, _=Depends(verify_admin)):
    """Release single-model lock, return to normal pool behavior."""
    from main import pool
    if pool_name not in pool.pools:
        raise HTTPException(status_code=404, detail=f"Pool '{pool_name}' not found")

    model_id = pool.clear_single_override(pool_name)
    return {"ok": True, "pool_name": pool_name, "cleared": model_id is not None}


@router.get("/pools/{pool_name}/single_override")
async def get_single_override(pool_name: str, _=Depends(verify_admin)):
    """Get current single-model override for a pool."""
    from main import pool
    if pool_name not in pool.pools:
        raise HTTPException(status_code=404, detail=f"Pool '{pool_name}' not found")

    model_id = pool.single_override.get(pool_name)
    return {"ok": True, "pool_name": pool_name, "model_id": model_id}


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


@router.post("/test_proxy")
async def test_proxy(request: Request, _=Depends(verify_admin)):
    """测试供应商代理连通性 + 拉取 Clash/Mihomo 当前节点。

    Body: {proxy_url, base_url}（都要）。
    通过 proxy 请求 base_url 测延迟；再从 Clash 控制 API (http://127.0.0.1:9090)
    读 GLOBAL.now 获取当前选中节点名。控制 API 不可达时 node 返回 null，不影响延迟结果。
    """
    import httpx as _httpx
    import time as _time

    body = await request.json()
    proxy_url = (body.get("proxy_url") or "").strip()
    base_url = (body.get("base_url") or "").strip()
    if not proxy_url or not base_url:
        raise HTTPException(status_code=400, detail="缺少 proxy_url / base_url")

    result = {"ok": False, "latency_ms": None, "node": None, "error": None}

    # 1. 通过代理拉 base_url 测延迟（不核验 TLS cert，能连通即算通）
    try:
        async with _httpx.AsyncClient(
            proxy=proxy_url,
            verify=False,
            timeout=_httpx.Timeout(15, connect=10),
        ) as client:
            t0 = _time.perf_counter()
            # 请求一个轻量端点判断连通性：/ 或 /v1/models 都行，401 也算通
            resp = await client.get(base_url.rstrip("/") + "/", follow_redirects=True)
            latency_ms = round((_time.perf_counter() - t0) * 1000, 1)
            result["ok"] = resp.status_code < 500  # <500 视为连通（401/403/404 都算代理通）
            result["latency_ms"] = latency_ms
            if resp.status_code >= 500:
                result["error"] = f"上游 HTTP {resp.status_code}"
            # 记录测试请求的 host，用于查真实出口节点
            try:
                from urllib.parse import urlparse
                result["_test_host"] = urlparse(base_url).hostname or ""
            except Exception:
                result["_test_host"] = ""
            # ★ Fix A: 在 client 仍存活时（连接池未关）解析 Clash 当前节点。
            # 延迟到 async with 退出后查 /connections，live chain 已消失，Step A 永远扑空。
            try:
                await _fetch_clash_node(result, _httpx)
            except Exception:
                pass
    except Exception as e:
        result["error"] = type(e).__name__ + ": " + str(e)[:160]
        return result

    return result


async def _fetch_clash_node(result: dict, _httpx, probe_host: str = ""):
    """Resolve the REAL active Clash node.

    1) Query /connections, find the live tunnel whose metadata.host matches our test
       host, take chains[0] (the actual node). This works in rule mode too.
    2) Fallback: prefer the user's master selector (e.g. "🌏 当前选择") — follow its
       `now` recursively (up to 3 hops) until we resolve to a real node, so nested
       groups like "🌏 当前选择 → 🇸🇬 新加坡自动 → TG-SG-1(hysteria)" map to the
       actual exit node instead of the first non-DIRECT group in JSON order
       (which is unreliable when multiple regions are configured).
       Only if the master group is missing do we fall back to the generic
       any-Selector/URLTest/Fallback scan.
    3) Last resort: GLOBAL.now (global mode only).
    """
    # 优先使用显式参数；兼容旧调用方（仅传 result 时回退到 result["_test_host"]）
    probe_host = probe_host or result.get("_test_host") or ""

    for port in (9097, 9090, 9091, 9098, 9898, 6170, 6189):
        base = f"http://127.0.0.1:{port}"
        try:
            async with _httpx.AsyncClient(timeout=1.5) as cc:
                # Step A: live connection match
                r = await cc.get(f"{base}/connections")
                if r.status_code == 200:
                    conns = r.json().get("connections", [])
                    for c in conns:
                        chains = c.get("chains") or []
                        meta = c.get("metadata") or {}
                        if probe_host and meta.get("host") == probe_host and chains:
                            node = chains[0]
                            if node and node.upper() != "DIRECT":
                                result["node"] = node
                                result["node_clash_port"] = port
                                return

                # Step B: 优先解析用户主选择组，下钻至真实节点
                r2 = await cc.get(f"{base}/proxies")
                if r2.status_code == 200:
                    proxies = r2.json().get("proxies", {})
                    master_keys = ("🌏 当前选择", "🌍 当前选择", "当前选择")
                    master_found = False
                    for mk in master_keys:
                        if mk in proxies:
                            master_found = True
                            target = mk
                            visited = set()
                            # 最多下钻 3 层（master → 区域自动组 → 节点）
                            for _hop in range(3):
                                if target in visited:
                                    break
                                visited.add(target)
                                grp = proxies.get(target) or {}
                                now = grp.get("now")
                                if not now or now.upper() == "DIRECT":
                                    break
                                sub = proxies.get(now)
                                if sub and sub.get("type") in ("Selector", "URLTest", "Fallback"):
                                    target = now
                                    continue
                                # now 指向真实节点（或非已知的 Selector）
                                result["node"] = now
                                result["node_clash_port"] = port
                                return
                            # 找到主选择组但解析不出节点 → 不再走原循环兜底，
                            # 避免被同 JSON 顺序中的另一个区域组（如"🇨🇳 台湾自动"）干扰
                            break
                    if not master_found:
                        # 没有任何主选择组 → 走原循环兜底
                        for pname, p in proxies.items():
                            if p.get("type") in ("Selector", "URLTest", "Fallback"):
                                now = p.get("now")
                                if now and now.upper() != "DIRECT" and now != pname:
                                    result["node"] = now
                                    result["node_clash_port"] = port
                                    return
        except Exception:
            continue

    # Step C: GLOBAL.now (only meaningful in global mode)
    try:
        async with _httpx.AsyncClient(timeout=1.5) as cc:
            for port in (9097, 9090, 9091):
                try:
                    r = await cc.get(f"http://127.0.0.1:{port}/proxies")
                    if r.status_code == 200:
                        gl = r.json().get("proxies", {}).get("GLOBAL") or {}
                        if gl.get("now"):
                            result["node"] = gl["now"]
                            result["node_clash_port"] = port
                            return
                except Exception:
                    continue
    except Exception:
        pass


# ── API Key 管理 ─────────────────────────────────────────────────────

@router.get("/keys")
async def list_keys(_=Depends(verify_admin)):
    import database as db
    import keyauth as ka
    keys = await db.list_api_keys()
    config = load_config()
    pools = config.get("pools", {})
    pool_by_key = {}
    for pname, pcfg in pools.items():
        oid = pcfg.get("owner_key_id")
        if oid:
            pool_by_key[str(oid)] = pname
    out = []
    for k in keys:
        summary = await ka.key_usage_summary(k)
        out.append({**k, "usage": summary, "own_pool": pool_by_key.get(str(k["id"]))})
    return {"keys": out}


@router.post("/keys")
async def create_key(request: Request, _=Depends(verify_admin)):
    import database as db
    import keyauth as ka
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="缺少密钥名称")
    ktype = body.get("type", "user")
    if ktype not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="type 必须为 admin 或 user")
    allowed_pools = body.get("allowed_pools") or []
    if not isinstance(allowed_pools, list):
        raise HTTPException(status_code=400, detail="allowed_pools 必须为数组")
    token_type = body.get("token_type", "")
    if token_type not in ("", "daily", "rolling_5h", "one_time"):
        raise HTTPException(status_code=400, detail="token_type 非法")
    billing_mode = body.get("billing_mode", "token")
    if billing_mode not in ("token", "request"):
        raise HTTPException(status_code=400, detail="billing_mode 必须为 token 或 request")
    try:
        limit_amount = int(body.get("limit_amount") or 0)
    except ValueError:
        raise HTTPException(status_code=400, detail="limit_amount 必须为数字")
    if limit_amount < 0:
        raise HTTPException(status_code=400, detail="limit_amount 不能为负")
    try:
        expire_seconds = int(body.get("expire_seconds") or 0)
    except ValueError:
        raise HTTPException(status_code=400, detail="expire_seconds 必须为数字")
    if expire_seconds < 0:
        raise HTTPException(status_code=400, detail="expire_seconds 不能为负")
    if ktype == "admin":
        token_type, billing_mode, limit_amount, expire_seconds = "", "token", 0, 0
        allowed_pools = []

    secret = ka.generate_key()
    key_id = await db.create_api_key(name, secret, ktype, allowed_pools,
                                     token_type, billing_mode, limit_amount,
                                     expire_seconds=expire_seconds)
    rec = await db.get_api_key_by_id(key_id)
    return {"ok": True, "key": rec}


@router.put("/keys/{key_id}")
async def update_key(key_id: int, request: Request, _=Depends(verify_admin)):
    import database as db
    body = await request.json()
    rec0 = await db.get_api_key_by_id(key_id)
    if not rec0:
        raise HTTPException(status_code=404, detail=f"API Key #{key_id} 不存在")
    fields = {}
    if "name" in body:
        nm = (body["name"] or "").strip()
        if nm:
            fields["name"] = nm
    if "type" in body:
        if body["type"] not in ("admin", "user"):
            raise HTTPException(status_code=400, detail="type 必须为 admin 或 user")
        fields["type"] = body["type"]
    if "enabled" in body:
        fields["enabled"] = 1 if body["enabled"] else 0
    if "allowed_pools" in body:
        if not isinstance(body["allowed_pools"], list):
            raise HTTPException(status_code=400, detail="allowed_pools 必须为数组")
        fields["allowed_pools"] = body["allowed_pools"]
    if "token_type" in body:
        if body["token_type"] not in ("", "daily", "rolling_5h", "one_time"):
            raise HTTPException(status_code=400, detail="token_type 非法")
        fields["token_type"] = body["token_type"]
    if "billing_mode" in body:
        if body["billing_mode"] not in ("token", "request"):
            raise HTTPException(status_code=400, detail="billing_mode 必须为 token 或 request")
        fields["billing_mode"] = body["billing_mode"]
    if "limit_amount" in body:
        try:
            v = int(body["limit_amount"] or 0)
        except ValueError:
            raise HTTPException(status_code=400, detail="limit_amount 必须为数字")
        if v < 0:
            raise HTTPException(status_code=400, detail="limit_amount 不能为负")
        fields["limit_amount"] = v
    if "expire_seconds" in body:
        try:
            v = int(body["expire_seconds"] or 0)
        except ValueError:
            raise HTTPException(status_code=400, detail="expire_seconds 必须为数字")
        if v < 0:
            raise HTTPException(status_code=400, detail="expire_seconds 不能为负")
        cur_v = rec0.get("expire_seconds") or 0
        if v != cur_v:
            fields["expire_seconds"] = v
            # 过期时长被修改时重置轮换时间，从 created_at 重新计时新周期
            fields["rotated_at"] = None
    # 管理员 Key 强制无限制
    if fields.get("type") == "admin":
        fields["allowed_pools"] = []
        fields["token_type"] = ""
        fields["limit_amount"] = 0
        fields["expire_seconds"] = 0
        fields["rotated_at"] = None
    if not fields:
        raise HTTPException(status_code=400, detail="没有可更新的字段")
    await db.update_api_key(key_id, fields)
    rec = await db.get_api_key_by_id(key_id)
    return {"ok": True, "key": rec}


@router.delete("/keys/{key_id}")
async def delete_key(key_id: int, _=Depends(verify_admin)):
    import database as db
    rec = await db.get_api_key_by_id(key_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"API Key #{key_id} 不存在")
    # 同时清理该密钥的专属模型池及引用
    config = load_config()
    pools = config.get("pools", {})
    targets = [n for n, p in pools.items() if p.get("owner_key_id") == str(key_id)]
    for t in targets:
        del pools[t]
        for pcfg in pools.values():
            pcfg["model_ids"] = [x for x in pcfg.get("model_ids", []) if x != t and x != f"pool:{t}"]
    if targets:
        save_config(config)
    await db.delete_api_key(key_id)
    _sync_pool()
    return {"ok": True}


@router.get("/keys/{key_id}/rotations")
async def get_key_rotations(key_id: int, _=Depends(verify_admin)):
    import database as db
    rec = await db.get_api_key_by_id(key_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"API Key #{key_id} 不存在")
    return {"rotations": await db.get_key_rotations(key_id)}


# ── 用户专属模型池（仅密钥编辑界面可创建/删除，功能与普通池一致）──────

def _find_key_pool(config: dict, key_id: int) -> tuple[str, dict] | None:
    pools = config.get("pools", {})
    for name, pcfg in pools.items():
        if pcfg.get("owner_key_id") == str(key_id):
            return name, pcfg
    return None


@router.get("/keys/{key_id}/pool")
async def get_key_pool(key_id: int, _=Depends(verify_admin)):
    import database as db
    rec = await db.get_api_key_by_id(key_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"API Key #{key_id} 不存在")
    found = _find_key_pool(load_config(), key_id)
    if not found:
        return {"ok": True, "pool": None}
    name, pcfg = found
    return {"ok": True, "pool": {**pcfg, "name": name}}


@router.post("/keys/{key_id}/pool")
async def create_key_pool(key_id: int, request: Request, _=Depends(verify_admin)):
    import database as db
    rec = await db.get_api_key_by_id(key_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"API Key #{key_id} 不存在")
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="缺少专属池名称")
    if "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="池名称不能包含 / 或 \\ 字符")

    config = load_config()
    pools = config.setdefault("pools", {})
    if _find_key_pool(config, key_id):
        raise HTTPException(status_code=409, detail="该密钥已存在专属模型池")
    if name in pools:
        raise HTTPException(status_code=409, detail=f"池名称 '{name}' 已存在")

    pools[name] = {
        "strategy": "sequential",
        "model_ids": [],
        "auto_order": False,
        "owner_key_id": str(key_id),
    }
    # 自动加入该密钥的授权池，使其可被调用
    allowed = list(rec["allowed_pools"])
    if name not in allowed:
        allowed.append(name)
        await db.update_api_key(key_id, {"allowed_pools": allowed})
    save_config(config)
    restart_scheduler()
    _sync_pool()
    return {"ok": True, "pool": pools[name]}


@router.delete("/keys/{key_id}/pool")
async def delete_key_pool(key_id: int, _=Depends(verify_admin)):
    import database as db
    config = load_config()
    found = _find_key_pool(config, key_id)
    if not found:
        raise HTTPException(status_code=404, detail="该密钥没有专属模型池")
    name, _ = found
    pools = config.get("pools", {})
    del pools[name]
    for pcfg in pools.values():
        pcfg["model_ids"] = [x for x in pcfg.get("model_ids", []) if x != name and x != f"pool:{name}"]
    # 从该密钥授权池移除
    rec = await db.get_api_key_by_id(key_id)
    if rec:
        allowed = [x for x in rec["allowed_pools"] if x != name]
        await db.update_api_key(key_id, {"allowed_pools": allowed})
    save_config(config)
    restart_scheduler()
    _sync_pool()
    return {"ok": True}


@router.get("/keys/{key_id}/usage")
async def key_usage_history(key_id: int, date: str = "", _=Depends(verify_admin)):
    """某 API Key 的用量历史：按 1h 粒度返回指定日期(YYYY-MM-DD，默认今天)的 24 小时用量。"""
    import database as db
    from datetime import datetime
    from zoneinfo import ZoneInfo
    rec = await db.get_api_key_by_id(key_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"API Key #{key_id} 不存在")
    if not date:
        date = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    hourly = await db.get_hourly_usage(key_id, date)
    hours = [{"hour": h, "used": hourly.get(f"{h:02d}", 0)} for h in range(24)]
    return {"key_id": key_id, "date": date, "hours": hours, "total": sum(x["used"] for x in hours)}


# ── 用户（预留：未来普通用户账号体系）──────────────────────────────────

@router.get("/users")
async def list_users(_=Depends(verify_admin)):
    import database as db
    return {"users": await db.list_users()}


@router.post("/users")
async def create_user(request: Request, _=Depends(verify_admin)):
    """预留接口：创建用户（当前仅管理员账号，普通用户体系待后续启用）"""
    import database as db
    body = await request.json()
    username = (body.get("username") or "").strip()
    password = str(body.get("password") or "")
    role = body.get("role", "user")
    if not username or not password:
        raise HTTPException(status_code=400, detail="缺少 username / password")
    if role not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="role 必须为 admin 或 user")
    import hashlib
    pwd_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
    uid = await db.create_user(username, pwd_hash, role)
    return {"ok": True, "id": uid}


@router.delete("/users/{user_id}")
async def delete_user(user_id: int, _=Depends(verify_admin)):
    import database as db
    await db.delete_user(user_id)
    return {"ok": True}
