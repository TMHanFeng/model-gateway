import time
import asyncio
import json
from pathlib import Path
from dataclasses import dataclass, field
from providers.openai_provider import OpenAIProvider, RateLimitError
from providers.anthropic_provider import AnthropicProvider
import database as db

CONFIG_PATH = Path(__file__).parent / "config.json"


@dataclass
class ModelEntry:
    id: str
    name: str
    provider: str
    base_url: str
    api_key: str
    daily_token_limit: int = 0
    rpm_limit: int = 0
    tpm_limit: int = 0
    token_type: str = "daily"
    max_tokens: int = 0
    ttl_seconds: int = 0
    refresh_time: str = ""
    timezone: str = "Asia/Shanghai"
    context_window: int = 0
    max_concurrency: int = 10
    billing_mode: str = "token"
    is_free: bool = False
    modality: str = "text"
    latency_ms: float | None = None
    cooldown_until: float = 0.0
    semaphore: asyncio.Semaphore = field(default=None, repr=False)

    def __post_init__(self):
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(self.max_concurrency)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(config: dict):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


class ModelPool:
    def __init__(self):
        self.config = load_config()
        self.registry: dict[str, ModelEntry] = {}
        self.by_name: dict[str, list[ModelEntry]] = {}
        self.pools: dict[str, dict] = {}
        self.providers_cache: dict[str, object] = {}
        self._load()

    def _load(self):
        for m in self.config.get("models", []):
            entry = ModelEntry(
                id=m["id"],
                name=m["name"],
                provider=m["provider"],
                base_url=m["base_url"],
                api_key=m["api_key"],
                daily_token_limit=m.get("daily_token_limit", 0),
                rpm_limit=m.get("rpm_limit", 0),
                tpm_limit=m.get("tpm_limit", 0),
                token_type=m.get("token_type", "daily"),
                max_tokens=m.get("max_tokens", 0),
                ttl_seconds=m.get("ttl_seconds", 0),
                refresh_time=m.get("refresh_time", ""),
                timezone="Asia/Shanghai",
                context_window=m.get("context_window", 0),
                max_concurrency=m.get("max_concurrency", 10),
                billing_mode=m.get("billing_mode", "token"),
                is_free=m.get("is_free", False),
                modality=m.get("modality", "text"),
            )
            self.registry[entry.id] = entry
            self.by_name.setdefault(entry.name, []).append(entry)

        for pool_name, pool_cfg in self.config.get("pools", {}).items():
            self.pools[pool_name] = {
                "model_ids": pool_cfg.get("model_ids", []),
                "auto_order": bool(pool_cfg.get("auto_order", False)),
            }

    def reload(self):
        old_latency = {mid: e.latency_ms for mid, e in self.registry.items()}
        self.providers_cache.clear()
        self.config = load_config()
        self.registry.clear()
        self.by_name.clear()
        self.pools.clear()
        self._load()
        for mid, e in self.registry.items():
            if mid in old_latency:
                e.latency_ms = old_latency[mid]

    def _get_provider(self, entry: ModelEntry):
        if entry.id not in self.providers_cache:
            if entry.provider == "anthropic":
                self.providers_cache[entry.id] = AnthropicProvider(entry.base_url, entry.api_key)
            else:
                self.providers_cache[entry.id] = OpenAIProvider(entry.base_url, entry.api_key)
        return self.providers_cache[entry.id]

    async def close_all(self):
        for p in self.providers_cache.values():
            await p.close()

    def _estimate_tokens(self, req) -> int:
        total = 0
        for m in req.messages:
            content = m.content
            if isinstance(content, str):
                total += len(content) // 3 + 1
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict):
                        if part.get("type") == "text":
                            total += len(part.get("text", "")) // 3 + 1
                        elif part.get("type") in ("image_url", "image"):
                            total += 300
        if req.max_tokens:
            total += req.max_tokens
        return total

    def _has_images(self, req) -> bool:
        for m in req.messages:
            content = m.content
            if isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") in ("image_url", "image"):
                        return True
        return False

    async def _check_available(self, entry: ModelEntry, estimated_tokens: int = 0, has_images: bool = False) -> tuple[bool, str]:
        now = time.time()

        if entry.token_type == "one_time":
            state = await db.get_one_time_state(entry.id)
            if state is not None:
                if state["expired"]:
                    return False, "one_time_expired"
                if entry.ttl_seconds > 0 and (now - state["created_at"]) > entry.ttl_seconds:
                    await db.expire_one_time(entry.id)
                    return False, "one_time_expired"
                if entry.max_tokens > 0 and state["used_tokens"] >= entry.max_tokens:
                    await db.expire_one_time(entry.id)
                    return False, "one_time_expired"
            else:
                await db.init_one_time(entry.id)
        elif entry.daily_token_limit > 0:
            used = await db.get_daily_usage(entry.id)
            if used >= entry.daily_token_limit:
                return False, "quota_exhausted"

        if entry.rpm_limit > 0:
            rpm = await db.get_rpm(entry.id)
            if rpm >= entry.rpm_limit:
                return False, "rpm_limited"

        if entry.tpm_limit > 0:
            tpm = await db.get_tpm(entry.id)
            if tpm >= entry.tpm_limit:
                return False, "tpm_limited"

        if has_images and entry.modality != "vision":
            return False, "no_vision"

        if entry.cooldown_until > now:
            return False, "cooldown"

        if entry.context_window > 0 and estimated_tokens > entry.context_window:
            return False, "context_exceeded"

        return True, "ok"

    def _unit_latency(self, kind: str, val, visiting: set) -> float:
        if kind == "model":
            lat = val.latency_ms
            return lat if lat is not None else float("inf")
        name = val
        if name in visiting:
            return float("inf")
        meta = self.pools.get(name) or {}
        best = float("inf")
        for raw in meta.get("model_ids", []):
            if raw.startswith("pool:"):
                best = min(best, self._unit_latency("pool", raw[5:], visiting | {name}))
            elif raw in self.registry:
                lat = self.registry[raw].latency_ms
                if lat is not None:
                    best = min(best, lat)
        return best

    async def _select_from_pool(self, pool_name: str, estimated_tokens: int = 0, exclude: set | None = None, has_images: bool = False, visiting: set | None = None):
        exclude = exclude or set()
        visiting = visiting or set()
        if pool_name in visiting:
            return None, [{"model": f"pool:{pool_name}", "reason": "cycle"}]
        visiting = visiting | {pool_name}

        meta = self.pools.get(pool_name) or self.pools.get("auto") or {}
        units = []
        for raw in meta.get("model_ids", []):
            if raw.startswith("pool:"):
                units.append(("pool", raw[5:]))
            elif raw in self.registry:
                units.append(("model", self.registry[raw]))

        if meta.get("auto_order"):
            units = sorted(units, key=lambda u: self._unit_latency(u[0], u[1], visiting))

        steps = []
        for kind, val in units:
            if kind == "model":
                entry = val
                if entry.id in exclude:
                    steps.append({"model": entry.id, "reason": "already_tried"})
                    continue
                ok, reason = await self._check_available(entry, estimated_tokens, has_images)
                if ok:
                    steps.append({"model": entry.id, "reason": "selected"})
                    return entry, steps
                steps.append({"model": entry.id, "reason": reason})
            else:
                sub_entry, sub_steps = await self._select_from_pool(val, estimated_tokens, exclude, has_images, visiting)
                steps.extend(sub_steps)
                if sub_entry is not None:
                    return sub_entry, steps
                steps.append({"model": f"pool:{val}", "reason": "pool_exhausted"})
        return None, steps

    async def select_model(self, pool_name: str, requested_model: str | None = None, estimated_tokens: int = 0, exclude: set | None = None, has_images: bool = False):
        exclude = exclude or set()
        steps = []

        if requested_model:
            if requested_model in self.registry:
                entries = [self.registry[requested_model]]
            else:
                entries = self.by_name.get(requested_model, [])
            for entry in entries:
                if entry.id in exclude:
                    steps.append({"model": entry.id, "reason": "already_tried"})
                    continue
                ok, reason = await self._check_available(entry, estimated_tokens, has_images)
                if ok:
                    steps.append({"model": entry.id, "reason": "selected"})
                    return entry, steps
                steps.append({"model": entry.id, "reason": reason})
            return None, steps

        return await self._select_from_pool(pool_name, estimated_tokens, exclude, has_images)

    def _record_latency(self, entry: ModelEntry, ms: float):
        if entry.latency_ms is None:
            entry.latency_ms = ms
        else:
            entry.latency_ms = round(0.7 * entry.latency_ms + 0.3 * ms, 1)

    async def execute(self, entry: ModelEntry, req) -> tuple:
        provider = self._get_provider(entry)
        t0 = time.perf_counter()
        async with entry.semaphore:
            try:
                response = await provider.chat(req, entry.name)
            except RateLimitError:
                entry.cooldown_until = time.time() + 60
                raise
        self._record_latency(entry, (time.perf_counter() - t0) * 1000)

        tokens_used = response.usage.total_tokens
        await db.log_request(entry.id, tokens_used)

        if entry.token_type == "one_time":
            charge = 1 if entry.billing_mode == "request" else tokens_used
            await db.add_one_time_usage(entry.id, charge)
            state = await db.get_one_time_state(entry.id)
            if state and entry.max_tokens > 0 and state["used_tokens"] >= entry.max_tokens:
                await db.expire_one_time(entry.id)
        else:
            charge = 1 if entry.billing_mode == "request" else tokens_used
            await db.add_daily_usage(entry.id, charge)

        return response, tokens_used

    async def execute_stream(self, entry: ModelEntry, req):
        provider = self._get_provider(entry)
        await db.log_request(entry.id, 0)
        if entry.token_type == "daily" and entry.billing_mode == "request":
            await db.add_daily_usage(entry.id, 1)
        raw = provider.chat_stream(req, entry.name)
        return self._wrap_stream(entry, raw)

    async def _wrap_stream(self, entry: ModelEntry, raw):
        captured = 0
        async for chunk in raw:
            if isinstance(chunk, str) and chunk.startswith("data: ") and "[DONE]" not in chunk:
                try:
                    obj = json.loads(chunk[6:].strip())
                    usage = obj.get("usage")
                    if usage and usage.get("total_tokens"):
                        captured = usage["total_tokens"]
                except Exception:
                    pass
            yield chunk
        if captured > 0:
            if entry.token_type == "one_time":
                await db.add_one_time_usage(entry.id, captured)
                state = await db.get_one_time_state(entry.id)
                if state and entry.max_tokens > 0 and state["used_tokens"] >= entry.max_tokens:
                    await db.expire_one_time(entry.id)
            elif entry.billing_mode == "token":
                await db.add_daily_usage(entry.id, captured)

    def failure_detail(self, steps: list[dict], has_images: bool) -> str:
        selected_any = any(s.get("reason") == "selected" for s in steps)
        if has_images and not selected_any:
            return "请求包含图片，但池内没有可用的多模态模型（均为纯文本或不可用）"
        if not steps:
            return "池为空或无匹配模型"
        return "所有候选模型均不可用：用量用尽 / RPM·TPM 触顶 / 冷却 / 超上下文"

    async def execute_with_fallback(self, pool_name: str, req, requested_model: str | None = None):
        tried: set[str] = set()
        estimated = self._estimate_tokens(req)
        has_images = self._has_images(req)
        max_attempts = len(self.registry) + 1
        all_steps: list[dict] = []

        for _ in range(max_attempts):
            entry, steps = await self.select_model(pool_name, requested_model, estimated, exclude=tried, has_images=has_images)
            all_steps.extend(steps)
            if entry is None:
                break
            tried.add(entry.id)

            try:
                response, tokens = await self.execute(entry, req)
                await db.log_decision(pool_name, requested_model, entry.id, estimated, all_steps)
                return response, tokens, all_steps
            except RateLimitError:
                all_steps.append({"model": entry.id, "reason": "switch_429"})
                continue
            except Exception:
                entry.cooldown_until = time.time() + 30
                all_steps.append({"model": entry.id, "reason": "switch_error"})
                continue

        await db.log_decision(pool_name, requested_model, None, estimated, all_steps)
        return None, 0, all_steps

    async def execute_stream_with_fallback(self, pool_name: str, req, requested_model: str | None = None):
        tried: set[str] = set()
        estimated = self._estimate_tokens(req)
        has_images = self._has_images(req)
        max_attempts = len(self.registry) + 1
        all_steps: list[dict] = []

        for _ in range(max_attempts):
            entry, steps = await self.select_model(pool_name, requested_model, estimated, exclude=tried, has_images=has_images)
            all_steps.extend(steps)
            if entry is None:
                break
            tried.add(entry.id)

            try:
                stream = await self.execute_stream(entry, req)
                await db.log_decision(pool_name, requested_model, entry.id, estimated, all_steps)
                return stream, entry, all_steps
            except RateLimitError:
                all_steps.append({"model": entry.id, "reason": "switch_429"})
                continue
            except Exception:
                entry.cooldown_until = time.time() + 30
                all_steps.append({"model": entry.id, "reason": "switch_error"})
                continue

        await db.log_decision(pool_name, requested_model, None, estimated, all_steps)
        return None, None, all_steps

    async def speedtest(self, model_ids: list[str] | None = None) -> list[dict]:
        targets = model_ids or list(self.registry.keys())
        results = []

        async def test_one(mid: str):
            entry = self.registry.get(mid)
            if not entry:
                return {"model_id": mid, "status": "not_found"}
            provider = self._get_provider(entry)
            r = await provider.speedtest(entry.name)
            r["model_id"] = mid
            r["model_name"] = entry.name
            r["provider"] = entry.provider
            if r.get("status") == "ok" and r.get("latency_ms") is not None:
                self._record_latency(entry, r["latency_ms"])
            return r

        tasks = [test_one(mid) for mid in targets]
        results = await asyncio.gather(*tasks)
        return list(results)

    async def get_stats(self) -> list[dict]:
        stats = []
        for entry in self.registry.values():
            s = {
                "id": entry.id,
                "name": entry.name,
                "provider": entry.provider,
                "token_type": entry.token_type,
                "billing_mode": entry.billing_mode,
                "is_free": entry.is_free,
                "modality": entry.modality,
                "unit": "次" if entry.billing_mode == "request" else "tokens",
                "context_window": entry.context_window,
                "rpm_limit": entry.rpm_limit,
                "tpm_limit": entry.tpm_limit,
                "max_concurrency": entry.max_concurrency,
                "cooldown_until": entry.cooldown_until,
                "current_rpm": await db.get_rpm(entry.id),
                "current_tpm": await db.get_tpm(entry.id),
                "latency_ms": entry.latency_ms,
            }
            if entry.token_type == "one_time":
                state = await db.get_one_time_state(entry.id)
                if state:
                    s["used_tokens"] = state["used_tokens"]
                    s["max_tokens"] = entry.max_tokens
                    s["expired"] = bool(state["expired"])
                    s["created_at"] = state["created_at"]
                else:
                    s["used_tokens"] = 0
                    s["max_tokens"] = entry.max_tokens
                    s["expired"] = False
            else:
                used = await db.get_daily_usage(entry.id)
                s["daily_used_tokens"] = used
                s["daily_token_limit"] = entry.daily_token_limit
                s["daily_remaining"] = max(0, entry.daily_token_limit - used) if entry.daily_token_limit > 0 else -1
                s["refresh_time"] = entry.refresh_time
                s["timezone"] = entry.timezone
            stats.append(s)
        return stats

    def list_models(self) -> list[dict]:
        result = []
        for entry in self.registry.values():
            result.append({
                "id": entry.id,
                "name": entry.name,
                "provider": entry.provider,
                "token_type": entry.token_type,
                "billing_mode": entry.billing_mode,
                "is_free": entry.is_free,
                "modality": entry.modality,
                "context_window": entry.context_window,
            })
        return result

    def pool_names(self) -> list[str]:
        return list(self.pools.keys())

    def list_model_names(self) -> list[str]:
        return list(self.by_name.keys())
