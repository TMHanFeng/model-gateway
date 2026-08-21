import time
import asyncio
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from dataclasses import dataclass, field
import httpx
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
    max_concurrency: int = 0  # 0 = unlimited (no semaphore gating)
    billing_mode: str = "token"
    is_free: bool = True
    modality: str = "text"
    provider_id: str = ""
    expire_date: str = ""
    latency_ms: float | None = None
    cooldown_until: float = 0.0
    one_time_created_at: float | None = None
    rolling5h_window_start: float | None = None
    semaphore: asyncio.Semaphore = field(default=None, repr=False)

    def __post_init__(self):
        if self.semaphore is None:
            # max_concurrency=0 means "unlimited". asyncio.Semaphore(0) blocks all
            # acquires, so substitute a very large permit count (effectively unlimited).
            permits = self.max_concurrency if self.max_concurrency > 0 else (1 << 31)
            self.semaphore = asyncio.Semaphore(permits)


ROLLING_5H_SECONDS = 5 * 3600


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
        self.providers: dict[str, dict] = {}
        self.providers_cache: dict[str, object] = {}
        # Single-model override: pool_name -> model_id. When set, only that model is used.
        self.single_override: dict[str, str] = {}
        self._load()

    def _load(self):
        for p in self.config.get("providers", []):
            self.providers[p["id"]] = {
                "id": p["id"],
                "name": p.get("name", p["id"]),
                "protocol": p.get("protocol", "openai"),
                "base_url": p.get("base_url", ""),
                "api_key": p.get("api_key", ""),
            }

        for m in self.config.get("models", []):
            pid = m.get("provider_id", "")
            prov = self.providers.get(pid)
            if prov:
                protocol = prov["protocol"]
                base_url = prov["base_url"]
                api_key = prov["api_key"]
            else:
                protocol = m.get("provider", "openai")
                base_url = m.get("base_url", "")
                api_key = m.get("api_key", "")
            entry = ModelEntry(
                id=m["id"],
                name=m["name"],
                provider=protocol,
                base_url=base_url,
                api_key=api_key,
                daily_token_limit=m.get("daily_token_limit", 0),
                rpm_limit=m.get("rpm_limit", 0),
                tpm_limit=m.get("tpm_limit", 0),
                token_type=m.get("token_type", "daily"),
                max_tokens=m.get("max_tokens", 0),
                ttl_seconds=m.get("ttl_seconds", 0),
                refresh_time=m.get("refresh_time", ""),
                timezone="Asia/Shanghai",
                context_window=m.get("context_window", 0),
                max_concurrency=m.get("max_concurrency", 0),
                billing_mode=m.get("billing_mode", "token"),
                is_free=m.get("is_free", True),
                modality=m.get("modality", "text"),
                provider_id=pid,
                expire_date=m.get("expire_date", ""),
            )
            self.registry[entry.id] = entry
            self.by_name.setdefault(entry.name, []).append(entry)

        for pool_name, pool_cfg in self.config.get("pools", {}).items():
            self.pools[pool_name] = {
                "model_ids": pool_cfg.get("model_ids", []),
                "auto_order": bool(pool_cfg.get("auto_order", False)),
                "fallback_pool": pool_cfg.get("fallback_pool"),
                "strategy": pool_cfg.get("strategy", "sequential"),
                "slow_latency_threshold": int(pool_cfg.get("slow_latency_threshold", 3000)),
                "owner_key_id": pool_cfg.get("owner_key_id"),
            }

        # Auto-create 兜底池 (fallback pool): empty by default, user manually adds models.
        # Migrate legacy __fallback__ to 兜底池 if present.
        legacy_pool = self.pools.pop("__fallback__", None)
        if "兜底池" not in self.pools:
            self.pools["兜底池"] = {
                "model_ids": (legacy_pool or {}).get("model_ids", []),
                "auto_order": (legacy_pool or {}).get("auto_order", False),
                "fallback_pool": None,
                "strategy": (legacy_pool or {}).get("strategy", "sequential"),
                "slow_latency_threshold": int((legacy_pool or {}).get("slow_latency_threshold", 3000)),
            }
            # Persist the migration so config.json stays in sync
            if "pools" in self.config:
                self.config["pools"].pop("__fallback__", None)
                self.config["pools"]["兜底池"] = self.pools["兜底池"]
                save_config(self.config)

        # Migrate stale fallback_pool references from legacy __fallback__ to 兜底池,
        # and repair any fallback_pool that points to a missing pool. Ensure the
        # auto pool always falls back to an existing pool (prefer 兜底池).
        migrated = False
        for pname, pcfg in self.pools.items():
            fb = pcfg.get("fallback_pool")
            if fb == "__fallback__":
                pcfg["fallback_pool"] = "兜底池"
                migrated = True
            elif fb and fb not in self.pools:
                pcfg["fallback_pool"] = "兜底池" if "兜底池" in self.pools else None
                migrated = True
        if "auto" in self.pools:
            fb = self.pools["auto"].get("fallback_pool")
            if not fb or fb not in self.pools:
                self.pools["auto"]["fallback_pool"] = "兜底池" if "兜底池" in self.pools else None
                migrated = True
        if migrated:
            # Mirror the fix into config and persist so it survives restarts
            for pname, pcfg in self.config.get("pools", {}).items():
                inmem = self.pools.get(pname)
                if inmem and pcfg.get("fallback_pool") != inmem["fallback_pool"]:
                    pcfg["fallback_pool"] = inmem["fallback_pool"]
            save_config(self.config)

        # Restore single_override from config so it survives reloads
        self.single_override = {
            k: v for k, v in self.config.get("single_override", {}).items()
            if k in self.pools and v in self.registry
        }

    # ── single_override persistence ────────────────────────────────────────────

    def _persist_single_override(self):
        """Write current single_override to config.json so it survives reloads."""
        try:
            config = load_config()
        except Exception:
            config = self.config
        config["single_override"] = dict(self.single_override)
        save_config(config)
        self.config = config

    def set_single_override(self, pool_name: str, model_id: str):
        self.single_override[pool_name] = model_id
        self._persist_single_override()

    def clear_single_override(self, pool_name: str):
        cleared = self.single_override.pop(pool_name, None)
        self._persist_single_override()
        return cleared

    def reload(self):
        old_latency = {mid: e.latency_ms for mid, e in self.registry.items()}
        self.providers_cache.clear()
        self.config = load_config()
        self.registry.clear()
        self.by_name.clear()
        self.pools.clear()
        self.providers.clear()
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

    async def _check_available(self, entry: ModelEntry, estimated_tokens: int = 0, has_images: bool = False) -> tuple[bool, str, dict | None]:
        """返回 (ok, reason, detail)。detail 用于调用记录展示具体数值（已用/上限、冷却剩余秒等）。"""
        now = time.time()
        detail = None

        if entry.token_type == "one_time":
            state = await db.get_one_time_state(entry.id)
            if state is None:
                await db.init_one_time(entry.id)
                state = {"used_tokens": 0, "created_at": now, "expired": 0}
            entry.one_time_created_at = state["created_at"]  # cache for _time_to_expiry
            if state["expired"]:
                return False, "one_time_expired", {"reason_detail": "已标记过期"}
            if entry.expire_date:
                try:
                    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
                    if today > datetime.strptime(entry.expire_date, "%Y-%m-%d").date():
                        await db.expire_one_time(entry.id)
                        return False, "one_time_expired", {"reason_detail": f"过期日期 {entry.expire_date} 已过"}
                except ValueError:
                    pass
            if entry.ttl_seconds > 0 and (now - state["created_at"]) > entry.ttl_seconds:
                await db.expire_one_time(entry.id)
                age = int(now - state["created_at"])
                return False, "one_time_expired", {
                    "reason_detail": f"TTL 已过",
                    "age_sec": age,
                    "ttl_sec": entry.ttl_seconds,
                }
            if entry.max_tokens > 0 and state["used_tokens"] >= entry.max_tokens:
                await db.expire_one_time(entry.id)
                return False, "one_time_expired", {
                    "reason_detail": "用量触顶",
                    "used": state["used_tokens"],
                    "limit": entry.max_tokens,
                }
        elif entry.token_type == "rolling_5h":
            state = await db.get_5h_state(entry.id)
            if state is not None and (now - state["window_start"]) < ROLLING_5H_SECONDS:
                entry.rolling5h_window_start = state["window_start"]
                if entry.daily_token_limit > 0 and state["used_amount"] >= entry.daily_token_limit:
                    remaining = int(ROLLING_5H_SECONDS - (now - state["window_start"]))
                    return False, "quota_exhausted", {
                        "used": state["used_amount"],
                        "limit": entry.daily_token_limit,
                        "window_remaining_sec": remaining,
                    }
            else:
                entry.rolling5h_window_start = None
        elif entry.daily_token_limit > 0:
            used = await db.get_daily_usage(entry.id)
            if used >= entry.daily_token_limit:
                return False, "quota_exhausted", {
                    "used": used,
                    "limit": entry.daily_token_limit,
                }

        if entry.rpm_limit > 0:
            rpm = await db.get_rpm(entry.id)
            if rpm >= entry.rpm_limit:
                return False, "rpm_limited", {"current": rpm, "limit": entry.rpm_limit}

        if entry.tpm_limit > 0:
            tpm = await db.get_tpm(entry.id)
            if tpm >= entry.tpm_limit:
                return False, "tpm_limited", {"current": tpm, "limit": entry.tpm_limit}

        if has_images and entry.modality != "vision":
            return False, "no_vision", {"modality": entry.modality}

        if entry.cooldown_until > now:
            remaining = int(entry.cooldown_until - now)
            return False, "cooldown", {"remaining_sec": remaining}

        if entry.context_window > 0 and estimated_tokens > entry.context_window:
            return False, "context_exceeded", {
                "estimated": estimated_tokens,
                "window": entry.context_window,
            }

        return True, "ok", None

    # Untested models get a moderate default so they still participate in
    # auto-order selection instead of being pushed to infinity.
    _UNTESTED_LATENCY = 1500.0

    def _unit_latency(self, kind: str, val, visiting: set, threshold: int = 3000) -> float:
        if kind == "model":
            lat = val.latency_ms
            if lat is None:
                return self._UNTESTED_LATENCY
            # Hard deprioritize slow models (> threshold) so the next model is preferred.
            if threshold > 0 and lat > threshold:
                return float("inf")
            return lat
        name = val
        if name in visiting:
            return float("inf")
        meta = self.pools.get(name) or {}
        sub_threshold = int(meta.get("slow_latency_threshold", 3000))
        best = float("inf")
        for raw in meta.get("model_ids", []):
            if raw.startswith("pool:"):
                best = min(best, self._unit_latency("pool", raw[5:], visiting | {name}, sub_threshold))
            elif raw in self.registry:
                best = min(best, self._unit_latency("model", self.registry[raw], visiting, sub_threshold))
        return best

    def _time_to_expiry(self, entry: ModelEntry) -> float:
        """Seconds until this model's quota is lost. Lower = more urgent (use it first).
        inf = no meaningful expiry."""
        now = time.time()
        if entry.token_type == "one_time":
            if entry.expire_date:
                try:
                    exp = datetime.strptime(entry.expire_date, "%Y-%m-%d").replace(
                        hour=23, minute=59, second=59, tzinfo=ZoneInfo("Asia/Shanghai")
                    )
                    return max(0.0, exp.timestamp() - now)
                except ValueError:
                    pass
            if entry.ttl_seconds > 0:
                if entry.one_time_created_at is not None:
                    return max(0.0, entry.one_time_created_at + entry.ttl_seconds - now)
                return float(entry.ttl_seconds)  # not started yet — treat full TTL as remaining
            return float("inf")
        if entry.token_type == "rolling_5h":
            if entry.rolling5h_window_start is not None:
                return max(0.0, entry.rolling5h_window_start + ROLLING_5H_SECONDS - now)
            return float(ROLLING_5H_SECONDS)
        # daily model: quota is lost at the next refresh
        if entry.refresh_time and entry.daily_token_limit > 0:
            try:
                h, m = map(int, entry.refresh_time.split(":"))
                now_dt = datetime.now(ZoneInfo("Asia/Shanghai"))
                refresh = now_dt.replace(hour=h, minute=m, second=0, microsecond=0)
                if refresh <= now_dt:
                    refresh += timedelta(days=1)
                return (refresh - now_dt).total_seconds()
            except (ValueError, IndexError):
                pass
        return float("inf")

    def _auto_order_key(self, kind: str, val, visiting: set, threshold: int = 3000):
        """Sort key for auto_order: (time-to-expiry, latency). Lower = chosen first.
        Prefers models whose quota is about to be lost, then faster ones.
        Untested models get a moderate default latency so they still participate."""
        if kind == "model":
            lat = val.latency_ms
            if lat is None:
                lat = self._UNTESTED_LATENCY
            elif threshold > 0 and lat > threshold:
                lat = float("inf")
            return (self._time_to_expiry(val), lat)
        name = val
        if name in visiting:
            return (float("inf"), float("inf"))
        meta = self.pools.get(name) or {}
        sub_threshold = int(meta.get("slow_latency_threshold", 3000))
        best = (float("inf"), float("inf"))
        for raw in meta.get("model_ids", []):
            if raw.startswith("pool:"):
                child = self._auto_order_key("pool", raw[5:], visiting | {name}, sub_threshold)
            elif raw in self.registry:
                child = self._auto_order_key("model", self.registry[raw], visiting, sub_threshold)
            else:
                continue
            if child < best:
                best = child
        return best

    def _collect_pool_models(self, pool_name: str, visiting: set | None = None) -> list[str]:
        """Recursively collect all model IDs reachable from a pool (including sub-pools)."""
        visiting = visiting or set()
        if pool_name in visiting:
            return []
        visiting = visiting | {pool_name}
        meta = self.pools.get(pool_name) or {}
        models = []
        for raw in meta.get("model_ids", []):
            if raw.startswith("pool:"):
                models.extend(self._collect_pool_models(raw[5:], visiting))
            elif raw in self.registry:
                models.append(raw)
        return models

    async def _select_from_pool(self, pool_name: str, estimated_tokens: int = 0, exclude: set | None = None, has_images: bool = False, visiting: set | None = None):
        exclude = exclude or set()
        visiting = visiting or set()
        if pool_name in visiting:
            return None, [{"model": f"pool:{pool_name}", "reason": "cycle"}]
        visiting = visiting | {pool_name}

        meta = self.pools.get(pool_name) or self.pools.get("auto") or {}

        # Single-model override: when set, only use the specified model
        override_id = self.single_override.get(pool_name)
        if override_id and override_id not in exclude:
            entry = self.registry.get(override_id)
            if entry:
                ok, reason, detail = await self._check_available(entry, estimated_tokens, has_images)
                if ok:
                    steps = [{"model": override_id, "reason": "single_override_selected"}]
                    return entry, steps
                step = {"model": override_id, "reason": f"single_override_unavailable:{reason}"}
                if detail:
                    step["detail"] = detail
                return None, [step]
            return None, [{"model": override_id, "reason": "single_override_not_found"}]

        units = []
        for raw in meta.get("model_ids", []):
            if raw.startswith("pool:"):
                units.append(("pool", raw[5:]))
            elif raw in self.registry:
                units.append(("model", self.registry[raw]))

        if meta.get("auto_order"):
            threshold = int(meta.get("slow_latency_threshold", 3000))
            units = sorted(units, key=lambda u: self._auto_order_key(u[0], u[1], visiting, threshold))

        steps = []
        for kind, val in units:
            if kind == "model":
                entry = val
                if entry.id in exclude:
                    steps.append({"model": entry.id, "reason": "already_tried"})
                    continue
                ok, reason, detail = await self._check_available(entry, estimated_tokens, has_images)
                if ok:
                    steps.append({"model": entry.id, "reason": "selected"})
                    return entry, steps
                step = {"model": entry.id, "reason": reason}
                if detail:
                    step["detail"] = detail
                steps.append(step)
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
                ok, reason, detail = await self._check_available(entry, estimated_tokens, has_images)
                if ok:
                    steps.append({"model": entry.id, "reason": "selected"})
                    return entry, steps
                step = {"model": entry.id, "reason": reason}
                if detail:
                    step["detail"] = detail
                steps.append(step)
            return None, steps

        return await self._select_from_pool(pool_name, estimated_tokens, exclude, has_images)

    def _record_latency(self, entry: ModelEntry, ms: float):
        if entry.latency_ms is None:
            entry.latency_ms = ms
        else:
            entry.latency_ms = round(0.7 * entry.latency_ms + 0.3 * ms, 1)

    async def _charge_rolling_5h(self, entry: ModelEntry, amount: int):
        """5h 滚动窗口计费：窗口过期（或首次）则重置并开启新窗口，再累加用量"""
        state = await db.get_5h_state(entry.id)
        now = time.time()
        if state is None or (now - state["window_start"]) >= ROLLING_5H_SECONDS:
            await db.reset_5h_window(entry.id)
            entry.rolling5h_window_start = now
        await db.add_5h_usage(entry.id, amount)

    async def execute(self, entry: ModelEntry, req) -> tuple:
        provider = self._get_provider(entry)
        t0 = time.perf_counter()
        async with entry.semaphore:
            try:
                response = await provider.chat(req, entry.name)
            except RateLimitError:
                entry.cooldown_until = time.time() + 20
                raise
        self._record_latency(entry, (time.perf_counter() - t0) * 1000)

        tokens_used = response.usage.total_tokens
        await db.log_request(entry.id, tokens_used)
        await db.add_model_call(entry.id, tokens_used)

        if entry.token_type == "one_time":
            charge = 1 if entry.billing_mode == "request" else tokens_used
            await db.add_one_time_usage(entry.id, charge)
            state = await db.get_one_time_state(entry.id)
            if state and entry.max_tokens > 0 and state["used_tokens"] >= entry.max_tokens:
                await db.expire_one_time(entry.id)
        elif entry.token_type == "rolling_5h":
            charge = 1 if entry.billing_mode == "request" else tokens_used
            await self._charge_rolling_5h(entry, charge)
        else:
            charge = 1 if entry.billing_mode == "request" else tokens_used
            await db.add_daily_usage(entry.id, charge)

        return response, tokens_used

    async def execute_stream(self, entry: ModelEntry, req):
        provider = self._get_provider(entry)
        t0 = time.perf_counter()
        # 计费在 execute_stream_with_fallback 验证首个分片（真正建连）成功之后进行，
        # 避免"连接即失败"的流式请求被错误计入配额。
        raw = provider.chat_stream(req, entry.name)
        return self._wrap_stream(entry, raw, t0)

    async def _wrap_stream(self, entry: ModelEntry, raw, t0: float):
        captured = 0
        try:
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
        finally:
            self._record_latency(entry, (time.perf_counter() - t0) * 1000)
            await db.log_request(entry.id, captured)
            await db.add_model_call(entry.id, captured)
            if captured > 0:
                if entry.token_type == "one_time":
                    await db.add_one_time_usage(entry.id, captured)
                    state = await db.get_one_time_state(entry.id)
                    if state and entry.max_tokens > 0 and state["used_tokens"] >= entry.max_tokens:
                        await db.expire_one_time(entry.id)
                elif entry.token_type == "rolling_5h":
                    if entry.billing_mode == "token":
                        await self._charge_rolling_5h(entry, captured)
                elif entry.billing_mode == "token":
                    await db.add_daily_usage(entry.id, captured)

    def failure_detail(self, steps: list[dict], has_images: bool) -> str:
        attempted = any(s.get("reason") in ("selected", "single_override_selected", "switch_429", "switch_error") for s in steps)
        locked = any(str(s.get("reason", "")).startswith("single_override") for s in steps)
        if has_images and not attempted:
            return "请求包含图片，但池内没有可用的多模态模型（均为纯文本或不可用）"
        if locked:
            return "单模型锁定：锁定模型不可用或调用失败，且兜底池无可用模型"
        if not steps:
            return "池为空或无匹配模型"
        return "所有候选模型均不可用：用量用尽 / RPM·TPM 触顶 / 冷却 / 超上下文"

    async def execute_with_fallback(self, pool_name: str, req, requested_model: str | None = None, caller: str = ""):
        tried: set[str] = set()
        estimated = self._estimate_tokens(req)
        has_images = self._has_images(req)
        max_attempts = len(self.registry) + 1
        actual_calls: list[dict] = []
        override_id = self.single_override.get(pool_name)
        fb_name = self.pools.get(pool_name, {}).get("fallback_pool")
        # When the locked single model fails (or the main pool is exhausted), escalate to the fallback pool.
        use_fallback = False

        for _ in range(max_attempts):
            if use_fallback:
                if not fb_name:
                    break
                entry, steps = await self.select_model(fb_name, None, estimated, exclude=tried, has_images=has_images)
            else:
                entry, steps = await self.select_model(pool_name, requested_model, estimated, exclude=tried, has_images=has_images)

            if steps:
                actual_calls.extend(s for s in steps if s["reason"] != "already_tried")

            if entry is None:
                # Main pool / single model cannot serve — escalate to fallback pool once.
                if not use_fallback and fb_name:
                    use_fallback = True
                    continue
                break
            tried.add(entry.id)

            t0 = time.perf_counter()
            try:
                response, tokens = await self.execute(entry, req)
                last_reason = actual_calls[-1]["reason"] if actual_calls else ""
                if last_reason in ("selected", "single_override_selected") and actual_calls[-1]["model"] == entry.id:
                    if use_fallback:
                        actual_calls[-1]["reason"] = "fallback_selected"
                else:
                    actual_calls.append({"model": entry.id, "reason": "fallback_selected" if use_fallback else "selected"})
                await db.log_decision(pool_name, requested_model, entry.id, estimated, actual_calls, caller)
                return response, tokens, actual_calls
            except RateLimitError:
                latency_ms = round((time.perf_counter() - t0) * 1000, 1)
                detail = {"cooldown_sec": 20, "status": 429, "latency_ms": latency_ms}
                actual_calls.append({
                    "model": entry.id,
                    "reason": "fallback_switch_429" if use_fallback else "switch_429",
                    "detail": detail,
                })
                if override_id and not use_fallback:
                    use_fallback = True
                continue
            except Exception as e:
                latency_ms = round((time.perf_counter() - t0) * 1000, 1)
                error_type = type(e).__name__
                # 按异常类型决定冷却时长：429→30s（响应 Retry-After 时另议），5xx→15s，网络抖动→10s，其他→30s
                status = getattr(e, "status_code", None) or getattr(getattr(e, "response", None), "status_code", None)
                if isinstance(e, httpx.HTTPStatusError) and status is not None and 500 <= status < 600:
                    cooldown_sec = 15
                elif isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
                    cooldown_sec = 10
                else:
                    cooldown_sec = 30
                entry.cooldown_until = time.time() + cooldown_sec
                detail = {
                    "cooldown_sec": cooldown_sec,
                    "error_type": error_type,
                    "latency_ms": latency_ms,
                }
                if status is not None:
                    detail["status"] = status
                err_msg = str(e)
                if err_msg and len(err_msg) < 200:
                    detail["error"] = err_msg
                actual_calls.append({
                    "model": entry.id,
                    "reason": "fallback_switch_error" if use_fallback else "switch_error",
                    "detail": detail,
                })
                if override_id and not use_fallback:
                    use_fallback = True
                continue

        await db.log_decision(pool_name, requested_model, None, estimated, actual_calls, caller)
        return None, 0, actual_calls

    async def execute_stream_with_fallback(self, pool_name: str, req, requested_model: str | None = None, caller: str = ""):
        tried: set[str] = set()
        estimated = self._estimate_tokens(req)
        has_images = self._has_images(req)
        max_attempts = len(self.registry) + 1
        actual_calls: list[dict] = []
        override_id = self.single_override.get(pool_name)
        fb_name = self.pools.get(pool_name, {}).get("fallback_pool")
        use_fallback = False

        for _ in range(max_attempts):
            if use_fallback:
                if not fb_name:
                    break
                entry, steps = await self.select_model(fb_name, None, estimated, exclude=tried, has_images=has_images)
            else:
                entry, steps = await self.select_model(pool_name, requested_model, estimated, exclude=tried, has_images=has_images)

            if steps:
                actual_calls.extend(s for s in steps if s["reason"] != "already_tried")

            if entry is None:
                if not use_fallback and fb_name:
                    use_fallback = True
                    continue
                break
            tried.add(entry.id)

            try:
                stream = await self.execute_stream(entry, req)
                # 预取首个分片：真正发起上游连接并检查 HTTP 状态。
                # 连接失败 / 429 / HTTP 错误会在此处抛出，从而触发下面的回退逻辑；
                # 否则流式请求会"假成功"（日志显示选中但实际无响应、不兜底）。
                got_first = False
                first = None
                try:
                    first = await stream.__anext__()
                    got_first = True
                except StopAsyncIteration:
                    got_first = False

                async def _replay(got=got_first, first=first, rest=stream):
                    if got:
                        yield first
                    async for chunk in rest:
                        yield chunk

                # 流建立成功后才计费（按次计费计 1 次；按 token 计费由 _wrap_stream 结束时按实际 token 计）
                if entry.token_type == "daily" and entry.billing_mode == "request":
                    await db.add_daily_usage(entry.id, 1)
                elif entry.token_type == "rolling_5h" and entry.billing_mode == "request":
                    await self._charge_rolling_5h(entry, 1)

                last_reason = actual_calls[-1]["reason"] if actual_calls else ""
                if last_reason in ("selected", "single_override_selected") and actual_calls[-1]["model"] == entry.id:
                    if use_fallback:
                        actual_calls[-1]["reason"] = "fallback_selected"
                else:
                    actual_calls.append({"model": entry.id, "reason": "fallback_selected" if use_fallback else "selected"})
                await db.log_decision(pool_name, requested_model, entry.id, estimated, actual_calls, caller)
                return _replay(), entry, actual_calls
            except RateLimitError:
                actual_calls.append({
                    "model": entry.id,
                    "reason": "fallback_switch_429" if use_fallback else "switch_429",
                    "detail": {"cooldown_sec": 20, "status": 429},
                })
                if override_id and not use_fallback:
                    use_fallback = True
                continue
            except Exception as e:
                # 按异常类型分发冷却：5xx→15s，网络→10s，其他→30s（429 在 execute() 单独处理为 30s）
                status = getattr(e, "status_code", None) or getattr(getattr(e, "response", None), "status_code", None)
                if isinstance(e, httpx.HTTPStatusError) and status is not None and 500 <= status < 600:
                    cooldown_sec = 15
                elif isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
                    cooldown_sec = 10
                else:
                    cooldown_sec = 30
                entry.cooldown_until = time.time() + cooldown_sec
                actual_calls.append({
                    "model": entry.id,
                    "reason": "fallback_switch_error" if use_fallback else "switch_error",
                    "detail": {"cooldown_sec": cooldown_sec, "error_type": type(e).__name__},
                })
                if override_id and not use_fallback:
                    use_fallback = True
                continue

        await db.log_decision(pool_name, requested_model, None, estimated, actual_calls, caller)
        return None, None, actual_calls

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
                # Record usage from speed test
                tokens = r.get("tokens", 0)
                if tokens > 0:
                    await db.add_model_call(entry.id, tokens)
                    if entry.token_type == "one_time":
                        await db.add_one_time_usage(entry.id, tokens)
                    elif entry.token_type == "rolling_5h":
                        await self._charge_rolling_5h(entry, tokens)
                    else:
                        await db.add_daily_usage(entry.id, tokens)
                    r["usage_recorded"] = tokens
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
            daily_stats = await db.get_model_daily_stats(entry.id)
            s["today_requests"] = daily_stats["request_count"]
            s["today_tokens"] = daily_stats["total_tokens"]
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
            elif entry.token_type == "rolling_5h":
                state = await db.get_5h_state(entry.id)
                now = time.time()
                if state and (now - state["window_start"]) < ROLLING_5H_SECONDS:
                    used = state["used_amount"]
                    remaining = int(ROLLING_5H_SECONDS - (now - state["window_start"]))
                else:
                    used = 0
                    remaining = ROLLING_5H_SECONDS
                s["daily_used_tokens"] = used
                s["daily_token_limit"] = entry.daily_token_limit
                s["daily_remaining"] = max(0, entry.daily_token_limit - used) if entry.daily_token_limit > 0 else -1
                s["refresh_time"] = ""
                s["window_remaining_sec"] = remaining
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
