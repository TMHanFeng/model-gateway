import time
import asyncio
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from dataclasses import dataclass, field
import httpx
from providers.openai_provider import OpenAIProvider, RateLimitError
from providers.anthropic_provider import AnthropicProvider
import database as db

logger = logging.getLogger(__name__)

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
    timeout_seconds: int | None = None  # None=系统默认120s；0=无限等待（本地非流式慢模型）；>0=指定秒数
    billing_mode: str = "token"
    is_free: bool = True
    modality: str = "text"
    json_output: bool = False  # 支持格式输出（json）——请求带 response_format json 时只选 true 的模型
    extra_params: dict = field(default_factory=dict)  # 用户自定义参数（注入上游 payload，黑名单过滤）
    provider_id: str = ""
    proxy_url: str = ""
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
# 配额预检缓存秒数（问题19）：预检结果短缓存，避免每个候选模型每次选择都串行打 sqlite；
# 该模型的每次调用计费后（log_request 处）立即失效，保证自身计数新鲜
QUOTA_CACHE_TTL = 5.0


class ContextOverflowPassThrough(Exception):
    """上游 400 上下文超限：请求过大、与模型健康无关——不冷却；
    整池无候选可容纳时携带上游 400 原文抛出，由 main.py 原样透传给客户端（与直连行为一致）。"""
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body
        super().__init__(f"upstream 400 context overflow: {body[:200]}")


def _is_context_overflow_error(e: Exception) -> bool:
    """判定上游错误是否为上下文超限类 400（sglang/vllm/openai 等常见报错文案）"""
    status = getattr(e, "status_code", None) or getattr(getattr(e, "response", None), "status_code", None)
    if status != 400:
        return False
    text = str(e)
    resp = getattr(e, "response", None)
    if resp is not None:
        try:
            text += " " + (resp.text or "")
        except Exception:
            pass
    low = text.lower()
    return ("context length" in low or "maximum context" in low
            or "token count exceeds" in low or "longer than the model" in low)


def _upstream_error_body(e: Exception) -> str:
    """取上游错误响应体原文；取不到时退化为异常消息"""
    resp = getattr(e, "response", None)
    if resp is not None:
        try:
            t = resp.text or ""
            if t:
                return t
        except Exception:
            pass
    return str(e)


_config_cache: dict | None = None
_config_mtime: float | None = None


def load_config() -> dict:
    # 热路径（每个请求的 verify_key）都会调用：mtime 未变化时直接返回缓存，
    # 避免每请求同步读盘+解析 config.json；外部改动（含手工编辑）会因 mtime 变化自动失效
    global _config_cache, _config_mtime
    try:
        mtime = CONFIG_PATH.stat().st_mtime
    except OSError:
        mtime = None
    if _config_cache is None or mtime != _config_mtime:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            _config_cache = json.load(f)
        _config_mtime = mtime
    return _config_cache


def save_config(config: dict):
    global _config_cache, _config_mtime
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    _config_cache = config
    try:
        _config_mtime = CONFIG_PATH.stat().st_mtime
    except OSError:
        _config_mtime = None


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
        # 负载均衡（round-robin）：pool_name -> 下一次起始下标
        self.round_robin: dict[str, int] = {}
        # 配额预检缓存：model_id -> (expires_at, ok, reason, detail)
        self._quota_cache: dict[str, tuple[float, bool, str, dict | None]] = {}
        self._load()

    def _load(self):
        for p in self.config.get("providers", []):
            self.providers[p["id"]] = {
                "id": p["id"],
                "name": p.get("name", p["id"]),
                "protocol": p.get("protocol", "openai"),
                "base_url": p.get("base_url", ""),
                "api_key": p.get("api_key", ""),
                "proxy_url": p.get("proxy_url", ""),
            }

        for m in self.config.get("models", []):
            pid = m.get("provider_id", "")
            prov = self.providers.get(pid)
            if prov:
                protocol = prov["protocol"]
                base_url = prov["base_url"]
                api_key = prov["api_key"]
                proxy_url = prov.get("proxy_url", "") or m.get("proxy_url", "")
            else:
                protocol = m.get("provider", "openai")
                base_url = m.get("base_url", "")
                api_key = m.get("api_key", "")
                proxy_url = m.get("proxy_url", "")
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
                timeout_seconds=(None if m.get("timeout_seconds") in (None, "") else int(m["timeout_seconds"])),
                billing_mode=m.get("billing_mode", "token"),
                is_free=m.get("is_free", True),
                modality=m.get("modality", "text"),
                json_output=bool(m.get("json_output", False)),
                extra_params=(m.get("extra_params") or {}),
                provider_id=pid,
                proxy_url=proxy_url,
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
                "load_balance": pool_cfg.get("load_balance", False),
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
                "load_balance": (legacy_pool or {}).get("load_balance", False),
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
        self._quota_cache.clear()
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
                self.providers_cache[entry.id] = AnthropicProvider(entry.base_url, entry.api_key, entry.proxy_url,
                                                                   timeout_seconds=entry.timeout_seconds)
            else:
                self.providers_cache[entry.id] = OpenAIProvider(entry.base_url, entry.api_key, entry.proxy_url,
                                                                timeout_seconds=entry.timeout_seconds)
        return self.providers_cache[entry.id]

    async def close_all(self):
        for p in self.providers_cache.values():
            await p.close()

    @staticmethod
    def _estimate_text_tokens(text: str) -> int:
        """按字符类型估算 token：CJK 字符约 1 字 = 1 token（GLM 等国产模型 tokenizer 实测），
        ASCII/拉丁约 4 字符 = 1 token。旧的 len//3 公式对中文低估约 3 倍，
        导致超长请求通过预检打到上游才报 400。"""
        if not text:
            return 0
        cjk = 0
        other = 0
        for ch in text:
            # CJK 统一表意文字/扩展、假名、谚文、全角标点等（> U+2E7F）按 1 字 1 token
            if ord(ch) > 0x2E7F:
                cjk += 1
            else:
                other += 1
        return cjk + (other + 3) // 4 + 1

    def _estimate_tokens(self, req) -> int:
        total = 0
        for m in req.messages:
            content = m.content
            if isinstance(content, str):
                total += self._estimate_text_tokens(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict):
                        if part.get("type") == "text":
                            total += self._estimate_text_tokens(part.get("text", ""))
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

    async def _quota_check(self, entry: ModelEntry, now: float) -> tuple[bool, str, dict | None]:
        """配额与限速预检（读 sqlite 的部分，由 _check_available 短缓存包装）。
        返回 (ok, reason, detail)。"""
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

        return True, "ok", None

    def _invalidate_quota_cache(self, model_id: str):
        """该模型发生计费/调用后调用：使配额预检缓存立即失效，保证下一次预检读到最新用量"""
        self._quota_cache.pop(model_id, None)

    async def _check_available(self, entry: ModelEntry, estimated_tokens: int = 0, has_images: bool = False,
                               required_modality: str | None = None, required_json_output: bool = False) -> tuple[bool, str, dict | None]:
        """返回 (ok, reason, detail)。detail 用于调用记录展示具体数值（已用/上限、冷却剩余秒等）。
        required_modality：要求特定模态（如 "embedding"/"rerank"）时，不匹配的模型一律排除；
                        None 表示普通 chat/通用调用（此时 embedding/rerank 模型也应被排除）。
        required_json_output：请求要求 json 输出时，仅 json_output=True 的模型可用。"""
        now = time.time()
        detail = None

        # 配额/限速预检（问题19）：这部分需读 sqlite（每候选 1-4 次串行查询），
        # 结果短缓存 QUOTA_CACHE_TTL 秒；该模型每次调用计费后立即失效，自身计数保持新鲜
        cached = self._quota_cache.get(entry.id)
        if cached and cached[0] > now:
            quota_ok, quota_reason, quota_detail = cached[1], cached[2], cached[3]
        else:
            quota_ok, quota_reason, quota_detail = await self._quota_check(entry, now)
            self._quota_cache[entry.id] = (now + QUOTA_CACHE_TTL, quota_ok, quota_reason, quota_detail)
        if not quota_ok:
            return False, quota_reason, quota_detail

        if has_images and entry.modality != "vision":
            return False, "no_vision", {"modality": entry.modality}

        # 模态双向硬门槛：chat 调用（required_modality=None）排除 embedding/rerank 模型；
        # embedding/rerank/专用调用要求特定模态时排除不匹配的。anthropic provider 无 embedding/rerank 端点也在此拦截。
        if required_modality is not None:
            if entry.modality != required_modality:
                return False, "wrong_modality", {"required": required_modality, "got": entry.modality}
            if entry.provider == "anthropic":
                return False, "wrong_modality", {"required": required_modality, "got": f"{entry.modality}（anthropic 协议无此端点）"}
        else:
            if entry.modality in ("embedding", "rerank"):
                return False, "wrong_modality", {"required": "chat", "got": entry.modality}

        # json 输出硬门槛：请求带 response_format json 时只允许 json_output=True 模型
        if required_json_output and not entry.json_output:
            return False, "no_json_output", {"modality": entry.modality, "json_output": entry.json_output}

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

    async def _select_from_pool(self, pool_name: str, estimated_tokens: int = 0, exclude: set | None = None,
                                has_images: bool = False, visiting: set | None = None,
                                required_modality: str | None = None, required_json_output: bool = False):
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
                ok, reason, detail = await self._check_available(
                    entry, estimated_tokens, has_images,
                    required_modality=required_modality, required_json_output=required_json_output,
                )
                if ok:
                    steps = [{"model": override_id, "reason": "single_override_selected"}]
                    return entry, steps
                step = {"model": override_id, "reason": f"single_override_unavailable:{reason}"}
                if detail:
                    step["detail"] = detail
                return None, [step]
            return None, [{"model": override_id, "reason": "single_override_not_found"}]

        units = []
        broken_refs = []
        for raw in meta.get("model_ids", []):
            if raw.startswith("pool:"):
                if raw[5:] in self.pools:
                    units.append(("pool", raw[5:]))
                else:
                    broken_refs.append(raw)
            elif raw in self.registry:
                units.append(("model", self.registry[raw]))
            else:
                broken_refs.append(raw)

        if meta.get("auto_order"):
            threshold = int(meta.get("slow_latency_threshold", 3000))
            units = sorted(units, key=lambda u: self._auto_order_key(u[0], u[1], visiting, threshold))
        elif meta.get("load_balance"):
            # 负载均衡：每次从 round_robin 指针处开始轮转（子池也是独立槽位）
            if units:
                idx = self.round_robin.get(pool_name, 0) % len(units)
                units = units[idx:] + units[:idx]
                self.round_robin[pool_name] = (idx + 1) % len(units)

        steps = []
        # 引用失效显式记录：不再静默跳过（此前断裂引用会让 json 判定/选模无声失败）
        for ref in broken_refs:
            steps.append({"model": ref, "reason": "ref_not_found",
                          "detail": {"hint": "引用的池或模型不存在，请检查池配置"}})
            logger.warning(f"[引用失效] 池 '{pool_name}' 中的 '{ref}' 不存在，已跳过")
        for kind, val in units:
            if kind == "model":
                entry = val
                if entry.id in exclude:
                    steps.append({"model": entry.id, "reason": "already_tried"})
                    continue
                ok, reason, detail = await self._check_available(
                    entry, estimated_tokens, has_images,
                    required_modality=required_modality, required_json_output=required_json_output,
                )
                if ok:
                    steps.append({"model": entry.id, "reason": "selected"})
                    return entry, steps
                step = {"model": entry.id, "reason": reason}
                if detail:
                    step["detail"] = detail
                steps.append(step)
            else:
                sub_entry, sub_steps = await self._select_from_pool(
                    val, estimated_tokens, exclude, has_images, visiting,
                    required_modality=required_modality, required_json_output=required_json_output,
                )
                steps.extend(sub_steps)
                if sub_entry is not None:
                    return sub_entry, steps
                steps.append({"model": f"pool:{val}", "reason": "pool_exhausted"})
        return None, steps

    async def select_model(self, pool_name: str, requested_model: str | None = None, estimated_tokens: int = 0,
                           exclude: set | None = None, has_images: bool = False,
                           required_modality: str | None = None, required_json_output: bool = False):
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
                ok, reason, detail = await self._check_available(
                    entry, estimated_tokens, has_images,
                    required_modality=required_modality, required_json_output=required_json_output,
                )
                if ok:
                    steps.append({"model": entry.id, "reason": "selected"})
                    return entry, steps
                step = {"model": entry.id, "reason": reason}
                if detail:
                    step["detail"] = detail
                steps.append(step)
            return None, steps

        return await self._select_from_pool(
            pool_name, estimated_tokens, exclude, has_images,
            required_modality=required_modality, required_json_output=required_json_output,
        )

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
                entry.cooldown_until = time.time() + 10
                raise
        self._record_latency(entry, (time.perf_counter() - t0) * 1000)

        tokens_used = response.usage.total_tokens
        await db.log_request(entry.id, tokens_used)
        self._invalidate_quota_cache(entry.id)
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

    async def execute_stream(self, entry: ModelEntry, req, decision_id: int | None = None):
        provider = self._get_provider(entry)
        t0 = time.perf_counter()
        # 计费在 execute_stream_with_fallback 验证首个分片（真正建连）成功之后进行，
        # 避免"连接即失败"的流式请求被错误计入配额。
        raw = provider.chat_stream(req, entry.name)
        return self._wrap_stream(entry, raw, t0, req, decision_id)

    async def _wrap_stream(self, entry: ModelEntry, raw, t0: float, req=None, decision_id: int | None = None):
        captured = 0
        billed = False          # 防重复计费：usage 到达即记一次；finally 仅补记未计过的流
        estimated = 0           # 仅供 warning 带请求上下文，不参与计费口径
        try:
            # 估算仅用于日志排查；计费仍只按上游真实 usage.total_tokens
            try:
                estimated = max(1, len(req.model_dump_json()) // 3) if req is not None else 0
            except Exception:
                estimated = 0

            async for chunk in raw:
                if isinstance(chunk, str) and chunk.startswith("data: ") and "[DONE]" not in chunk:
                    try:
                        obj = json.loads(chunk[6:].strip())
                        usage = obj.get("usage")
                        total = usage.get("total_tokens") if usage else 0
                        if total and total > 0:
                            captured = int(total)
                            if not billed:  # 同一请求只计费一次
                                try:
                                    billed = True
                                    await self._settle_stream_tokens(entry, captured)
                                    if decision_id:
                                        await db.update_decision_actual_tokens(decision_id, captured)
                                except Exception:
                                    # 入账抛错时回退 billed，交由 finally 兜底重试，避免静默漏计
                                    billed = False
                                    logger.warning(
                                        f"[流式计费失败] 已捕获 usage 但入账抛错，将由 finally 兜底 "
                                        f"(模型={entry.id}, tokens={captured}, decision_id={decision_id})"
                                    )
                                    raise
                    except Exception:
                        pass
                yield chunk
        finally:
            self._record_latency(entry, (time.perf_counter() - t0) * 1000)
            if captured > 0 and not billed:
                # 兜底：usage 已捕获但还没入账（例如 usage 出现在最后一个 chunk 后消费者未再触发）
                try:
                    await self._settle_stream_tokens(entry, captured)
                    if decision_id:
                        await db.update_decision_actual_tokens(decision_id, captured)
                except Exception:
                    # 兜底也失败时不能静默丢计费，至少告警留存，避免 token 无告警漏计
                    logger.warning(
                        f"[流式计费兜底失败] usage 已捕获但两次入账均抛错，需人工核查 "
                        f"(模型={entry.id}, tokens={captured}, decision_id={decision_id})"
                    )
                    raise
            elif captured == 0:
                # 上游全程未返回 usage：绝不静默丢计费记录；记一次调用次数，并告警要求核查。
                logger.warning(
                    f"[流式缺失usage] 流式请求未返回 usage，未计 token，需核查 "
                    f"(模型={entry.id}, 估算={estimated}tok)"
                )
                await db.log_request(entry.id, 0)
                self._invalidate_quota_cache(entry.id)
                await db.add_model_call(entry.id, 0)
                if decision_id:
                    await db.update_decision_actual_tokens(decision_id, 0)

    async def _settle_stream_tokens(self, entry: ModelEntry, tokens: int):
        """按流式真实 usage.total_tokens 入账；计费口径与非流式 execute() 保持一致。

        计费单位与非流式 execute() 一致：request 型按次记 1，token 型按真实 token 数。
        差异在于：流式路径的 request 型已在流建立时按次预扣 1 次（见 execute_with_fallback：
        daily+request 与 rolling_5h+request 均预扣），流式不再重复计入；one_time 从不预扣，
        故必须在此入账。
        """
        await db.log_request(entry.id, tokens)
        self._invalidate_quota_cache(entry.id)
        await db.add_model_call(entry.id, tokens)
        # request 型模型按次计 1，token 型按真实 usage.total_tokens
        charge = 1 if entry.billing_mode == "request" else tokens
        if entry.token_type == "one_time":
            await db.add_one_time_usage(entry.id, charge)
            # 保留与 execute() 一致的到期判断：用满 max_tokens 则过期该一次性额度
            state = await db.get_one_time_state(entry.id)
            if state and entry.max_tokens > 0 and state["used_tokens"] >= entry.max_tokens:
                await db.expire_one_time(entry.id)
        elif entry.token_type == "rolling_5h" and entry.billing_mode == "token":
            # rolling_5h 仅 token 模式入账；request 型已在流建立时按次预扣，不重复计
            await self._charge_rolling_5h(entry, charge)
        elif entry.billing_mode == "token":
            # daily 仅 token 模式入账；daily+request 同样已预扣 1 次，不重复计
            await db.add_daily_usage(entry.id, charge)


    async def execute_embedding(self, entry: ModelEntry, req) -> tuple[dict, int]:
        """执行 embedding 调用：走 provider.embeddings()，按 prompt_tokens 计费（复用现有 quota/token_type）。"""
        provider = self._get_provider(entry)
        t0 = time.perf_counter()
        async with entry.semaphore:
            try:
                response = await provider.embeddings(req, entry.name, entry.extra_params or {})
            except RateLimitError:
                entry.cooldown_until = time.time() + 10
                raise
        self._record_latency(entry, (time.perf_counter() - t0) * 1000)

        usage = response.get("usage") or {}
        tokens_used = int(usage.get("prompt_tokens", 0) or usage.get("total_tokens", 0))
        await db.log_request(entry.id, tokens_used)
        self._invalidate_quota_cache(entry.id)
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

    async def execute_rerank(self, entry: ModelEntry, req) -> tuple[dict, int]:
        """执行 rerank 调用：走 provider.rerank()，按上游 usage 计费（复用现有 quota/token_type）。"""
        provider = self._get_provider(entry)
        t0 = time.perf_counter()
        async with entry.semaphore:
            try:
                response = await provider.rerank(req, entry.name, entry.extra_params or {})
            except RateLimitError:
                entry.cooldown_until = time.time() + 10
                raise
        self._record_latency(entry, (time.perf_counter() - t0) * 1000)

        usage = response.get("usage") or {}
        tokens_used = int(usage.get("total_tokens", 0) or usage.get("prompt_tokens", 0))
        await db.log_request(entry.id, tokens_used)
        self._invalidate_quota_cache(entry.id)
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

    async def execute_embedding_with_fallback(self, pool_name: str, req, requested_model: str | None = None, caller: str = ""):
        """embedding 专用：仅选 modality==embedding 的模型；禁用 fallback（chat 池不能兜底 embedding）。"""
        tried: set[str] = set()
        # EmbeddingRequest 无 messages 字段，需按 input 长度安全估算
        est_base = 0
        try:
            for msg in getattr(req, "messages", []) or []:
                c = msg.content
                if isinstance(c, str):
                    est_base += len(c) // 3 + 1
        except Exception:
            pass
        _inp = getattr(req, "input", "") or ""
        if isinstance(_inp, str):
            est_base += max(1, len(_inp) // 3)
        elif isinstance(_inp, list):
            est_base += sum(max(1, (len(x) + 2) // 3) for x in _inp if isinstance(x, str))
        estimated = est_base
        max_attempts = len(self.registry) + 1
        actual_calls: list[dict] = []
        override_id = self.single_override.get(pool_name)

        for _ in range(max_attempts):
            entry, steps = await self.select_model(pool_name, requested_model, estimated, exclude=tried,
                                                   required_modality="embedding")
            if steps:
                actual_calls.extend(st for st in steps if st["reason"] != "already_tried")
            if entry is None:
                break
            tried.add(entry.id)
            try:
                response, tokens = await self.execute_embedding(entry, req)
                last_reason = actual_calls[-1]["reason"] if actual_calls else ""
                if last_reason in ("selected", "single_override_selected") and actual_calls[-1]["model"] == entry.id:
                    pass
                else:
                    actual_calls.append({"model": entry.id, "reason": "selected"})
                await db.log_decision(pool_name, requested_model, entry.id, estimated, actual_calls, caller, actual_tokens=tokens)
                return response, tokens, actual_calls
            except RateLimitError:
                logger.warning(
                    f"[上游429-embedding] pool={pool_name} model={entry.id} caller={caller!r} 冷却10s"
                )
                actual_calls.append({"model": entry.id, "reason": "switch_429", "detail": {"cooldown_sec": 10, "status": 429}})
                continue
            except Exception as e:
                entry.cooldown_until = time.time() + 15
                err_type = type(e).__name__
                status = getattr(e, "status_code", None) or getattr(getattr(e, "response", None), "status_code", None)
                detail = {"cooldown_sec": 15, "error_type": err_type}
                if status is not None:
                    detail["status"] = status
                if str(e) and len(str(e)) < 200:
                    detail["error"] = str(e)
                resp_obj = getattr(e, "response", None)
                resp_body = ""
                if resp_obj is not None:
                    try:
                        await resp_obj.aread()
                        resp_body = resp_obj.text or ""
                    except Exception:
                        resp_body = ""  # 流式响应未读 body 时降级为空，避免 ResponseNotRead 二次异常
                entry_url = getattr(entry, "base_url", "") or ""
                logger.error(
                    f"[上游错误-embedding] pool={pool_name} model={entry.id} caller={caller!r} "
                    f"status={status} type={err_type} cooldown=15s url={entry_url}"
                )
                if str(e):
                    logger.error(f"[上游错误-embedding] 异常消息: {str(e)[:300]}")
                if resp_body:
                    logger.error(f"[上游错误-embedding] 上游响应体: {resp_body[:600]}")
                actual_calls.append({"model": entry.id, "reason": "switch_error", "detail": detail})
                continue

        await db.log_decision(pool_name, requested_model, None, estimated, actual_calls, caller, actual_tokens=None)
        return None, 0, actual_calls

    async def execute_rerank_with_fallback(self, pool_name: str, req, requested_model: str | None = None, caller: str = ""):
        """rerank 专用：仅选 modality==rerank 的模型；禁用 fallback（chat 池不能兜底 rerank）。"""
        tried: set[str] = set()
        # RerankRequest 无 messages 字段，按 query + documents 长度安全估算
        _q = getattr(req, "query", "") or ""
        estimated = max(1, len(_q) // 3)
        for _d in getattr(req, "documents", []) or []:
            _txt = _d if isinstance(_d, str) else (str(_d.get("text", "")) if isinstance(_d, dict) else str(_d))
            estimated += max(1, len(_txt) // 3)
        max_attempts = len(self.registry) + 1
        actual_calls: list[dict] = []

        for _ in range(max_attempts):
            entry, steps = await self.select_model(pool_name, requested_model, estimated, exclude=tried,
                                                   required_modality="rerank")
            if steps:
                actual_calls.extend(st for st in steps if st["reason"] != "already_tried")
            if entry is None:
                break
            tried.add(entry.id)
            try:
                response, tokens = await self.execute_rerank(entry, req)
                last_reason = actual_calls[-1]["reason"] if actual_calls else ""
                if last_reason in ("selected", "single_override_selected") and actual_calls[-1]["model"] == entry.id:
                    pass
                else:
                    actual_calls.append({"model": entry.id, "reason": "selected"})
                await db.log_decision(pool_name, requested_model, entry.id, estimated, actual_calls, caller, actual_tokens=tokens)
                return response, tokens, actual_calls
            except RateLimitError:
                logger.warning(
                    f"[上游429-rerank] pool={pool_name} model={entry.id} caller={caller!r} 冷却10s"
                )
                actual_calls.append({"model": entry.id, "reason": "switch_429", "detail": {"cooldown_sec": 10, "status": 429}})
                continue
            except Exception as e:
                entry.cooldown_until = time.time() + 15
                err_type = type(e).__name__
                status = getattr(e, "status_code", None) or getattr(getattr(e, "response", None), "status_code", None)
                detail = {"cooldown_sec": 15, "error_type": err_type}
                if status is not None:
                    detail["status"] = status
                if str(e) and len(str(e)) < 200:
                    detail["error"] = str(e)
                resp_obj = getattr(e, "response", None)
                resp_body = ""
                if resp_obj is not None:
                    try:
                        await resp_obj.aread()
                        resp_body = resp_obj.text or ""
                    except Exception:
                        resp_body = ""  # 流式响应未读 body 时降级为空，避免 ResponseNotRead 二次异常
                entry_url = getattr(entry, "base_url", "") or ""
                logger.error(
                    f"[上游错误-rerank] pool={pool_name} model={entry.id} caller={caller!r} "
                    f"status={status} type={err_type} cooldown=15s url={entry_url}"
                )
                if str(e):
                    logger.error(f"[上游错误-rerank] 异常消息: {str(e)[:300]}")
                if resp_body:
                    logger.error(f"[上游错误-rerank] 上游响应体: {resp_body[:600]}")
                actual_calls.append({"model": entry.id, "reason": "switch_error", "detail": detail})
                continue

        await db.log_decision(pool_name, requested_model, None, estimated, actual_calls, caller, actual_tokens=None)
        return None, 0, actual_calls

    def failure_detail(self, steps: list[dict], has_images: bool) -> str:
        attempted = any(s.get("reason") in ("selected", "single_override_selected", "switch_429", "switch_error") for s in steps)
        locked = any(str(s.get("reason", "")).startswith("single_override") for s in steps)
        if has_images and not attempted:
            return "请求包含图片，但池内没有可用的多模态模型（均为纯文本或不可用）"
        if locked:
            return "单模型锁定：锁定模型不可用或调用失败，且兜底池无可用模型"
        if not steps:
            return "池为空或无匹配模型"
        # 引用断裂：池配置里引用了不存在的池/模型（此前完全静默，只报笼统不可用）
        broken = [str(s.get("model")) for s in steps if s.get("reason") == "ref_not_found"]
        if broken:
            return f"池配置存在失效引用: {'、'.join(broken[:5])}（引用的池或模型不存在）。请到模型池管理检查引用名称"
        # json 硬门槛：全部候选因 no_json_output 被拦 → 明确指引而非笼统的不可用提示
        if any(s.get("reason") == "no_json_output" for s in steps) and not attempted:
            return "请求要求 JSON 格式输出，但该池没有任何支持格式输出（json）的模型。请在模型管理勾选“支持格式输出（json）”（默认不支持）"
        # 模态硬门槛：全部候选因模态不匹配被拦（rerank/embedding 请求打进无此类模型的池）→ 明确指引
        if not attempted and steps and all(s.get("reason") == "wrong_modality" for s in steps):
            _required = {str(s["detail"].get("required")) for s in steps if isinstance(s.get("detail"), dict)}
            if "rerank" in _required:
                return "请求要求 rerank（重排）模型，但该池没有任何 rerank 模型。请在模型管理将对应模型的模态设为 rerank"
            if "embedding" in _required:
                return "请求要求 embedding（嵌入）模型，但该池没有任何 embedding 模型。请在模型管理将对应模型的模态设为 embedding"
        return "所有候选模型均不可用：用量用尽 / RPM·TPM 触顶 / 冷却 / 超上下文"

    async def execute_with_fallback(self, pool_name: str, req, requested_model: str | None = None, caller: str = "",
                                    required_json_output: bool = False):
        tried: set[str] = set()
        estimated = self._estimate_tokens(req)
        has_images = self._has_images(req)
        max_attempts = len(self.registry) + 1
        actual_calls: list[dict] = []
        override_id = self.single_override.get(pool_name)
        fb_name = self.pools.get(pool_name, {}).get("fallback_pool")
        # When the locked single model fails (or the main pool is exhausted), escalate to the fallback pool.
        use_fallback = False
        last_overflow = None            # 最近一次"上下文超限 400"（用于整池失败时透传原文）
        last_failure_overflow = False   # 最后一次上游失败是否为上下文超限

        for _ in range(max_attempts):
            if use_fallback:
                if not fb_name:
                    break
                entry, steps = await self.select_model(fb_name, None, estimated, exclude=tried, has_images=has_images,
                                                       required_json_output=required_json_output)
            else:
                entry, steps = await self.select_model(pool_name, requested_model, estimated, exclude=tried, has_images=has_images,
                                                       required_json_output=required_json_output)

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
                await db.log_decision(pool_name, requested_model, entry.id, estimated, actual_calls, caller, actual_tokens=tokens)
                return response, tokens, actual_calls
            except RateLimitError:
                latency_ms = round((time.perf_counter() - t0) * 1000, 1)
                detail = {"cooldown_sec": 10, "status": 429, "latency_ms": latency_ms}
                logger.warning(
                    f"[上游429] pool={pool_name} model={entry.id} req_model={requested_model or '-'} "
                    f"caller={caller!r} latency={latency_ms}ms 冷却10s"
                )
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
                status = getattr(e, "status_code", None) or getattr(getattr(e, "response", None), "status_code", None)
                # 上下文超限（400 + context length 类报错）：请求过大、模型本身健康——不冷却，
                # 继续尝试其他候选；整池都容不下时把该 400 原样透传给客户端（与直连行为一致）
                if status == 400 and _is_context_overflow_error(e):
                    last_overflow = e
                    last_failure_overflow = True
                    logger.warning(
                        f"[上游上下文超限] pool={pool_name} model={entry.id} caller={caller!r} "
                        f"status=400 不冷却，尝试下一候选 | {str(e)[:180]}"
                    )
                    actual_calls.append({
                        "model": entry.id,
                        "reason": "context_overflow",
                        "detail": {"status": 400, "no_cooldown": True, "error": str(e)[:500]},
                    })
                    if override_id and not use_fallback:
                        use_fallback = True
                    continue
                last_failure_overflow = False
                # 按异常类型决定冷却时长：429→10s，5xx→5s，网络抖动→5s，其他→15s
                if isinstance(e, httpx.HTTPStatusError) and status is not None and 500 <= status < 600:
                    cooldown_sec = 5
                elif isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
                    cooldown_sec = 5
                else:
                    cooldown_sec = 15
                entry.cooldown_until = time.time() + cooldown_sec
                detail = {
                    "cooldown_sec": cooldown_sec,
                    "error_type": error_type,
                    "latency_ms": latency_ms,
                }
                if status is not None:
                    detail["status"] = status
                err_msg = str(e)
                if err_msg:
                    detail["error"] = err_msg[:500]
                # 详细错误日志：含上游原始响应体（前 600 字符），用于定位 4xx/5xx 根因
                resp_obj = getattr(e, "response", None)
                resp_body = ""
                if resp_obj is not None:
                    try:
                        await resp_obj.aread()
                        resp_body = resp_obj.text or ""
                    except Exception:
                        resp_body = ""  # 流式响应未读 body 时降级为空，避免 ResponseNotRead 二次异常
                entry_url = getattr(entry, "base_url", "") or ""
                logger.error(
                    f"[上游错误] pool={pool_name} model={entry.id} req_model={requested_model or '-'} "
                    f"caller={caller!r} status={status} type={error_type} "
                    f"cooldown={cooldown_sec}s latency={latency_ms}ms url={entry_url}"
                )
                if err_msg:
                    logger.error(f"[上游错误] 异常消息: {err_msg[:300]}")
                if resp_body:
                    logger.error(f"[上游错误] 上游响应体: {resp_body[:600]}")
                actual_calls.append({
                    "model": entry.id,
                    "reason": "fallback_switch_error" if use_fallback else "switch_error",
                    "detail": detail,
                })
                if override_id and not use_fallback:
                    use_fallback = True
                continue

        if last_failure_overflow and last_overflow is not None:
            # 整池都无法容纳该请求：把上游上下文超限 400 原样透传（与直连一致，客户端可据此自愈）
            await db.log_decision(pool_name, requested_model, None, estimated, actual_calls, caller, actual_tokens=None)
            raise ContextOverflowPassThrough(400, _upstream_error_body(last_overflow))
        await db.log_decision(pool_name, requested_model, None, estimated, actual_calls, caller, actual_tokens=None)
        return None, 0, actual_calls

    async def execute_stream_with_fallback(self, pool_name: str, req, requested_model: str | None = None, caller: str = "",
                                           required_json_output: bool = False):
        tried: set[str] = set()
        estimated = self._estimate_tokens(req)
        has_images = self._has_images(req)
        max_attempts = len(self.registry) + 1
        actual_calls: list[dict] = []
        override_id = self.single_override.get(pool_name)
        fb_name = self.pools.get(pool_name, {}).get("fallback_pool")
        use_fallback = False
        last_overflow = None            # 最近一次"上下文超限 400"（用于整池失败时透传原文）
        last_failure_overflow = False

        for _ in range(max_attempts):
            if use_fallback:
                if not fb_name:
                    break
                entry, steps = await self.select_model(fb_name, None, estimated, exclude=tried, has_images=has_images,
                                                       required_json_output=required_json_output)
            else:
                entry, steps = await self.select_model(pool_name, requested_model, estimated, exclude=tried, has_images=has_images,
                                                       required_json_output=required_json_output)

            if steps:
                actual_calls.extend(s for s in steps if s["reason"] != "already_tried")

            if entry is None:
                if not use_fallback and fb_name:
                    use_fallback = True
                    continue
                break
            tried.add(entry.id)

            try:
                # 先记录路由决策（actual_tokens 暂为 0，usage 到达后由 _wrap_stream 补写真实值）
                last_reason = actual_calls[-1]["reason"] if actual_calls else ""
                if last_reason in ("selected", "single_override_selected") and actual_calls[-1]["model"] == entry.id:
                    if use_fallback:
                        actual_calls[-1]["reason"] = "fallback_selected"
                else:
                    actual_calls.append({"model": entry.id, "reason": "fallback_selected" if use_fallback else "selected"})
                decision_id = await db.log_decision(pool_name, requested_model, entry.id, estimated, actual_calls, caller, actual_tokens=0)

                stream = await self.execute_stream(entry, req, decision_id=decision_id)
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

                # 流建立成功后才计费（按次计费计 1 次；按 token 计费由 _wrap_stream usage 到达即按实际 token 计）
                if entry.token_type == "daily" and entry.billing_mode == "request":
                    await db.add_daily_usage(entry.id, 1)
                elif entry.token_type == "rolling_5h" and entry.billing_mode == "request":
                    await self._charge_rolling_5h(entry, 1)
                self._invalidate_quota_cache(entry.id)
                return _replay(), entry, actual_calls
            except RateLimitError:
                entry.cooldown_until = time.time() + 10
                logger.warning(
                    f"[上游429-流式] pool={pool_name} model={entry.id} req_model={requested_model or '-'} "
                    f"caller={caller!r} 冷却10s"
                )
                actual_calls.append({
                    "model": entry.id,
                    "reason": "fallback_switch_429" if use_fallback else "switch_429",
                    "detail": {"cooldown_sec": 10, "status": 429},
                })
                if override_id and not use_fallback:
                    use_fallback = True
                continue
            except Exception as e:
                # 按异常类型分发冷却：5xx→5s，网络→5s，其他→15s
                status = getattr(e, "status_code", None) or getattr(getattr(e, "response", None), "status_code", None)
                # 上下文超限（400 + context length 类报错）：请求过大、模型本身健康——不冷却，
                # 继续尝试其他候选；整池都容不下时把该 400 原样透传给客户端（与直连行为一致）
                if status == 400 and _is_context_overflow_error(e):
                    last_overflow = e
                    last_failure_overflow = True
                    logger.warning(
                        f"[上游上下文超限-流式] pool={pool_name} model={entry.id} caller={caller!r} "
                        f"status=400 不冷却，尝试下一候选 | {str(e)[:180]}"
                    )
                    actual_calls.append({
                        "model": entry.id,
                        "reason": "context_overflow",
                        "detail": {"status": 400, "no_cooldown": True, "error": str(e)[:500]},
                    })
                    if override_id and not use_fallback:
                        use_fallback = True
                    continue
                last_failure_overflow = False
                if isinstance(e, httpx.HTTPStatusError) and status is not None and 500 <= status < 600:
                    cooldown_sec = 5
                elif isinstance(e, (httpx.TimeoutException, httpx.ConnectError)):
                    cooldown_sec = 5
                else:
                    cooldown_sec = 15
                entry.cooldown_until = time.time() + cooldown_sec
                detail = {"cooldown_sec": cooldown_sec, "error_type": type(e).__name__}
                if status is not None:
                    detail["status"] = status
                err_msg = str(e)
                if err_msg:
                    detail["error"] = err_msg[:500]
                # 详细错误日志：含上游原始响应体（前 600 字符）
                resp_obj = getattr(e, "response", None)
                resp_body = ""
                if resp_obj is not None:
                    try:
                        await resp_obj.aread()
                        resp_body = resp_obj.text or ""
                    except Exception:
                        resp_body = ""  # 流式响应未读 body 时降级为空，避免 ResponseNotRead 二次异常
                entry_url = getattr(entry, "base_url", "") or ""
                logger.error(
                    f"[上游错误-流式] pool={pool_name} model={entry.id} req_model={requested_model or '-'} "
                    f"caller={caller!r} status={status} type={type(e).__name__} "
                    f"cooldown={cooldown_sec}s url={entry_url}"
                )
                if err_msg:
                    logger.error(f"[上游错误-流式] 异常消息: {err_msg[:300]}")
                if resp_body:
                    logger.error(f"[上游错误-流式] 上游响应体: {resp_body[:600]}")
                actual_calls.append({
                    "model": entry.id,
                    "reason": "fallback_switch_error" if use_fallback else "switch_error",
                    "detail": detail,
                })
                if override_id and not use_fallback:
                    use_fallback = True
                continue

        if last_failure_overflow and last_overflow is not None:
            # 整池都无法容纳该请求：把上游上下文超限 400 原样透传（与直连一致，客户端可据此自愈）
            await db.log_decision(pool_name, requested_model, None, estimated, actual_calls, caller, actual_tokens=None)
            raise ContextOverflowPassThrough(400, _upstream_error_body(last_overflow))
        await db.log_decision(pool_name, requested_model, None, estimated, actual_calls, caller, actual_tokens=None)
        return None, None, actual_calls

    async def speedtest(self, model_ids: list[str] | None = None) -> list[dict]:
        if model_ids is None:
            targets = list(self.registry.keys())
        else:
            targets = list(model_ids)
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
                    self._invalidate_quota_cache(entry.id)
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
