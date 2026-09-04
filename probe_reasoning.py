"""全池模型思考参数探测：摸清每个上游模型支持的思考开关/档位写法，生成 reasoning_map 建议。

用法：
  python probe_reasoning.py                     # 探测所有未缓存模型并生成报告
  python probe_reasoning.py --force             # 忽略缓存重新探测
  python probe_reasoning.py --filter minimax    # 只探测 id/上游名含关键字的模型（不区分大小写）
  python probe_reasoning.py --report            # 只根据缓存重新生成报告
  python probe_reasoning.py --apply             # 把建议映射写入 config.json（先备份，再尝试 /admin/reload）

说明：
- 直接调用各 provider 的上游接口（不经过网关），小 prompt + max_tokens 限制，少量费用。
- 相同 (协议, base_url, 上游模型名) 的条目只探测一次，结果复用到同组合的所有模型。
- 缓存：reasoning_probe_cache.json（增量，断点续跑）；报告：思考参数探测报告.md。
"""

import argparse
import copy
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
CACHE_PATH = BASE_DIR / "reasoning_probe_cache.json"
REPORT_PATH = BASE_DIR / "思考参数探测报告.md"

PROBE_PROMPT = "一个房间里有3支蜡烛，吹灭了2支，最后房间里还剩下几支？请简要回答。"
OPENAI_MAX_TOKENS = 1200
TIMEOUT = httpx.Timeout(90, connect=10)
SKIP_MODALITY = ("embedding", "rerank")
EFFORT_LEVELS = ["minimal", "low", "medium", "high", "max"]
ANTHROPIC_BUDGETS = {"minimal": 1024, "low": 4096, "medium": 10240, "high": 20480, "max": 32768}


def load_cache() -> dict:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_cache(cache: dict):
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")


def build_targets(config: dict, keyword: str = "") -> list[dict]:
    """待探测去重组合：{key, protocol, base_url, api_key, proxy_url, model_name, example_ids}"""
    providers = {p["id"]: p for p in config.get("providers", [])}
    combos: dict[tuple, dict] = {}
    for m in config.get("models", []):
        if m.get("modality") in SKIP_MODALITY:
            continue
        pid = m.get("provider_id", "")
        prov = providers.get(pid)
        if prov:
            protocol = prov.get("protocol", "openai")
            base_url = (prov.get("base_url") or "").rstrip("/")
            api_key = prov.get("api_key", "")
            proxy_url = prov.get("proxy_url", "") or ""
        else:  # legacy 内联连接信息
            protocol = m.get("provider", "openai")
            base_url = (m.get("base_url") or "").rstrip("/")
            api_key = m.get("api_key", "")
            proxy_url = m.get("proxy_url", "") or ""
        if not base_url or not api_key:
            continue
        name = m.get("name", "")
        if not name:
            continue
        key = f"{protocol}|{base_url}|{name}"
        if key not in combos:
            combos[key] = {
                "key": key, "protocol": protocol, "base_url": base_url,
                "api_key": api_key, "proxy_url": proxy_url, "name": name,
                "example_ids": [],
            }
        combos[key]["example_ids"].append(m["id"])
    targets = list(combos.values())
    if keyword:
        kw = keyword.lower()
        targets = [t for t in targets
                   if kw in t["name"].lower() or any(kw in i.lower() for i in t["example_ids"])]
    return targets


def reasoning_of_message(msg: dict) -> int:
    if not isinstance(msg, dict):
        return 0
    for k in ("reasoning_content", "reasoning"):
        v = msg.get(k)
        if isinstance(v, str):
            return len(v)
    return 0


def call_upstream(target: dict, fragment: dict | None) -> dict:
    """发一次探测请求。返回 {status, error, reasoning_len, content_len, completion_tokens}"""
    if target["protocol"] == "anthropic":
        body = {
            "model": target["name"],
            "max_tokens": 3072,
            "messages": [{"role": "user", "content": PROBE_PROMPT}],
        }
        if fragment:
            th = copy.deepcopy(fragment)
            if isinstance(th.get("thinking"), dict) and th["thinking"].get("type") == "enabled":
                budget = int(th["thinking"].get("budget_tokens") or 2048)
                body["max_tokens"] = max(body["max_tokens"], budget + 1024)
            body.update(th)
        url = target["base_url"] + "/v1/messages"
        headers = {"x-api-key": target["api_key"], "anthropic-version": "2023-06-01",
                   "Content-Type": "application/json"}
    else:
        body = {
            "model": target["name"],
            "max_tokens": OPENAI_MAX_TOKENS,
            "messages": [{"role": "user", "content": PROBE_PROMPT}],
        }
        if fragment:
            body.update(fragment)
        url = target["base_url"] + "/chat/completions"
        headers = {"Authorization": "Bearer " + target["api_key"], "Content-Type": "application/json"}

    kwargs = {"timeout": TIMEOUT}
    if target["proxy_url"]:
        kwargs["proxy"] = target["proxy_url"]
    out = {"status": 0, "error": "", "reasoning_len": 0, "content_len": 0, "completion_tokens": 0}
    try:
        with httpx.Client(**kwargs) as client:
            resp = client.post(url, json=body, headers=headers)
        out["status"] = resp.status_code
        if resp.status_code != 200:
            out["error"] = (resp.text or "")[:160].replace("\n", " ")
            return out
        data = resp.json()
        if target["protocol"] == "anthropic":
            blocks = data.get("content") or []
            out["reasoning_len"] = sum(len(b.get("thinking", "") or "") for b in blocks
                                       if isinstance(b, dict) and b.get("type") == "thinking")
            out["content_len"] = sum(len(b.get("text", "") or "") for b in blocks
                                     if isinstance(b, dict) and b.get("type") == "text")
            out["completion_tokens"] = (data.get("usage") or {}).get("output_tokens", 0)
        else:
            msg = ((data.get("choices") or [{}])[0].get("message") or {})
            rc = reasoning_of_message(msg)
            content = msg.get("content") or ""
            # 部分模型（MiniMax-M3 等）把思考内联在 content 的 <think>...</think> 里
            if isinstance(content, str):
                mm = re.search(r"<think>(.*?)</think>", content, re.S)
                if mm:
                    rc = max(rc, len(mm.group(1)))
                elif content.lstrip().startswith("<think>"):
                    rc = max(rc, len(content))
            out["reasoning_len"] = rc
            out["content_len"] = len(msg.get("content") or "")
            out["completion_tokens"] = (data.get("usage") or {}).get("completion_tokens", 0)
    except Exception as e:
        out["error"] = str(e)[:160]
    return out


def call_with_retry(target: dict, fragment: dict | None) -> dict:
    r = call_upstream(target, fragment)
    if r["status"] in (0, 429, 500, 502, 503, 504) or (r["status"] == 200 and r["completion_tokens"] == 0 and not r["error"]):
        time.sleep(4)
        r2 = call_upstream(target, fragment)
        if r2["status"] == 200:
            return r2
    return r


def probe_target(target: dict) -> dict:
    """探测一个去重组合，返回缓存条目。"""
    proto = target["protocol"]
    rec = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "protocol": proto, "name": target["name"],
           "calls": [], "baseline_reasoning": 0, "default_thinking": "?", "family": "",
           "off_fragment": None, "on_fragment": None, "level_lens": {}, "levels_effective": False,
           "suggested": {}, "notes": []}

    def log(fragment, r):
        rec["calls"].append({"fragment": fragment, "status": r["status"], "error": r["error"],
                             "reasoning_len": r["reasoning_len"], "content_len": r["content_len"],
                             "completion_tokens": r["completion_tokens"]})

    # 1) 基线
    base = call_with_retry(target, None)
    log(None, base)
    if base["status"] != 200:
        rec["default_thinking"] = f"不可用({base['status'] or base['error'][:60]})"
        rec["notes"].append("基线请求失败，跳过该组合")
        return rec
    rec["baseline_reasoning"] = base["reasoning_len"]
    rec["default_thinking"] = "开" if base["reasoning_len"] > 0 else "关"

    time.sleep(0.3)
    if proto == "anthropic":
        probe_anthropic(target, rec, log)
    else:
        probe_openai(target, rec, log)
    return rec


def _has_gradient(level_lens: dict, baseline: int) -> bool:
    lens = [v for v in level_lens.values() if v]
    if len(lens) >= 2 and max(lens) / max(1, min(lens)) >= 1.5:
        return True
    for v in level_lens.values():
        if baseline and v and (v > baseline * 1.5 or v < baseline * 0.5):
            return True
        if not baseline and v:
            return True
    return False


def _off_dropped(off_len: int, baseline: int, on_len: int = 0) -> bool:
    if baseline:
        return off_len < max(1, baseline * 0.3)
    return on_len > 0  # 基线不思考时，用"开了之后确实思考"证明开关有效


def probe_anthropic(target: dict, rec: dict, log):
    rec["family"] = "anthropic thinking(type,budget_tokens)"
    off = {"thinking": {"type": "disabled"}}
    r_off = call_with_retry(target, off)
    log(off, r_off)
    time.sleep(0.3)
    on = {"thinking": {"type": "enabled", "budget_tokens": 4096}}
    r_on = call_with_retry(target, on)
    log(on, r_on)
    if r_off["status"] != 200 or r_on["status"] != 200:
        rec["notes"].append(f"thinking 参数未全部接受(off={r_off['status']},on={r_on['status']})，不生成映射")
        return
    rec["off_fragment"] = off
    rec["on_fragment"] = on
    if _off_dropped(r_off["reasoning_len"], rec["baseline_reasoning"], r_on["reasoning_len"]):
        lens = {}
        for lv, budget in ANTHROPIC_BUDGETS.items():
            time.sleep(0.3)
            frag = {"thinking": {"type": "enabled", "budget_tokens": budget}}
            r = call_with_retry(target, frag)
            log(frag, r)
            lens[lv] = r["reasoning_len"]
        rec["level_lens"] = lens
        rec["levels_effective"] = _has_gradient(lens, rec["baseline_reasoning"])
        rec["suggested"] = {lv: {"thinking": {"type": "enabled", "budget_tokens": ANTHROPIC_BUDGETS[lv]}}
                            for lv in EFFORT_LEVELS}
        rec["suggested"]["off"] = off
    else:
        rec["notes"].append("thinking 开关未见效果（接受但行为不变），映射留空")
    if rec["suggested"]:
        ordered = {k: rec["suggested"][k] for k in ["off", "minimal", "low", "medium", "high", "max"] if k in rec["suggested"]}
        rec["suggested"] = ordered


def _find_true_off(target: dict, rec: dict, log, baseline: int) -> dict | None:
    """档位族生效后，寻找能真正完全关闭思考的写法（部分模型 off 与档位分属不同参数族）"""
    for cand in ({"thinking": {"type": "disabled"}}, {"enable_thinking": False}, {"reasoning": {"enabled": False}}):
        time.sleep(0.3)
        r = call_with_retry(target, cand)
        log(cand, r)
        if r["status"] == 200 and r["reasoning_len"] <= max(1, baseline * 0.05):
            return cand
    return None


def probe_openai(target: dict, rec: dict, log):
    baseline = rec["baseline_reasoning"]
    families = [
        ("reasoning_effort",
         lambda lv: {"reasoning_effort": lv},
         {"reasoning_effort": "minimal"}, {"reasoning_effort": "high"}),
        ("reasoning{effort}",
         lambda lv: {"reasoning": {"effort": lv}},
         {"reasoning": {"enabled": False}}, {"reasoning": {"enabled": True}}),
        ("thinking{type}",
         lambda lv: None,
         {"thinking": {"type": "disabled"}}, {"thinking": {"type": "enabled"}}),
        ("enable_thinking",
         lambda lv: None,
         {"enable_thinking": False}, {"enable_thinking": True}),
    ]
    for fam, lv_frag, off_frag, on_frag in families:
        # off 候选探针
        r_off = call_with_retry(target, off_frag)
        log(off_frag, r_off)
        time.sleep(0.3)
        r_on = call_with_retry(target, on_frag)
        log(on_frag, r_on)
        time.sleep(0.3)
        if r_off["status"] != 200 or r_on["status"] != 200:
            continue  # 上游拒绝该写法（400/422 等），试下一族
        rec["off_fragment"] = off_frag
        rec["on_fragment"] = on_frag
        off_ok = _off_dropped(r_off["reasoning_len"], baseline, r_on["reasoning_len"])
        # 档位扫描（带档位的族）
        lens = {}
        if lv_frag:
            for lv in EFFORT_LEVELS:
                frag = lv_frag(lv)
                r = call_with_retry(target, frag)
                log(frag, r)
                lens[lv] = r["reasoning_len"]
                time.sleep(0.3)
            rec["level_lens"] = lens
            rec["levels_effective"] = _has_gradient(lens, baseline)
        rec["family"] = fam
        if rec["levels_effective"]:
            rec["suggested"] = {lv: lv_frag(lv) for lv in EFFORT_LEVELS}
            true_off = _find_true_off(target, rec, log, baseline)
            if true_off:
                rec["suggested"]["off"] = true_off
                rec["off_fragment"] = true_off
                rec["notes"].append("档位梯度实测有效；off 采用完全关闭写法（与档位不同参数族）")
            elif off_ok:
                rec["suggested"]["off"] = off_frag
                rec["off_fragment"] = off_frag
                rec["notes"].append("档位梯度实测有效；无完全关闭写法，off 映射为最低档")
            else:
                rec["notes"].append("档位有梯度但不支持关闭，未映射 off")
        elif off_ok:
            # 仅开/关有效：minimal..max 全部映射为"开"片段（上游无真实档位）
            rec["suggested"] = {lv: on_frag for lv in ["minimal", "low", "medium", "high", "max"]}
            rec["suggested"]["off"] = off_frag
            rec["notes"].append("仅开/关两档有效（档位无梯度），minimal~max 统一映射为开")
        elif rec["levels_effective"]:
            rec["suggested"] = {lv: lv_frag(lv) for lv in EFFORT_LEVELS}
            rec["notes"].append("档位有梯度但不支持关闭，未映射 off")
        else:
            rec["off_fragment"] = None
            rec["on_fragment"] = None
            rec["family"] = ""
            rec["notes"].append(f"{fam} 写法被接受但行为无变化，继续试下一族")
            continue
        ordered = {k: rec["suggested"][k] for k in ["off", "minimal", "low", "medium", "high", "max"] if k in rec["suggested"]}
        rec["suggested"] = ordered
        return
    if not rec["family"]:
        rec["notes"].append("所有思考参数写法均无效/被拒绝，映射留空（不支持思考控制）")


def cached_suggestion(model: dict, providers: list[dict]) -> dict | None:
    """该模型的上游组合已有探测缓存时直接返回建议映射（不发起任何请求）；否则 None。"""
    targets = build_targets({"providers": providers, "models": [model]})
    if not targets:
        return None
    rec = load_cache().get(targets[0]["key"])
    return (rec or {}).get("suggested") or None


def probe_single(model: dict, providers: list[dict]) -> dict | None:
    """探测单个模型条目（命中缓存则复用），返回建议 reasoning_map；不支持/失败返回 None。

    供 admin 新增模型时自动探测调用；结果同步写入缓存。
    """
    targets = build_targets({"providers": providers, "models": [model]})
    if not targets:
        return None
    t = targets[0]
    cache = load_cache()
    rec = cache.get(t["key"])
    if not rec or not rec.get("calls"):
        rec = probe_target(t)
        cache[t["key"]] = rec
        save_cache(cache)
    return rec.get("suggested") or None


def generate_report(cache: dict, config: dict):
    providers = {p["id"]: p for p in config.get("providers", [])}
    lines = ["# 思考参数探测报告", "",
             f"生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')}｜探测 prompt：{PROBE_PROMPT!r}",
             "",
             "统一档位：off/minimal/low/medium/high/max；客户端在 /v1/chat/completions 顶层传 `reasoning_effort`，",
             "/v1/messages 传 `thinking`（自动归一化），网关按各模型 reasoning_map 注入上游参数。",
             ""]
    # 汇总表（按 config 模型顺序）
    lines += ["## 汇总（按 config.json 模型顺序）", "",
              "| 模型 | 上游名 | 协议 | 默认思考 | 映射方案 |", "|---|---|---|---|---|"]
    for m in config.get("models", []):
        if m.get("modality") in SKIP_MODALITY:
            continue
        pid = m.get("provider_id", "")
        prov = providers.get(pid) or {}
        protocol = prov.get("protocol") or m.get("provider", "openai")
        key = f"{protocol}|{(prov.get('base_url') or m.get('base_url','')).rstrip('/')}|{m.get('name','')}"
        rec = cache.get(key)
        if not rec:
            lines.append(f"| {m['id']} | {m.get('name','')} | {protocol} | ? | （未探测） |")
            continue
        plan = rec.get("family") or "不支持思考控制"
        if rec.get("suggested"):
            plan = "off+开关全映射" if rec.get("off_fragment") and not rec.get("levels_effective") else (
                "六档全梯度" if rec.get("levels_effective") else plan)
        lines.append(f"| {m['id']} | {m.get('name','')} | {protocol} | {rec.get('default_thinking','?')} | {plan} |")
    # 明细：每个去重组合
    lines += ["", "## 探测明细（按 (协议, base_url, 上游模型名) 去重）", ""]
    for key, rec in sorted(cache.items()):
        proto, base_url, name = key.split("|", 2)
        lines.append(f"### `{name}` @ {base_url}（{proto}）— 默认思考: {rec.get('default_thinking','?')}，探测于 {rec.get('ts','')}")
        lines.append("")
        if rec.get("notes"):
            for n in rec["notes"]:
                lines.append(f"- 结论：{n}")
        if rec.get("calls"):
            lines += ["", "| 注入片段 | HTTP | 思考字数 | 正文 | completion tokens | 备注 |",
                      "|---|---|---|---|---|---|"]
            for c in rec["calls"]:
                frag = json.dumps(c["fragment"], ensure_ascii=False) if c["fragment"] else "（基线）"
                note = c["error"] if c["error"] else ""
                lines.append(f"| `{frag}` | {c['status']} | {c['reasoning_len']} | {c['content_len']} | {c['completion_tokens']} | {note} |")
        if rec.get("suggested"):
            lines += ["", "建议 reasoning_map：", "", "```json",
                      json.dumps(rec["suggested"], ensure_ascii=False, indent=2), "```"]
        lines.append("")
    # MiniMax 对比
    mm_keys = [k for k in cache if "minimax" in k.lower()]
    if len(mm_keys) >= 2:
        lines += ["## MiniMax M3 vs M2.7-highspeed", ""]
        for k in mm_keys:
            rec = cache[k]
            lines.append(f"- `{k.split('|',2)[2]}`：默认思考={rec.get('default_thinking')}，族={rec.get('family') or '无'}，"
                         f"档位梯度={rec.get('levels_effective')}，off有效={bool(rec.get('off_fragment'))}，"
                         f"各档思考字数={rec.get('level_lens') or '—'}")
        lines.append("")
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"报告已生成: {REPORT_PATH}")


def apply_to_config(cache: dict, config: dict) -> int:
    providers = {p["id"]: p for p in config.get("providers", [])}
    changed = 0
    for m in config.get("models", []):
        if m.get("modality") in SKIP_MODALITY:
            continue
        pid = m.get("provider_id", "")
        prov = providers.get(pid) or {}
        protocol = prov.get("protocol") or m.get("provider", "openai")
        key = f"{protocol}|{(prov.get('base_url') or m.get('base_url','')).rstrip('/')}|{m.get('name','')}"
        rec = cache.get(key)
        if not rec:
            continue
        if rec.get("suggested"):
            m["reasoning_map"] = rec["suggested"]
            changed += 1
        else:
            m.pop("reasoning_map", None)  # 不支持的模型：显式留空
            changed += 1
    return changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="忽略缓存重新探测")
    ap.add_argument("--filter", default="", help="只探测 id/上游名含关键字的模型")
    ap.add_argument("--report", action="store_true", help="只根据缓存重新生成报告")
    ap.add_argument("--apply", action="store_true", help="把建议映射写入 config.json")
    ap.add_argument("--workers", type=int, default=5)
    args = ap.parse_args()

    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    cache = load_cache()

    if not args.report:
        targets = build_targets(config, args.filter)
        todo = [t for t in targets if args.force or t["key"] not in cache]
        print(f"待探测组合 {len(todo)} 个（共 {len(targets)} 个，缓存命中 {len(targets) - len(todo)}）")
        done = 0
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futs = {ex.submit(probe_target, t): t for t in todo}
            for fut in as_completed(futs):
                t = futs[fut]
                done += 1
                try:
                    rec = fut.result()
                except Exception as e:
                    rec = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "protocol": t["protocol"],
                           "name": t["name"], "calls": [], "baseline_reasoning": 0,
                           "default_thinking": f"探测异常:{str(e)[:80]}", "family": "",
                           "off_fragment": None, "on_fragment": None, "level_lens": {},
                           "levels_effective": False, "suggested": {}, "notes": [f"探测异常: {e}"]}
                cache[t["key"]] = rec
                save_cache(cache)
                print(f"[{done}/{len(todo)}] {t['name']} @ {t['base_url']} -> "
                      f"默认{rec.get('default_thinking')} 族={rec.get('family') or '无'} "
                      f"档位有效={rec.get('levels_effective')} off={'有' if rec.get('off_fragment') else '无'}")

    generate_report(cache, config)

    if args.apply:
        n = apply_to_config(cache, config)
        bak = CONFIG_PATH.with_name("config.json.bak_reasoning")
        if not bak.exists():
            pass  # 第 0 步已备份；此处不再覆盖首份备份
        CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"已写入 {n} 个模型的 reasoning_map 到 config.json")
        # 尝试让运行中的网关热重载
        try:
            host = config.get("server", {}).get("host", "127.0.0.1") or "127.0.0.1"
            port = int(config.get("server", {}).get("port", 8650))
            r = httpx.post(f"http://{host}:{port}/admin/reload",
                           headers={"Authorization": "Bearer " + config.get("server", {}).get("api_key", "")},
                           timeout=10)
            print(f"/admin/reload -> {r.status_code}")
        except Exception as e:
            print(f"/admin/reload 调用失败（网关未运行？稍后重启即可）: {e}")


if __name__ == "__main__":
    sys.exit(main())
