# -*- coding: utf-8 -*-
"""计费回归套件(问题24 不变量固化)+ 思考/估算回归。

用法:python test_billing_regression.py
- 自行启动隔离实例(端口 8651,共享 gateway.db,结束清理测试数据),不影响 8650 生产。
- mock 上游(127.0.0.1:8125):流式 SSE / 非流式 JSON,usage 可控,记录收到的请求体。
- 全部断言通过 exit 0;任何失败 exit 1(优化阶段必须全绿才继续)。
"""
import io
import json
import os
import sqlite3
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx

BASE = "http://127.0.0.1:8651"
MOCK_PORT = 8125
REPO = os.path.dirname(os.path.abspath(__file__))
USAGE = {"prompt_tokens": 100, "completion_tokens": 33, "total_tokens": 133}

captured_bodies = []      # mock 收到的请求体(供思考映射断言)
mock_mode = {"usage": True, "think": False}


class MockHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("content-length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        captured_bodies.append(body)
        content = "答案"
        if mock_mode["think"]:
            content = "<think>思考过程</think>答案"
        if body.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            def sse(obj):
                return ("data: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode()
            self.wfile.write(sse({"id": "m", "choices": [{"index": 0, "delta": {"content": content, "role": "assistant"}}]}))
            self.wfile.flush()
            if mock_mode["usage"] and "nousage" not in json.dumps(body, ensure_ascii=False):
                self.wfile.write(sse({"id": "m", "choices": [], "usage": USAGE}))
                self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        else:
            out = {"id": "m", "object": "chat.completion", "choices": [
                {"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
                "usage": USAGE}
            out_b = json.dumps(out, ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out_b)))
            self.end_headers()
            self.wfile.write(out_b)

    def log_message(self, *a):
        pass


srv = HTTPServer(("127.0.0.1", MOCK_PORT), MockHandler)
threading.Thread(target=srv.serve_forever, daemon=True).start()

cfg = json.load(open(os.path.join(REPO, "config.json"), encoding="utf-8"))
ADMIN = {"Authorization": "Bearer " + cfg["server"]["api_key"], "Content-Type": "application/json"}
DB = sqlite3.connect(os.path.join(REPO, "gateway.db"))
DB.row_factory = sqlite3.Row

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(("PASS | " if cond else "FAIL | ") + name + (" | " + str(detail) if detail else ""), flush=True)


TEST_MODELS = [
    {"id": "zzbt/echo-token", "name": "mock-echo-token", "provider_id": "zzmock", "modality": "text",
     "is_free": True, "daily_token_limit": 1000000000,
     "reasoning_map": {"off": {"thinking": {"type": "disabled"}}, "low": {"reasoning_effort": "low"}}},
    {"id": "zzbt/echo-req", "name": "mock-echo-req", "provider_id": "zzmock", "modality": "text",
     "is_free": True, "daily_token_limit": 1000000000, "billing_mode": "request"},
    {"id": "zzbt/echo-once", "name": "mock-echo-once", "provider_id": "zzmock", "modality": "text",
     "is_free": True, "token_type": "one_time", "max_tokens": 200},
    {"id": "zzbt/echo-5h", "name": "mock-echo-5h", "provider_id": "zzmock", "modality": "text",
     "is_free": True, "token_type": "rolling_5h", "daily_token_limit": 1000000000},
    {"id": "zzbt/echo-smart", "name": "mock-echo-smart", "provider_id": "zzmock", "modality": "text",
     "is_free": True, "daily_token_limit": 1000000000, "smart_estimate": True},
    {"id": "zzbt/echo-nso", "name": "mock-echo-nso", "provider_id": "zzmock", "modality": "text",
     "is_free": True, "daily_token_limit": 1000000000, "no_stream_options": True},
]
TEST_IDS = [m["id"] for m in TEST_MODELS]


def db_exec(sql, args=()):
    cur = DB.execute(sql, args)
    DB.commit()
    return cur


def deep_clean():
    c = json.load(open(os.path.join(REPO, "config.json"), encoding="utf-8"))
    c["providers"] = [p for p in c.get("providers", []) if p["id"] != "zzmock"]
    c["models"] = [m for m in c.get("models", []) if not str(m.get("id", "")).startswith("zzbt/")]
    c.get("pools", {}).pop("zzall", None)
    for pn in ("zzreq", "zzonce", "zzsmart", "zznso"):
        c.get("pools", {}).pop(pn, None)
    json.dump(c, open(os.path.join(REPO, "config.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    q = " OR ".join([f"model_name='{i}'" for i in TEST_IDS])
    for t in ["token_usage", "model_daily_stats", "call_metrics", "request_log"]:
        db_exec(f"DELETE FROM {t} WHERE {q}")
    db_exec(f"DELETE FROM decision_log WHERE selected IN ({','.join(chr(39)+i+chr(39) for i in TEST_IDS)}) OR pool_name='zzall'")
    db_exec("DELETE FROM one_time_state WHERE model_name='zzbt/echo-once'")
    db_exec("DELETE FROM api_keys WHERE name='zzkey'")
    db_exec("DELETE FROM api_key_usage WHERE key_id NOT IN (SELECT id FROM api_keys)")
    db_exec("DELETE FROM api_key_hourly_usage WHERE key_id NOT IN (SELECT id FROM api_keys)")


def chat(model, effort=None, stream=False, content="hi", max_tokens=2000, auth=None, timeout=60):
    body = {"model": model, "max_tokens": max_tokens, "messages": [{"role": "user", "content": content}]}
    if effort is not None:
        body["reasoning_effort"] = effort
    if stream:
        body["stream"] = True
    h = dict(ADMIN)
    if auth:
        h["Authorization"] = "Bearer " + auth
    return httpx.post(f"{BASE}/v1/chat/completions", headers=h, json=body, timeout=timeout)


def token_used(mid):
    r = DB.execute("SELECT used_tokens FROM token_usage WHERE model_name=?", (mid,)).fetchone()
    return r["used_tokens"] if r else 0


def call_count(mid):
    r = DB.execute("SELECT request_count FROM model_daily_stats WHERE model_name=?", (mid,)).fetchone()
    return r["request_count"] if r else 0


def last_decision(selected):
    r = DB.execute("SELECT actual_tokens, estimated_tokens, id FROM decision_log WHERE selected=? ORDER BY id DESC LIMIT 1", (selected,)).fetchone()
    return dict(r) if r else None


def main():
    deep_clean()
    # 注册 mock provider + 测试模型 + 池
    c = json.load(open(os.path.join(REPO, "config.json"), encoding="utf-8"))
    c["providers"].append({"id": "zzmock", "name": "zzmock", "protocol": "openai",
                           "base_url": f"http://127.0.0.1:{MOCK_PORT}/v1", "api_key": "x"})
    c["models"].extend(TEST_MODELS)
    c["pools"]["zzall"] = {"model_ids": TEST_IDS, "strategy": "sequential"}
    c["pools"]["zzreq"] = {"model_ids": ["zzbt/echo-req"], "strategy": "sequential"}
    c["pools"]["zzonce"] = {"model_ids": ["zzbt/echo-once"], "strategy": "sequential"}
    c["pools"]["zzsmart"] = {"model_ids": ["zzbt/echo-smart"], "strategy": "sequential"}
    c["pools"]["zznso"] = {"model_ids": ["zzbt/echo-nso"], "strategy": "sequential"}
    json.dump(c, open(os.path.join(REPO, "config.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    # 启动隔离实例
    env = dict(os.environ, MODEL_GATEWAY_PORT="8651")
    proc = subprocess.Popen([sys.executable, "main.py"], cwd=REPO, env=env,
                            stdout=open(os.path.join(REPO, "logs", "regression_8651.log"), "ab"),
                            stderr=subprocess.STDOUT)
    try:
        up = False
        for _ in range(40):
            time.sleep(0.5)
            try:
                if httpx.get(f"{BASE}/health", timeout=2).status_code == 200:
                    up = True
                    break
            except Exception:
                pass
        if not up:
            raise RuntimeError("8651 实例未启动")
        httpx.post(f"{BASE}/admin/reload", headers=ADMIN, timeout=30)
        time.sleep(0.5)

        # ===== T1 流式正常 usage =====
        used0 = token_used("zzbt/echo-token")
        r = chat("zzall", stream=True)
        tail_ok = "[DONE]" in r.text
        d = last_decision("zzbt/echo-token")
        check("T1a 流式[DONE]+无think泄漏", r.status_code == 200 and tail_ok and "<think>" not in r.text, r.status_code)
        check("T1b 流式入账=133", token_used("zzbt/echo-token") - used0 == 133, token_used("zzbt/echo-token") - used0)
        check("T1c decision.actual_tokens=133", d and d["actual_tokens"] == 133, d)
        mrow = DB.execute("SELECT total_tokens FROM call_metrics WHERE model_name='zzbt/echo-token' ORDER BY id DESC LIMIT 1").fetchone()
        check("T1d call_metrics样本", mrow and mrow["total_tokens"] == 133, dict(mrow) if mrow else None)

        # ===== T2 流式缺失 usage =====
        used0 = token_used("zzbt/echo-token")
        cnt0 = call_count("zzbt/echo-token")
        r = chat("zzall", stream=True, content="nousage")
        d = last_decision("zzbt/echo-token")
        check("T2a 缺失usage不静默:actual=0", r.status_code == 200 and d and d["actual_tokens"] == 0, d)
        check("T2b 缺失usage仍记调用次数", call_count("zzbt/echo-token") - cnt0 == 1, call_count("zzbt/echo-token") - cnt0)
        check("T2c 缺失usage不计token", token_used("zzbt/echo-token") - used0 == 0, token_used("zzbt/echo-token") - used0)

        # ===== T3 request 计费型流式:预扣1次,settle不重复 =====
        used0 = token_used("zzbt/echo-req")
        r = chat("zzreq", stream=True)
        d = last_decision("zzbt/echo-req")
        delta = token_used("zzbt/echo-req") - used0
        check("T3a request型流式计费=1次", delta == 1, delta)
        check("T3b decision.actual_tokens=真实133", d and d["actual_tokens"] == 133, d)

        # ===== T4 one_time 流式:入账+到顶自动过期 =====
        r = chat("zzonce", stream=True)
        s1 = DB.execute("SELECT used_tokens, expired FROM one_time_state WHERE model_name='zzbt/echo-once'").fetchone()
        check("T4a one_time第1次入账133", s1 and s1["used_tokens"] == 133 and not s1["expired"], dict(s1) if s1 else None)
        r = chat("zzonce", stream=True)
        s2 = DB.execute("SELECT used_tokens, expired FROM one_time_state WHERE model_name='zzbt/echo-once'").fetchone()
        check("T4b 第2次后used=266且自动过期", s2 and s2["used_tokens"] == 266 and s2["expired"] == 1, dict(s2) if s2 else None)
        r = chat("zzonce", stream=True)
        check("T4c 过期后调用被拒(503无可用接口)", r.status_code == 503, r.status_code)

        # ===== T5 非流式:5项写入全部落库 =====
        used0 = token_used("zzbt/echo-token")
        mc0 = call_count("zzbt/echo-token")
        cm0 = DB.execute("SELECT count(*) c FROM call_metrics WHERE model_name='zzbt/echo-token'").fetchone()["c"]
        r = chat("zzall", max_tokens=700)
        d = last_decision("zzbt/echo-token")
        row = DB.execute("SELECT count(*) c FROM request_log WHERE model_name='zzbt/echo-token'").fetchone()["c"]
        check("T5a 非流式200+content", r.status_code == 200 and (r.json().get("choices") or [{}])[0].get("message", {}).get("content"), r.status_code)
        check("T5b token_usage入账", token_used("zzbt/echo-token") - used0 == 133, token_used("zzbt/echo-token") - used0)
        check("T5c model_daily_stats调用+1", call_count("zzbt/echo-token") - mc0 == 1, call_count("zzbt/echo-token") - mc0)
        check("T5d request_log落库", row > 0, row)
        check("T5e call_metrics落库", DB.execute("SELECT count(*) c FROM call_metrics WHERE model_name='zzbt/echo-token'").fetchone()["c"] == cm0 + 1, "")
        check("T5f decision.actual_tokens=133", d and d["actual_tokens"] == 133, d)

        # ===== T6 用户 key 计费 =====
        r = httpx.post(f"{BASE}/admin/keys", headers=ADMIN,
                       json={"name": "zzkey", "type": "user", "allowed_pools": ["zzall"],
                             "token_type": "daily", "billing_mode": "token", "limit_amount": 1000000}, timeout=15)
        key_id = r.json().get("key", {}).get("id")
        secret = DB.execute("SELECT secret FROM api_keys WHERE name='zzkey'").fetchone()["secret"]
        u0 = DB.execute("SELECT used_amount FROM api_key_usage WHERE key_id=?", (key_id,)).fetchone()
        u0 = u0["used_amount"] if u0 else 0
        r = chat("zzall", auth=secret, max_tokens=500)
        u1 = DB.execute("SELECT used_amount FROM api_key_usage WHERE key_id=?", (key_id,)).fetchone()
        u1 = u1["used_amount"] if u1 else 0
        h1 = DB.execute("SELECT used_amount FROM api_key_hourly_usage WHERE key_id=?", (key_id,)).fetchone()
        check("T6a 用户key调用200", r.status_code == 200, r.status_code)
        check("T6b key_usage入账133", u1 - u0 == 133, u1 - u0)
        check("T6c hourly入账133", h1 and h1["used_amount"] == 133, dict(h1) if h1 else None)

        # ===== T7 并发10路(5流式+5非流式) =====
        used0 = token_used("zzbt/echo-token")
        import concurrent.futures
        def one(i):
            if i % 2 == 0:
                return chat("zzall", stream=True, max_tokens=500).status_code
            return chat("zzall", max_tokens=500).status_code
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            codes = list(ex.map(one, range(10)))
        delta = token_used("zzbt/echo-token") - used0
        check("T7a 并发10路全部200", all(c == 200 for c in codes), codes)
        check("T7b 并发总入账=1330(无重复无丢失)", delta == 1330, delta)

        # ===== T8 思考回归(mock 透传) =====
        captured_bodies.clear()
        chat("zzall", effort="off", max_tokens=500)
        b = captured_bodies[-1]
        check("T8a off→thinking disabled", b.get("thinking") == {"type": "disabled"} and "reasoning_effort" not in b, b.get("thinking"))
        captured_bodies.clear()
        chat("zzall", effort="low", max_tokens=500)
        b = captured_bodies[-1]
        check("T8b low→reasoning_effort=low", b.get("reasoning_effort") == "low" and "thinking" not in b, b.get("reasoning_effort"))
        captured_bodies.clear()
        chat("zzall", effort="auto", max_tokens=500)
        b = captured_bodies[-1]
        check("T8c auto→不注入", "reasoning_effort" not in b and "thinking" not in b, {k: b.get(k) for k in ("reasoning_effort", "thinking")})
        captured_bodies.clear()
        mock_mode["think"] = True
        r = chat("zzall", stream=True, max_tokens=500)
        mock_mode["think"] = False
        check("T8d 流式<think>→reasoning_content增量", r.status_code == 200 and '"reasoning_content"' in r.text and "<think>" not in r.text, "")
        r = httpx.post(f"{BASE}/v1/messages", headers={**ADMIN, "anthropic-version": "2023-06-01"},
                       json={"model": "zzall", "max_tokens": 2000, "thinking": {"type": "disabled"},
                             "messages": [{"role": "user", "content": "hi"}]}, timeout=60)
        b = captured_bodies[-1]
        check("T8e anthropic入口disabled→thinking disabled", r.status_code == 200 and b.get("thinking") == {"type": "disabled"}, b.get("thinking"))

        # ===== T9 智能估算 =====
        m0 = httpx.get(f"{BASE}/admin/model/zzbt/echo-smart/metrics", headers=ADMIN, timeout=15).json()
        r = chat("zzsmart", max_tokens=500)
        m1 = httpx.get(f"{BASE}/admin/model/zzbt/echo-smart/metrics", headers=ADMIN, timeout=15).json()
        check("T9a smart模型metrics样本增长", r.status_code == 200 and m1.get("sample_count", 0) > m0.get("sample_count", 0), (m0, m1))
        d = last_decision("zzbt/echo-smart")
        check("T9b decision估算字段存在", d and d["estimated_tokens"] is not None, d)
        r = httpx.get(f"{BASE}/admin/decisions?limit=3", headers=ADMIN, timeout=15)
        j = r.json()
        arr = j.get("decisions") if isinstance(j, dict) else j
        check("T9c 决策API含actual_tokens", r.status_code == 200 and arr and all("actual_tokens" in x for x in arr[:3]), r.status_code)

        # ===== T10 no_stream_options 开关(问题21-B) =====
        captured_bodies.clear()
        r = chat("zznso", stream=True, max_tokens=500)
        b = captured_bodies[-1]
        check("T10a nso模型流式不含stream_options", r.status_code == 200 and "stream_options" not in b, b.get("stream_options", "(无)"))
        captured_bodies.clear()
        r = chat("zzall", stream=True, max_tokens=500)
        b = captured_bodies[-1]
        check("T10b 普通模型流式保留stream_options", r.status_code == 200 and b.get("stream_options") == {"include_usage": True}, b.get("stream_options"))

    finally:
        try:
            deep_clean()
        except Exception:
            pass
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            pass
        srv.shutdown()

    fails = [n for n, okk in RESULTS if not okk]
    print(f"\n===== 回归套件 {len(RESULTS) - len(fails)}/{len(RESULTS)} 通过 =====", flush=True)
    for n in fails:
        print("  失败:", n, flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    import subprocess
    try:
        main()
    except Exception:
        traceback.print_exc()
        try:
            deep_clean()
        except Exception:
            pass
        sys.exit(1)
