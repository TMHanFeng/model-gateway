import time
import json
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, RedirectResponse, HTMLResponse
from pydantic import ValidationError
from models import ChatCompletionRequest
from pool import ModelPool, load_config
from database import init_db, close_db
import database as db
import keyauth
from scheduler import start_scheduler
from admin import router as admin_router, GATEWAY_VERSION, GATEWAY_COMMIT, get_gateway_version
from format_adapter import (
    is_anthropic_request,
    anthropic_to_openai,
    openai_to_anthropic_response,
    openai_sse_to_anthropic,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    start_scheduler()
    cfg = load_config().get("server", {})
    base = f"http://{cfg.get('host', '127.0.0.1')}:{cfg.get('port', 8650)}"
    print(f"\n  后台管理地址:  {base}/admin/\n  API 服务地址:  {base}/v1\n")
    yield
    await pool.close_all()
    await close_db()


app = FastAPI(title="Model Gateway", lifespan=lifespan)
app.include_router(admin_router)
pool = ModelPool()


async def verify_key(request: Request) -> dict:
    """认证：服务器密钥(管理员账号) / 管理员 Key / 用户 Key。返回认证上下文。"""
    config = load_config()
    expected = config.get("server", {}).get("api_key", "")
    auth = request.headers.get("Authorization", "")
    token = auth[7:].strip() if auth.startswith("Bearer ") else ""
    # Anthropic SDK 用 x-api-key 请求头认证：无 Bearer token 时兜底
    if not token:
        token = request.headers.get("x-api-key", "").strip()
    # 未配置服务器密钥时保持向后兼容：全部放行
    if not expected:
        return {"kind": "server_admin", "key": None}
    if token and token == expected:
        return {"kind": "server_admin", "key": None}
    if token:
        rec = await db.get_api_key_by_secret_or_previous(token)
        if rec and rec.get("enabled"):
            # 到期自动轮换：旧 secret 在宽限期内仍可认证，用轮换后的新记录继续本次认证（用户无感）
            rec = await keyauth.maybe_rotate_expired(rec) or rec
            return {"kind": "key_admin" if rec["type"] == "admin" else "key_user", "key": rec}
    raise HTTPException(status_code=401, detail="Invalid API key")


async def require_admin(auth: dict = Depends(verify_key)):
    """仅管理员（服务器密钥或管理员 Key）可访问：用量统计 / 测速。"""
    if auth["kind"] == "key_user":
        raise HTTPException(status_code=403, detail="用户 API Key 无权访问该接口")
    return auth


def _resolve(model_field: str) -> str | None:
    # Externally only model pools are callable; individual models are not exposed.
    if not model_field or model_field == "auto":
        return "auto"
    if model_field in pool.pools:
        return model_field
    return None


def _wrap_key_stream(stream, key: dict, billing_mode: str):
    """流式响应用户 Key 计量：按 token 捕获 usage，按次则计 1；流结束后入账。"""
    captured = 0

    async def gen():
        nonlocal captured
        try:
            async for chunk in stream:
                if isinstance(chunk, str) and chunk.startswith("data: ") and "[DONE]" not in chunk:
                    try:
                        obj = json.loads(chunk[6:].strip())
                        u = obj.get("usage")
                        if u and u.get("total_tokens"):
                            captured = u["total_tokens"]
                    except Exception:
                        pass
                yield chunk
        finally:
            amount = 1 if billing_mode == "request" else captured
            if amount > 0:
                await keyauth.charge_key_usage(key, amount)

    return gen()


async def _chat_handler(request: Request, auth: dict):
    # 请求体大小预检：Content-Length 超 20MB 直接拒绝（在解析 JSON 之前）
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > 20 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="请求体过大（上限 20MB）")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    is_anthropic = is_anthropic_request(dict(request.headers), body)
    if is_anthropic:
        # Anthropic 格式暂不支持工具调用（tool_use 转换未实现）：显式拒绝而非静默降级
        if body.get("tools") or body.get("tool_choice"):
            raise HTTPException(
                status_code=400,
                detail="Anthropic 格式暂不支持工具调用（tool_use 转换未实现），请使用 OpenAI 格式",
            )
        body = anthropic_to_openai(body)

    try:
        req = ChatCompletionRequest(**body)
    except ValidationError as e:
        raise HTTPException(
            status_code=422,
            detail="请求格式错误: " + str(e.errors(include_input=False, include_url=False))[:500],
        )

    pool_name = _resolve(req.model or "auto")
    if pool_name is None:
        raise HTTPException(
            status_code=404,
            detail=f"未知模型池 '{req.model}'：对外仅可调用模型池（如 auto），不能直接指定单个模型",
        )

    key = auth.get("key")
    is_user_key = auth["kind"] == "key_user"
    if auth["kind"] == "server_admin":
        caller = "管理员"
    else:
        caller = key.get("name", "") if key else ""
    if is_user_key:
        if not keyauth.is_pool_allowed(key, pool_name):
            raise HTTPException(status_code=403, detail=f"该 API Key 无权访问模型池 '{pool_name}'")
        ok, reason = await keyauth.key_usage_available(key)
        if not ok:
            raise HTTPException(status_code=429, detail=f"API Key 用量已达限额: {reason}")

    has_images = pool._has_images(req)

    if req.stream:
        stream, entry, steps = await pool.execute_stream_with_fallback(pool_name, req, None, caller)
        if stream is None:
            raise HTTPException(status_code=503, detail=pool.failure_detail(steps, has_images))
        if is_user_key:
            stream = _wrap_key_stream(stream, key, key.get("billing_mode", "token"))
        if is_anthropic:
            stream = openai_sse_to_anthropic(stream)

        async def generate():
            async for chunk in stream:
                yield chunk

        return StreamingResponse(generate(), media_type="text/event-stream")

    response, tokens, steps = await pool.execute_with_fallback(pool_name, req, None, caller)
    if response is None:
        raise HTTPException(status_code=503, detail=pool.failure_detail(steps, has_images))
    if is_user_key:
        amount = 1 if key.get("billing_mode") == "request" else response.usage.total_tokens
        if amount > 0:
            await keyauth.charge_key_usage(key, amount)
    if is_anthropic:
        return openai_to_anthropic_response(response.model_dump())
    return response


@app.post("/v1/chat/completions")
async def chat_completions(request: Request, auth: dict = Depends(verify_key)):
    return await _chat_handler(request, auth)


@app.post("/v1/messages")
async def messages_endpoint(request: Request, auth: dict = Depends(verify_key)):
    # Anthropic Messages 格式端点：同处理逻辑（is_anthropic 因 anthropic-version 头为 True → 响应自动转 Anthropic）
    return await _chat_handler(request, auth)


@app.get("/v1/models")
async def list_models(auth: dict = Depends(verify_key)):
    if auth["kind"] == "key_user":
        allowed = keyauth.parse_allowed_pools(auth["key"])
        pool_names = [p for p in pool.pool_names() if p in allowed]
    else:
        pool_names = pool.pool_names()
    data = [
        {"id": pname, "object": "model", "owned_by": "gateway-pool"}
        for pname in pool_names
    ]
    return {"object": "list", "data": data}


@app.get("/stats")
async def stats(_=Depends(require_admin)):
    return {"models": await pool.get_stats()}


@app.post("/speedtest")
async def speedtest(request: Request, _=Depends(require_admin)):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    model_ids = body.get("model_ids", None)
    results = await pool.speedtest(model_ids)
    return {"results": results}


@app.get("/health")
async def health():
    return {"status": "ok", "time": int(time.time())}


@app.get("/version")
async def version_info():
    return {"version": GATEWAY_VERSION, "commit": GATEWAY_COMMIT}


@app.get("/hfadmin", response_class=HTMLResponse)
async def hfadmin_page():
    """HF 科技感管理面板：与 /admin 共用同一套后端 API（verify_admin 认证），
    页面本身无需认证（与原 /admin 一致），所有 /admin/* API 均受 Bearer 保护。"""
    html = (Path(__file__).parent / "static" / "hfadmin.html").read_text(encoding="utf-8")
    return html.replace("__GATEWAY_VERSION__", get_gateway_version())


if __name__ == "__main__":
    import uvicorn
    config = load_config()
    server_cfg = config.get("server", {})
    uvicorn.run(
        "main:app",
        host=server_cfg.get("host", "127.0.0.1"),
        port=server_cfg.get("port", 8650),
        reload=False,
    )
