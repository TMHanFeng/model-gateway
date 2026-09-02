import os
import time
import json
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, RedirectResponse, HTMLResponse, JSONResponse
from pydantic import ValidationError
from models import ChatCompletionRequest, EmbeddingRequest, RerankRequest
from pool import ModelPool, load_config, ContextOverflowPassThrough
from database import init_db, close_db
import database as db
import keyauth
from scheduler import start_scheduler, sync_all_refresh_times
from admin import router as admin_router, GATEWAY_VERSION, GATEWAY_COMMIT, get_gateway_version
from format_adapter import (
    is_anthropic_request,
    anthropic_to_openai,
    openai_to_anthropic_response,
    openai_sse_to_anthropic,
)

import logging
from logging_config import setup_logging, LOG_FILE
logger = logging.getLogger(__name__)


def server_bind():
    """返回 (host, port)：优先环境变量 MODEL_GATEWAY_HOST/PORT（或 --host/--port 命令行），否则读 config.json。"""
    cfg = load_config().get("server", {})
    host = os.environ.get("MODEL_GATEWAY_HOST") or cfg.get("host", "127.0.0.1")
    port = int(os.environ.get("MODEL_GATEWAY_PORT") or cfg.get("port", 8650))
    return host, port


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    await init_db()
    await sync_all_refresh_times()
    start_scheduler()
    host, port = server_bind()
    base = f"http://{host}:{port}"
    logger.info(f"Model Gateway 启动 | 后台: {base}/admin/  {base}/hfadmin | API: {base}/v1 | 日志: {LOG_FILE}")
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


def _overflow_passthrough_response(e: ContextOverflowPassThrough):
    """上下文超限整池透传：原样返回上游 400 错误体（与直连行为一致，客户端可据此自愈）"""
    try:
        body_obj = json.loads(e.body) if e.body else None
    except Exception:
        body_obj = None
    if not isinstance(body_obj, dict):
        body_obj = {"error": {"message": e.body or "context length exceeded",
                              "type": "BadRequestError", "param": None, "code": e.status_code}}
    logger.warning(f"[上下文超限透传] status={e.status_code} body={str(e.body)[:220]}")
    return JSONResponse(status_code=e.status_code, content=body_obj)


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
    # json 输出硬门槛：请求带 response_format(type 以 json 开头) 时仅选 json_output=True 模型
    try:
        rf = req.response_format or {}
        required_json_output = isinstance(rf, dict) and str(rf.get("type", "")).lower().startswith("json")
    except Exception:
        required_json_output = False

    if req.stream:
        try:
            stream, entry, steps = await pool.execute_stream_with_fallback(
                pool_name, req, None, caller, required_json_output=required_json_output)
        except ContextOverflowPassThrough as e:
            return _overflow_passthrough_response(e)
        if stream is None:
            _detail = pool.failure_detail(steps, has_images)
            logger.error(f"[调用失败-流式] pool={pool_name} caller={caller!r} detail={_detail}")
            logger.error(f"[调用失败-流式] 决策明细: {steps}")
            raise HTTPException(status_code=503, detail=_detail)
        if is_user_key:
            stream = _wrap_key_stream(stream, key, key.get("billing_mode", "token"))
        if is_anthropic:
            stream = openai_sse_to_anthropic(stream)

        async def generate():
            async for chunk in stream:
                yield chunk

        return StreamingResponse(generate(), media_type="text/event-stream")

    try:
        response, tokens, steps = await pool.execute_with_fallback(pool_name, req, None, caller,
                                                                   required_json_output=required_json_output)
    except ContextOverflowPassThrough as e:
        return _overflow_passthrough_response(e)
    # === Issue 6 诊断日志（DEBUG 级别）===
    # 截断 content 再记日志：根 logger 为 DEBUG 时此分支恒真，整包 model_dump_json
    # 会序列化全部 choices 全文（推理模型可达数百 KB），拖慢每个非流式请求
    if logger.isEnabledFor(logging.DEBUG):
        if response is None:
            logger.debug("[gateway return] None")
        else:
            try:
                _c0 = response.choices[0] if response.choices else None
                logger.debug(
                    f"[gateway return] model={response.model} usage={response.usage.total_tokens}tok "
                    f"finish={_c0.finish_reason if _c0 else '-'} "
                    f"content={((_c0.message.content or '') if _c0 else '')[:200]!r} "
                    f"tool_calls={bool(_c0.message.tool_calls) if _c0 else False}"
                )
            except Exception:
                logger.debug("[gateway return] <日志序列化失败>")
    if response is None:
        _detail = pool.failure_detail(steps, has_images)
        logger.error(f"[调用失败] pool={pool_name} caller={caller!r} detail={_detail}")
        logger.error(f"[调用失败] 决策明细: {steps}")
        raise HTTPException(status_code=503, detail=_detail)
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


@app.post("/v1/embeddings")
async def embeddings_handler(request: Request, auth: dict = Depends(verify_key)):
    """OpenAI 兼容 embedding 端点：仅匹配 modality=embedding 的模型，响应完全透传上游。"""
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > 20 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="请求体过大（上限 20MB）")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    try:
        req = EmbeddingRequest(**body)
    except ValidationError as e:
        raise HTTPException(
            status_code=422,
            detail="请求格式错误: " + str(e.errors(include_input=False, include_url=False))[:500],
        )

    pool_name = _resolve(req.model or "auto")
    if pool_name is None:
        raise HTTPException(
            status_code=404,
            detail=f"未知模型池 '{req.model or 'auto'}'：对外仅可调用模型池，不能直接指定单个模型",
        )

    key = auth.get("key")
    is_user_key = auth["kind"] == "key_user"
    caller = "管理员" if auth["kind"] == "server_admin" else (key.get("name", "") if key else "")
    if is_user_key:
        if not keyauth.is_pool_allowed(key, pool_name):
            raise HTTPException(status_code=403, detail=f"该 API Key 无权访问模型池 '{pool_name}'")
        ok, reason = await keyauth.key_usage_available(key)
        if not ok:
            raise HTTPException(status_code=429, detail=f"API Key 用量已达限额: {reason}")

    response, tokens, steps = await pool.execute_embedding_with_fallback(pool_name, req, None, caller)
    if response is None:
        _detail = pool.failure_detail(steps, has_images=False)
        logger.error(f"[调用失败-embedding] pool={pool_name} caller={caller!r} detail={_detail}")
        logger.error(f"[调用失败-embedding] 决策明细: {steps}")
        raise HTTPException(
            status_code=503,
            detail=_detail,
        )
    if is_user_key:
        usage = response.get("usage") or {}
        amount = 1 if key.get("billing_mode") == "request" else int(usage.get("total_tokens", 0) or tokens or 0)
        if amount > 0:
            await keyauth.charge_key_usage(key, amount)
    return response


@app.post("/v1/rerank")
async def rerank_handler(request: Request, auth: dict = Depends(verify_key)):
    """Jina/Cohere/SiliconFlow 兼容 rerank 端点：仅匹配 modality=rerank 的模型，响应完全透传上游。"""
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > 20 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="请求体过大（上限 20MB）")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    try:
        req = RerankRequest(**body)
    except ValidationError as e:
        raise HTTPException(
            status_code=422,
            detail="请求格式错误: " + str(e.errors(include_input=False, include_url=False))[:500],
        )

    pool_name = _resolve(req.model or "auto")
    if pool_name is None:
        raise HTTPException(
            status_code=404,
            detail=f"未知模型池 '{req.model or 'auto'}'：对外仅可调用模型池，不能直接指定单个模型",
        )

    key = auth.get("key")
    is_user_key = auth["kind"] == "key_user"
    caller = "管理员" if auth["kind"] == "server_admin" else (key.get("name", "") if key else "")
    if is_user_key:
        if not keyauth.is_pool_allowed(key, pool_name):
            raise HTTPException(status_code=403, detail=f"该 API Key 无权访问模型池 '{pool_name}'")
        ok, reason = await keyauth.key_usage_available(key)
        if not ok:
            raise HTTPException(status_code=429, detail=f"API Key 用量已达限额: {reason}")

    response, tokens, steps = await pool.execute_rerank_with_fallback(pool_name, req, None, caller)
    if response is None:
        _detail = pool.failure_detail(steps, has_images=False)
        logger.error(f"[调用失败-rerank] pool={pool_name} caller={caller!r} detail={_detail}")
        logger.error(f"[调用失败-rerank] 决策明细: {steps}")
        raise HTTPException(
            status_code=503,
            detail=_detail,
        )
    if is_user_key:
        usage = response.get("usage") or {}
        amount = 1 if key.get("billing_mode") == "request" else int(usage.get("total_tokens", 0) or tokens or 0)
        if amount > 0:
            await keyauth.charge_key_usage(key, amount)
    return response


# ================= 探测端点兼容（Ollama / Open WebUI） =================
# 中转站 / 客户端在启动与轮询时会按 Ollama 或 Open WebUI 协议探测本网关
# （/api/tags、/api/show、/props 等）。以下端点返回合理响应，避免 404 刷屏、
# 并让客户端"连接检测"通过；真实对话仍走 OpenAI / Anthropic 协议，互不影响。

@app.get("/api/tags")
async def oapi_tags():
    """Ollama 模型列表：列出所有池名，供客户端模型发现。"""
    _t = int(time.time())
    return {"models": [{"name": n, "model": n, "modified_at": None} for n in pool.pool_names()]}


@app.post("/api/show")
async def oapi_show(request: Request):
    """Ollama 模型详情：池名存在则返回基础信息，否则 404。"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    name = (body.get("name") or body.get("model") or "").strip()
    if name not in pool.pool_names():
        raise HTTPException(status_code=404, detail=f"model '{name}' not found")
    return {
        "modelfile": "",
        "parameters": "",
        "template": "",
        "details": {
            "parent_model": "",
            "format": "gguf",
            "family": "",
            "families": None,
            "parameter_size": "",
            "quantization_level": "",
        },
        "model_info": {},
    }


@app.get("/api/v1/models")
async def oapi_v1_models():
    """部分 Ollama 兼容客户端会先探测 /api/v1/models 再回退 /v1/models。"""
    return {"object": "list", "data": [{"id": n, "object": "model", "owned_by": "model-gateway"} for n in pool.pool_names()]}


@app.get("/props")
@app.get("/v1/props")
async def webui_props():
    """Open WebUI 前端初始化探测：返回版本与能力标记，使其信任连接可用。"""
    return {
        "author": False,
        "database": True,
        "version": "v0.3.0",
        "auth": True,
        "pipelines": False,
    }


@app.get("/v1/models/{model_name}")
async def v1_model_detail(model_name: str):
    """OpenAI 模型详情端点：客户端以池名查询是否存在（如 GET /v1/models/localglm）。"""
    if model_name not in pool.pool_names():
        raise HTTPException(status_code=404, detail=f"未知模型池 '{model_name}'")
    return {"id": model_name, "object": "model", "created": int(time.time()), "owned_by": "model-gateway"}


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


@app.get("/")
async def root_redirect():
    """根路径友好跳转：浏览器直接打开端口时转到科技感管理面板，避免 404 造成"打不开"的错觉。"""
    return RedirectResponse("/hfadmin")


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
    import argparse
    import uvicorn
    parser = argparse.ArgumentParser(description="Model Gateway 启动器")
    parser.add_argument("--host", default=None, help="监听地址（覆盖 config.json 的 server.host）")
    parser.add_argument("--port", type=int, default=None, help="监听端口（覆盖 config.json 的 server.port）")
    args = parser.parse_args()
    if args.host is not None:
        os.environ["MODEL_GATEWAY_HOST"] = args.host
    if args.port is not None:
        os.environ["MODEL_GATEWAY_PORT"] = str(args.port)
    host, port = server_bind()
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=False,
        log_config=None,  # 使用统一日志配置（时间戳 + 控制台 + 文件），不启用 uvicorn 默认配置
    )
