import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, RedirectResponse
from models import ChatCompletionRequest
from pool import ModelPool, load_config
from database import init_db, close_db
from scheduler import start_scheduler
from admin import router as admin_router


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


def verify_key(request: Request):
    config = load_config()
    expected = config.get("server", {}).get("api_key", "")
    if not expected:
        return
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        auth = auth[7:]
    if auth != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


def _resolve(model_field: str) -> str | None:
    # Externally only model pools are callable; individual models are not exposed.
    if not model_field or model_field == "auto":
        return "auto"
    if model_field in pool.pools:
        return model_field
    return None


@app.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionRequest, _=Depends(verify_key)):
    pool_name = _resolve(req.model or "auto")
    if pool_name is None:
        raise HTTPException(
            status_code=404,
            detail=f"未知模型池 '{req.model}'：对外仅可调用模型池（如 auto），不能直接指定单个模型",
        )
    has_images = pool._has_images(req)

    if req.stream:
        stream, entry, steps = await pool.execute_stream_with_fallback(pool_name, req, None)
        if stream is None:
            raise HTTPException(status_code=503, detail=pool.failure_detail(steps, has_images))

        async def generate():
            async for chunk in stream:
                yield chunk

        return StreamingResponse(generate(), media_type="text/event-stream")

    response, tokens, steps = await pool.execute_with_fallback(pool_name, req, None)
    if response is None:
        raise HTTPException(status_code=503, detail=pool.failure_detail(steps, has_images))
    return response


@app.get("/v1/models")
async def list_models(_=Depends(verify_key)):
    data = [
        {"id": pname, "object": "model", "owned_by": "gateway-pool"}
        for pname in pool.pool_names()
    ]
    return {"object": "list", "data": data}


@app.get("/stats")
async def stats(_=Depends(verify_key)):
    return {"models": await pool.get_stats()}


@app.post("/speedtest")
async def speedtest(request: Request, _=Depends(verify_key)):
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
