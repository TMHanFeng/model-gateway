import httpx
import time
import uuid
import asyncio
from typing import AsyncGenerator
from models import ChatCompletionRequest, ChatCompletionResponse, UsageInfo, Choice, ChoiceMessage


class RateLimitError(Exception):
    pass


class OpenAIProvider:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(120, connect=10),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    async def close(self):
        await self.client.aclose()

    def _build_payload(self, req: ChatCompletionRequest, model_name: str, stream: bool = False) -> dict:
        payload = {
            "model": model_name,
            "messages": [m.model_dump() for m in req.messages],
        }
        if req.temperature is not None:
            payload["temperature"] = req.temperature
        if req.max_tokens is not None:
            payload["max_tokens"] = req.max_tokens
        if req.top_p is not None:
            payload["top_p"] = req.top_p
        if req.stop is not None:
            payload["stop"] = req.stop
        if req.presence_penalty is not None:
            payload["presence_penalty"] = req.presence_penalty
        if req.frequency_penalty is not None:
            payload["frequency_penalty"] = req.frequency_penalty
        if stream:
            payload["stream"] = True
            payload["stream_options"] = {"include_usage": True}
        return payload

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def chat(self, req: ChatCompletionRequest, model_name: str) -> ChatCompletionResponse:
        payload = self._build_payload(req, model_name)
        resp = await self.client.post(
            f"{self.base_url}/chat/completions",
            json=payload,
            headers=self._headers(),
        )
        if resp.status_code == 429:
            raise RateLimitError("upstream 429")
        resp.raise_for_status()
        data = resp.json()

        usage = data.get("usage", {})
        return ChatCompletionResponse(
            id=data.get("id", f"chatcmpl-{uuid.uuid4().hex[:8]}"),
            created=data.get("created", int(time.time())),
            model=data.get("model", model_name),
            choices=[
                Choice(
                    index=c.get("index", 0),
                    message=ChoiceMessage(
                        role=c["message"]["role"],
                        content=c["message"].get("content") or "",
                    ),
                    finish_reason=c.get("finish_reason", "stop"),
                )
                for c in data.get("choices", [])
            ],
            usage=UsageInfo(
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                total_tokens=usage.get("total_tokens", 0),
            ),
        )

    async def chat_stream(self, req: ChatCompletionRequest, model_name: str) -> AsyncGenerator[str, None]:
        payload = self._build_payload(req, model_name, stream=True)
        async with self.client.stream(
            "POST",
            f"{self.base_url}/chat/completions",
            json=payload,
            headers=self._headers(),
        ) as resp:
            if resp.status_code == 429:
                raise RateLimitError("upstream 429")
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    yield line + "\n\n"
                elif line.strip() == "":
                    continue

    async def speedtest(self, model_name: str) -> dict:
        payload = {
            "model": model_name,
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 5,
        }
        start = time.perf_counter()
        try:
            resp = await self.client.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=self._headers(),
            )
            elapsed = time.perf_counter() - start
            if resp.status_code == 429:
                return {"model_id": model_name, "status": "rate_limited", "latency_ms": round(elapsed * 1000)}
            resp.raise_for_status()
            data = resp.json()
            tokens = data.get("usage", {}).get("total_tokens", 0)
            return {
                "status": "ok",
                "latency_ms": round(elapsed * 1000),
                "tokens": tokens,
                "tps": round(tokens / elapsed, 1) if elapsed > 0 else 0,
            }
        except Exception as e:
            elapsed = time.perf_counter() - start
            return {"status": "error", "error": str(e), "latency_ms": round(elapsed * 1000)}
