import httpx
import time
import uuid
import json
from typing import AsyncGenerator
from models import ChatCompletionRequest, ChatCompletionResponse, UsageInfo, Choice, ChoiceMessage
from .openai_provider import RateLimitError


class AnthropicProvider:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(120, connect=10),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    async def close(self):
        await self.client.aclose()

    def _convert_content(self, content):
        if isinstance(content, str):
            return content
        blocks = []
        for part in content:
            if not isinstance(part, dict):
                continue
            ptype = part.get("type")
            if ptype == "text":
                blocks.append({"type": "text", "text": part.get("text", "")})
            elif ptype == "image_url":
                url = part.get("image_url", {}).get("url", "")
                if url.startswith("data:"):
                    header, _, b64 = url.partition(",")
                    media_type = header[len("data:"):].split(";")[0] or "image/png"
                    blocks.append({
                        "type": "image",
                        "source": {"type": "base64", "media_type": media_type, "data": b64},
                    })
                else:
                    blocks.append({"type": "image", "source": {"type": "url", "url": url}})
            elif ptype == "image":
                blocks.append(part)
        return blocks

    def _map_finish(self, stop_reason: str) -> str:
        if stop_reason == "max_tokens":
            return "length"
        return "stop"

    def _build_payload(self, req: ChatCompletionRequest, model_name: str, stream: bool = False) -> dict:
        system_msg = ""
        messages = []
        for m in req.messages:
            if m.role == "system":
                if isinstance(m.content, str):
                    system_msg += m.content + "\n"
                elif isinstance(m.content, list):
                    for part in m.content:
                        if isinstance(part, dict) and part.get("type") == "text":
                            system_msg += part.get("text", "") + "\n"
            else:
                messages.append({"role": m.role, "content": self._convert_content(m.content)})

        payload = {
            "model": model_name,
            "messages": messages,
            "max_tokens": req.max_tokens or 4096,
        }
        if system_msg.strip():
            payload["system"] = system_msg.strip()
        if req.temperature is not None:
            payload["temperature"] = req.temperature
        if req.top_p is not None:
            payload["top_p"] = req.top_p
        if req.stop is not None:
            stops = req.stop if isinstance(req.stop, list) else [req.stop]
            payload["stop_sequences"] = stops
        if stream:
            payload["stream"] = True
        return payload

    def _headers(self) -> dict:
        return {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }

    async def chat(self, req: ChatCompletionRequest, model_name: str) -> ChatCompletionResponse:
        payload = self._build_payload(req, model_name)
        resp = await self.client.post(
            f"{self.base_url}/v1/messages",
            json=payload,
            headers=self._headers(),
        )
        if resp.status_code == 429:
            raise RateLimitError("upstream 429")
        resp.raise_for_status()
        data = resp.json()

        usage = data.get("usage", {})
        content_blocks = data.get("content", [])
        text = "".join(b.get("text", "") for b in content_blocks if b.get("type") == "text")

        prompt_tokens = usage.get("input_tokens", 0)
        completion_tokens = usage.get("output_tokens", 0)

        return ChatCompletionResponse(
            id=f"chatcmpl-{uuid.uuid4().hex[:8]}",
            created=int(time.time()),
            model=model_name,
            choices=[
                Choice(
                    index=0,
                    message=ChoiceMessage(role="assistant", content=text),
                    finish_reason=self._map_finish(data.get("stop_reason", "end_turn")),
                )
            ],
            usage=UsageInfo(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
            ),
        )

    async def chat_stream(self, req: ChatCompletionRequest, model_name: str) -> AsyncGenerator[str, None]:
        payload = self._build_payload(req, model_name, stream=True)
        completion_id = f"chatcmpl-{uuid.uuid4().hex[:8]}"
        created = int(time.time())

        async with self.client.stream(
            "POST",
            f"{self.base_url}/v1/messages",
            json=payload,
            headers=self._headers(),
        ) as resp:
            if resp.status_code == 429:
                raise RateLimitError("upstream 429")
            resp.raise_for_status()
            in_tokens = 0
            out_tokens = 0
            stop_reason = "end_turn"
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                raw = line[6:]
                if raw.strip() == "[DONE]":
                    break
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                event_type = event.get("type", "")
                if event_type == "message_start":
                    in_tokens = event.get("message", {}).get("usage", {}).get("input_tokens", in_tokens)
                elif event_type == "content_block_delta":
                    delta_text = event.get("delta", {}).get("text", "")
                    chunk = {
                        "id": completion_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model_name,
                        "choices": [{
                            "index": 0,
                            "delta": {"content": delta_text},
                            "finish_reason": None,
                        }],
                    }
                    yield f"data: {json.dumps(chunk)}\n\n"
                elif event_type == "message_delta":
                    out_tokens = event.get("usage", {}).get("output_tokens", out_tokens)
                    stop_reason = event.get("delta", {}).get("stop_reason", stop_reason)
                elif event_type == "message_stop":
                    chunk = {
                        "id": completion_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model_name,
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": self._map_finish(stop_reason),
                        }],
                    }
                    yield f"data: {json.dumps(chunk)}\n\n"
                    usage_chunk = {
                        "id": completion_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model_name,
                        "choices": [],
                        "usage": {
                            "prompt_tokens": in_tokens,
                            "completion_tokens": out_tokens,
                            "total_tokens": in_tokens + out_tokens,
                        },
                    }
                    yield f"data: {json.dumps(usage_chunk)}\n\n"
                    yield "data: [DONE]\n\n"

    async def speedtest(self, model_name: str) -> dict:
        payload = {
            "model": model_name,
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 5,
        }
        start = time.perf_counter()
        try:
            resp = await self.client.post(
                f"{self.base_url}/v1/messages",
                json=payload,
                headers=self._headers(),
            )
            elapsed = time.perf_counter() - start
            if resp.status_code == 429:
                return {"status": "rate_limited", "latency_ms": round(elapsed * 1000)}
            resp.raise_for_status()
            data = resp.json()
            usage = data.get("usage", {})
            tokens = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
            return {
                "status": "ok",
                "latency_ms": round(elapsed * 1000),
                "tokens": tokens,
                "tps": round(tokens / elapsed, 1) if elapsed > 0 else 0,
            }
        except Exception as e:
            elapsed = time.perf_counter() - start
            return {"status": "error", "error": str(e), "latency_ms": round(elapsed * 1000)}
