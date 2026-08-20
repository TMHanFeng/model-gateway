"""Anthropic <-> OpenAI 请求/响应格式适配（纯函数，无外部依赖）。

对外暴露 Anthropic Messages API 调用能力：
- 请求：Anthropic 格式 -> 内部 OpenAI 格式（anthropic_to_openai）
- 响应：OpenAI 格式 -> Anthropic Messages 格式（openai_to_anthropic_response）
- 流式：OpenAI SSE chunk -> Anthropic SSE 事件（openai_sse_to_anthropic）
"""

import json

_OPENAI_REQUEST_KEYS = [
    "model",
    "messages",
    "temperature",
    "max_tokens",
    "top_p",
    "stream",
    "stop",
    "presence_penalty",
    "frequency_penalty",
    "tools",
    "tool_choice",
]

_STOP_REASON_MAP = {
    "stop": "end_turn",
    "length": "max_tokens",
    "tool_calls": "tool_use",
    "content_filter": "stop_sequence",
}


def is_anthropic_request(headers: dict, body: dict) -> bool:
    """判断请求是否为 Anthropic Messages 格式（任一特征命中即 True）。"""
    # 1. anthropic-version 头（键名大小写不敏感）
    for key in headers:
        if str(key).lower() == "anthropic-version":
            return True
    # 2. 顶层 system 字段（str 或 list）
    if "system" in body and isinstance(body.get("system"), (str, list)):
        return True
    # 3. messages 中含 Anthropic 图片块（type=image + source；OpenAI 为 type=image_url）
    messages = body.get("messages")
    if isinstance(messages, list):
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            content = msg.get("content")
            if isinstance(content, list):
                for block in content:
                    if (
                        isinstance(block, dict)
                        and block.get("type") == "image"
                        and "source" in block
                    ):
                        return True
    # 4. 顶层 stop_sequences（Anthropic 特有）
    if "stop_sequences" in body:
        return True
    return False


def _block_to_openai(block) -> dict | None:
    """单个 Anthropic 内容块 -> OpenAI 内容块。未知块跳过或转 text 占位，绝不抛异常。"""
    if not isinstance(block, dict):
        return None
    btype = block.get("type")
    if btype == "text":
        text = block.get("text")
        if isinstance(text, str):
            return {"type": "text", "text": text}
        return None
    if btype == "image":
        source = block.get("source")
        if isinstance(source, dict):
            stype = source.get("type")
            if stype == "base64":
                data = source.get("data")
                if isinstance(data, str):
                    media = source.get("media_type") or "image/png"
                    return {
                        "type": "image_url",
                        "image_url": {"url": f"data:{media};base64,{data}"},
                    }
            elif stype == "url":
                url = source.get("url")
                if isinstance(url, str):
                    return {"type": "image_url", "image_url": {"url": url}}
        return None
    # 其他未知块（tool_use / tool_result / thinking 等）：有 text 则占位，否则跳过
    text = block.get("text")
    if isinstance(text, str) and text:
        return {"type": "text", "text": text}
    return None


def anthropic_to_openai(body: dict) -> dict:
    """Anthropic Messages 请求 -> OpenAI Chat Completion 请求。返回新 dict，不修改入参。"""
    out = {k: body[k] for k in _OPENAI_REQUEST_KEYS if k in body}

    messages = body.get("messages")
    if not isinstance(messages, list):
        messages = []
    new_messages = []

    # 顶层 system（str 或 text 块列表）-> 首条 system 消息
    system = body.get("system")
    if isinstance(system, str):
        new_messages.append({"role": "system", "content": system})
    elif isinstance(system, list):
        parts = []
        for block in system:
            if (
                isinstance(block, dict)
                and block.get("type") == "text"
                and isinstance(block.get("text"), str)
            ):
                parts.append(block["text"])
        if parts:
            new_messages.append({"role": "system", "content": "".join(parts)})

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        nm = {"role": msg.get("role") or "user"}
        content = msg.get("content")
        if isinstance(content, str):
            nm["content"] = content
        elif isinstance(content, list):
            blocks = []
            has_non_text = False
            for block in content:
                conv = _block_to_openai(block)
                if conv is not None:
                    blocks.append(conv)
                    if conv.get("type") != "text":
                        has_non_text = True
            if not blocks:
                nm["content"] = ""
            elif has_non_text:
                nm["content"] = blocks  # multimodal: keep as list
            else:
                # text-only: flatten to joined string (OpenAI-compatible APIs prefer string)
                nm["content"] = "".join(b.get("text", "") for b in blocks)
        else:
            nm["content"] = ""
        new_messages.append(nm)

    out["messages"] = new_messages

    # stop_sequences -> stop
    stop_sequences = body.get("stop_sequences")
    if isinstance(stop_sequences, list):
        out["stop"] = stop_sequences

    return out


def _map_stop_reason(finish_reason) -> str:
    if finish_reason is None:
        return "end_turn"
    return _STOP_REASON_MAP.get(str(finish_reason), "end_turn")


def openai_to_anthropic_response(resp: dict) -> dict:
    """OpenAI 响应 dict -> Anthropic Messages 响应 dict。"""
    choices = resp.get("choices") or []
    first = choices[0] if isinstance(choices, list) and choices else {}
    message = first.get("message") or {}
    content = message.get("content")
    if isinstance(content, list):
        # 多模态回复（罕见）：逐项保留
        out_content = content
    else:
        out_content = [
            {"type": "text", "text": content if isinstance(content, str) else ""}
        ]
    usage = resp.get("usage") or {}
    return {
        "id": resp.get("id", ""),
        "type": "message",
        "role": "assistant",
        "model": resp.get("model", ""),
        "content": out_content,
        "stop_reason": _map_stop_reason(first.get("finish_reason")),
        "stop_sequence": None,
        "usage": {
            "input_tokens": usage.get("prompt_tokens", 0),
            "output_tokens": usage.get("completion_tokens", 0),
        },
    }


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def openai_sse_to_anthropic(openai_sse_stream):
    """OpenAI SSE chunk 文本流 -> Anthropic SSE 事件文本流。

    输入 chunk 形如 `data: {...}\n\n` 或 `data: [DONE]\n\n`；
    产出 `event: xxx\ndata: {...}\n\n` 格式的 Anthropic 事件。
    流中途异常：发 error 事件后停止（不抛出）。
    """
    index = 0
    started = False
    finished = False
    error_emitted = False
    message_delta_sent = False
    last_usage = None  # 记录所有 chunk 的 usage（含 usage-only），供 message_delta 输出真实 output_tokens
    pending_finish = None  # 缓存 finish_reason，延迟到 [DONE]/流结束 时与 usage 一起发出
    sent_content_block_stop = False

    def _delta_stop_payload(reason) -> dict:
        # message_delta 消息体：output_tokens 尽量取真实 usage（无则 0）
        output_tokens = (last_usage or {}).get("completion_tokens", 0) or 0
        return {
            "type": "message_delta",
            "delta": {"stop_reason": _map_stop_reason(reason), "stop_sequence": None},
            "usage": {"output_tokens": output_tokens},
        }

    try:
        async for chunk in openai_sse_stream:
            if not isinstance(chunk, str):
                continue
            text = chunk.strip()
            if not text.startswith("data: "):
                continue
            payload = text[6:].strip()
            if payload == "[DONE]":
                # 幂等收尾：content_block_stop → message_delta → message_stop，且不重复。
                # 若 finish chunk 已先行（pending_finish 已缓存），此处用最新的 last_usage 发出真实 tokens。
                # 用 break 而非 continue：上游在 [DONE] 后仍可能滞留/多发 chunk，
                # break 可避免 message_stop 之后再产出 content_block_delta 等乱序事件。
                if started:
                    if not finished:
                        finished = True
                        pending_finish = pending_finish or "stop"
                    if not sent_content_block_stop:
                        yield _sse(
                            "content_block_stop",
                            {"type": "content_block_stop", "index": index},
                        )
                        sent_content_block_stop = True
                    if pending_finish:
                        yield _sse("message_delta", _delta_stop_payload(pending_finish))
                        yield _sse("message_stop", {"type": "message_stop"})
                        pending_finish = None
                        message_delta_sent = True
                break
            try:
                obj = json.loads(payload)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue

            # 记录所有 chunk 的 usage（含 usage-only chunk），不产生额外事件
            usage = obj.get("usage")
            if isinstance(usage, dict):
                last_usage = usage

            if not started:
                started = True
                usage = obj.get("usage")
                input_tokens = 0
                if isinstance(usage, dict):
                    input_tokens = usage.get("prompt_tokens", 0) or 0
                yield _sse(
                    "message_start",
                    {
                        "type": "message_start",
                        "message": {
                            "id": obj.get("id", ""),
                            "type": "message",
                            "role": "assistant",
                            "model": obj.get("model", ""),
                            "content": [],
                            "usage": {"input_tokens": input_tokens, "output_tokens": 0},
                        },
                    },
                )
                yield _sse(
                    "content_block_start",
                    {
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {"type": "text", "text": ""},
                    },
                )

            choices = obj.get("choices")
            if not isinstance(choices, list) or not choices:
                continue  # usage-only 等 chunk：仅记录 usage，不产生事件
            choice = choices[0]
            if not isinstance(choice, dict):
                continue

            delta = choice.get("delta")
            if isinstance(delta, dict):
                piece = delta.get("content")
                if isinstance(piece, str) and piece:
                    yield _sse(
                        "content_block_delta",
                        {
                            "type": "content_block_delta",
                            "index": index,
                            "delta": {"type": "text_delta", "text": piece},
                        },
                    )

            finish = choice.get("finish_reason")
            if finish and not finished:
                finished = True
                pending_finish = finish
                if not sent_content_block_stop:
                    yield _sse(
                        "content_block_stop",
                        {"type": "content_block_stop", "index": index},
                    )
                    sent_content_block_stop = True
                # message_delta/message_stop 延迟到 [DONE]：等 usage-only chunk 更新 last_usage 后发真实值
    except Exception as e:  # 流中途异常：error 事件后停止（不补发收尾）
        error_emitted = True
        yield _sse(
            "error",
            {
                "type": "error",
                "error": {"type": "api_error", "message": str(e)},
            },
        )
        return
    # 收尾兜底（循环正常耗尽后）：只要还没发过 message_delta/message_stop 就补发，
    # 覆盖 finish chunk 已到但上游省略 [DONE]、以及流直接提前结束（无 finish 无 [DONE]）
    # 两类场景，避免 Anthropic 客户端一直等待结束事件而挂起。
    # 注意不用 finally 块：async generator 在 finally 中 yield 时，若客户端断开
    # （aclose → GeneratorExit）会抛 "async generator ignored GeneratorExit" 且
    # 无法在生成器内可靠捕获；放循环外则 GeneratorExit 的传播路径不经过此处，天然安全。
    if started and not error_emitted and not message_delta_sent:
        if not sent_content_block_stop:
            yield _sse(
                "content_block_stop",
                {"type": "content_block_stop", "index": index},
            )
        yield _sse("message_delta", _delta_stop_payload(pending_finish or "stop"))
        yield _sse("message_stop", {"type": "message_stop"})
