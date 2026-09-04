from pydantic import BaseModel
from typing import Optional, Union, Any


class ChatMessage(BaseModel):
    role: str
    content: Optional[Union[str, list]] = None
    name: Optional[str] = None
    tool_call_id: Optional[str] = None
    tool_calls: Optional[list[dict]] = None


class ChatCompletionRequest(BaseModel):
    model: str
    messages: list[ChatMessage]
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    top_p: Optional[float] = None
    stream: Optional[bool] = False
    stop: Optional[str | list[str]] = None
    presence_penalty: Optional[float] = None
    frequency_penalty: Optional[float] = None
    tools: Optional[list[dict]] = None
    tool_choice: Optional[Union[str, dict]] = None
    response_format: Optional[dict] = None
    # 统一思考档位：off/minimal/low/medium/high/max（按模型 reasoning_map 映射为上游参数）
    reasoning_effort: Optional[str] = None


class EmbeddingRequest(BaseModel):
    """OpenAI 兼容 /v1/embeddings 请求体（完全透传上游）。"""
    model: str
    input: Union[str, list[str]]
    encoding_format: Optional[str] = None
    dimensions: Optional[int] = None
    user: Optional[str] = None
    extra_params: Optional[dict[str, Any]] = None


class RerankRequest(BaseModel):
    """Jina/Cohere/SiliconFlow 兼容 /v1/rerank 请求体（完全透传上游）。"""
    model: str
    query: str
    documents: list[Union[str, dict]]
    top_n: Optional[int] = None
    return_documents: Optional[bool] = None
    extra_params: Optional[dict[str, Any]] = None


class UsageInfo(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChoiceMessage(BaseModel):
    role: str = "assistant"
    content: Optional[str] = ""
    reasoning_content: Optional[str] = None
    tool_calls: Optional[list[dict]] = None


class Choice(BaseModel):
    index: int = 0
    message: ChoiceMessage
    finish_reason: str = "stop"


class ChatCompletionResponse(BaseModel):
    id: str = ""
    object: str = "chat.completion"
    created: int = 0
    model: str = ""
    choices: list[Choice] = []
    usage: UsageInfo = UsageInfo()
