"""统一思考档位（reasoning_effort）映射：纯函数，无外部依赖。

统一档位：off < minimal < low < medium < high < max。
客户端在 /v1/chat/completions 顶层传 reasoning_effort（/v1/messages 传 thinking，
由 format_adapter 归一化为同档位），网关按目标模型 config 里的 reasoning_map
把档位换算成上游请求体片段注入（OpenAI 协议 /chat/completions body、
anthropic 协议 /v1/messages body）。未配置映射或未传参数 = 上游默认行为。
"""

LEVELS = ["off", "minimal", "low", "medium", "high", "max"]
_LEVEL_INDEX = {lv: i for i, lv in enumerate(LEVELS)}

# 映射片段中禁止出现的键（防止覆盖核心传输字段；reasoning_effort 本身是部分上游的
# 合法参数，允许通过片段下发）
RESERVED_KEYS = {
    "model", "messages", "stream", "stream_options", "input",
    "tools", "tool_choice", "max_tokens", "system", "temperature",
    "top_p", "stop", "stop_sequences", "presence_penalty", "frequency_penalty",
}


def normalize_effort(value) -> str | None:
    """归一化客户端传入的 reasoning_effort；非法/缺失返回 None。"""
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    return v if v in _LEVEL_INDEX else None


def anthropic_thinking_to_effort(thinking) -> str | None:
    """Anthropic 顶层 thinking 参数 -> 统一档位；无法识别返回 None。

    budget 分档：≤1024→minimal，≤4096→low，≤10240→medium，≤20480→high，>20480→max。
    """
    if not isinstance(thinking, dict):
        return None
    ttype = thinking.get("type")
    if ttype == "disabled":
        return "off"
    if ttype in ("enabled", "auto"):
        try:
            budget = int(thinking.get("budget_tokens") or 0)
        except (TypeError, ValueError):
            budget = 0
        if budget <= 0:
            return "medium"  # enabled 但未带 budget：取中档
        if budget <= 1024:
            return "minimal"
        if budget <= 4096:
            return "low"
        if budget <= 10240:
            return "medium"
        if budget <= 20480:
            return "high"
        return "max"
    return None


def resolve_fragment(reasoning_map, effort) -> dict | None:
    """按统一档位从 reasoning_map 取上游请求体片段（含缺档回落）。

    回落规则：请求档位未配置时取更低档位中最近的；无更低配置时取最低配置档。
    返回 None 表示不注入（未配置映射 / 未指定档位 / 片段被保留键过滤后为空）。
    """
    if not effort or not isinstance(reasoning_map, dict) or not reasoning_map:
        return None
    configured = {}
    for lv, frag in reasoning_map.items():
        key = str(lv).strip().lower()
        if key in _LEVEL_INDEX and isinstance(frag, dict) and frag:
            cleaned = {k: v for k, v in frag.items() if str(k) not in RESERVED_KEYS}
            if cleaned:
                configured[_LEVEL_INDEX[key]] = cleaned
    want = _LEVEL_INDEX.get(effort)
    if want is None or not configured:
        return None
    idx = want
    while idx >= 0 and idx not in configured:
        idx -= 1
    if idx < 0:
        idx = min(configured)
    return configured[idx]


def merge_fragment(payload: dict, fragment: dict | None) -> dict:
    """把映射片段合并进上游 payload（保留键跳过）；返回 payload 本身。"""
    if not fragment:
        return payload
    for k, v in fragment.items():
        if str(k) not in RESERVED_KEYS:
            payload[k] = v
    return payload


class ThinkTagSplitter:
    """把流式 content 增量中的内联 `<think>...</think>` 段切分为 reasoning_content。

    用于部分模型（MiniMax-M3 等）把思考以 `<think>` 标签内联在 content 里的场景，
    使流式与非流式回传口径统一（均为 reasoning_content）。跨 chunk 的标签边界安全。
    仅识别位于流最开头的 think 块；一旦进入正文模式，后续内容原样透传。
    """

    _OPEN = "<think>"
    _CLOSE = "</think>"

    def __init__(self):
        self._state = "head"  # head=尚未判定开头 | think=思考中 | body=正文
        self._buf = ""
        self.done = False     # 正文态后置 True：后续增量无需再做任何解析（提速快速路径）

    def feed(self, piece: str) -> tuple[str, str]:
        """喂入一段 content 增量，返回 (reasoning片段, 正文片段)。"""
        if self.done:
            return "", piece
        rc = ""
        content = ""
        self._buf += piece
        while self._buf:
            if self._state == "body":
                self.done = True
                content += self._buf
                self._buf = ""
            elif self._state == "head":
                if self._buf.startswith(self._OPEN):
                    self._buf = self._buf[len(self._OPEN):]
                    self._state = "think"
                    continue
                n = min(len(self._buf), len(self._OPEN) - 1)
                if self._buf[:n] == self._OPEN[:n]:
                    if len(self._buf) < len(self._OPEN):
                        break  # 可能是未到齐的 "<thi…"，等下一个增量再判
                    content += self._buf  # 开头不是 think 标签：全部为正文
                    self._buf = ""
                    self._state = "body"
                else:
                    content += self._buf
                    self._buf = ""
                    self._state = "body"
            elif self._state == "think":
                idx = self._buf.find(self._CLOSE)
                if idx >= 0:
                    rc += self._buf[:idx]
                    self._buf = self._buf[idx + len(self._CLOSE):]
                    self._state = "body"
                    continue
                keep = 0
                for k in range(min(len(self._buf), len(self._CLOSE) - 1), 0, -1):
                    if self._buf.endswith(self._CLOSE[:k]):
                        keep = k
                        break
                rc += self._buf[:len(self._buf) - keep]
                self._buf = self._buf[len(self._buf) - keep:]
                break  # 等更多数据找闭合标签
            else:  # body
                self.done = True
                content += self._buf
                self._buf = ""
        return rc, content
