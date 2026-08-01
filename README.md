# Model Gateway — 本地模型自动切换网关

一个运行在本地的 LLM API 网关，对外暴露 **OpenAI 兼容接口**，对内聚合多个上游模型提供商，自动完成**配额管理、限流保护、故障切换、多模态降级**，并附带一个可视化管理后台。

---

## 核心特性

- **自动故障切换**：模型用量用尽 / RPM·TPM 触顶 / 上游报错时，自动切换到下一个可用模型
- **多提供商聚合**：同名模型（如 OpenAI 与 Azure 的 `gpt-4o`）自动归入一个"大池子"统一调度
- **模型池 + 嵌套池**：可建任意多个池，池内可再嵌套子池；每个池名即调用名
- **两种择优策略**：按用户设定顺序，或开启"自动择优"按实时延迟优先
- **多模态感知**：请求含图片时自动跳过纯文本模型，选用多模态模型；无可用多模态时明确拒绝
- **双计费模式**：按 Token 或按请求次数计费；人工标注免费（绿）/付费（红）
- **一次性令牌**：到期或用完即永久失效，不刷新
- **可视化管理后台**：模型/池的增删改、拖拽排序、用量图表、接口测速、调用记录审计
- **流式支持**：SSE 流式响应透传，Anthropic 自动转换为 OpenAI chunk 格式

---

## 项目结构

```
model-gateway/
├── config.json               # 全部配置：服务、模型注册、池定义（后台可改）
├── main.py                   # FastAPI 入口，对外 API 路由
├── admin.py                  # 管理后台 API（模型/池/记录 CRUD）
├── models.py                 # 请求/响应 Pydantic 模型
├── pool.py                   # 核心：模型池、可用性判定、择优、切换、计费
├── database.py               # SQLite 持久化（用量/请求日志/一次性/决策记录）
├── scheduler.py              # 每模型独立定时刷新（APScheduler）
├── providers/
│   ├── openai_provider.py    # OpenAI 兼容适配器（含流式、测速）
│   └── anthropic_provider.py # Anthropic 适配器（含格式转换、流式）
├── static/
│   └── index.html            # 管理后台单页应用
├── requirements.txt
└── gateway.db                # 运行时自动生成
```

---

## 快速开始

```bash
cd model-gateway
pip install -r requirements.txt
python main.py
```

启动后终端会打印：

```
后台管理地址:  http://127.0.0.1:8650/admin/
API 服务地址:  http://127.0.0.1:8650/v1
```

- 管理后台：浏览器打开 `/admin/`，首次进入输入管理密码（即 `config.json` 的 `server.api_key`，默认 `123456`）
- 在客户端（hermes 等）填写 Base URL `http://127.0.0.1:8650/v1` 和同一 API Key

> **Ubuntu / Linux** 同样支持（纯 Python，无平台依赖）：`python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt && python3 main.py`。如需外部访问，将 `server.host` 改为 `0.0.0.0`。

---

## 调用方式

客户端的 `model` 字段决定路由：

| 填写内容 | 效果 |
|---|---|
| `auto` | 走 auto 池（默认） |
| `池名`（如 `deepseek`） | 走该池，按池内配置调度 |
| `池名:模型名` | 在指定池中匹配该模型 |
| `模型名`（如 `gpt-4o`） | 大池子：所有同名模型按序尝试 |
| `标签/模型名`（如 `openai/gpt-4o`） | 精确指定某个模型实例 |

---

## 核心逻辑

### 1. 可用性判定（硬门槛）

每次调用时，候选模型按以下优先级依次检查，**任一不通过即排除**：

1. **用量**：每日 Token / 请求次数用尽 → 排除直到刷新（一次性模型到期/用完永久失效）
2. **RPM**：最近 60 秒请求数触顶 → 排除（实时重算，恢复快）
3. **TPM**：最近 60 秒 Token 总量触顶 → 排除（实时重算）
4. **多模态**：请求含图片但模型为纯文本 → 排除
5. **冷却**：上游 429 后冷却 60s，其他错误冷却 30s
6. **上下文**：估算 Token 超过模型上下文窗口 → 排除

### 2. 调用顺序（仅在可用模型中生效）

- **非自动模式**：严格按池内设定顺序，第一个可用的即被选中
- **自动择优模式**：在可用模型中按**实时延迟从低到高**优先（延迟来自测速 + 真实请求耗时的滑动平均）

两种模式都遵守硬门槛——用完/触顶立即切换下一个。

### 3. 故障切换

选中模型调用上游，成功则返回；遇到 429 或异常则标记冷却并切换下一个，直到成功或候选耗尽（返回 503 及具体原因）。

### 4. 多模态降级

- 请求含图片时，按顺序跳过纯文本模型，选中第一个可用的多模态模型
- 若整个调用链中无任何可用多模态模型，返回 503：「请求包含图片，但池内没有可用的多模态模型」

### 5. 模型池嵌套

- 池内条目可以是**模型**，也可以是**子池**（`pool:池名`）
- 轮到子池时，按该子池自身配置（自动/顺序）调用其内部模型；子池耗尽则继续下一项
- 内置循环引用保护（A→B→A 不会死循环）
- **计费不重复**：所有嵌套最终解析到同一模型注册表条目（按模型 ID 唯一计数），同一模型无论直接引入还是经子池引入，都共享同一用量计数器，且不会重复调用

### 6. 一次性令牌

`token_type: "one_time"` 的模型：首次调用记录创建时间，累加用量，到达 `max_tokens` 或超过 `ttl_seconds` 后永久失效，任何刷新都不重置。

### 7. 计费模式

- `billing_mode: "token"`：按消耗 Token 计数
- `billing_mode: "request"`：按请求次数计数（每次 +1）
- 流式请求同样计入每日配额（从响应的 usage 提取 Token）

---

## 配置说明（config.json）

> 推荐通过管理后台修改（实时生效）；也可直接编辑此文件后重启。

```jsonc
{
  "server": {
    "host": "127.0.0.1",
    "port": 8650,
    "api_key": "123456"          // 客户端与管理后台共用此密钥
  },
  "default_mode": "auto",
  "models": [
    {
      "id": "openai/gpt-4o",      // 唯一标识（标签/模型名）
      "name": "gpt-4o",           // 模型名（同名自动归入大池子）
      "provider": "openai",       // openai（兼容格式）或 anthropic
      "base_url": "https://api.openai.com/v1",
      "api_key": "sk-xxx",
      "daily_token_limit": 1000000, // 每日上限（0=不限；按次计费时单位为次）
      "rpm_limit": 60,            // 每分钟请求上限（0=不限）
      "tpm_limit": 100000,        // 每分钟 Token 上限（0=不限）
      "context_window": 128000,   // 上下文窗口（0=不校验）
      "max_concurrency": 10,      // 最大并发
      "token_type": "daily",      // daily 或 one_time
      "billing_mode": "token",    // token 或 request
      "is_free": false,           // 免费(绿)/付费(红)标注
      "modality": "vision",       // text（纯文本）或 vision（多模态）
      "refresh_time": "00:00",    // 每日刷新时间（北京时间，固定时区）
      "timezone": "Asia/Shanghai",
      // 以下仅 one_time 使用：
      "max_tokens": 0,            // 一次性最大用量
      "ttl_seconds": 0            // 一次性存活时长（秒）
    }
  ],
  "pools": {
    "auto": {                     // auto 池为内置，不可删除
      "strategy": "sequential",
      "auto_order": false,        // 开启后按延迟自动择优
      "model_ids": [
        "openai/gpt-4o",          // 模型条目
        "pool:fixed"              // 嵌套子池条目
      ]
    },
    "fixed": {
      "auto_order": false,
      "model_ids": ["openai/gpt-4o-mini"]
    }
  }
}
```

> **时区**：统一固定为北京时间（Asia/Shanghai），不可修改。

---

## 管理后台

访问 `/admin/`，包含五个标签页：

| 标签页 | 功能 |
|---|---|
| **模型管理** | 增删改模型接口；标注免费/付费、纯文本/多模态、计费模式；同名模型自动归入大池子 |
| **模型池** | 新增/删除池；拖拽 ⠿ 调整优先级；加入模型或子池；开关"自动择优"；⚡ 测速本池；悬停 ⓘ 查看接口详情 |
| **用量统计** | 卡片式可视化，横向进度条展示 已用/总量（青→琥珀→红分级），每 5 秒自动刷新 |
| **接口测速** | 并发测试所有模型的延迟与吞吐 |
| **调用记录** | 每次调用的完整选模过程与切换依据（选中/排除原因），支持按池筛选 |

### 调用记录的原因标签

| 标签 | 含义 |
|---|---|
| 选中调用 | 最终选中该模型 |
| 用量已用尽 / RPM 触顶 / TPM 触顶 | 配额或限流排除 |
| 不支持图片 | 纯文本模型遇到图片请求 |
| 超上下文窗口 | 请求过长 |
| 冷却中 | 上游刚报错，临时跳过 |
| 一次性已失效 | 一次性模型到期/用完 |
| 子池无可用接口 | 嵌套子池内全部不可用 |
| 上游限流/错误 → 切换 | 调用失败后切换到下一个 |

---

## 上游适配

| provider | 协议 | 端点 |
|---|---|---|
| `openai` | OpenAI 兼容 | `{base_url}/chat/completions` |
| `anthropic` | Anthropic Messages | `{base_url}/v1/messages` |

Anthropic 适配器自动完成格式转换：
- 提取 OpenAI 的 system 消息为 `system` 字段
- 将 `image_url`（base64 / URL）转换为 Anthropic 的 `image` 块
- 响应转换回 OpenAI 格式（usage、finish_reason 映射、流式 chunk）

---

## 对外 API

| 端点 | 方法 | 说明 |
|---|---|---|
| `/v1/chat/completions` | POST | 对话（支持 `stream`） |
| `/v1/models` | GET | 模型与池列表 |
| `/health` | GET | 健康检查 |
| `/stats` | GET | 各模型用量统计 |
| `/speedtest` | POST | 测速（可传 `model_ids`） |
| `/admin/*` | — | 管理后台 API |

### 验证示例

```bash
# 健康检查
curl http://127.0.0.1:8650/health

# 对话
curl -X POST http://127.0.0.1:8650/v1/chat/completions \
  -H "Authorization: Bearer 123456" \
  -H "Content-Type: application/json" \
  -d '{"model": "auto", "messages": [{"role": "user", "content": "你好"}]}'
```

---

## 数据存储

SQLite（`gateway.db`）持久化：

- `token_usage`：每模型每日累计用量
- `request_log`：最近 60 秒请求记录（RPM/TPM 滑动窗口）
- `one_time_state`：一次性模型的用量、创建时间、是否过期
- `decision_log`：最近 500 条调用决策记录（选模过程与切换依据）

---

## 已知限制

- 流式请求的 **TPM** 统计为近似值（RPM 与每日配额正常）
- 服务器在刷新时刻宕机会错过当次重置（不补跑）
- 上游提供商需支持 OpenAI 兼容或 Anthropic 协议
