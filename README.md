<div align="center">

# 🚀 Model Gateway

### 本地 LLM 自动切换网关 · 聚合多上游 · 配额保护 · 可视化管控

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Version](https://img.shields.io/badge/version-v2.9.2-orange)](CHANGELOG.md)
[![Status](https://img.shields.io/badge/status-stable-brightgreen)](#)

对外暴露 **OpenAI 兼容**与 **Anthropic Messages** 接口，
对内聚合多上游模型，**自动配额管理 · 限流保护 · 故障切换 · 多模态降级**。

[**快速开始**](#-快速开始) · [**核心特性**](#-核心特性) · [**架构**](#-架构) · [**API 文档**](#-对外-api) · [**版本**](#-版本)

</div>

---

## 📑 目录

- [✨ 核心特性](#-核心特性)
- [🏗️ 架构](#-架构)
- [🚀 快速开始](#-快速开始)
- [📂 项目结构](#-项目结构)
- [🎯 路由规则](#-路由规则)
- [🔌 上游适配](#-上游适配)
- [🧠 核心逻辑](#-核心逻辑)
- [⚙️ 配置说明](#-配置说明)
- [🎨 管理后台](#-管理后台)
- [📡 对外 API](#-对外-api)
- [💾 数据存储](#-数据存储)
- [📜 版本](#-版本)
- [⚠️ 已知限制](#-已知限制)

---

## ✨ 核心特性

| 🚨 故障转移 | 📊 配额管理 | 🎯 智能择优 |
|:---|:---|:---|
| 用量耗尽 / 限流触顶 / 上游报错 → **自动切换下一个** | 每日 Token、用量、5h 滚动窗口、`one_time` 一次性令牌 | 顺序模式 或 **自动择优**（按实时延迟） |

| 🪆 多提供商聚合 | 🔀 模型池嵌套 | 📷 多模态降级 |
|:---|:---|:---|
| 同名模型（如 OpenAI + Azure `gpt-4o`）自动归入"大池子" | 池内可嵌套子池（`pool:子池名`），内置循环引用保护 | 请求含图片时自动跳过纯文本模型；无多模态时明确拒绝 |

| 🛡️ 兜底池可配置 | ⏱️ 延迟阈值可调 | 🔄 API 密钥自动轮换 |
|:---|:---|:---|
| 从兜底池卡片一键勾选为哪些池兜底 | 每池可配置 `slow_latency_threshold`（默认 3000ms，0=不限） | 用户密钥可设过期时长，到期自动生成新 secret，旧 secret 宽限期内仍可认证 |

| 🎨 双管理后台 | 🖱️ 拖拽排序 | 📜 详细调用记录 |
|:---|:---|:---|
| 传统 `/admin` + 科技感 `/hfadmin`，共用同一套后端 API | 池内模型、模型列表、池/供应商顺序均可拖拽 | 每条 step 携带具体数值（ms/RPM/错误类型/HTTP 状态） |

| 🔌 OpenAI / Anthropic 双协议 | 📡 流式支持 | ⚡ 自动测速 |
|:---|:---|:---|
| 客户端可用任一协议调用，网关自动转换 | SSE 流式透传，Anthropic 自动转 OpenAI chunk | 管理后台一键并发测速，滑动平均延迟 |

---

## 🏗️ 架构

```
                          ┌─────────────────────────────┐
   客户端 (hermes 等)      │        Model Gateway         │
   ────────────────        │                             │
   OpenAI 协议 ───────────►│  /v1/chat/completions       │──► 上游 A: OpenAI / 兼容
                          │  /v1/messages (Anthropic)   │──► 上游 B: Anthropic
                          │                             │──► 上游 C: 自建 LLM
   x-api-key / Bearer ────►│  ┌───────────────────────┐  │
                          │  │  Auth  (verify_key)    │  │
                          │  └──────────┬────────────┘  │
                          │             ▼               │
                          │  ┌───────────────────────┐  │
                          │  │  ModelPool            │  │
                          │  │  ├─ select_model      │──► 配额 / RPM / TPM / 多模态
                          │  │  ├─ auto_order        │     冷却 / 上下文 / 嵌套
                          │  │  ├─ fallback_pool     │──► 兜底池回退
                          │  │  └─ usage + cooldown │     SQLite 持久化
                          │  └──────────┬────────────┘  │
                          │             ▼               │
                          │  ┌───────────────────────┐  │
                          │  │  providers/           │  │
                          │  │  ├─ openai_provider.py │──► OpenAI Chat Completions
                          │  │  └─ anthropic_provider │──► Anthropic Messages
                          │  └───────────────────────┘  │
                          └─────────────────────────────┘
```

**请求生命周期**：客户端 → Auth 鉴权 → Pool 选模（硬门槛过滤 + 择优排序） → Provider 适配 → 调用上游 → 计费入库 → 流式/非流式响应。

---

## 🚀 快速开始

### 环境要求

- **Python 3.10+**（用到了 `match` 语句、`dict | None` 等新语法）
- 网络可访问至少一个 OpenAI 兼容或 Anthropic 提供商

### 三步启动

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 启动
python main.py

# 3. 访问
# 管理后台:  http://127.0.0.1:8650/admin/
# API 地址:  http://127.0.0.1:8650/v1
```

### 首次配置

1. 浏览器打开 `/admin/`，输入管理密码（默认 `config.json` 的 `server.api_key`，即 `123456`）
2. 在 **模型管理** 添加你的模型（OpenAI / Anthropic / 自建）
3. 在 **模型池** 调整 `auto` 池的模型顺序
4. 在客户端（如 hermes）填 Base URL `http://127.0.0.1:8650/v1` 和同一 API Key

> 💡 **外部访问**：将 `config.json` 的 `server.host` 改为 `0.0.0.0`。Windows 防火墙会首次弹窗询问是否放行，需同意。
>
> 💡 **Ubuntu / Linux**：`python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt && python3 main.py`

---

### 🔁 开机自启（Windows）

已内置启动器脚本：`start_gateway.ps1`（开窗显示启动进度 → 健康检查通过后自动关窗，主程序隐藏窗口后台静默运行；失败则窗口停留提示日志）与 `stop_gateway.ps1`（按 8650 端口停止）。

注册当前用户开机自启（无需管理员，登录后自动运行启动器）：

```bat
reg add "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v ModelGateway /t REG_SZ /d "powershell.exe -NoProfile -ExecutionPolicy Bypass -File D:\AIcoding\model-gateway\start_gateway.ps1" /f
```

手动重启：先运行 `stop_gateway.ps1`（或 taskkill 旧进程），再双击/运行 `start_gateway.ps1` 即可。

## 📂 项目结构

```
model-gateway/
├── main.py                   # FastAPI 入口，对外 API 路由
├── admin.py                  # 管理后台 API（CRUD + 拖拽 reorder + 兜底配置）
├── pool.py                   # 核心：模型池、可用性判定、择优、切换、计费
├── models.py                 # 请求/响应 Pydantic 模型
├── database.py               # SQLite 持久化（用量/请求日志/决策记录/密钥）
├── scheduler.py              # 每模型独立定时刷新（APScheduler）
├── format_adapter.py         # Anthropic ↔ OpenAI 格式双向转换
├── config.json               # 全部配置：服务、模型注册、池定义
├── requirements.txt
├── providers/
│   ├── openai_provider.py    # OpenAI 兼容适配器（含流式、测速）
│   └── anthropic_provider.py # Anthropic 适配器（含格式转换、流式）
├── static/
│   ├── index.html            # 传统管理后台
│   └── hfadmin.html          # 科技感管理面板（v2.1bate 引入）
└── gateway.db                # 运行时自动生成（SQLite）
```

---

## 🎯 路由规则

客户端请求的 `model` 字段决定网关如何路由：

| 填写内容 | 路由行为 | 示例 |
|:---|:---|:---|
| `auto` | 走 `auto` 池（默认） | `"model": "auto"` |
| 池名 | 走该池，按池配置调度 | `"model": "deepseek"` |
| `池名:模型名` | 在指定池中匹配该模型 | `"model": "deepseek:gpt-4o-mini"` |
| 模型名 | 大池子：所有同名模型按序尝试 | `"model": "gpt-4o"` |
| `标签/模型名` | 精确指定某个模型实例 | `"model": "openai/gpt-4o"` |

### Anthropic Messages 格式调用

客户端可用 Anthropic SDK 直接调用（`/v1/messages` 或 `/v1/chat/completions` 均可），网关自动转换：

| 方向 | 处理 |
|:---|:---|
| **请求** | Anthropic Messages → OpenAI Chat Completions：`system` 字段提取、`messages` 内容块（text/image）转换、`stop_sequences` → `stop` |
| **认证** | `x-api-key: <密钥>` 或 `Authorization: Bearer <密钥>` 均可 |
| **响应** | OpenAI → Anthropic Messages：含流式事件 `message_start` / `content_block_delta` / `content_block_stop` / `message_delta` / `message_stop`；`message_delta.usage.output_tokens` 反映真实 Token 用量 |
| **限制** | Anthropic 格式暂不支持 `tools` / `tool_choice`（返回 400 明确拒绝，请用 OpenAI 格式） |

---

## 🔌 上游适配

| `provider` | 协议 | 端点 |
|:---|:---|:---|
| `openai` | OpenAI 兼容 | `{base_url}/chat/completions` |
| `anthropic` | Anthropic Messages | `{base_url}/v1/messages` |

Anthropic 适配器自动完成：

- ✅ 提取 OpenAI 的 `system` 消息 → Anthropic `system` 字段
- ✅ `image_url`（base64 / URL）→ Anthropic `image` 块
- ✅ 响应转回 OpenAI 格式（`usage`、`finish_reason` 映射、流式 chunk）

---

## 🧠 核心逻辑

### 1️⃣ 可用性判定（硬门槛）

每次调用时，候选模型按以下优先级依次检查，**任一不通过即排除**：

| # | 条件 | 含义 |
|:---:|:---|:---|
| 1 | **用量** | 每日 Token / 请求次数用尽 → 排除（一次性模型到期永久失效） |
| 2 | **RPM** | 最近 60 秒请求数触顶 → 排除（实时重算，恢复快） |
| 3 | **TPM** | 最近 60 秒 Token 总量触顶 → 排除 |
| 4 | **多模态** | 请求含图片但模型为纯文本 → 排除 |
| 5 | **冷却** | 上游 429 / 5xx / 超时后冷却（v2.3.8 时长分级） → 排除 |
| 6 | **上下文** | 估算 Token 超过模型 `context_window` → 排除 |

### 2️⃣ 调用顺序（仅在可用模型中生效）

- **顺序模式**：严格按池内设定顺序，第一个可用的即被选中
- **自动择优模式**：在可用模型中按**实时延迟从低到高**优先（延迟来自测速 + 真实请求耗时的滑动平均）；超过池配置 `slow_latency_threshold`（默认 3000ms，0=不限）的模型直接排到末尾（nested 子池用各自阈值）
- **负载均衡模式**（`load_balance: true`，与自动择优互斥）：每次请求从**上次选中模型的下一个**开始轮转（round-robin），失败仍顺序尝试后续模型——即在多个可用模型间轮流分配请求，而非固定用第一个

### 3️⃣ 故障切换

选中模型调用上游，成功则返回；遇到 429 / 异常则标记冷却并切换下一个，直到成功或候选耗尽（返回 503 及具体原因）。

**冷却时长分级**（v2.3.8）：

| 错误类型 | 冷却 |
|:---|:---|
| 上游 429 限流 | 20s |
| 上游 5xx（500-599） | 15s |
| 网络超时 / 连接失败 | 10s |
| 其他异常 | 30s（兜底） |

**兜底池回退**：当主池（及其子池）所有模型都不可用时，自动回退到该池 `fallback_pool` 指定的兜底池。可在管理后台从兜底池卡片一键勾选"为哪些池兜底"（auto 池强制兜底）。

### 4️⃣ 多模态降级

- 请求含图片时，按顺序跳过纯文本模型，选中第一个可用的多模态模型
- 若整个调用链中无任何可用多模态模型，返回 503：`请求包含图片，但池内没有可用的多模态模型`

### 5️⃣ 模型池嵌套

- 池内条目可以是**模型**，也可以是**子池**（`pool:池名`）
- 轮到子池时，按该子池自身配置（自动/顺序）调用其内部模型；子池耗尽则继续下一项
- 内置循环引用保护（A→B→A 不会死循环）
- **计费不重复**：所有嵌套最终解析到同一模型注册表条目（按模型 ID 唯一计数），同一模型无论直接引入还是经子池引入，都共享同一用量计数器

### 6️⃣ 一次性令牌（`token_type: "one_time"`）

- 首次调用记录创建时间，累加用量
- 到达 `max_tokens` 或超过 `ttl_seconds` 后永久失效
- 任何刷新都不重置

### 7️⃣ 计费模式

- `billing_mode: "token"`：按消耗 Token 计数
- `billing_mode: "request"`：按请求次数计数（每次 +1）
- 流式请求同样计入每日配额（从响应的 `usage` 提取 Token）

---

## ⚙️ 配置说明

> 推荐通过管理后台修改（实时生效）；也可直接编辑 `config.json` 后重启。

```jsonc
{
  "server": {
    "host": "0.0.0.0",                  // 0.0.0.0 = 监听所有网卡；127.0.0.1 = 仅本机
    "port": 8650,
    "api_key": "your-strong-key"        // ⚠️ 暴露到局域网前务必修改
  },
  "default_mode": "auto",
  "models": [
    {
      "id": "openai/gpt-4o",            // 唯一标识（标签/模型名）
      "name": "gpt-4o",                 // 模型名（同名自动归入大池子）
      "provider": "openai",             // openai（兼容）或 anthropic
      "base_url": "https://api.openai.com/v1",
      "api_key": "sk-xxx",
      "daily_token_limit": 1000000,     // 每日上限（0=不限；按次计费时单位为次）
      "rpm_limit": 60,                  // 每分钟请求上限（0=不限）
      "tpm_limit": 100000,              // 每分钟 Token 上限（0=不限）
      "context_window": 128000,         // 上下文窗口（0=不校验）
      "max_concurrency": 0,             // 最大并发（0=不限，默认 0）
      "token_type": "daily",            // daily / rolling_5h / one_time
      "billing_mode": "token",          // token / request
      "is_free": true,                  // 默认免费（v2.3.3+）；付费模型显式设为 false
      "modality": "vision",             // text（纯文本）/ vision（多模态）/ embedding（嵌入）/ rerank（重排）
      "refresh_time": "00:00",          // 每日刷新时间（北京时间）
      "timezone": "Asia/Shanghai"
      // one_time 专属:
      // "max_tokens": 0,              // 一次性最大用量
      // "ttl_seconds": 0              // 一次性存活时长（秒）
    }
  ],
  "pools": {
    "auto": {
      "strategy": "sequential",
      "auto_order": false,              // true = 按延迟自动择优
      "slow_latency_threshold": 3000,  // 自动择优降级阈值（ms，0=不限）
      "fallback_pool": "兜底池",        // 主池耗尽时回退到此处
      "model_ids": ["openai/gpt-4o", "pool:fixed"]
    },
    "fixed": {
      "auto_order": false,
      "slow_latency_threshold": 3000,
      "fallback_pool": null,            // 留空则不启用兜底
      "model_ids": ["openai/gpt-4o-mini"]
    }
  }
}
```

> ⏰ **时区**：统一固定为北京时间（Asia/Shanghai），不可修改。

---

## 🎨 管理后台

网关同时提供两套前端，**共用同一套 `/admin/*` 后端 API**：

| 入口 | 风格 | 适用 |
|:---|:---|:---|
| **`/admin/`** | 传统表格 | 习惯 classic 风格 |
| **`/hfadmin`** | 科技感卡片 | 现代化 UI，新功能优先在此落地 |

### 标签页功能矩阵

| 标签页 | 核心功能 |
|:---|:---|
| 📦 **模型管理** | 增删改模型接口；标注免费/付费、纯文本/多模态/嵌入/重排、计费模式；同名模型自动归入大池子；**模型条目可拖拽排序并持久化** |
| 🪆 **模型池** | 新增/删除池；拖拽 ⠿ 调整优先级与池内模型顺序；加入模型或子池；开关"自动择优"；**配置延迟阈值 ms（0=不限）**；⚡ 测速本池；**从兜底池卡片一键选择为哪些池兜底**；悬停 ⓘ 查看接口详情 |
| 📊 **用量统计** | 卡片式可视化，横向进度条展示 已用/总量（青→琥珀→红分级），支持 **计费量 / 调用次数 / Token 用量** 三维度切换，每 5 秒自动刷新 |
| 📜 **调用记录** | 每次调用的完整选模过程与切换依据（选中/排除原因），支持按池筛选 |

### 调用记录的原因标签（v2.3.4+）

每条 step 除静态标签外还携带 `detail` 对象，前端渲染时**追加具体数值**（用 ` · ` 分隔），鼠标悬停看完整信息。无需 SQLite 迁移——`steps` 已经是 JSON 列。

| 标签 + detail 示例 | 含义 |
|:---|:---|
| 选中调用 | 最终选中该模型 |
| 用量已尽 · 1,500/1,000 (150%) | 配额或限流排除 |
| RPM 触顶 · 60/60 RPM | RPM 触发上限 |
| TPM 触顶 · 120,000/100,000 TPM | TPM 触发上限 |
| 冷却中 · 剩 45s | 上游刚报错，临时跳过 |
| 超上下文窗口 · 请求估算 150,000 tok，上限 128,000 | 请求过长 |
| 不支持图片 · 模型为 text，请求含图 | 纯文本模型遇到图片请求 |
| 一次性已失效 · 已用 50,000/50,000 | 一次性模型到期/用完 |
| 一次性已失效 · 已存活 3700s / TTL 3600s | 同上，TTL 触发 |
| 子池无可用接口 | 嵌套子池内全部不可用 |
| **上游限流 → 切换 · HTTP 429 · 234ms · 冷却 20s** | 上游 429 限流后切换 |
| **上游错误 → 切换 · TimeoutError · HTTP 500 · 2300ms · 冷却 15s** | 上游错误后切换 |

**`detail` 字段参考**：

| reason | detail 字段 |
|:---|:---|
| `quota_exhausted` (daily) | `{used, limit}` |
| `quota_exhausted` (rolling_5h) | `{used, limit, window_remaining_sec}` |
| `rpm_limited` / `tpm_limited` | `{current, limit}` |
| `cooldown` | `{remaining_sec}` |
| `context_exceeded` | `{estimated, window}` |
| `no_vision` | `{modality}` |
| `one_time_expired` | `{used, limit}` / `{age_sec, ttl_sec}` / `{expire_date}` |
| `switch_429` / `fallback_switch_429` | `{status: 429, latency_ms, cooldown_sec}` |
| `switch_error` / `fallback_switch_error` | `{error_type, status?, latency_ms, cooldown_sec, error?}` |

---

## 📡 对外 API

### 客户端 API

| 端点 | 方法 | 说明 |
|:---|:---:|:---|
| `/v1/chat/completions` | POST | 对话（支持 `stream`，OpenAI 与 Anthropic 格式均可） |
| `/v1/messages` | POST | Anthropic Messages 格式对话（同 `/v1/chat/completions`） |
| `/v1/embeddings` | POST | OpenAI 兼容 embedding（仅路由到模态=embedding 的模型，响应透传上游） |
| `/v1/rerank` | POST | 重排（Jina/Cohere/SiliconFlow 兼容；仅路由到模态=rerank 的模型，响应透传上游） |
| `/v1/models` | GET | 模型与池列表 |
| `/health` | GET | 健康检查 |
| `/version` | GET | 网关版本号（从 git tag 读取） |
| `/stats` | GET | 各模型用量统计 |
| `/speedtest` | POST | 测速（可传 `model_ids`） |

### 管理 API

| 端点 | 方法 | 说明 |
|:---|:---:|:---|
| `/admin/models/reorder` | PUT | 调整模型管理列表顺序 |
| `/admin/pools/reorder` | PUT | 调整池顺序 |
| `/admin/providers/reorder` | PUT | 调整供应商顺序 |
| `/admin/pools/fallback_targets` | PUT | 一键设置哪些池的 fallback 指向兜底池 |
| `/admin/pools/{name}` | PUT | 更新池（model_ids / strategy / auto_order / slow_latency_threshold） |
| `/admin/keys` | POST/PUT/DELETE | 用户 API 密钥 CRUD（含 `expire_seconds` 自动轮换） |

### 调用示例

```bash
# 健康检查
curl http://127.0.0.1:8650/health

# 发起对话
curl -X POST http://127.0.0.1:8650/v1/chat/completions \
  -H "Authorization: Bearer your-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "auto",
    "messages": [{"role": "user", "content": "你好"}]
  }'

# 流式调用
curl -X POST http://127.0.0.1:8650/v1/chat/completions \
  -H "Authorization: Bearer your-key" \
  -H "Content-Type: application/json" \
  -d '{"model": "auto", "stream": true, "messages": [{"role":"user","content":"hi"}]}'

# Embedding（池内需有模态=embedding 的模型）
curl -X POST http://127.0.0.1:8650/v1/embeddings \
  -H "Authorization: Bearer your-key" \
  -H "Content-Type: application/json" \
  -d '{"model": "auto", "input": "你好"}'

# Rerank 重排（池内需有模态=rerank 的模型）
curl -X POST http://127.0.0.1:8650/v1/rerank \
  -H "Authorization: Bearer your-key" \
  -H "Content-Type: application/json" \
  -d '{"model": "auto", "query": "什么是重排", "documents": ["文档A", "文档B"], "top_n": 2}'

# 统一思考控制：reasoning_effort 六档 off/minimal/low/medium/high/max
# 网关按各模型配置的 reasoning_map 自动换算为上游思考参数；未配置的模型忽略该参数
curl -X POST http://127.0.0.1:8650/v1/chat/completions \
  -H "Authorization: Bearer your-key" \
  -H "Content-Type: application/json" \
  -d '{"model": "auto", "reasoning_effort": "high", "messages": [{"role":"user","content":"9.11 和 9.8 哪个大？"}]}'
```

### 🧠 统一思考控制（reasoning_effort）

客户端传 `reasoning_effort`，取值六档：`off / minimal / low / medium / high / max`（大小写不敏感；非法值 422）。不带参数 = 默认 `low` 档（v2.10.2+，config.json 顶层 `default_reasoning_effort` 可调）；传 `"reasoning_effort": "auto"`（v2.10.3+）= 本次用模型默认行为。

- **OpenAI 格式入口**：请求体顶层直接传 `"reasoning_effort": "high"`。
- **Anthropic 格式入口**（`/v1/messages`）：传 `thinking: {"type": "enabled", "budget_tokens": 8192}`，网关自动归一化——`disabled`→off；enabled 按 budget 分档（≤1024→minimal，≤4096→low，≤10240→medium，≤20480→high，>20480→max）。
- **模型配置 `reasoning_map`**（管理面板模型编辑 → "思考映射" JSON 框，或 config.json 模型条目）：显式声明"档位 → 透传上游的请求体片段"，例如：

```json
"reasoning_map": {
  "off":     {"thinking": {"type": "disabled"}},
  "minimal": {"reasoning_effort": "minimal"},
  "low":     {"reasoning_effort": "low"},
  "medium":  {"reasoning_effort": "medium"},
  "high":     {"reasoning_effort": "high"},
  "max":     {"reasoning_effort": "max"}
}
```

规则：片段原样 merge 进上游请求体（禁止覆盖 model/messages/tools/max_tokens 等核心字段）；请求档位未配置时回落到更低档位中最近的（无更低取最低配置档）；未配置 `reasoning_map` 的模型不注入任何参数。anthropic 协议上游注入 `thinking.budget_tokens` 时若大于 max_tokens 会自动抬高（+1024）。

- **思考内容回传**：非流式 OpenAI 响应统一带 `reasoning_content` 字段（MiniMax 等 `<think>` 内联的模型自动提取）；Anthropic 客户端方向自动转换为 `thinking` 块 / 流式 `thinking_delta`。流式 OpenAI→OpenAI 原样透传。
- **自动探测**：新增模型（非 embedding/rerank）保存时自动探测思考档位——缓存命中瞬间套用，新组合后台探测完成后自动写入并热重载；embedding/rerank 模态不参与思考控制（输入框隐藏、映射自动剥离）。存量模型/批量补测用 `python probe_reasoning.py`：实测每个上游模型支持的写法与档位并生成 `思考参数探测报告.md`；`--apply` 写入 config.json；`--filter 关键字` / `--force` / `--report` 控制范围。

---

## 💾 数据存储

SQLite（`gateway.db`）持久化以下表：

| 表 | 用途 |
|:---|:---|
| `token_usage` | 每模型每日累计用量 |
| `request_log` | 最近 60 秒请求记录（RPM/TPM 滑动窗口） |
| `one_time_state` | 一次性模型的用量、创建时间、是否过期 |
| `decision_log` | 最近 500 条调用决策记录（选模过程与切换依据） |
| `api_keys` | 用户/管理员 API 密钥（`expire_seconds` / `rotated_at` / `previous_secret`） |
| `key_rotation_log` | 密钥轮换审计日志（仅存前 8 字符前缀，不存完整密钥） |
| `model_daily_stats` | 模型每日调用次数 / Token 用量统计 |
| `5h_state` | 滚动 5h 窗口限额的用量与窗口起点 |

> 🔐 **API 密钥轮换**：密钥可设置过期时长（秒），到期自动轮换新 secret，旧 secret 宽限期（约过期时长的 20%，1~24 小时）内仍可认证。

---

## 📜 版本

### 最新

**`v2.11.1`** — **管理面板模型编辑重构 + JSON 徽章 + 手机适配修复**：①双面板（admin/hfadmin）模型列表新增 `JSON` 徽章标识 `json_output` 能力（embedding/rerank 不显示），无需逐个点开查看 ②编辑弹窗移除"自定义参数名/值"（`extra_params` 废弃，保存时显式提交空对象避免 PUT 合并语义保留旧值；现网无模型使用该字段）③编辑弹窗重构：按"计费与额度/配额与限流/流式与输出选项"分组带标题分隔，智能估算/不发 stream_options/支持 JSON 三选项改为勾选高亮的选项卡片（原挤在超时时间/模型模态标签内），底部操作栏吸底（桌面/手机双档内边距贴合）④手机适配根因修复：index.html 的 `@media(max-width:640px)` 块原写在基础样式之前，`.f2` 单列等规则被后置同权重规则覆盖致手机版布局从未生效——移至样式表末尾 ⑤交互提速：点击"编辑"改为复用页面已加载数据 0 请求直开（原为 2 个串行请求先行），卡片入场动画延迟封顶 400ms（50 模型时原最长 2s 才显完），修复 hfadmin 编辑模式按钮文字（添加→保存）与重复 `onSmartToggle` 调用 ⑥计费回归套件 33/33 全绿（改动前后各跑一遍），UI 层经浏览器实测两面板桌面+手机视图

**`v2.11.0`** — **计费安全网 + 中转提速 + 策略精简（整合 v2.10.6~v2.10.10）**：①新增 `test_billing_regression.py` 计费回归套件（33 断言，隔离实例+mock 上游：流式/非流式入账、缺失 usage 记 0、request/one_time/5h 语义、用户 key 计费、并发 10 路无重复无丢失、思考透传、智能估算、stream_options 开关），任何改动先跑套件再上线 ②流式热路径提速：`<think>` 切分器 done 快速路径 + 仅 usage 行解析，每 chunk JSON 处理 3 次→0~1 次；输入估算按请求缓存；决策日志后置（流结束一次写入，移出首包路径）；`db.bulk()` 事务上下文，非流式 5 次串行 commit 合并为 1、keyauth 两写合一 ③决策明细新增 route_ms/upstream_ms 耗时分解 ④模型级 `no_stream_options` 开关、空 api_key 不发鉴权头、无 tools 不下发 tool_choice（修本地模型偶现 400）⑤修复 one_time 未校准模型被窗口估算永久误杀的死锁 ⑥回退问题23：配额预检恢复 `used >= limit` 旧口径，est_effective 仅用于决策日志展示

**`v2.10.10`** — **回退问题23（配额预检不再计入本次估算）**：应用户研判，配额预检恢复旧口径——仅 `used >= limit` 才拒绝，不再计算"调用后是否超额"（省估算计算、逻辑更简、消除未校准误杀风险）；est_effective 保留仅用于决策日志展示；修复过程中曾发现并保留 one_time 死锁防护

**`v2.10.9`** — **中转提速（问题21-A）+ 本地模型 400 修复（问题21-B），计费回归套件 33 断言全绿**：①`test_billing_regression.py` 计费回归套件（隔离实例+mock 上游，固化问题24 全部计费不变量与并发场景，任何改动先跑套件再上线）②流式热路径：`<think>` 切分器 done 快速路径 + 仅 usage 行解析，每 chunk JSON 处理 3 次→0~1 次 ③输入估算按请求缓存 ④流式决策日志后置（结束时一次写入含 actual_tokens，INSERT 移出 TTFB）⑤`db.bulk()` 事务上下文，非流式 5 次串行 commit 合并为 1、keyauth 两写合一 ⑥决策明细新增 route_ms/upstream_ms 耗时分解 ⑦模型级 `no_stream_options` 开关、空 api_key 不发鉴权头、无 tools 不下发 tool_choice ⑧修复 v2.10.4 引入的 one_time 未校准死锁

**`v2.10.5`** — **冷却时间策略统一**：应用户要求全部缩短——chat 非流式/流式"其他异常"15s→5s（429 保持 10s、5xx/网络保持 5s、上下文超限 400 保持不冷却）；embedding/rerank"其他错误"从一刀切 15s→5s；最坏不可用窗口 15s×N→5s×N

**`v2.10.4`** — **问题22/23/24 本地重做（以本地主线为基，含全部思考控制功能）**：①问题24 流式计费静默漏记修复移植——流式计费从 finally 移到"usage 到达即记"（billed 防重+finally 兜底+缺失 usage 告警记次数），decision_log 新增 actual_tokens 列（幂等迁移）并新增 `_settle_stream_tokens` 统一计费口径（request 型按次/token 型按真实用量），前端"估算/实际"同屏 ②问题22 Token 智能估算——新增 call_metrics 校准表，估算拆 est_window（窗口预检保守）/est_effective（输入+平均输出 EMA）两路，smart_estimate 模型非流式动态超时（吞吐 EMA×1.3 缓冲，样本<10 回退固定值），`GET /admin/model/{id}/metrics` + 双面板智能超时开关与内联 live 区 ③问题23 配额预检计入本次有效估算——daily/rolling_5h/one_time 三段均 `used+估算>=limit → 拒绝`（修复"151.6w/150w 仍放行单次冲线"），one_time 估算触顶不误标过期，配额缓存键带上估算值

**`v2.10.3`** — **请求级 `reasoning_effort: "auto"`**：客户端可显式传 auto 表示"本次请求不注入思考参数、用模型默认行为"（优先级高于服务端 default_reasoning_effort，含 auto/low 等任何默认档），作为默认档 low 的单次豁免；422 提示文案同步列出 auto

**`v2.10.2`** — **默认思考档位 low**：请求不传 `reasoning_effort` 时不再放任模型默认（多数思考模型默认开思考且深度自选），改为注入默认档 `low`（按各模型 reasoning_map 映射；未配置映射的模型不受影响）。config.json 顶层新增可选 `default_reasoning_effort`（六档任一，缺省 low；设 `auto`/`""` 恢复旧的"模型默认"行为；非法值回落 low），/admin/reload 热生效

**`v2.10.1`** — **新增模型自动探测思考档位**：① 非 embedding/rerank 模型新增保存后自动探测——上游组合已有探测缓存时瞬间套用映射，新组合后台探测（约 1~3 分钟）完成后自动写入 reasoning_map 并热重载，面板按探测状态提示 ② "思考映射"输入框对 embedding/rerank 模态自动隐藏，模态改为 embedding/rerank 保存时自动剥离已有映射 ③ probe_reasoning.py 新增 cached_suggestion/probe_single 接口供管理端复用

**`v2.10.0`** — **统一思考控制（reasoning_effort）**：① `/v1/chat/completions` 顶层新增 `reasoning_effort` 六档（off/minimal/low/medium/high/max，非法值 422），`/v1/messages` 的 `thinking{type,budget_tokens}` 自动归一化到同档位 ② 模型条目新增 `reasoning_map` 配置（档位 → 透传上游的请求体片段，缺档自动回落到更低档），在 pool 层统一注入，未配置的模型不注入 ③ `probe_reasoning.py` 全池实测各上游思考参数写法与档位并生成《思考参数探测报告》，`--apply` 一键写入 config.json ④ 思考内容回传统一口径：非流式 OpenAI 响应统一带 `reasoning_content`（MiniMax 等 `<think>` 内联自动提取）、流式 `<think>` 内联自动转 `reasoning_content` 增量并补发缺失的 `[DONE]`、Anthropic 客户端方向转 thinking 块/thinking_delta（此前非流式与 anthropic 协议上游的思考内容均被丢弃）⑤ 双面板模型编辑新增"思考映射"JSON 框 + 列表徽章

### 历史（按时间倒序）

| 版本 | 主要变更 |
|:---|:---|
| **v2.9.2** | updater 更新提速：版本查询并行化、fetch+ff-only 合并、requirements 未变跳过 pip install、启动/重启探测 1 秒粒度 |
| **v2.9.1** | 守护面板交互安全加固：诊断卡片移至操作下方；所有操作按钮二次确认；强制释放端口需输入操作密码（服务端校验） |
| **v2.9.0** | 双管理面板手机适配：hfadmin（≤760px）与 index（≤640px）完整响应式，桌面端零改动 |
| **v2.8.5** | "重启 updater"交互闭环：按钮"重启中"态 + 已等待秒数倒计时 + 轮询 /health 自动探测恢复 + 45s 超时排查指引，修复静默失联 |
| **v2.8.4** | 守护面板手机适配：≤560px 断点（状态卡 2×2、行上下堆叠、按钮双列满宽加高、Toast 通栏） |
| **v2.8.3** | updater 守护服务加固：git 自愈（unmerged 温和恢复 + 硬重置云端兜底，杜绝死循环）；端口强杀（SIGKILL 升级 + 残留进程清理 + 面板按钮）；诊断增强（systemctl 状态行/journalctl 尾部/错误时间线上屏）；仪表盘重做（苹果浅色主题）；新增 /action/restart-updater |
| **v2.8.2** | 模型级超时时间设置（120=默认/0=无限等待/指定秒数，解决本地非流式模型被 120s 误判超时）；OpenAI/Anthropic 适配器按模型独立超时；双面板表单同步；旧配置完全兼容 |
| **v2.8.1** | 池配置「加入模型」下拉按供应商分组排序（供应商持久化顺序 + 组内原顺序，独立模型排最后），新增 `====模型接口====` / `====子池====` 分割标题；移除「接口测速」标签页（⚡ 测速本池已覆盖，`/speedtest` 保留） |
| **v2.8.0** | rerank（重排）模型支持：`/v1/rerank` 端点（Jina/Cohere/SiliconFlow 兼容）；第三种专用模态进模型池（双向硬门槛 + 自动切换 + 计费复用）；test_model 按模态分流；双面板 rerank 徽章；anthropic 协议 embedding/rerank 误配拦截 |
| **v2.7.2** | 池配置引用失效显式记录（ref_not_found）；test_model 按模态分流（embedding 直测）；双面板 embedding 徽章；load_balance 轮转语义确认 |
| **v2.7.1** | json 严格路由：移除兼容降级，无 json 模型时 503 + 勾选指引 |
| **v2.7.0** | embedding 支持 + json 输出门槛；Ollama/WebUI 探测端点兼容；日志体系升级 + --port 参数；前端 json_output/extra_params |
| **v2.5.1** | 后端日志统一加时间戳；负载均衡提示框+滑块浅蓝色；hfadmin 补自动择优提示 |
| **v2.5.0** | 每日额度跨天惰性重置 + 负载均衡（round-robin 互斥自动化择优）+ models reorder 路由 + 5h 展示与 Token 单位 |
| **v2.4.1** | 代理连接测试 / 供应商排序 / 节点解析 / 缓存隔离等 5 项修复 |
| **v2.4.0** | VPN/clash proxy 支持（每供应商独立代理，httpx.AsyncClient proxy 参数） |
| **v2.3.8** | 429 冷却 30s → 20s；统一流式路径 detail |
| **v2.3.7** | 移除延迟阈值预设按钮（保留输入框+单位+hint） |
| **v2.3.6** | 美化延迟阈值输入框（玻璃拟态 + 内嵌单位 + 含义提示） |
| **v2.3.5** | 冷却时长按错误类型分级（429=30s / 5xx=15s / 网络=10s） |
| **v2.3.4** | 调用记录 step.detail 透传具体数值（限流/RPM/错误类型/HTTP 状态/延迟） |
| **v2.3.3** | 新建模型默认免费（`is_free=true`） |
| **v2.3.2** | 兜底池可配置化 + 延迟阈值可配置 + 新建模型并发默认0 + admin closeModal bug |
| **v2.3bate** | 修复问题 1/2/3/4/5/6 诊断（统计双维度 / Anthropic 格式 / API 密钥轮换 / 拖拽 / 模型顺序 / 内容空诊断） |
| **v2.2.1bate** | hfadmin 窄屏版本号紧凑显示 |
| **v2.2bate** | 密钥限额窗口归一化、授权池复选框对齐、供应商拖拽排序 |

> 💡 **版本号机制**：由 `git tag` 决定，启动时通过 `/version` 接口读取并展示在前端页脚。新版本通过 `git tag v2.3.X` 打 tag 后，前端立刻能看到。

---

## ⚠️ 已知限制

- ⚠️ 流式请求的 **TPM** 统计为近似值（RPM 与每日配额正常）
- ⚠️ 服务器在刷新时刻宕机会错过当次重置（不补跑）
- ⚠️ 上游提供商需支持 OpenAI 兼容或 Anthropic 协议
- ⚠️ 部分上游服务对纯文本块列表 `content: [{type:text, text:"..."}]` 兼容性差，本网关已自动合并为字符串（v2.3bate+）

---

<div align="center">

**Made with ❤️ for the local LLM community**

如果这个项目对你有帮助，欢迎 ⭐ Star！

</div>