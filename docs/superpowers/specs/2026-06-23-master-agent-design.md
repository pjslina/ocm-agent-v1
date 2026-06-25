# MasterAgent (MA) 设计书

- **状态**：v1 设计草案，待最终评审
- **日期**：2026-06-23
- **范围**：企业级智能助手服务 MasterAgent 的 v1 工程化方案
- **目标读者**：MA 项目研发团队、对接前端/移动端、运维 SRE、相关业务方
- **关联文档**：本仓库 `CLAUDE.md`（编码准则）

---

## 0. 摘要

MasterAgent（简称 MA）是前端（PC SSE / 移动端 WS）与多个第三方 AI Agent（MetaGC SSE、Uniioc HTTP、知识中心 WS，以及未来下游）之间的智能桥梁。MA 负责：

- 接收用户在不同"专题"（代表小管家 / 经营小助手 / 销售合同责任人 Agent 等）下的自然语言提问
- 调外部 API 取用户上下文 → 业务鉴权（专题 + 问题范围）
- 按上下文补全问题，由 LLM 做意图识别决定路由
- 调用一个或多个下游 Agent，把响应以统一的事件流推送给前端
- 持久化对话历史，跨设备、跨会话可见，并参与上下文拼接以支持追问

**v1 架构形态**：分层 + 插件化 + LangGraph 编排，全异步栈（FastAPI + uvicorn + asyncpg + httpx + websockets）。

---

## 1. 系统总体架构

### 1.1 分层视图

```
┌─────────────────────────────────────────────────────────┐
│ Transport 层（FastAPI Routers）                          │
│  • SSE Endpoint     /api/v1/chat/sse                    │
│  • WebSocket        /api/v1/chat/ws                     │
│  • REST             /api/v1/sessions, /messages 等       │
│  职责：协议解析 → ChatRequest；ChatEvent → 协议帧         │
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ Application 层（ChatService）                            │
│  • 加载 Session、装配 Context                            │
│  • 选择 Topic                                            │
│  • 启动 LangGraph 执行                                   │
│  • 将 LangGraph 事件转写为 ChatEvent 流                  │
│  • 触发消息持久化                                        │
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ Topic / Graph 层（LangGraph 编排）                        │
│  每个专题 = 一个编译后的 StateGraph                       │
│  节点 = 插件（Auth / Enrich / Intent / Adapter / Reply） │
│  条件边 = 意图识别结果 → 路由                              │
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ Adapter 层（下游接口）                                    │
│  • MetaGCAdapter (SSE)                                  │
│  • UniiocAdapter (HTTP)                                 │
│  • KnowledgeCenterAdapter (WS)                          │
│  • LLMRouterAdapter（意图识别用 LLM）                    │
│  统一接口：async def run(...) -> AsyncIterator[ChatEvent]│
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ Infrastructure 层                                         │
│  • OpenGauss Repository（SQL 模板化）                    │
│  • ConfigLoader（YAML，预留 ConfigSource 接口）           │
│  • Logging（structlog + traceId）                        │
│  • Telemetry（OTel + OpenInference for LLM）             │
└─────────────────────────────────────────────────────────┘
```

### 1.2 核心抽象

```python
# 统一事件流
class ChatEvent:
    type: Literal["meta", "thinking", "progress", "delta", "tool", "error", "done"]
    data: Any
    # 上下游统一用这个类型流动

# 适配器统一接口
class DownstreamAdapter(Protocol):
    name: str
    async def run(self, ctx: AgentContext) -> AsyncIterator[ChatEvent]: ...

# 插件统一接口（=LangGraph 节点的封装基类）
class GraphNodePlugin(Protocol):
    name: str
    async def __call__(self, state: GraphState) -> dict: ...

# 仓储
class MessageRepository(Protocol):
    async def list_recent(self, thread_id, limit) -> list[Message]: ...
    async def append(self, msg: Message) -> None: ...

class SessionRepository(Protocol):
    async def get_or_create(self, thread_id, biz_id, w3) -> Session: ...
```

**Transport 层登录态中间件占位**（v1 预留接口，具体机制待定）：

```python
class AuthnMiddleware(Protocol):
    """从请求中提取 w3Account 并验证身份合法性。
    v1 阶段：默认实现 = 信任 header 中的 w3Account（开发模式）。
    生产部署前必须替换为具体实现（JWT / 中心 Session / Gateway 信任 等）。
    """
    async def authenticate(self, request: Request) -> AuthnIdentity: ...

@dataclass
class AuthnIdentity:
    w3_account: str
    raw_claims: dict
```

> 登录态校验与"业务鉴权"是不同关注点：登录态在 Transport 层，失败即 HTTP 401 / WS close 4401；业务鉴权在 LangGraph 节点，失败走对话流出兜底话术。两者分层避免合并到同一个插件。

### 1.3 一次请求的运行轨迹（以"代表小管家 + SSE"为例）

0. **AuthnMiddleware** 从请求中提取并验证身份。失败：REST/SSE → 401；WebSocket → 4401。通过后将 `AuthnIdentity` 注入 `ChatRequest.identity`。
1. 浏览器发 POST `/api/v1/chat/sse`，header 携 `traceId`，body 含 `bizId=daibiao_xiaoguanjia, threadId, w3Account, question, 业务参数...`
2. **Transport 层** 校验请求 → 构造 `ChatRequest` → 调 `ChatService.handle(request)` 返回 `AsyncIterator[ChatEvent]` → 编码为 SSE 帧逐 chunk 写回
3. **ChatService** 加载 Session、近 N 条历史 → 装入 `AgentContext`
4. **ChatService** 用 `bizId` 从 `TopicRegistry` 取出已编译好的 `StateGraph`
5. **LangGraph 执行**：
    - `auth` 节点：调 `AuthPlugin`（外部 API 取用户上下文 → 专题权限 + 问题范围权限）；不通过 → 走 `reject` 节点输出固定话术 → END
    - `enrich` 节点：根据上下文补全 question
    - `intent` 节点：调 `LLMRouterAdapter` 分类 → 写入 `state.route = "metagc" | "uniioc" | "kc"`
    - 条件边按 `state.route` 进入对应的 `metagc_adapter / uniioc_adapter / kc_adapter` 节点
    - 命中节点产生 token 流，挂 `tags=["user_output"]`；写入 `state.answer_chunks`
6. **ChatService** 通过 `graph.astream_events()` 监听事件 → 仅对带 `user_output` tag 的事件转 `ChatEvent(type="delta")` → 推给 Transport
7. 完结时 ChatService append 完整 assistant message → 落 OpenGauss
8. Transport 写 `event: done` → 关闭连接

### 1.4 失败模型（关键路径）

| 失败点 | 处理 |
|---|---|
| 鉴权外部 API 超时 | 默认按"鉴权不通过"返回兜底话术，记 error trace |
| 意图识别 LLM 失败 | fallback 到预设默认路由（每个 Topic 在 YAML 里声明 `default_route`）|
| 下游 Adapter 错误（连接/限流/解析）| ChatEvent(type="error") → 用户看到友好提示，trace 记原始错误 |
| 客户端中途断开 | LangGraph 任务被 cancel；已落的 message 标记 `partial=True` |
| OpenGauss 写失败 | 不影响本次响应，重试 3 次，最终入 dead-letter 日志 |

---

## 2. LangGraph 编排详细设计

### 2.1 GraphState：图状态对象

```python
from typing import TypedDict, Annotated, Literal
from operator import add

class GraphState(TypedDict, total=False):
    # ── 输入区（请求进入时填好，节点只读）─────────
    request_id: str              # = traceId，贯穿整条链
    biz_id: str                  # 专题 ID
    thread_id: str
    identity: AuthnIdentity      # w3Account + claims
    question: str                # 用户原始提问
    biz_params: dict             # 各专题自有的业务参数
    history: list[Message]       # 已加载的近 N 条历史

    # ── 鉴权区（auth 节点写）─────────────────
    user_ctx: dict               # 外部 API 拉到的用户上下文
    auth: AuthResult             # passed / rejected + reject_message

    # ── 补全区（enrich 节点写）───────────────
    enriched_question: str
    enrich_meta: dict

    # ── 路由区（intent 节点写）───────────────
    route: Literal["metagc", "uniioc", "kc", "reject", "default"]
    route_confidence: float
    route_reason: str

    # ── 下游输出区（adapter 节点写）────────────
    answer_chunks: Annotated[list[str], add]
    answer_meta: dict

    # ── 错误区 ────────────────────────────
    errors: Annotated[list[dict], add]
```

**约定**：只有写入 `answer_chunks` 的节点（挂 `user_output` tag）的产出会被流给用户；其它节点输出仅作内部数据传递。

### 2.2 节点目录（v1）

| 节点 | 类型 | 职责 | 是否产 token 流 |
|---|---|---|---|
| `load_session` | 内置 | 拉历史、装 state | 否 |
| `auth` | 插件 (`AuthPlugin`) | 业务鉴权（专题 + 问题范围） | 否 |
| `reject` | 内置 | 输出固定话术、终止 | 是（一次性吐出整段文本） |
| `enrich` | 插件 (`EnrichPlugin`) | 补全 question | 否 |
| `intent` | 插件 (`IntentPlugin`) | LLM 分类 → 写 `route` | 否 |
| `metagc_adapter` | 适配器节点 | 调 MetaGC SSE | **是** |
| `uniioc_adapter` | 适配器节点 | 调 Uniioc HTTP，整段拆 chunk 流出 | **是** |
| `kc_adapter` | 适配器节点 | 调知识中心 WS | **是** |
| `persist` | 内置 | 把 assistant 消息落库 | 否 |

### 2.3 边与控制流

```
[START]
   ↓
load_session
   ↓
auth ──── failed? ──→ reject ──→ persist ──→ [END]
   │ passed
   ↓
enrich
   ↓
intent ─── (conditional edge by state.route) ───┐
   │                                            │
   ├── route=="metagc" → metagc_adapter ───┐    │
   ├── route=="uniioc" → uniioc_adapter ───┤    │
   ├── route=="kc"     → kc_adapter ───────┤    │
   └── route=="default"→ <topic 默认节点>   ┤    │
                                            ↓    │
                                          persist
                                            ↓
                                          [END]
```

- 条件边由 LangGraph 的 `add_conditional_edges` 实现
- 每个专题在 YAML 里声明 `default_route`（intent 失败时兜底）
- 多步编排（"先 MetaGC 再 Uniioc"）只是把 adapter 节点串成线，最终节点仍是唯一的"出 token 节点"——v1 不在意图分支里用到，但图结构天然支持

### 2.4 专题图的声明式定义（YAML）

```yaml
# config/topics/daibiao_xiaoguanjia.yaml
topic_id: daibiao_xiaoguanjia
display_name: 代表小管家
default_route: metagc
history:
  max_turns: 10
  scope: per_topic
auth:
  plugin: representative_auth
  params:
    required_role: REPRESENTATIVE
enrich:
  plugin: representative_enrich
  params:
    fields_to_inject: [region, customer_group]
intent:
  plugin: llm_classifier
  params:
    model: ${env:INTENT_MODEL}
    labels: [metagc, uniioc, kc]
    prompt_template: prompts/intent/representative.txt
graph:
  nodes:
    - id: metagc_adapter
      adapter: metagc
      params: { stream: true }
      output: true
    - id: uniioc_adapter
      adapter: uniioc
      output: true
    - id: kc_adapter
      adapter: kc
      output: true
  edges:
    - from: intent
      type: conditional
      route_by: route
      mapping:
        metagc: metagc_adapter
        uniioc: uniioc_adapter
        kc:     kc_adapter
```

YAML schema 用 Pydantic 校验，启动时任何专题配置不合法 → fail fast 不启动。

### 2.5 流式事件标签约定（关键实现）

LangGraph 用 `astream_events(version="v2")` 时会发出大量事件。必须只把"用户该看到的"事件挑出来转给 Transport，否则会把 intent 阶段的 LLM token 也流给用户。

| tag | 触发的 ChatEvent | 谁挂 |
|---|---|---|
| `user_output` | `delta` | 最终输出节点（adapter / reply） |
| `user_thinking` | `thinking` | 任何想发"我开始干活了"信号的节点 |
| `user_progress` | `progress` | 业务关键节点（auth/intent/downstream/final） |
| `user_tool` | `tool` | 产出引用、卡片等附加信息的节点 |

ChatService 的事件转发循环：

```python
async def _stream(self, graph, state):
    async for event in graph.astream_events(state, version="v2"):
        tags = set(event.get("tags", []))

        # delta —— 仅最终输出节点的 LLM 流
        if event["event"] == "on_chat_model_stream" and "user_output" in tags:
            yield ChatEvent(type="delta", data={"content": event["data"]["chunk"].content})
            continue

        # thinking / progress / tool —— 节点通过 dispatch_custom_event 主动发
        if event["event"] == "on_custom_event":
            name = event["name"]
            if name == "thinking" and "user_thinking" in tags:
                yield ChatEvent(type="thinking", data=event["data"])
            elif name == "progress" and "user_progress" in tags:
                yield ChatEvent(type="progress", data=event["data"])
            elif name == "tool" and "user_tool" in tags:
                yield ChatEvent(type="tool", data=event["data"])
            continue
```

节点端用 `adispatch_custom_event` 发事件，注册时挂对应 tag。

### 2.6 多步编排的预留（v1 不实现，但骨架已就位）

将来 "先 MetaGC 再 Uniioc 再查库" 这种场景，YAML 只需要声明多个 `output: false` 的中间节点 + 一个 `output: true` 的最终节点。这保证：**只新增专题 YAML 即可解锁多步能力，不动代码**。

### 2.7 失败处理细化

- 节点抛异常：默认捕获 → 写 `state.errors` → 跳到 `reject` 节点 → 出固定话术
- LangGraph 自身的 retry：每个外部调用节点支持配 `retry: { max: 2, backoff_ms: 200 }`
- 客户端断开：FastAPI 的 `Request.is_disconnected()` 检测 → 取消 LangGraph 任务 → `persist` 节点把已积累的 `answer_chunks` 打 `partial`

---

## 3. 数据模型与 OpenGauss 设计

### 3.1 设计原则

MA v1 的持久化只服务两件事：
1. 跨设备 / 跨会话查看历史对话
2. 重启请求时把上下文重新装回 GraphState

**不做**：不存 GraphState 全量快照、不存原始下游响应体、不做会话归档/冷热分级。

核心实体三个表：`ma_session`、`ma_message`、`ma_message_event`。其它一切（鉴权日志、错误日志、LLM 调用记录）都不进业务库，全部走可观测性栈。

### 3.2 表结构

#### 3.2.1 `ma_session`

**关键语义**：`thread_id` 由前端生成且全局唯一，**直接作为主键**。同一 `thread_id` 的 `biz_id` 必须保持一致；用户新开会话 = 换 `thread_id`。

```sql
CREATE TABLE ma_session (
    thread_id       VARCHAR(128) PRIMARY KEY,
    biz_id          VARCHAR(64)  NOT NULL,
    w3_account      VARCHAR(128) NOT NULL,
    title           VARCHAR(256),
    status          VARCHAR(16)  NOT NULL DEFAULT 'active',
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_message_at TIMESTAMP,
    ext             JSONB        NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_ma_session_w3_biz_time ON ma_session(w3_account, biz_id, last_message_at DESC);
```

应用层在 `SessionRepository.get_or_create` 中校验"同一 `thread_id` 的 `biz_id` 不能变"，前端传错 → 报 400。

#### 3.2.2 `ma_message`

```sql
CREATE TABLE ma_message (
    message_id      VARCHAR(64)  PRIMARY KEY,
    thread_id       VARCHAR(128) NOT NULL,
    seq             BIGINT       NOT NULL,
    role            VARCHAR(16)  NOT NULL,
    content         TEXT         NOT NULL,
    content_meta    JSONB        NOT NULL DEFAULT '{}',
    status          VARCHAR(16)  NOT NULL DEFAULT 'complete',
    route           VARCHAR(32),
    request_id      VARCHAR(64),
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ext             JSONB        NOT NULL DEFAULT '{}',
    UNIQUE (thread_id, seq)
);

CREATE INDEX idx_ma_message_thread_seq ON ma_message(thread_id, seq);
CREATE INDEX idx_ma_message_request_id ON ma_message(request_id);
```

- `seq` 单调递增，应用层取上一条 + 1，唯一约束兜底
- `status`：`complete` / `partial`（客户端断开但部分 chunk 已写入） / `failed`（全程报错但要留痕）
- `route` 字段便于运维直接看路由结果

#### 3.2.3 `ma_message_event`（v1 不建表，留设计）

如果将来需要"Replay 流式对话"，开启此表，每个 ChatEvent 一行。v1 默认不创建。

### 3.3 SQL 模板化

需求 2.3 明确要求 SQL 模板化。**不引入 ORM**（ORM 抽象与 SQL 模板化是两条不同思路）。

#### 3.3.1 模板存放

每个文件单条 SQL，参数用具名占位符 `:name`：

```
sql/
├── session/
│   ├── create.sql
│   ├── get_by_thread_biz.sql
│   ├── list_by_w3.sql
│   └── touch_updated.sql
└── message/
    ├── append.sql
    ├── list_recent.sql
    ├── mark_partial.sql
    └── ...
```

#### 3.3.2 模板加载与执行

```python
class SqlLoader:
    """启动时一次性把所有 .sql 文件读进内存。路径作 key。"""
    def get(self, name: str) -> str: ...

class SqlExecutor:
    def __init__(self, pool_rw: AsyncPool, pool_ro: AsyncPool, loader: SqlLoader): ...

    async def fetch_all(self, name: str, **params) -> list[Record]:
        sql = self._loader.get(name)
        async with self._ro.acquire() as conn:
            return await conn.fetch(sql, **params)

    async def fetch_one(self, name: str, **params) -> Record | None: ...
    async def execute(self, name: str, **params) -> int: ...
    async def execute_many(self, name: str, rows: list[dict]) -> int: ...
    async def tx(self) -> AsyncContextManager["Tx"]: ...
```

**关键约束（写在代码 docstring，code review 强制）：**

1. SQL 字符串永远不允许包含 f-string / `.format()` / `%`。动态值必走 `:name` 占位 → 驱动绑定 → 天然防注入
2. 表名 / 列名不允许动态拼接；动态字段统一在 Python 端用枚举映射到预写好的 SQL
3. `fetch_*` 走只读池，`execute*` 走读写池。v1 单实例时两个池指向同一个 DSN；将来加只读副本只换配置

#### 3.3.3 驱动选型

OpenGauss 对外协议兼容 PostgreSQL，**异步驱动用 `asyncpg`**。连接池用 `asyncpg.create_pool`。

OpenGauss 兼容性已知小坑（实现时验证）：
- JSONB 默认支持，但运算符部分版本要 enable 扩展
- 一些 PG 系统视图（pg_stat_statements）OpenGauss 不一定有
- 时区：统一存 UTC，由应用层格式化

#### 3.3.4 事务边界

只有"接收用户提问 + 创建/更新 session + 插入 user message"必须事务化。Assistant 消息的落盘不放在事务里：失败重试 3 次即可。

### 3.4 仓储层接口

```python
class SessionRepository:
    async def get_or_create(self, *, thread_id, biz_id, w3_account, ext=None) -> Session:
        """若 thread_id 已存在且 biz_id 不一致 → 抛 SessionBizMismatchError（400）。"""
    async def list_by_user(self, *, w3_account, biz_id, limit, before) -> list[Session]: ...
    async def touch(self, thread_id) -> None: ...

class MessageRepository:
    async def list_recent(self, *, thread_id, limit) -> list[Message]: ...
    async def append(self, *, msg, in_tx=None) -> None: ...
    async def mark_partial(self, message_id) -> None: ...
    async def mark_failed(self, message_id, err) -> None: ...
```

**纪律**：ChatService 只通过这些方法访问 DB，绝不直接拿 `SqlExecutor`。

### 3.5 历史拼接的具体规则

| 维度 | 规则 |
|---|---|
| 范围 | 同一 `thread_id`（业务上等价于同 thread × 同 biz） |
| 数量 | YAML 中 `history.max_turns` 控制，默认 10 轮 |
| 过滤 | `status='complete'`，跳过 `partial` / `failed` |
| 字段 | 只取 `role` / `content`，不取 `content_meta`（避免把卡片塞进 LLM 上下文） |
| 顺序 | DB 取 `ORDER BY seq DESC LIMIT N`，应用层 reverse 拼成正序 |
| 截断 | 拼接后若超过 LLM context 限制，**最早的整轮**先丢，绝不切句 |

### 3.6 迁移与版本化

用 **Alembic** 管理迁移。

```
db/
├── alembic.ini
├── env.py
└── versions/
    ├── 0001_init_session_message.py
    └── ...
```

约定：
- 每次表结构变化新增一个 version，**不修改历史 version**
- 启动 MA 时**不**自动 upgrade（生产环境人为执行）
- 破坏性 DDL 必须 PR 评审 + 上线 checklist

### 3.7 容量规划（v1 不优化，但要心里有数）

假设：1 万活跃用户 × 每天 30 条消息 = 30 万 / 天 → 一年约 1 亿条 ma_message，平均 1KB → 100GB/年，OpenGauss 单表完全可承受。v1 **不分表 不分区**。

### 3.8 刻意没做的事

| 没做 | 理由 |
|---|---|
| 不存 GraphState 快照 | LangGraph 内部 state 是图实现的一部分，存了反而绑死实现 |
| 不存 LLM token 数 / 延迟 | 进 OTel，与业务表解耦 |
| 不引入 Redis 做历史缓存 | OpenGauss 索引足够；先测量再优化 |
| 不做 soft delete | 企业内对话历史一般不允许用户自删 |
| 不做"消息编辑/重新生成" | 需求未提；如要加则是另一个 message |

---

## 4. Transport 层与协议契约

### 4.1 Endpoint 总览

```
POST   /api/v1/chat/sse              ← PC 端流式对话
WS     /api/v1/chat/ws               ← 移动端流式对话
GET    /api/v1/sessions              ← 历史会话列表
GET    /api/v1/sessions/{thread_id}  ← 单会话元信息
GET    /api/v1/sessions/{thread_id}/messages  ← 历史消息
GET    /api/v1/healthz               ← 健康检查
GET    /api/v1/readyz                ← 就绪检查
```

所有 endpoint 共用 `AuthnMiddleware`（占位）。所有响应附 `X-Request-Id` header（= traceId）。

### 4.2 SSE 通道（PC 端）

#### 4.2.1 请求

```http
POST /api/v1/chat/sse
Content-Type: application/json
Accept: text/event-stream
X-Request-Id: <可选>

{
  "biz_id": "daibiao_xiaoguanjia",
  "thread_id": "th_01HXY...",
  "w3_account": "zhangsan",
  "question": "上个月华东大区的销售额是多少？",
  "biz_params": { "region": "huadong" }
}
```

#### 4.2.2 响应：SSE 事件协议

服务端按 SSE 规范（`text/event-stream`）逐事件返回：

```
event: <event_name>
id: <message_id or chunk_seq>
data: <json string>

```

**7 种事件类型：**

| event | 何时发 | 发送方 | 频次 | data 示例 |
|---|---|---|---|---|
| `meta` | 流开始 | ChatService | 恰好 1 次 | `{"thread_id":"th_…","message_id":"msg_…","route":"metagc"}` |
| `thinking` | 节点开始处理但尚无可展示内容时 | 节点（带 tag `user_thinking`） | 0~N 次 | `{"phase":"auth","label":"正在校验权限…"}` |
| `progress` | 关键业务里程碑发生时 | 节点（带 tag `user_progress`） | 0~N 次 | `{"step":"intent","status":"done","extra":{"route":"metagc"}}` |
| `delta` | 每个 token 块 | 最终节点（带 tag `user_output`） | N 次 | `{"content":"上个月"}` |
| `tool` | 附加可展示信息 | 节点（带 tag `user_tool`） | 0~N 次 | `{"kind":"citation","items":[…]}` |
| `error` | 流中出错 | ChatService | 0~N 次 | `{"code":"DOWNSTREAM_TIMEOUT","message":"…","retryable":false}` |
| `done` | 流结束 | ChatService | 恰好 1 次 | `{"message_id":"msg_…","status":"complete","tokens":123}` |

**`thinking` 事件 schema：**

```json
{
  "phase": "auth | enrich | intent | downstream | <自定义>",
  "label": "正在校验权限…"
}
```

- 不承诺频次；一个 phase 最多 1 条；label 可缺省
- 重复刷状态请用 `progress`

**`progress` 事件 schema：**

```json
{
  "step": "auth | enrich | intent | downstream.metagc | <自定义>",
  "status": "start | done | skipped",
  "detail": "已识别为业务数据查询",
  "extra": { "route": "metagc" }
}
```

- `progress` 与 `delta` **并行**，不互斥
- `progress` 不用来传输答案内容
- `step` 字符串语义稳定，前端可做 i18n 与 UI 映射

**事件次序约束：**

```
meta (1)
   ↓
[ thinking | progress | tool ] *  ─┐
   ↓                                │  并行交错
delta *                            ─┘
   ↓
[ error ] *
   ↓
done (1)
```

- `meta` 必首发，`done` 必尾发
- 中间任意 5 种事件可任意交错
- `error` 不一定终止流（如 INTENT_FAILED 之后还会走 default_route 出 delta）

#### 4.2.3 心跳

- 服务端每 15 秒发一次注释帧 `: keep-alive\n\n`（SSE 规范中合法注释行）
- 不计为业务事件

#### 4.2.4 取消语义

- 浏览器关闭页面 → FastAPI 检测到 `request.is_disconnected()`
- ChatService 取消 LangGraph 任务 → `persist` 节点把已累积内容标 `status=partial`

### 4.3 WebSocket 通道（移动端）

#### 4.3.1 握手与会话生命周期

```
ws://host/api/v1/chat/ws?w3_account=zhangsan&biz_id=daibiao_xiaoguanjia
```

- 一个 WS 连接 = 一个用户在一个专题下的"在线通道"，可承载多次问答
- 握手阶段执行 `AuthnMiddleware`，失败 → close code 4401
- 连接建立后服务端立即发 `ready` 帧

#### 4.3.2 消息协议（双向 JSON）

客户端 → 服务端：

```json
{
  "op": "ask",
  "request_id": "req_01HXY...",
  "thread_id": "th_01HXY...",
  "question": "继续刚才的话题",
  "biz_params": {"region":"huadong"}
}
```

| op | 含义 |
|---|---|
| `ask` | 发起一次问答 |
| `cancel` | 中断当前 request_id 对应的流 |
| `ping` | 客户端心跳 |

服务端 → 客户端：

```json
{
  "op": "event",
  "request_id": "req_01HXY...",
  "event": "delta",
  "data": {"content":"上个月"}
}
```

| op | 含义 |
|---|---|
| `ready` | 握手成功，附带服务器版本、心跳间隔 |
| `event` | 业务事件（type 同 SSE 的 7 种）|
| `pong` | 心跳应答 |
| `closed` | 服务端主动关闭通知 |

**纪律：**
- 每个 `ask` 必带 `request_id`；同一连接可并发多个 request；服务端用 request_id 区分流
- 7 种事件 type 与 SSE 完全对齐

#### 4.3.3 心跳

- 客户端每 20 秒发 `ping`，服务端立即回 `pong`
- 服务端 45 秒未收到任何帧 → close（code 4408）
- 不依赖 WS 协议层 ping/pong（代理实现差异大）

#### 4.3.4 取消语义

- 客户端发 `cancel` → 服务端取消对应 LangGraph 任务，已积累内容标 `partial`
- 客户端断开 → 所有在跑的 request 取消

### 4.4 REST endpoint（历史查看）

#### 4.4.1 会话列表

```http
GET /api/v1/sessions?biz_id=daibiao_xiaoguanjia&limit=20&before=2026-06-23T10:00:00Z
```

`w3_account` 从 AuthnMiddleware 注入的 identity 取，**不**从 query 取（避免越权）。

#### 4.4.2 单会话消息

```http
GET /api/v1/sessions/th_01HXY.../messages?limit=50&before_seq=1234
```

越权防御：必校验 `thread_id.w3_account == identity.w3_account`。

### 4.5 错误码与异常下发

| code | HTTP 等价 | 流内 event | 含义 |
|---|---|---|---|
| `BAD_REQUEST` | 400 | error | 入参错误 |
| `UNAUTHENTICATED` | 401 | （流尚未开始即终止） | 登录态校验失败 |
| `FORBIDDEN_TOPIC` | 200 + error frame | error | 业务鉴权：专题不允许 |
| `FORBIDDEN_SCOPE` | 200 + error frame | error | 业务鉴权：问题范围不允许 |
| `TOPIC_NOT_FOUND` | 404 | - | biz_id 在 TopicRegistry 找不到 |
| `INTENT_FAILED` | 200 | error + 继续走 default_route | 意图识别失败 |
| `DOWNSTREAM_TIMEOUT` | 200 + error | error | 下游 Adapter 超时 |
| `DOWNSTREAM_ERROR` | 200 + error | error | 下游 Adapter 非超时类错误 |
| `INTERNAL` | 500 | error | 兜底 |

**两条核心纪律：**

1. **流一旦建立，所有业务错误都通过 `error` 事件下发**，HTTP 状态码保持 200
2. **流尚未开始时的错误**走标准 HTTP 状态码 + JSON body

**`error` 事件 schema：**

```json
{
  "code": "DOWNSTREAM_TIMEOUT",
  "message": "MetaGC 接口响应超时，请稍后重试",
  "retryable": false,
  "request_id": "req_01HXY..."
}
```

- `message` 是面向用户的友好话术，每个 code 在配置里维护默认文案，专题 YAML 可覆盖
- 原始堆栈和下游响应**绝不**进 message，进 trace 系统

### 4.6 请求体 schema 校验

#### 4.6.1 公共参数

```python
class ChatRequest(BaseModel):
    biz_id: str
    thread_id: str
    w3_account: str
    question: str
    biz_params: dict[str, Any] = {}
```

#### 4.6.2 专题特有参数

`biz_params` 的 schema 由每个 Topic YAML 声明，启动时编译为 Pydantic Model：

```yaml
biz_params_schema:
  type: object
  required: [region]
  properties:
    region:
      type: string
      enum: [huadong, huabei, huanan, ...]
```

任一校验失败 → `BAD_REQUEST`，**流尚未开始**。

### 4.7 工程细节

#### 4.7.1 SSE 实现要点

- 用 `fastapi.responses.StreamingResponse(media_type="text/event-stream")`
- 关闭 buffering：响应 header 加 `Cache-Control: no-cache`、`X-Accel-Buffering: no`
- 单 chunk 即 flush

#### 4.7.2 WebSocket 实现要点

- 用 FastAPI 原生 WebSocket route
- **不**用 socketio / python-socketio（自有协议反而和移动端原生 WS 不通）
- 单连接内消息处理用 `asyncio.create_task` 起协程

#### 4.7.3 连接亲和

- SSE：HTTP 请求 → 任一 worker → 无亲和
- WebSocket：连接绑定一个 worker 直到断开。LB 侧需要 sticky session 或长连分散
- **不**引入 Redis pub/sub（v1 用不到）

#### 4.7.4 限流

v1 **不**在 MA 内做限流，由上游 Gateway 负责。MA 仅保留：单连接内同时在跑的 LangGraph 任务上限 5，超过则 `BAD_REQUEST`。

#### 4.7.5 敏感数据纪律

`thinking` / `progress` 不允许携带敏感数据。由于这两类事件直接打到用户屏幕，且服务端不会再做脱敏，节点开发者必须自检；code review 强制。

### 4.8 协议演进策略

`/api/v1/` 前缀已留好。任何破坏性协议变更 → 出 `/api/v2/`，老版本保留至少一个发版周期。事件类型新增属于向后兼容的演进 —— 前端见到不认识的 event 应当忽略。

---

## 5. 插件体系与扩展点

### 5.1 插件的边界

插件化的标准是**"会因为业务增长而新增同类实现"**。

| 项 | 是否插件 | 理由 |
|---|---|---|
| `AuthPlugin`（业务鉴权） | ✅ | 每个专题鉴权逻辑不同，会持续新增 |
| `EnrichPlugin`（问题补全） | ✅ | 每个专题补全字段不同 |
| `IntentPlugin`（意图识别） | ✅ | 将来可能有规则版、外部 API 版 |
| `DownstreamAdapter` | ✅ | 下游 API 会持续增加 |
| `AuthnMiddleware`（登录态） | ✅ | 将来 JWT / Gateway / Session 三选一 |
| `Reject` / `Persist` / `LoadSession` 节点 | ❌ | 通用、不会变出第二种实现 |
| Topic 配置 | ❌（是配置不是插件） | YAML 装配 |
| SQL 模板 | ❌ | 不是同类多实现的关系 |

### 5.2 统一插件契约

```python
# core/plugin/base.py
@runtime_checkable
class Plugin(Protocol):
    name: str
    plugin_kind: str
    def configure(self, params: dict) -> None: ...
```

#### 5.2.1 AuthPlugin

```python
class AuthPlugin(Plugin):
    plugin_kind = "auth"
    async def authorize(self, state: GraphState) -> AuthResult: ...

@dataclass
class AuthResult:
    passed: bool
    reject_code: str | None = None
    reject_message: str | None = None
    user_ctx: dict | None = None
```

#### 5.2.2 EnrichPlugin

```python
class EnrichPlugin(Plugin):
    plugin_kind = "enrich"
    async def enrich(self, state: GraphState) -> EnrichResult: ...

@dataclass
class EnrichResult:
    enriched_question: str
    enrich_meta: dict
```

#### 5.2.3 IntentPlugin

```python
class IntentPlugin(Plugin):
    plugin_kind = "intent"
    async def classify(self, state: GraphState) -> IntentResult: ...

@dataclass
class IntentResult:
    route: str
    confidence: float
    reason: str
```

如果 `route` 不在该 Topic 声明的 routes 集合内 → 视为失败，触发 `INTENT_FAILED`，由 ChatService 走 `default_route`。

#### 5.2.4 DownstreamAdapter

```python
class DownstreamAdapter(Plugin):
    plugin_kind = "adapter"
    async def run(self, state: GraphState, *, output: bool = True) -> AsyncIterator[ChatEvent]:
        """yield ChatEvent 序列。
        - output=True: 节点会被挂 user_output tag，delta 流给用户
        - output=False: 仅作内部数据来源
        """
```

**纪律：**
- Adapter 内部不感知 SSE / WS
- Adapter 不直接落库，不发 progress（节点封装层负责）
- Adapter 的超时 / 重试参数来自 YAML，由节点封装层施加

### 5.3 节点封装层

插件本身和 LangGraph 解耦。**节点封装层**是插件与 LangGraph 之间的薄壳：

```python
def auth_node(plugin: AuthPlugin):
    async def node(state, config) -> dict:
        await adispatch_custom_event("thinking",
            {"phase": "auth", "label": "正在校验权限…"}, config=config)
        result = await plugin.authorize(state)
        await adispatch_custom_event("progress",
            {"step": "auth", "status": "done",
             "extra": {"passed": result.passed}}, config=config)
        if not result.passed:
            return {"auth": result, "route": "reject"}
        return {"auth": result, "user_ctx": result.user_ctx}
    return node
```

封装层职责：发 thinking/progress 事件；挂 tag；桥 ChatEvent；应用 retry/timeout。

### 5.4 插件注册与发现

```python
class PluginRegistry:
    def __init__(self):
        self._by_kind_name: dict[tuple[str, str], type[Plugin]] = {}
    def register(self, cls): ...
    def create(self, kind, name, params) -> Plugin: ...

registry = PluginRegistry()
```

装饰器注册：

```python
@registry.register
class RepresentativeAuth(AuthPlugin):
    name = "representative_auth"
    def configure(self, params): ...
    async def authorize(self, state): ...
```

启动时**显式 import**触发装饰器（不做自动扫描，让"插件被加载"的因果链可追踪）。

### 5.5 Topic 编译

`TopicCompiler` 把 YAML 编译成 `CompiledStateGraph`，缓存到 `TopicRegistry`：

```python
class TopicCompiler:
    def compile(self, topic_yaml) -> CompiledTopic:
        auth = registry.create("auth", topic_yaml.auth.plugin, topic_yaml.auth.params)
        # ...
        sg = StateGraph(GraphState)
        sg.add_node("load_session", load_session_node)
        sg.add_node("auth", auth_node(auth))
        # ...
        return CompiledTopic(...)
```

### 5.6 新增专题的分步骤手册

#### 场景 A：用现有插件就能搭起来

只需新增一个 YAML 文件（不写一行 Python）。部署：YAML 进 PR → 合并 → 服务重启 → 新专题立即可用。

#### 场景 B：需要新写一个鉴权策略

1. 在 `plugins/auth/` 新建 `xx_auth.py`，实现 `AuthPlugin`
2. 在 `core/plugin/bootstrap.py` 加 import
3. 写单元测试
4. 在 Topic YAML 的 `auth.plugin` 引用新插件名

典型 ≤ 50 行。

#### 场景 C：新增下游 API

1. 在 `plugins/adapter/` 新建 `xx_adapter.py`
2. 处理对应协议 → yield `ChatEvent`
3. bootstrap 加 import
4. 在 `IntentPlugin` 的 prompt 中告诉它新路由
5. 相关 Topic YAML 的 `intent.labels` 与 `graph` 中声明
6. 单测 + 集成测试

典型 100–300 行。

#### 场景 D：多步编排

完全靠 YAML，参见 2.6。零 Python 改动。

### 5.7 插件配置参数的校验

```python
class RepresentativeAuthParams(BaseModel):
    required_role: str
    user_ctx_api_timeout_ms: int = 800

@registry.register
class RepresentativeAuth(AuthPlugin):
    def configure(self, params):
        p = RepresentativeAuthParams(**params)
        self._required_role = p.required_role
```

任一专题 YAML 引用的插件 `configure` 失败 → **整个服务不启动**。

### 5.8 插件版本化

v1 阶段不引入版本字段。将来需要灰度，两条路：

1. **新插件名法（推荐）**：新名字 `representative_auth_v2`，灰度专题 YAML 引用新名
2. **params 开关法**：插件内通过 params 区分行为

不做：协议版本号、热替换、A/B 框架。

### 5.9 目录结构

```
src/ma/
├── api/                       # Transport 层
│   ├── sse.py
│   ├── ws.py
│   └── rest.py
├── core/
│   ├── chat_service.py
│   ├── graph/
│   │   ├── state.py
│   │   ├── node_wrappers.py
│   │   ├── builtins.py
│   │   └── events.py
│   ├── plugin/
│   │   ├── base.py
│   │   ├── registry.py
│   │   └── bootstrap.py
│   ├── topic/
│   │   ├── config.py
│   │   ├── compiler.py
│   │   └── registry.py
│   └── repo/
│       ├── session_repo.py
│       ├── message_repo.py
│       └── sql_executor.py
├── plugins/
│   ├── auth/
│   ├── enrich/
│   ├── intent/
│   └── adapter/
│       ├── metagc.py
│       ├── uniioc.py
│       └── kc.py
├── infra/
│   ├── db.py
│   ├── http_client.py
│   ├── ws_client.py
│   ├── logging.py
│   ├── tracing.py
│   └── settings.py
└── main.py
```

`config/`、`sql/`、`prompts/`、`db/` 放仓库根而非 src 下。

### 5.10 刻意没做的事

| 没做 | 理由 |
|---|---|
| 不做 entry point 自动发现 | 显式 import 更可控；MA 不是给第三方装插件用的 |
| 不做插件热加载 | YAML 已覆盖大部分变更，剩下接受重启代价 |
| 不做插件间依赖图 | 通过 GraphState 传值，不互相调用 |
| 不引入 DI 容器框架 | 手写简单 ServiceRegistry 足够 |
| 不让插件直接访问 OpenGauss | 走 Repository |

---

## 6. 可观测性、错误处理与运行时纪律

### 6.1 核心理念

MA 是"流式 + 异步 + 多下游 + LLM 编排"叠加系统。出问题不能复现的概率比传统 CRUD 高一个量级，所以可观测性**和业务代码一起写**。

| 层级 | 回答的问题 | 工具 |
|---|---|---|
| **结构化日志** | "刚才那个用户发生了什么？" | structlog + JSON + traceId |
| **分布式追踪** | "这次请求走了哪些节点、各自耗时多少？" | OpenTelemetry |
| **LLM Trace** | "这次 LLM 调用的 prompt 是什么？输出是什么？多少 token？" | OpenInference |

v1 **不**做 Metrics，但所有数据点写日志时都标准化字段，将来想接 Prometheus 只是一层 exporter。

### 6.2 traceId / requestId / threadId 的贯穿

| ID | 谁生成 | 生命周期 | 何处使用 |
|---|---|---|---|
| `request_id` | 前端可选传，否则 Transport 用 ULID 生成 | 一次问答 | 日志、trace、`X-Request-Id` header、消息表 `request_id` 字段 |
| `trace_id` | OTel SDK 生成（16 字节 hex） | 同一次问答 | OTel span tree |
| `thread_id` | 前端生成 | 整个会话 | 业务实体 ID |

structlog 用 `contextvars` 自动注入三个 ID。LangGraph 的 RunnableConfig 里塞 `request_id`，节点函数通过 `config["configurable"]["request_id"]` 拿到。

### 6.3 结构化日志规范

#### 6.3.1 字段约定

每条日志必须有：`timestamp` / `level` / `event` / `request_id` / `thread_id` / `w3_account`，业务事件再附 `biz_id` / `adapter` / `node` / `route` / `duration_ms` / `status` / `error_code`。

**event 命名约定：**

| 粒度 | 示例 |
|---|---|
| 业务里程碑 | `chat_started`, `auth_passed`, `intent_classified`, `chat_completed` |
| 节点边界 | `node_started`, `node_finished`, `node_failed` |
| 下游调用 | `downstream_called`, `downstream_succeeded`, `downstream_failed` |
| LLM 调用 | `llm_called`, `llm_streamed`, `llm_failed` |

不允许出现裸字符串日志，code review 强制。

#### 6.3.2 等级使用

| level | 用法 |
|---|---|
| `error` | 真异常 |
| `warning` | 业务降级（intent 失败走 default、retry 命中）|
| `info` | 业务里程碑、节点边界 |
| `debug` | 详细变量、prompt 内容（默认关闭） |

生产默认 `info`，按 request_id 临时开 debug。

#### 6.3.3 敏感数据脱敏

| 字段 | 处理 |
|---|---|
| `question` | info 级记 hash + 前 32 字符，全文进 debug |
| 用户上下文（客户名单、薪资）| **不允许进日志**，进 trace 也要标 `pii=true` |
| LLM prompt 全文 | 进 OpenInference，日志只记 hash |
| 鉴权 token / Cookie | 一律不写入，logger formatter 兜底过滤 |

全局兜底脱敏 processor 用一份字段名黑名单。

### 6.4 OpenTelemetry 接入

#### 6.4.1 Span 设计

```
chat.request                     (Transport 层根 span)
├── chat.load_session
├── graph.run                    (LangGraph 整图执行)
│   ├── graph.node.auth
│   │   └── http.client          (调用用户上下文 API)
│   ├── graph.node.enrich
│   ├── graph.node.intent
│   │   └── llm.classify         (← OpenInference 接管)
│   ├── graph.node.metagc_adapter
│   │   └── sse.client           (流式下游 SSE)
│   └── graph.node.persist
│       └── db.query             (写 ma_message)
└── chat.respond
```

每个 span attributes：`request_id`, `thread_id`, `w3_account`, `biz_id`。节点 span name 稳定（如 `graph.node.intent`），便于聚合。

#### 6.4.2 LangGraph 与 OTel 衔接

每个节点函数用装饰器包一层 `_traced_node(name, fn)`，统一开 span、记 attribute、捕获异常。

#### 6.4.3 导出器

- v1 用 OTLP gRPC 导到企业内 collector，或本地 Jaeger
- 采样：v1 全采（量不大）
- 配置：`OTEL_EXPORTER_OTLP_ENDPOINT`、`OTEL_SERVICE_NAME=master-agent`

### 6.5 LLM Trace（OpenInference）

#### 6.5.1 自动接入点

MA 中 LLM 调用走两条路：
1. **IntentPlugin (llm_classifier)** 内部用 LangChain 客户端 → 自动接 OpenInference
2. **MetaGC Adapter** 转发的 SSE 是 MetaGC 在调 LLM → 不进 LLM Trace

只对第 1 类装上 `LangChainInstrumentor().instrument()`。每次 IntentPlugin 调 LLM 产生 `llm.classify` span，attributes 自动包含 model_name / input_messages / output_messages / token_count.* / latency_ms。

#### 6.5.2 数据保留策略

- LLM Trace 全采 + 本地 retention 7 天
- 不进 ELK（结构化日志只记 hash）
- 团队若有 Arize Phoenix / Langfuse，直接连

### 6.6 错误处理总章

| 类别 | 典型 | 处理位置 | 用户感知 |
|---|---|---|---|
| 入参错误 | biz_params 不合 schema | Transport 层 | HTTP 400 |
| 登录态失败 | AuthnMiddleware 拒绝 | Transport 层 | HTTP 401 / WS 4401 |
| 业务鉴权失败 | AuthPlugin passed=False | LangGraph reject 节点 | 流内 error + 兜底话术 + done |
| 下游/LLM 失败 | 超时、5xx、解析错 | 节点封装层 | 流内 error event；视配置降级或终止 |
| 程序异常 | 未捕获 | ChatService 兜底 try/except | 流内 `INTERNAL` error + done |

#### 6.6.1 节点级 retry / timeout

YAML 配，封装层施加：

```yaml
intent:
  plugin: llm_classifier
  retry: { max: 2, backoff_ms: 200, on: [timeout, 5xx] }
  timeout_ms: 3000
```

retry 命中时记 warning + OTel event，**不**发 error 给前端（用户不该感知重试）。

#### 6.6.2 降级策略

| 节点 | 失败时 |
|---|---|
| `auth` | 不降级 —— 走 reject 节点出固定话术 |
| `enrich` | 降级为不补全，使用原始 question；记 warning |
| `intent` | 降级为 `default_route`；发 progress(step=intent, status=fallback)；记 warning |
| `adapter` (output 节点) | **不**降级。发 error event + done |
| `persist` | 异步重试 3 次，最终失败进死信日志，不影响响应 |

判据：**此节点失败后能否让用户拿到"还算说得过去"的回答？** 能 → 降级；不能 → 终止 + 错误话术。

#### 6.6.3 错误话术配置

```yaml
error_messages:
  FORBIDDEN_TOPIC:     "您暂无权限使用代表小管家。"
  FORBIDDEN_SCOPE:     "您当前的角色无法查询这部分数据。"
  DOWNSTREAM_TIMEOUT:  "服务繁忙，请稍后再试。"
  DOWNSTREAM_ERROR:    "查询遇到问题，请稍后再试或联系管理员。"
  INTERNAL:            "服务异常，请稍后再试。"
```

每个 Topic 完整一份（启动校验强制）；reject 节点按 error_code 取话术。

#### 6.6.4 客户端断开

- SSE：FastAPI `request.is_disconnected()` 轮询
- WS：`WebSocketDisconnect` 异常
- 检测到断开 → ChatService 取消 LangGraph → `persist` 节点已积累内容标 `status=partial`
- 日志 `event=client_disconnected, has_partial=true/false`

### 6.7 健康检查与就绪检查

```
GET /api/v1/healthz   ← liveness：进程活着即 200
GET /api/v1/readyz    ← readiness：DB 可连 + 所有 Topic 编译成功
```

下游 API（MetaGC / Uniioc / KC）的 readiness **不**纳入 readyz —— 下游短暂抖动不应让 MA 整个摘了，单请求级降级足够。

### 6.8 SLO 边界（v1 估算，不承诺）

| 指标 | 目标 |
|---|---|
| 首 chunk 延迟 (TTFB) | P95 < 2s |
| 整段回答完成 | P95 < 15s |
| MA 自身处理延迟（去掉下游）| P95 < 200ms |
| 可用性 | 99.5% |
| 鉴权失败误判率 | < 0.1% |

数字目前是心智锚点而非 SLO。

### 6.9 运行时纪律清单

| # | 规则 |
|---|---|
| 1 | 不允许在节点 / 插件里 `print`，一律 structlog |
| 2 | 任何外部调用必须有 timeout |
| 3 | 任何 `except Exception` 必须 `logger.exception`，不允许吞 |
| 4 | 不允许 `time.sleep`，用 `await asyncio.sleep` |
| 5 | 不允许在协程里执行同步阻塞 IO |
| 6 | 不允许把用户上下文整体扔进 prompt，按白名单 |
| 7 | 节点内不准直接访问 OpenGauss，走 Repository |
| 8 | SQL 模板里不允许 f-string / `.format()` |
| 9 | 任何下发到前端的 message 字段必须面向用户话术 |
| 10 | 任何新增 LLM 调用必须自动进 OpenInference span |

### 6.10 刻意没做的事

| 没做 | 理由 |
|---|---|
| 不引入 Prometheus / Grafana | v1 范围外 |
| 不接 Sentry | 错误已进日志 + OTel |
| 不做日志采样 | v1 量不大 |
| 不做实时告警 | 由可观测平台侧告警规则负责 |
| 不在节点里自己实现熔断器 | 单 worker 内部熔断意义有限 |

---

## 7. 测试策略

### 7.1 总体原则

```
                ▲ 越靠上越少
              ┌─┴─┐
              │E2E│         少量，关键用户旅程
            ┌─┴───┴─┐
            │ Integ │       中等，组件间契约
          ┌─┴───────┴─┐
          │   Unit    │     大量，纯逻辑
        ┌─┴───────────┴─┐
        │ Stream Contract │  专门一类，事件流语义
        └─────────────────┘
```

### 7.2 测试分层

#### 7.2.1 单元测试

| 模块 | 单测要点 |
|---|---|
| `core/graph/state.py` | GraphState 字段合并语义 |
| `core/graph/builtins.py` | 三个内置节点的纯逻辑 |
| `core/graph/node_wrappers.py` | 事件 dispatch、retry 计数、timeout |
| `core/topic/compiler.py` | YAML → CompiledTopic、非法 YAML fail fast |
| `core/topic/config.py` | Pydantic schema 校验边界 |
| `core/plugin/registry.py` | 注册冲突、未知 kind/name、configure 失败 |
| `core/repo/sql_executor.py` | SQL 加载、占位符替换 |
| 各插件 | 业务规则核心逻辑，外部 client 用 fake |
| `infra/logging.py` | contextvars 注入、脱敏 |

工具：pytest + pytest-asyncio + pytest-cov。每个测试只测一件事，名字读出"被测行为"。不允许测试间相互依赖。

#### 7.2.2 流式契约测试

**MA 独有硬需求。** 必须覆盖的契约清单：

| # | 契约 | 反向断言 |
|---|---|---|
| 1 | `meta` 恰好 1 次且在首 | 不在中间、不在尾 |
| 2 | `done` 恰好 1 次且在末 | 不在 error 之前 |
| 3 | 仅 `user_output` tag 节点产生 `delta` | intent / enrich / auth 的 LLM token 不进 delta |
| 4 | `progress.step` 命名稳定 | 不随实现变动 |
| 5 | `thinking` / `progress` payload 不含敏感字段 | 不漏 |
| 6 | error event 后流可继续（如 INTENT_FAILED）| 不被立即终止 |
| 7 | 客户端断开 → message 标 partial | 不丢消息 |
| 8 | 节点 retry 命中 → 用户不见 error | retry 对用户透明 |
| 9 | 鉴权失败 → reject 话术 + done | 不进 enrich/intent/adapter |
| 10 | 多 request 并发于同一 WS 连接 → 事件不串流 | request_id 严格隔离 |

#### 7.2.3 集成测试

| 边界 | 替身策略 |
|---|---|
| MA ↔ OpenGauss | testcontainers 起 OpenGauss 容器；Alembic 真跑迁移 |
| MA ↔ MetaGC (SSE) | 本地 mock server |
| MA ↔ Uniioc (HTTP) | mock server |
| MA ↔ 知识中心 (WS) | 本地 `websockets.serve` |
| MA ↔ LLM (intent) | LangChain `FakeListChatModel` |

每个测试独立准备 + 独立清理。**必须用真 OpenGauss 镜像**（兼容性差异在 3.3.3 已点出）。

#### 7.2.4 端到端测试

| 用例 | 覆盖目的 |
|---|---|
| PC SSE 全流程（代表小管家 → MetaGC）| 主路径 + 流式编码 |
| 移动端 WS 全流程 | WS 协议封装、并发 ask |
| 鉴权失败 | reject 路径、固定话术 |
| intent 失败降级 default_route | 降级语义 |
| 下游超时 → error event + done | 错误流终止符 |
| 追问保留上下文 | 历史拼接生效 |
| WS 单连接两个并发 ask | request_id 隔离 |

不做：跑生产下游做 e2e。

#### 7.2.5 性能测试（v1 仅建基线）

| 场景 | 工具 | 度量 |
|---|---|---|
| 单 SSE 首 chunk 延迟 | k6 | P50 / P95 |
| 100 并发 SSE | k6 | 错误率、P95 |
| 100 WS 并发，各 5 轮 | k6 | 同上 |
| 历史拉取（10 轮 × 100 并发）| k6 | P95 |

每次发版前跑一遍，记录到 `bench/baseline.md`。退化 > 30% 必须解释。不进 CI 阻塞。

### 7.3 测试金字塔比例

| 类型 | 数量级 | 跑一次的耗时 |
|---|---|---|
| Unit | 200~400 | < 10s |
| Stream Contract | 30~50 | < 30s |
| Integration | 30~50 | < 3min |
| E2E | 5~10 | < 5min |

CI：
1. **PR 触发**：unit + stream contract + integration
2. **合并到 main**：上述 + e2e（mock 下游）
3. **每日定时**：上述 + e2e（staging 真下游）+ 性能基线

### 7.4 测试基础设施

#### 7.4.1 关键 fixture

- `opengauss_pool`：容器 + 迁移 + asyncpg pool（session scope，事务级隔离）
- `chat_app`：完整组装的 FastAPI app
- `fake_intent_returning`：快速注入 intent fake
- `stream_collector`：收集 ChatEvent AsyncIterator 工具

#### 7.4.2 Mock 下游服务

```
tests/mock_downstreams/
├── metagc_sse.py
├── uniioc_http.py
└── kc_ws.py
```

真实下游协议变更，PR 必须同步改 mock。

#### 7.4.3 时间冻结

引入 `freezegun` 或自写 `Clock` 抽象，测试里用 `FakeClock` 瞬间快进。

#### 7.4.4 LangGraph 测试辅助

- `build_test_topic(auth=None, ...)`：构造最小 CompiledTopic
- `run_single_node(name, state)`：直接测某个节点
- `assert_graph_has_edges(topic, [...])`：图结构断言

### 7.5 刻意不做的事

| 不做 | 理由 |
|---|---|
| 不引入 BDD 框架 | pytest 已够 |
| 不做 snapshot testing | LLM 输出不稳定 |
| 不在 unit 启 OpenGauss | unit 必须 ms 级 |
| 不 mock LangGraph 本身 | 等于自欺欺人 |
| 不追求 100% 覆盖率 | 90% 已够，强行 100% 会写无意义测试 |
| 不在 CI 跑性能测试 | 太慢 |

### 7.6 覆盖率门槛

| 模块 | 行覆盖率门槛 |
|---|---|
| `core/graph/`、`core/topic/`、`core/plugin/`、`core/repo/` | 90% |
| `plugins/` | 85% |
| `infra/` | 70% |
| `api/` | 80% |
| 总体 | 80% |

PR 不达标拒合并。新增代码必须有测试。

### 7.7 调试支持

| 工具 | 用法 |
|---|---|
| `replay_request_id <id>` | 从日志拉一条 request 的完整事件流重放 |
| `dump_graph_state <thread_id>` | 重建一次问答的 GraphState |
| `gen_yaml_skeleton <topic>` | 新建专题 YAML 骨架 |

### 7.8 测试驱动 vs 测试伴随

v1 **不**强制 TDD。约定：

- **流式契约**必须 TDD
- **插件 / Adapter** 推荐"先写最小骨架 + 单测一起"
- **集成 / e2e** 测试伴随实现

---

## 8. 部署、配置与运维生命周期

### 8.1 交付物形态

唯一交付物：**一个 Docker 镜像**。镜像内：

```
/app/
├── src/ma/
├── config/topics/
├── prompts/
├── sql/
├── db/                        # Alembic
└── pyproject.toml
```

镜像**不**包含：环境敏感配置、下游 URL、Alembic 自动执行入口。

### 8.2 配置体系

| 层 | 形式 | 内容 | 变更频率 | 变更方式 |
|---|---|---|---|---|
| **静态配置** | YAML | Topic、prompt、SQL | 周/月 | PR → 发版 |
| **环境配置** | env | DB 连接串、下游 URL、模型 endpoint | 部署时 | K8s ConfigMap / Secret |
| **运行时开关** | env | DEBUG 标记、采样率 | 随时 | 改 ConfigMap → 重启 |

v1 **不**做"运行时配置热更新"。重启代价可控。

#### 8.2.2 环境配置

```python
class Settings(BaseSettings):
    env: Literal["dev", "test", "staging", "prod"]
    log_level: str = "info"

    pg_dsn_rw: SecretStr
    pg_dsn_ro: SecretStr | None = None
    pg_pool_min: int = 4
    pg_pool_max: int = 32

    metagc_base_url: str
    metagc_timeout_ms: int = 30000
    uniioc_base_url: str
    uniioc_timeout_ms: int = 15000
    kc_ws_url: str

    intent_llm_provider: Literal["openai-compatible", "anthropic", "internal"]
    intent_llm_base_url: str
    intent_llm_api_key: SecretStr
    intent_llm_model: str

    otel_exporter_otlp_endpoint: str | None = None
    otel_service_name: str = "master-agent"

    authn_mode: Literal["trust_header", "jwt", "gateway"] = "trust_header"
    authn_trust_header_name: str = "X-User-Account"

    class Config:
        env_prefix = "MA_"
```

任何缺失项启动即崩溃。

#### 8.2.3 启动顺序

```
1. 加载 Settings → 失败即退出
2. 初始化 logging / tracing → 失败即退出
3. 创建 DB pool → 测试连通性 → 失败即退出
4. load_all_plugins() → 失败即退出
5. 加载 config/topics/*.yaml → 编译每个 Topic → 任一失败即退出
6. 启动 FastAPI app，/healthz=200，/readyz=503（未就绪）
7. 运行 readiness 检查
8. /readyz=200，开始接流量
```

任何在 6 之前的错误，进程不应启动 listener。

### 8.3 镜像构建

多阶段 Dockerfile：

```dockerfile
FROM python:3.12-slim AS builder
WORKDIR /build
COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen --no-dev

FROM python:3.12-slim AS runtime
RUN groupadd -g 1000 ma && useradd -u 1000 -g ma ma
WORKDIR /app
COPY --from=builder /build/.venv /app/.venv
COPY src/ ./src/
COPY config/ ./config/
COPY prompts/ ./prompts/
COPY sql/ ./sql/
COPY db/ ./db/
COPY pyproject.toml ./
ENV PATH=/app/.venv/bin:$PATH
USER ma
EXPOSE 8000
CMD ["uvicorn", "ma.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

锁版本（`uv.lock` 进仓库）；非 root 运行；镜像标签 `master-agent:<git-sha>` 是唯一权威；**不**在镜像内运行 Alembic。

### 8.4 Kubernetes 部署形态

#### 8.4.1 资源清单

```yaml
resources:
  requests: { cpu: 500m,  memory: 512Mi }
  limits:   { cpu: 2000m, memory: 2Gi }

spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
```

**每 Pod 跑 1 个 uvicorn 进程**，横向靠副本扩。理由：避免两层调度、单 Pod 多 worker 时 OTel / asyncpg 易出怪问题、单 Pod 挂只损失 1/N 容量。

#### 8.4.2 探针

```yaml
livenessProbe:
  httpGet:    { path: /api/v1/healthz, port: 8000 }
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:    { path: /api/v1/readyz,  port: 8000 }
  periodSeconds: 5
  failureThreshold: 2
  successThreshold: 2
```

#### 8.4.3 优雅退出

```
1. K8s 发 SIGTERM
2. FastAPI lifespan 收到
3. /readyz 立即 503 → K8s 摘流量
4. 拒绝新连接
5. 等待存量流式请求自然结束（最长 45s）
6. 超时未结束的：广播 ChatEvent(error=GRACEFUL_SHUTDOWN) + done
7. 关 DB pool
8. 进程退出
```

K8s 侧 `terminationGracePeriodSeconds: 60`。

#### 8.4.4 Service 与会话粘性

| 通道 | 粘性 |
|---|---|
| SSE | 不需要 |
| WebSocket | **需要 sticky**（连接级、物理绑定 worker） |

用 ingress-nginx 的 `affinity: cookie` 或 LB 层 5-tuple hashing。**不**引入 Redis pub/sub。

#### 8.4.5 Pod 反亲和

让 3 个副本尽量分散到不同节点，单机故障影响最小（`podAntiAffinity` + `topologyKey: kubernetes.io/hostname`）。

### 8.5 数据库生命周期

#### 8.5.1 迁移执行

**独立 K8s Job 跑 Alembic**：

```
1. 构建镜像 master-agent:<sha>
2. 跑 Job: ma-migrate-<sha>
   - command: alembic upgrade head
   - 失败 → 部署中止
3. 滚动升级 Deployment 到新镜像
```

#### 8.5.2 破坏性迁移的纪律

"破坏性"= drop column / rename column / change type / drop table。

| 步骤 | 内容 |
|---|---|
| 1 | 新版本只新增字段 |
| 2 | 应用代码逐步切换到新字段（双写）|
| 3 | 至少一个发版周期后确认旧字段无引用 |
| 4 | 单独一次迁移只做 drop |

v1 **不允许同一次发版中同时改 schema 和切代码读写路径**。

#### 8.5.3 连接池起点

```
pool_min = 4
pool_max = 32  (per Pod)
```

3 Pod × 32 = 96 并发连接。OpenGauss 默认 max_connections=200，留余量。

### 8.6 环境分级

| 环境 | 数据 | 下游 |
|---|---|---|
| dev | 本地 compose | mock_downstreams |
| test | testcontainer | mock_downstreams |
| staging | staging 库（独立 schema）| staging 下游 |
| prod | prod 库 | prod 下游 |

prod 与 staging 用**完全不同的** DB 实例。

### 8.7 滚动升级与回滚

#### 8.7.1 升级

```
1. 镜像 push
2. 跑迁移 Job（如有 schema 变更）
3. kubectl set image deployment/master-agent ma=<new-sha>
4. RollingUpdate：maxSurge=1, maxUnavailable=0
5. 监控 error 率 / P95 / readiness 失败
6. 全部副本就绪 + 5 分钟稳定 → 完成
```

#### 8.7.2 回滚

```
kubectl rollout undo deployment/master-agent
```

前提：本次没做破坏性 schema 变更。这是 8.5.2 严格分多次发版的理由。

#### 8.7.3 配置变更

修 Topic YAML / prompt / SQL 仍走完整发版流程，**不**做配置热加载。

### 8.8 灰度策略（v1 不实现）

| 路径 | 改动 |
|---|---|
| Pod 级灰度 | K8s Deployment 拆两个，Ingress 按权重 |
| Topic 级灰度 | 同 Topic 出 experiment 变体，按 w3 取模 |
| 插件级灰度 | 新插件名法（见 5.8）|

### 8.9 备份与灾备

| 项 | v1 |
|---|---|
| OpenGauss 备份 | DBA 团队负责 |
| 历史对话恢复 | 仅靠 DB 备份 |
| 跨机房 DR | v1 不做 |
| 配置文件备份 | Git 仓库天然版本化 |

### 8.10 运维 Runbook

每条单独成文（`docs/runbook/`）：

| 场景 | Runbook |
|---|---|
| 下游 MetaGC 全部 5xx | runbook/downstream_metagc_5xx.md |
| OpenGauss 连接不上 | runbook/db_unreachable.md |
| 某专题大量鉴权失败 | runbook/auth_anomaly.md |
| 流式请求大量断流 | runbook/stream_disconnect_surge.md |
| LLM intent 调用超额 | runbook/intent_llm_quota.md |
| 单 Pod OOM | runbook/pod_oom.md |
| 滚动升级卡住 | runbook/rollout_stuck.md |

每条 Runbook 必含：症状识别 / 影响范围评估 / 缓解步骤 / 根因复盘模板。

### 8.11 配置变更与代码变更的边界

| 变更类型 | 需要发版？ | 走 PR 评审？ |
|---|---|---|
| 修 Python 代码 | ✅ | ✅ |
| 新增 / 改 Topic YAML | ✅ | ✅ |
| 改 prompt 模板 | ✅ | ✅ |
| 改 SQL 模板 | ✅ | ✅ |
| 改环境变量 / Secret | ❌（重启即可）| ⚠️ ConfigMap PR |
| 改日志级别（紧急） | ❌ | 事后补记录 |

**核心约束：所有"业务行为变更"必须通过 Git 留痕。** 不允许生产直接改 YAML 不进仓库。

### 8.12 刻意没做的事

| 没做 | 理由 |
|---|---|
| 不做配置热加载 | 业务行为变更必须留痕 |
| 不在镜像里跑 Alembic | 多副本 + 自动迁移 = 灾难 |
| 不内置灰度框架 | v1 流量不大 |
| 不做跨集群 DR | DBA / SRE 团队职责 |
| 不引入 Helm / Kustomize | 留给运维团队 |
| 不在应用层做限流 | Gateway 负责 |

---

## 9. 里程碑与实施计划

### 9.1 范围边界

| 维度 | v1 范围 | v1 明确不做 |
|---|---|---|
| 协议 | PC SSE + 移动 WS + REST 历史 | gRPC、HTTP/2 push、长轮询 |
| 专题 | 代表小管家 / 经营小助手 / 销售合同责任人 | 第 4 个及以后（按手册新增） |
| 编排 | 单步路由（intent → 单 adapter） | 多步 DAG 实跑（骨架预留） |
| 鉴权 | 业务鉴权插件化 | 登录态机制（占位 trust_header） |
| 历史 | 持久化 + 拼接 + 列表查询 | 总结压缩 / 摘要 / RAG-style |
| 可观测 | 日志 + OTel + OpenInference | Prometheus、Sentry、实时告警 |
| 灰度 | 无 | A/B、按 w3 取模分流 |
| 配置 | YAML + env | 热加载、配置中心、动态 prompt |
| 部署 | K8s + 单 Docker 镜像 | 多镜像、Helm chart、跨集群 DR |

v1 不做项不应在代码里出现"半成品"。

### 9.2 里程碑划分原则

不按"功能模块"切，按"**可交付的能力切片**"切。每个里程碑结束**能跑通某个端到端场景**。

### 9.3 里程碑总览

```
M0  脚手架 + 骨架               (≈ 1 周)   能起来，输出"hello"
M1  最小端到端 + MetaGC          (≈ 2 周)   单专题、单下游、SSE 跑通
M2  历史 + 鉴权 + 三专题完备     (≈ 2 周)   业务可联调
M3  Uniioc + 知识中心适配器      (≈ 2 周)   三下游齐
M4  WS 通道 + 可观测性闭环       (≈ 1.5 周) 移动端可联调
M5  硬化 + 文档 + 上线           (≈ 1.5 周) 能投产
─────────────────────────────────────────
总计                              ≈ 10 周   (2~3 人)
```

### 9.4 里程碑详情

#### M0 — 脚手架与骨架（约 1 周）

| 任务 | 验证 |
|---|---|
| 仓库初始化、目录结构（按 5.9）| `tree src/` 与设计一致 |
| pyproject.toml + uv.lock + Dockerfile | `docker build` 成功 |
| FastAPI app + `/healthz`/`/readyz` + Settings | 容器跑起来，curl 通 |
| structlog + contextvars 注入 | 一条日志含三 ID |
| OTel SDK + 根 span | 本地 Jaeger 看到 |
| GraphState + events + 最小 ChatService 骨架 | 单测：手喂事件流可编码成 SSE |
| `/api/v1/chat/sse` 返回 stub 流 | curl -N 看到合法 SSE 帧 |
| CI：ruff + mypy + pytest | PR 走 CI 通过 |

**出口：** 端到端 curl SSE 拿合法事件帧；日志/trace/CI 联动；**无业务逻辑**。

#### M1 — 最小端到端 + MetaGC（约 2 周）

| 任务 | 验证 |
|---|---|
| OpenGauss 容器接入 + asyncpg pool + SqlLoader/Executor | 集成测试 |
| Alembic 初始迁移 ma_session + ma_message | 跑迁移 + 回滚 |
| SessionRepository + MessageRepository | 单测 + 集成测试 |
| PluginRegistry + 四类 Plugin 基类与契约 | 单测 |
| TopicConfig + TopicCompiler | 单测：非法 YAML fail fast |
| 内置节点：load_session / reject / persist | 单测 |
| 节点封装层：auth/enrich/intent/adapter | 单测：tag 挂对、dispatch 正确 |
| 最小插件：representative_auth / generic_enrich / llm_classifier | 单测 |
| MetaGCAdapter（SSE 客户端） | 集成测试：mock_downstreams |
| 代表小管家 Topic YAML | 启动加载通过 |
| ChatService 完整循环 | e2e：mock 下游全链路通 |
| **流式契约测试 10 条** | 全绿 |

**出口：** 本地起完整链路，curl SSE 问代表小管家，收 MetaGC 流，历史落库，OTel 看完整 span tree。

> **守住边界**：M1 只做"代表小管家 + MetaGC"一条直线。

#### M2 — 历史 + 鉴权 + 三专题（约 2 周）

| 任务 | 验证 |
|---|---|
| 历史拼接（3.5 整轮一起丢） | 单测多种边界 |
| 追问场景 | e2e |
| REST `/sessions` / `/sessions/{id}/messages` | 集成测试，含越权防御 |
| 经营小助手 + 销售合同责任人 Topic YAML | e2e |
| 三套 AuthPlugin（专题 + 问题范围鉴权） | 单测各 reject 场景 |
| Reject 路径完整 | 集成测试 |
| `biz_params_schema` 真生效 | 单测 |
| 流式契约：鉴权失败 / WS 多 request 并发 | 全绿 |

**出口：** 三专题在 staging 用 mock 下游全部联调通；业务方可做 UAT。

#### M3 — Uniioc + 知识中心（约 2 周）

| 任务 | 验证 |
|---|---|
| UniiocAdapter（HTTP → 切 chunk 流） | 集成测试 |
| KnowledgeCenterAdapter（WS 客户端） | 集成测试 |
| LLMClassifier 真接企业 LLM | staging 验证 |
| Topic YAML intent.labels + graph.mapping 三路 | e2e 三条分支 |
| intent 失败降级 default_route | 单测 + e2e |
| 节点级 retry + timeout（YAML 配） | 集成测试：故意慢/抛错 |
| error_messages 配置生效 | 集成测试 |
| 流式契约:intent retry 透明 / 下游超时 | 全绿 |

**出口：** 三专题 × 三下游全部跑通；下游故意挂时用户拿到友好话术，OTel 能定位节点。

#### M4 — WS 通道 + 可观测性闭环（约 1.5 周）

| 任务 | 验证 |
|---|---|
| `/api/v1/chat/ws` + 握手鉴权占位 + 心跳 | 集成测试 |
| WS 信封协议（4.3.2） | 集成测试 |
| 单连接多 ask 并发，request_id 隔离 | 流式契约 |
| WS 优雅断开 + partial 标记 | 集成测试 |
| OpenInference 接入 | staging 看到 llm.classify span |
| OTel 导出到企业 collector / 本地 Jaeger | 手动验证 |
| 7.4.1 关键 fixture 全部就位 | CI 稳定 |
| `replay_request_id` CLI | 手动验证 |

**出口：** 移动端 demo 接入跑通；任意 prod 请求都能用 request_id 在三套系统反查到完整轨迹。

#### M5 — 硬化、文档、上线（约 1.5 周）

| 任务 | 验证 |
|---|---|
| K8s manifest 样例 | staging K8s 部起来 |
| Alembic 迁移 Job 模板 | staging 部署跑通 |
| 优雅退出实测 | rollout 期间无连接异常断 |
| 性能基线首跑 + `bench/baseline.md` | k6 报告 |
| 运维 Runbook 至少 4 条 | 评审通过 |
| 协议契约文档（给前端 / 移动端） | 前端 review 通过 |
| 设计书更新：偏差回填 | 提交 |
| 安全 review：依赖 / 容器 / Secret 扫描 | 报告无 Critical |
| 完整 e2e 回归 | 全绿 |
| 灰度上线计划 | 评审会通过 |

**出口：** staging 完整压测与回归通过；prod 灰度计划已批；具备首次切流条件。

### 9.5 关键依赖图

```
M0  ───▶  M1  ───▶  M2  ───▶  M5
              │              ▲
              ▼              │
              M3  ───▶  M4 ──┘
```

- M2 与 M3 可并行
- M4 依赖 M3
- 2 人团队建议 M2/M3 一人一线；1 人则严格串行

### 9.6 团队拆分建议

| 角色 | 负责 |
|---|---|
| A（主导，资深）| M0 全部 / M1 编排骨架 / M5 主导 / 全程评审 |
| B（业务向）| M1 插件 / M2 三专题 + 鉴权 / M4 OpenInference |
| C（基础设施向）| M1 DB 层 / M3 适配器 / M4 WS + OTel / M5 部署 |

任何插件 / 适配器 / 编排变更必须 A 二审。

### 9.7 PR 大小

| 类型 | 单 PR 行数目标 |
|---|---|
| 基建（M0）| ≤ 500 |
| 单插件新增 | ≤ 300（含测试）|
| 单 Adapter 新增 | ≤ 500（含测试）|
| Schema 迁移 | 1 个迁移 = 1 个 PR |

PR 模板必含"属于哪个里程碑、范围内还是范围外"自我声明。

### 9.8 风险点

| 风险 | 概率 | 影响 | 缓解 |
|---|---|---|---|
| LangGraph `astream_events` 在企业代理/WS 下行为不一致 | 中 | 高 | M1 早期 staging 跑通；备选：自己 subscribe dispatch_custom_event |
| OpenGauss vs PG 兼容性踩坑（JSONB / 时区） | 中 | 中 | M1 集成测试用真 OpenGauss 镜像 |
| MetaGC SSE 协议变体（非标 SSE） | 中 | 中 | M1 与 MetaGC 团队握手；保留原始 chunk 透传开关 |
| WS sticky session 在企业 LB 不支持 | 低 | 高 | M0 期间向运维确认；备选：先用 SSE 顶 |
| LLM intent 延迟太高 | 中 | 中 | 配 1.5~2s 超时 + fallback；M3 联调测一遍 |
| 历史拼接超 LLM context | 中 | 中 | 整轮一起丢 + adapter 层兜底裁剪 |
| 并发断流 partial 状态错乱 | 低 | 中 | 流式契约测试覆盖 |
| 三个 AuthPlugin 行为差异大 | 高 | 中 | M2 早期与用户中心团队对齐 SLA |
| 镜像数量爆炸 | 低 | 低 | registry 侧 retention |

**特别提醒（LangGraph）**：这是最大未知。M1 周一就要在 staging 跑一次 mini 链路验证 `astream_events` 在真实代理后的行为；若有问题切回"节点内 `dispatch_custom_event` + ChatService 自己 subscribe"的备选实现。这条备选路径不要等出问题才想。

### 9.9 验收与上线策略

```
Step 1: staging 全功能回归通过
Step 2: prod 部署 0% 流量（dry-run）
Step 3: 灰度 1% → 24h
Step 4: 灰度 10% → 48h
Step 5: 灰度 50% → 24h
Step 6: 全量
Step 7: 7 天稳定 → v1.0.0
```

分流键：`w3_account` 取模。

**回滚阈值（任一触发立即回滚）：**
- error 率 > 基线 × 3
- P95 首 chunk 延迟 > 5s
- 同一 request_id 出现 `INTERNAL` error 占比 > 1%
- 任一专题鉴权拒绝率突变 > 30%

### 9.10 v1 之后（v2 远景）

| v2 候选 | v1 预留 | 落地路径 |
|---|---|---|
| 多步 DAG 真跑 | ✅ Topic YAML schema 已支持 | YAML + 节点封装小改 |
| 配置中心 / 热加载 | ✅ ConfigSource 接口预留 | 实现 DB 或配置中心的 ConfigSource |
| 真实登录态机制 | ✅ AuthnMiddleware 占位 | 替换具体实现 |
| 跨 Pod WS 推送 | ❌ | Redis pub/sub |
| LLM 调用统一治理 | ❌ | LLM gateway 层 |
| 灰度框架 | ❌ | 取决于运维侧能力 |
| 多语种 error 话术 | ❌ | error_messages 加 i18n |
| 实时告警 / SLO 看板 | ❌ | Prometheus + alertmanager |

### 9.11 刻意没排进里程碑的事

| 没排 | 理由 |
|---|---|
| 单独"DB 设计"里程碑 | 拆出来无验证物 |
| 单独"测试补充"里程碑 | 测试必须每个里程碑同步建设 |
| "可观测性"单独里程碑 | OTel/log 从 M0 就织进去 |
| "性能优化"里程碑 | v1 只建基线 |
| "重构"里程碑 | 边写边整；如有大重构应回 brainstorming |

---

## 附录 A：术语表

| 术语 | 含义 |
|---|---|
| MA | MasterAgent，本设计书的主题 |
| Topic / 专题 | 代表小管家 / 经营小助手 / 销售合同责任人 Agent 等 |
| bizId | 专题的唯一 ID |
| thread_id | 前端生成的会话 ID，等同于 session_id |
| w3Account | 用户身份标识 |
| MetaGC / Uniioc / 知识中心 | 三个下游第三方 AI Agent 接口 |
| GraphState | LangGraph 图执行的共享状态对象 |
| ChatEvent | MA 内部统一的事件流元素 |
| StateGraph / CompiledStateGraph | LangGraph 的图与编译产物 |
| user_output / user_thinking / user_progress / user_tool | LangGraph 节点标签，决定其事件是否下发用户 |

## 附录 B：与 CLAUDE.md 编码准则的对齐

| 准则 | 本设计书的体现 |
|---|---|
| 1. 编码前先思考 | 9 节设计书完整列出权衡与决策 |
| 2. 简洁优先 | 每节末尾"刻意没做的事"清单 |
| 3. 手术刀式改动 | PR 大小约束（9.7）|
| 4. 目标驱动 | 每个里程碑都有可验证的出口标准 |

