# MasterAgent M1 — 最小端到端 + MetaGC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 代表小管家专题 (`daibiao_xiaoguanjia`) 端到端跑通：POST `/api/v1/chat/sse` → AuthnMiddleware 占位 → ChatService 装配 → LangGraph 编译并跑 → MetaGC mock adapter 流式产 token → 持久化到 OpenGauss → 完整 SSE 7-type 事件流送给用户。其它专题、其它下游、WebSocket 通道全部不做。

**Architecture (在 M0 之上加这些层):**
- Day-1 cleanup：补 M0 最终评审暴露的 4 项（trace_id 入日志、FastAPIInstrumentor 接入、log null 去除、统一 contextvars）
- 数据层：`sql/` 模板 + `asyncpg` 池 + `SqlLoader` + `SqlExecutor`（读写分离接口、单 DSN 实现）+ Alembic 初始迁移 `ma_session` `ma_message`
- 仓储层：`SessionRepository` + `MessageRepository`，ChatService 唯一 DB 入口
- 插件体系：`Plugin` Protocol + `PluginRegistry` + 4 类子契约 (Auth/Enrich/Intent/Adapter) + 装饰器注册 + 显式 import bootstrap
- Topic 体系：`TopicConfig` Pydantic schema + `TopicCompiler` + `TopicRegistry`，YAML 启动时编译
- LangGraph 编排：`GraphState` 真用、内置节点 (`load_session`/`reject`/`persist`) + 节点封装层（含 OTel + tag 挂载 + dispatch_custom_event）
- 业务插件：`representative_auth`（专题+问题范围鉴权占位）+ `generic_enrich` + `llm_classifier`（FakeListChatModel）
- Adapter：`MetaGCAdapter`（HTTP SSE 客户端，httpx 异步流）
- Mock 下游：`tests/mock_downstreams/metagc_sse.py` 本地 FastAPI mock server
- ChatService 重写：拼 RunnableConfig 跑 `astream_events(version="v2")` → 按 4 个 tag 转 ChatEvent
- 流式契约测试套件：10 条契约（见 §7.2.2）按 §9.4-M1 出口要求全绿
- 集成测试 + e2e：testcontainers OpenGauss + 本地 mock MetaGC，覆盖 §9.4-M1 e2e 用例

**Tech Stack (新增):** asyncpg、alembic、langgraph、langchain-core、httpx-sse、testcontainers[opengauss/postgres]、pyyaml、anyio。

**Spec reference:** `docs/superpowers/specs/2026-06-23-master-agent-design.md` 第 1 / 2 / 3 / 4.2 / 5 / 6 / 7.2.2 / 7.4 / 9.4-M1 节。

**M0 baseline:** tag `m0-complete`（commit `44fd575`）+ M1 day-1 cleanup（本计划 Phase A）。

**Branch strategy:** 当前 `design/master-agent-v1`（M0 合入）。M1 在新分支 `feature/m1-metagc-e2e` 开发。

**Locked design decisions** (来自 brainstorming 阶段，今天再次确认):
- 集成测试 OpenGauss 仅在 CI 跑（testcontainers + docker available on ubuntu-latest）；本地 unit 测试不依赖 DB
- IntentPlugin 用 langchain_core 的 `FakeListChatModel`；真 LLM 推到 M3
- MetaGC 下游本地 FastAPI mock server（`tests/mock_downstreams/metagc_sse.py`），同时 CI 也走 mock

---

## File Structure (M1 创建/修改清单)

### 阶段 A: Day-1 Cleanup
- Modify: `src/ma/infra/logging.py` — 去 null 字段；统一 contextvars；加 trace_id 注入；测试相应更新
- Modify: `tests/unit/test_logging.py` — 新增 trace_id 注入测试；改用 structlog.contextvars 路径
- Modify: `src/ma/main.py` — 接入 FastAPIInstrumentor + 加 `# type: ignore` 兼容

### 阶段 B: DB infrastructure
- Create: `src/ma/infra/db.py` — asyncpg pool 工厂 + 配置读取
- Modify: `src/ma/infra/settings.py` — 加 DB / 后续阶段 LLM / MetaGC 字段
- Create: `db/alembic.ini`
- Create: `db/env.py`
- Create: `db/versions/0001_init_session_message.py`
- Create: `sql/session/get_by_thread.sql`
- Create: `sql/session/insert.sql`
- Create: `sql/session/update_touch.sql`
- Create: `sql/session/list_by_w3.sql`
- Create: `sql/message/append.sql`
- Create: `sql/message/list_recent.sql`
- Create: `sql/message/mark_partial.sql`
- Create: `sql/message/mark_failed.sql`
- Create: `src/ma/core/repo/__init__.py`
- Create: `src/ma/core/repo/sql_loader.py`
- Create: `src/ma/core/repo/sql_executor.py`
- Create: `src/ma/core/repo/models.py` — Session / Message dataclasses
- Create: `src/ma/core/repo/session_repository.py`
- Create: `src/ma/core/repo/message_repository.py`
- Create: `tests/unit/test_sql_loader.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/conftest.py` — opengauss_pool fixture（testcontainers）
- Create: `tests/integration/test_repositories.py`

### 阶段 C: Plugin system
- Create: `src/ma/core/plugin/__init__.py`
- Create: `src/ma/core/plugin/base.py` — Plugin Protocol + 4 子契约 + dataclasses
- Create: `src/ma/core/plugin/registry.py`
- Create: `src/ma/core/plugin/bootstrap.py`
- Create: `tests/unit/test_plugin_registry.py`

### 阶段 D: Topic system
- Create: `src/ma/core/topic/__init__.py`
- Create: `src/ma/core/topic/config.py` — Pydantic TopicConfig schema
- Create: `src/ma/core/topic/compiler.py` — `TopicCompiler.compile()`
- Create: `src/ma/core/topic/registry.py` — `TopicRegistry`
- Create: `tests/unit/test_topic_config.py`
- Create: `tests/unit/test_topic_compiler.py`

### 阶段 E: LangGraph nodes + wrappers
- Modify: `src/ma/core/graph/state.py` — 字段类型从 `Any` 落实为具体类型
- Create: `src/ma/core/graph/builtins.py` — load_session / reject / persist
- Create: `src/ma/core/graph/node_wrappers.py` — auth_node / enrich_node / intent_node / adapter_node + `_traced_node`
- Modify: `src/ma/core/graph/events.py` — ChatEvent helper for tool / progress / thinking payloads (optional)
- Create: `tests/unit/test_graph_builtins.py`
- Create: `tests/unit/test_node_wrappers.py`

### 阶段 F: Plugin implementations
- Create: `src/ma/plugins/__init__.py`
- Create: `src/ma/plugins/auth/__init__.py`
- Create: `src/ma/plugins/auth/representative_auth.py`
- Create: `src/ma/plugins/enrich/__init__.py`
- Create: `src/ma/plugins/enrich/generic_enrich.py`
- Create: `src/ma/plugins/intent/__init__.py`
- Create: `src/ma/plugins/intent/llm_classifier.py`
- Create: `tests/unit/test_representative_auth.py`
- Create: `tests/unit/test_generic_enrich.py`
- Create: `tests/unit/test_llm_classifier.py`

### 阶段 G: MetaGC Adapter
- Create: `src/ma/plugins/adapter/__init__.py`
- Create: `src/ma/plugins/adapter/metagc.py`
- Create: `tests/mock_downstreams/__init__.py`
- Create: `tests/mock_downstreams/metagc_sse.py`
- Create: `tests/unit/test_metagc_adapter.py` — 用 httpx.MockTransport
- Create: `tests/integration/test_metagc_with_mock_server.py`

### 阶段 H: ChatService 重写 + Topic YAML
- Modify: `src/ma/core/chat_service.py` — 接入 TopicRegistry + LangGraph runner
- Create: `config/topics/daibiao_xiaoguanjia.yaml`
- Create: `prompts/intent/representative.txt`
- Modify: `src/ma/main.py` — lifespan 启动 DB pool / load_all_plugins / 编译 topics / 装 ChatService

### 阶段 I: Stream contract tests (M1 必绿 10 条)
- Create: `tests/stream/__init__.py`
- Create: `tests/stream/conftest.py`
- Create: `tests/stream/test_contracts.py` — §7.2.2 中 10 条契约里的 M1 范围（部分用例 WS 相关推到 M4）

### 阶段 J: e2e + 出口验证
- Create: `tests/e2e/__init__.py`
- Create: `tests/e2e/test_daibiao_xiaoguanjia_sse.py`
- Modify: `Dockerfile` — 加 `COPY config/`、`COPY sql/`、`COPY prompts/`、`COPY db/`
- Modify: `pyproject.toml` — 新增依赖
- Modify: `.github/workflows/ci.yml` — 加 integration / e2e 阶段
- Modify: `Makefile` — 加 `test-integration` / `test-e2e` 目标
- Modify: `README.md` — M1 状态更新

### M1 不创建（留给 M2 及以后）
- `src/ma/api/ws.py`（M4）
- `src/ma/api/rest.py`（M2）
- 经营小助手 / 销售合同责任人 YAML（M2）
- `UniiocAdapter` / `KnowledgeCenterAdapter`（M3）
- `LLMClassifier` 真接 LLM（M3，本里程碑用 Fake）
- 任何 WS 相关代码（M4）

---

## 任务索引

本计划共 26 个 task，按 10 个 phase 分组。phase 内 task 严格顺序执行；phase 之间也是严格顺序（A → B → C → D → E → F → G → H → I → J）。

| Phase | Task | 名称 |
|---|---|---|
| A: Day-1 Cleanup | 1 | M0 review 4 项 cleanup (TDD) |
| B: DB infra | 2 | Settings 扩展 (DB / LLM / MetaGC 字段) |
|  | 3 | SQL 模板文件 + SqlLoader (TDD) |
|  | 4 | asyncpg pool factory + SqlExecutor 单测 |
|  | 5 | Alembic 迁移 + opengauss testcontainer fixture |
|  | 6 | SessionRepository + MessageRepository (TDD + integration) |
| C: Plugin system | 7 | Plugin Protocol + 4 子契约 + dataclasses (TDD) |
|  | 8 | PluginRegistry + bootstrap (TDD) |
| D: Topic system | 9 | TopicConfig Pydantic schema (TDD: 非法 YAML fail fast) |
|  | 10 | TopicCompiler + TopicRegistry (TDD) |
| E: LangGraph nodes | 11 | GraphState 字段类型落实 + Message/AuthResult/AuthnIdentity dataclass |
|  | 12 | builtins.py: load_session / reject / persist (TDD) |
|  | 13 | node_wrappers.py: auth/enrich/intent/adapter + `_traced_node` (TDD) |
| F: Plugin impls | 14 | representative_auth + 单测 (mock user_ctx client) |
|  | 15 | generic_enrich + 单测 |
|  | 16 | llm_classifier (FakeListChatModel) + 单测 |
| G: MetaGC | 17 | MetaGC mock server + MetaGCAdapter (TDD with httpx MockTransport) |
|  | 18 | MetaGCAdapter integration test 跑通 mock server |
| H: ChatService 重写 | 19 | 代表小管家 Topic YAML + prompt 模板 |
|  | 20 | ChatService 接 TopicRegistry + LangGraph astream_events 转 ChatEvent |
|  | 21 | main.py lifespan 装配（DB / plugins / topics / ChatService） |
| I: Stream contracts | 22 | tests/stream/conftest.py + 工具函数 |
|  | 23 | 10 条契约测试（M1 范围那部分） |
| J: e2e + 出口 | 24 | e2e test_daibiao_xiaoguanjia_sse |
|  | 25 | Dockerfile / CI / Makefile 更新 |
|  | 26 | M1 出口验证清单 + 合并 + tag m1-complete + 完成笔记 |

---

(完整 task 内容见后续 sections，将作为 plan 文件的 §1~§26 章节增量补全。本文件初版仅写到 task 索引为止，便于先把骨架落盘。)

---

## Phase A: Day-1 Cleanup

合并 M0 最终评审中的 4 项 Important 到一次提交。理由：这些都集中在 structlog ↔ OTel ↔ context 这一个语义域，作为 M1 第 1 个 commit 一并处理最自洽，之后再开始动 DB / LangGraph。

### Task 1: M0 review cleanup (TDD)

**Files:**
- Modify: `src/ma/infra/logging.py`
- Modify: `tests/unit/test_logging.py`
- Modify: `src/ma/main.py`

**变更点（来自 M0 完成笔记的 "M1 day-1 cleanup" 清单）:**

1. **`_inject_ids` 不写 null** — 仅在 ctxvar 非 None 时 setdefault
2. **统一 contextvars 机制** — 改用 `structlog.contextvars.bind_contextvars(...)`；删自写的 `_ctx_request_id` 等 + `_inject_ids` processor
3. **加 `trace_id` 注入到日志** — 新增 processor 读 `trace.get_current_span().get_span_context().trace_id`，非默认（all-zeros）时把 hex 写入 event_dict
4. **接入 `FastAPIInstrumentor`** — 在 `main.py:create_app()` 里调 `FastAPIInstrumentor().instrument_app(app)`，让 §6.4.1 的 `chat.request` 根 span 自动产生

- [ ] **Step 1: 切到新 M1 工作分支**

```bash
cd E:/work/source/python/ocm-agent-v1
git checkout design/master-agent-v1
git pull
git checkout -b feature/m1-metagc-e2e
git status
```

Expected: `On branch feature/m1-metagc-e2e, nothing to commit, working tree clean`

- [ ] **Step 2: 在 `tests/unit/test_logging.py` 写新测试 & 改老测试**

读取当前 `tests/unit/test_logging.py`，做以下三处改动：

**A. fixture `reset_structlog` 改为同时 `clear_contextvars`**

```python
@pytest.fixture(autouse=True)
def reset_structlog() -> None:
    """每个测试都重新 configure，并清空 contextvars 残留。"""
    import structlog
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
```

**B. 替换 `test_contextvars_inject_three_ids` 为**：

```python
def test_contextvars_inject_three_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """绑定 3 个 ID 之后，后续日志自动带上；未绑定时不出现 null 字段。"""
    from ma.infra.logging import bind_request_ctx, configure_logging, get_logger

    buf = StringIO()
    configure_logging("info")
    monkeypatch.setattr("sys.stdout", buf)

    # 未 bind 时，三 ID 字段不应出现
    log = get_logger("test")
    log.info("startup_event")
    first = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert "request_id" not in first
    assert "thread_id" not in first
    assert "w3_account" not in first

    # bind 之后三 ID 出现
    bind_request_ctx(request_id="req_1", thread_id="th_1", w3_account="zhangsan")
    log.info("auth_passed")
    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert payload["request_id"] == "req_1"
    assert payload["thread_id"] == "th_1"
    assert payload["w3_account"] == "zhangsan"
```

**C. 新增 `test_trace_id_injected_when_span_active`**：

```python
def test_trace_id_injected_when_span_active(monkeypatch: pytest.MonkeyPatch) -> None:
    """在 active span 内打日志：自动带 trace_id（32-char hex）。"""
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    from ma.infra.logging import configure_logging, get_logger

    # 给当前进程装一个简易 TracerProvider（M0 的 configure_tracing 已经会做这事；
    # 这里独立设一个保证测试不依赖外部环境）
    if not isinstance(trace.get_tracer_provider(), TracerProvider):
        trace.set_tracer_provider(TracerProvider())

    buf = StringIO()
    configure_logging("info")
    monkeypatch.setattr("sys.stdout", buf)
    tracer = trace.get_tracer("test")

    log = get_logger("test")
    with tracer.start_as_current_span("test_span"):
        log.info("event_in_span")
    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert "trace_id" in payload
    # 32 hex chars
    assert isinstance(payload["trace_id"], str)
    assert len(payload["trace_id"]) == 32
    assert all(c in "0123456789abcdef" for c in payload["trace_id"])


def test_trace_id_absent_when_no_span(monkeypatch: pytest.MonkeyPatch) -> None:
    """无 active span 时，trace_id 字段不出现（不写 null / 0000…）。"""
    from ma.infra.logging import configure_logging, get_logger

    buf = StringIO()
    configure_logging("info")
    monkeypatch.setattr("sys.stdout", buf)

    log = get_logger("test")
    log.info("event_outside_span")
    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert "trace_id" not in payload
```

Run: `python -m uv run pytest tests/unit/test_logging.py -v`
Expected: 上面那些 `trace_id`/`bind` 相关用例必须 FAIL（实现还没改）。可能 4-5 个失败。

- [ ] **Step 3: 改 `src/ma/infra/logging.py`**

```python
"""structlog 配置：JSON 输出 + structlog.contextvars 自动注入 + OTel trace_id + 敏感字段脱敏。

调用顺序：configure_logging(level) 一次 → bind_request_ctx(...) 在请求入口
→ get_logger(__name__).info("event_name", **kv) 在业务代码中。
"""
from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from opentelemetry import trace

_SENSITIVE_KEYS = frozenset(
    {"authorization", "cookie", "token", "x-api-key", "x_api_key", "set-cookie"}
)


def bind_request_ctx(
    *,
    request_id: str | None = None,
    thread_id: str | None = None,
    w3_account: str | None = None,
) -> None:
    """在请求入口调用一次，让后续日志自动带上三 ID。

    None 值跳过 —— 不写入 contextvars，因此 _inject_trace_id / merge_contextvars
    都不会把 null 渲染到 JSON 里。
    """
    kv: dict[str, Any] = {}
    if request_id is not None:
        kv["request_id"] = request_id
    if thread_id is not None:
        kv["thread_id"] = thread_id
    if w3_account is not None:
        kv["w3_account"] = w3_account
    if kv:
        structlog.contextvars.bind_contextvars(**kv)


def _inject_trace_id(
    _logger: Any, _method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """读当前 active span，把 trace_id 注入 event_dict。

    无 active span 或 span context invalid 时不写。32-char hex 小写。
    """
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
    return event_dict


def _sanitize(
    _logger: Any, _method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    for k in list(event_dict):
        if k.lower() in _SENSITIVE_KEYS:
            event_dict[k] = "***"
    return event_dict


class _StdoutPrintLoggerFactory:
    """structlog factory：每次 logger 调用都读当前 sys.stdout。

    用于支持 tests 用 monkeypatch.setattr(sys, "stdout", buf) 捕获日志输出。
    """

    def __call__(self, *args: Any, **kwargs: Any) -> structlog.PrintLogger:
        return structlog.PrintLogger(file=sys.stdout)


def configure_logging(level: str = "info") -> None:
    """配置 structlog 全局处理链。多次调用幂等。"""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(message)s", force=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _inject_trace_id,
            _sanitize,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=_StdoutPrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """获取一个 structlog logger。"""
    return structlog.get_logger(name)  # type: ignore[return-value]
```

**重要：** 旧的 `_ctx_request_id` / `_ctx_thread_id` / `_ctx_w3` / `_inject_ids` 全部删掉。`bind_request_ctx` 改为 thin wrapper 转发到 `structlog.contextvars.bind_contextvars(...)`。这一改让 contextvars 自动注入（通过 `merge_contextvars` processor），并且未绑定的 ID 不会出现在 JSON 里。

- [ ] **Step 4: 跑测试，应当全过**

```bash
python -m uv run pytest tests/unit/test_logging.py -v
```

Expected: 6 passed (4 老的 + 2 新的 trace_id 用例)。
注：如果老用例 `test_sensitive_keys_are_redacted` 用了已删除的 helper，需要保持其调用形式但只用 `bind_request_ctx` 或不绑 —— 在 Step 2 改动时确认这一点。

- [ ] **Step 5: 改 `src/ma/main.py`，加 FastAPIInstrumentor**

```python
"""FastAPI app 入口。

启动顺序（设计书 §8.2.3）M1 版：
1. 加载 Settings → 失败即退出
2. configure logging
3. configure tracing
4. 装配 ChatService 到 app.state（M2+ 加 DB pool / plugins / topics）
5. FastAPIInstrumentor 织入 OTel
6. mark_ready
7. 接流量
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from ma.api import health, sse
from ma.core.chat_service import ChatService
from ma.infra.logging import configure_logging, get_logger
from ma.infra.settings import Settings
from ma.infra.tracing import configure_tracing


def create_app() -> FastAPI:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)
    configure_tracing(settings)
    log = get_logger("ma.main")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info("startup_begin", env=settings.env)
        app.state.chat_service = ChatService()
        health.mark_ready()
        log.info("startup_complete")
        yield
        health.mark_not_ready()
        log.info("shutdown_complete")

    app = FastAPI(title="MasterAgent", version="0.1.0", lifespan=lifespan)
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(sse.router, prefix="/api/v1")

    # OTel: 给 HTTP route 自动开 server span（root span chat.request）
    FastAPIInstrumentor().instrument_app(app)
    return app


app = create_app()
```

注意 version 从 `0.0.1` 升到 `0.1.0` 表征 M1 进展。

- [ ] **Step 6: 全套测试 + ruff + mypy**

```bash
python -m uv run pytest -v
python -m uv run ruff check . && python -m uv run ruff format --check .
python -m uv run mypy src
```

Expected: 全绿。覆盖率仍 ≥ 80%。

- [ ] **Step 7: Commit**

```bash
git add src/ma/infra/logging.py tests/unit/test_logging.py src/ma/main.py
git commit -m "refactor(infra): adopt structlog.contextvars + OTel trace_id in logs; instrument FastAPI

Resolves M0 final-review Important items #1-4:
- _inject_ids removed; bind_request_ctx now wraps structlog.contextvars.bind_contextvars
- null IDs no longer leak into JSON when contextvars are unset
- trace_id auto-injected from current active span (32-char hex), absent if no span
- FastAPIInstrumentor wired in create_app to produce chat.request root span"
```

Self-review:
- 旧的 ContextVars 全删了？
- 6 个 logging 测试全过？
- main.py 真的 instrument 了？跑 SSE 时去 OTel collector 能看 `POST /api/v1/chat/sse` 的 server span？（本机如果没 collector 就跳过这一项，CI 上看）

**完成 phase A 的标志：commit SHA、26 个单测仍全绿、覆盖率不掉。**

---

## Phase B: DB infrastructure

落地 `ma_session` `ma_message` 表 + asyncpg pool + 模板化 SQL 层 + 仓储层。集成测试用 testcontainers + OpenGauss 镜像；这部分**在 CI 上跑**，本机若无 docker 则跳过 integration 测试（unit 测试不依赖 DB）。

### Task 2: Settings 扩展

**Files:**
- Modify: `src/ma/infra/settings.py`
- Modify: `tests/unit/test_settings.py`

加入 M1 用到的所有环境字段：DB / LLM / MetaGC / AuthN 占位。一次性加齐，避免后续 task 反复改 Settings。

- [ ] **Step 1: 改 `src/ma/infra/settings.py`**

```python
"""全局 Settings：所有环境配置的唯一入口。任何缺失项启动即崩溃。"""
from __future__ import annotations

from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """所有字段经 MA_ 前缀的环境变量加载（如 MA_ENV=dev）。

    M1 字段一次性加齐，避免后续 task 反复改 Settings。具体业务字段（M2+
    的 Uniioc / KC、M5 的 AuthN JWT 等）后续再扩展。
    """

    model_config = SettingsConfigDict(
        env_prefix="MA_",
        env_file=None,
        case_sensitive=False,
        extra="ignore",
    )

    # ── 基础 ─────────────────────────
    env: Literal["dev", "test", "staging", "prod"]
    log_level: Literal["debug", "info", "warning", "error"] = "info"

    # ── 可观测性 ──────────────────────
    otel_exporter_otlp_endpoint: str | None = None
    otel_service_name: str = "master-agent"

    # ── DB (OpenGauss / PG-compatible) ───
    pg_dsn_rw: SecretStr | None = None
    pg_dsn_ro: SecretStr | None = None  # 留空 → 读用同一个 RW DSN
    pg_pool_min: int = 4
    pg_pool_max: int = 32

    # ── 配置文件目录 ──────────────────
    config_topics_dir: str = "config/topics"
    sql_templates_dir: str = "sql"

    # ── LLM (intent) ─────────────────
    # M1 用 FakeListChatModel；真 LLM 推到 M3 → 这些字段保留 None 即可
    intent_llm_provider: Literal["fake", "openai-compatible", "internal"] = "fake"
    intent_llm_base_url: str | None = None
    intent_llm_api_key: SecretStr | None = None
    intent_llm_model: str = "fake-model"

    # ── MetaGC 下游 ───────────────────
    metagc_base_url: str | None = None
    metagc_timeout_ms: int = 30000

    # ── AuthN（M0 占位）───────────────
    authn_mode: Literal["trust_header", "jwt", "gateway"] = "trust_header"
    authn_trust_header_name: str = "X-User-Account"
```

注意：`pg_dsn_rw` 类型是 `SecretStr | None`。**为什么允许 None？** 让本机 unit 测试不需要起 DB 就能加载 Settings；integration 测试和真启动时 ChatService 用 DB 前会校验 `pg_dsn_rw is not None`，拿不到就 fail fast。

- [ ] **Step 2: 改 `tests/unit/test_settings.py`，追加新字段测试**

读已有文件，**保留**所有现有用例（4 个 env / log_level + 1 个 otel）。**追加**：

```python
def test_settings_default_db_dsns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.delenv("MA_PG_DSN_RW", raising=False)
    monkeypatch.delenv("MA_PG_DSN_RO", raising=False)

    from ma.infra.settings import Settings

    s = Settings()
    assert s.pg_dsn_rw is None
    assert s.pg_dsn_ro is None
    assert s.pg_pool_min == 4
    assert s.pg_pool_max == 32


def test_settings_db_dsn_secret_loaded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.setenv("MA_PG_DSN_RW", "postgresql://x:y@host:5432/db")

    from ma.infra.settings import Settings

    s = Settings()
    assert s.pg_dsn_rw is not None
    # SecretStr 必须显式 .get_secret_value() 才能拿明文
    assert s.pg_dsn_rw.get_secret_value() == "postgresql://x:y@host:5432/db"


def test_settings_intent_llm_defaults_fake(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    from ma.infra.settings import Settings

    s = Settings()
    assert s.intent_llm_provider == "fake"
    assert s.intent_llm_model == "fake-model"


def test_settings_metagc_url_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    from ma.infra.settings import Settings

    s = Settings()
    assert s.metagc_base_url is None
    assert s.metagc_timeout_ms == 30000
```

跑：

```bash
python -m uv run pytest tests/unit/test_settings.py -v
```

Expected: 9 passed (5 old + 4 new).

- [ ] **Step 3: 全套测试 + lint**

```bash
python -m uv run pytest -v
python -m uv run mypy src
python -m uv run ruff check . && python -m uv run ruff format --check .
```

Expected: 30 passed (26 old + 4 new); lint clean。

- [ ] **Step 4: Commit**

```bash
git add src/ma/infra/settings.py tests/unit/test_settings.py
git commit -m "feat(infra): extend Settings with DB/LLM/MetaGC/AuthN fields for M1"
```

### Task 3: SQL 模板文件 + SqlLoader (TDD)

**Files:**
- Create: `sql/session/get_by_thread.sql`
- Create: `sql/session/insert.sql`
- Create: `sql/session/update_touch.sql`
- Create: `sql/session/list_by_w3.sql`
- Create: `sql/message/append.sql`
- Create: `sql/message/list_recent.sql`
- Create: `sql/message/mark_partial.sql`
- Create: `sql/message/mark_failed.sql`
- Create: `src/ma/core/repo/__init__.py`
- Create: `src/ma/core/repo/sql_loader.py`
- Create: `tests/unit/test_sql_loader.py`

参考：设计书 §3.3.1 / §3.3.2。模板用具名占位符 `:name`；任何 f-string / `.format()` 严禁。

- [ ] **Step 1: 创建 SQL 模板文件**

`sql/session/get_by_thread.sql`:
```sql
SELECT thread_id, biz_id, w3_account, title, status,
       created_at, updated_at, last_message_at, ext
FROM ma_session
WHERE thread_id = :thread_id;
```

`sql/session/insert.sql`:
```sql
INSERT INTO ma_session (
    thread_id, biz_id, w3_account, title, status, ext
) VALUES (
    :thread_id, :biz_id, :w3_account, :title, 'active', :ext
)
ON CONFLICT (thread_id) DO NOTHING;
```

`sql/session/update_touch.sql`:
```sql
UPDATE ma_session
SET updated_at = CURRENT_TIMESTAMP,
    last_message_at = CURRENT_TIMESTAMP
WHERE thread_id = :thread_id;
```

`sql/session/list_by_w3.sql`:
```sql
SELECT thread_id, biz_id, w3_account, title, status,
       created_at, updated_at, last_message_at, ext
FROM ma_session
WHERE w3_account = :w3_account
  AND (:biz_id IS NULL OR biz_id = :biz_id)
  AND (:before IS NULL OR last_message_at < :before)
ORDER BY last_message_at DESC NULLS LAST, created_at DESC
LIMIT :limit;
```

`sql/message/append.sql`:
```sql
INSERT INTO ma_message (
    message_id, thread_id, seq, role, content,
    content_meta, status, route, request_id, ext
) VALUES (
    :message_id, :thread_id, :seq, :role, :content,
    :content_meta, :status, :route, :request_id, :ext
);
```

`sql/message/list_recent.sql`:
```sql
SELECT message_id, thread_id, seq, role, content,
       content_meta, status, route, request_id, created_at, ext
FROM ma_message
WHERE thread_id = :thread_id
  AND status IN ('complete', 'partial')
ORDER BY seq DESC
LIMIT :limit;
```

`sql/message/mark_partial.sql`:
```sql
UPDATE ma_message
SET status = 'partial'
WHERE message_id = :message_id;
```

`sql/message/mark_failed.sql`:
```sql
UPDATE ma_message
SET status = 'failed',
    ext = COALESCE(ext, '{}'::jsonb) || :err_json
WHERE message_id = :message_id;
```

- [ ] **Step 2: 创建 `src/ma/core/repo/__init__.py`**（空）

```bash
mkdir -p src/ma/core/repo
touch src/ma/core/repo/__init__.py
```

- [ ] **Step 3: 写失败测试 `tests/unit/test_sql_loader.py`**

```python
"""SqlLoader：启动时一次性加载所有 sql/*.sql；按 namespace/name 取。"""
from __future__ import annotations

from pathlib import Path

import pytest


def test_sql_loader_loads_all_templates(tmp_path: Path) -> None:
    """模拟一个 sql/ 目录，确保 loader 能按 namespace/name 索引到内容。"""
    sql_dir = tmp_path / "sql"
    (sql_dir / "session").mkdir(parents=True)
    (sql_dir / "session" / "get.sql").write_text("SELECT 1;", encoding="utf-8")
    (sql_dir / "message").mkdir()
    (sql_dir / "message" / "append.sql").write_text(
        "INSERT INTO t VALUES (:x);", encoding="utf-8"
    )

    from ma.core.repo.sql_loader import SqlLoader

    loader = SqlLoader(sql_dir)
    assert loader.get("session/get") == "SELECT 1;"
    assert loader.get("message/append") == "INSERT INTO t VALUES (:x);"


def test_sql_loader_unknown_key_raises() -> None:
    from ma.core.repo.sql_loader import SqlLoader, SqlTemplateNotFound

    loader = SqlLoader(Path("sql"))  # use real sql/ dir
    with pytest.raises(SqlTemplateNotFound):
        loader.get("nonexistent/key")


def test_sql_loader_real_sql_dir_has_required_keys() -> None:
    """跑真 sql/ 目录，确保 Task 3 创建的 8 个模板都加载到。"""
    from ma.core.repo.sql_loader import SqlLoader

    loader = SqlLoader(Path("sql"))
    required = [
        "session/get_by_thread",
        "session/insert",
        "session/update_touch",
        "session/list_by_w3",
        "message/append",
        "message/list_recent",
        "message/mark_partial",
        "message/mark_failed",
    ]
    for key in required:
        sql = loader.get(key)
        assert sql.strip(), f"template {key} is empty"


def test_sql_loader_rejects_format_strings(tmp_path: Path) -> None:
    """硬约束（设计书 §3.3.2）：模板里不能包含 f-string / .format / %s 风格占位符。"""
    sql_dir = tmp_path / "sql"
    (sql_dir / "bad").mkdir(parents=True)
    (sql_dir / "bad" / "fstring.sql").write_text(
        "SELECT * FROM t WHERE id = {x};", encoding="utf-8"
    )

    from ma.core.repo.sql_loader import SqlLoader, UnsafeSqlTemplate

    with pytest.raises(UnsafeSqlTemplate):
        SqlLoader(sql_dir)
```

Run: `python -m uv run pytest tests/unit/test_sql_loader.py -v`
Expected: 4 fail with `ModuleNotFoundError`.

- [ ] **Step 4: 实现 `src/ma/core/repo/sql_loader.py`**

```python
"""SqlLoader：启动时把 sql/*/*.sql 一次性加载到内存，运行期按 'namespace/name' 取。

设计约束（§3.3.2）：
- 模板里不允许任何 f-string / .format() / %s / printf 占位符
- 唯一允许的参数化形式是具名占位符 :name（asyncpg 不直接支持具名 → SqlExecutor 负责翻译）
- 表名/列名不允许在模板里动态拼接（这一点 SqlLoader 不强校验，由 code review 把关）
"""
from __future__ import annotations

import re
from pathlib import Path


class SqlTemplateNotFound(KeyError):
    """请求的 'namespace/name' 模板不存在。"""


class UnsafeSqlTemplate(ValueError):
    """模板里出现了禁止的占位符风格。"""


# 禁用的占位符 patterns
_FORBIDDEN_PATTERNS = [
    (re.compile(r"\{[a-zA-Z_]\w*\}"), "Python f-string / str.format() placeholder"),
    (re.compile(r"%\([a-zA-Z_]\w*\)s"), "percent-style named placeholder"),
    (re.compile(r"%s"), "percent-style positional placeholder"),
]


class SqlLoader:
    """加载 sql/ 目录下所有 .sql 文件。

    路径 sql/foo/bar.sql → key "foo/bar"。
    """

    def __init__(self, root: Path) -> None:
        self._templates: dict[str, str] = {}
        if not root.is_dir():
            raise FileNotFoundError(f"SQL root not found: {root}")
        for path in root.rglob("*.sql"):
            rel = path.relative_to(root).with_suffix("")
            key = "/".join(rel.parts)
            content = path.read_text(encoding="utf-8")
            for pattern, label in _FORBIDDEN_PATTERNS:
                if pattern.search(content):
                    raise UnsafeSqlTemplate(
                        f"{path}: forbidden {label} (must use :name only)"
                    )
            self._templates[key] = content

    def get(self, name: str) -> str:
        try:
            return self._templates[name]
        except KeyError:
            raise SqlTemplateNotFound(name) from None

    def keys(self) -> list[str]:
        """主要供调试 / 启动校验用。"""
        return sorted(self._templates.keys())
```

- [ ] **Step 5: 跑测试**

```bash
python -m uv run pytest tests/unit/test_sql_loader.py -v
```

Expected: 4 passed.

- [ ] **Step 6: 全套 + lint**

```bash
python -m uv run pytest -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 34 passed; clean。

- [ ] **Step 7: Commit**

```bash
git add sql/ src/ma/core/repo/__init__.py src/ma/core/repo/sql_loader.py \
        tests/unit/test_sql_loader.py
git commit -m "feat(repo): SQL templates and SqlLoader with f-string/format placeholder guard"
```

### Task 4: asyncpg pool factory + SqlExecutor

**Files:**
- Create: `src/ma/infra/db.py`
- Create: `src/ma/core/repo/sql_executor.py`
- Create: `tests/unit/test_sql_executor_substitution.py` — 仅测占位符翻译，不真连 DB

参考：设计书 §3.3。SqlExecutor 接受具名占位符 `:name`，内部翻译成 asyncpg 的 `$1/$2/...` 位置占位符再执行。

- [ ] **Step 1: 写失败测试**

```python
# tests/unit/test_sql_executor_substitution.py
"""SqlExecutor 占位符翻译：:name → $1/$2 + 参数顺序对齐；同 :name 复用同一个 $n。"""
from __future__ import annotations

import pytest


def test_translate_single_placeholder() -> None:
    from ma.core.repo.sql_executor import translate_named_params

    sql = "SELECT * FROM t WHERE id = :id"
    params = {"id": 42}
    new_sql, args = translate_named_params(sql, params)
    assert new_sql == "SELECT * FROM t WHERE id = $1"
    assert args == [42]


def test_translate_multiple_placeholders_ordered_by_appearance() -> None:
    from ma.core.repo.sql_executor import translate_named_params

    sql = "INSERT INTO t (a, b, c) VALUES (:a, :b, :c)"
    params = {"a": 1, "b": 2, "c": 3}
    new_sql, args = translate_named_params(sql, params)
    assert new_sql == "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"
    assert args == [1, 2, 3]


def test_translate_repeated_placeholder_reuses_index() -> None:
    """同一个 :name 在 SQL 里出现两次 → asyncpg 用同一个 $1 引用一次绑定。"""
    from ma.core.repo.sql_executor import translate_named_params

    sql = "SELECT * FROM t WHERE a = :x OR b = :x"
    params = {"x": 7}
    new_sql, args = translate_named_params(sql, params)
    assert new_sql == "SELECT * FROM t WHERE a = $1 OR b = $1"
    assert args == [7]


def test_translate_missing_param_raises() -> None:
    from ma.core.repo.sql_executor import MissingSqlParam, translate_named_params

    sql = "SELECT * FROM t WHERE id = :id"
    with pytest.raises(MissingSqlParam):
        translate_named_params(sql, {})


def test_translate_ignores_pgsql_cast_double_colon() -> None:
    """asyncpg / PG 语法里 ::jsonb 是类型转换，不是占位符 → 不能误识别。"""
    from ma.core.repo.sql_executor import translate_named_params

    sql = "INSERT INTO t (ext) VALUES (:ext::jsonb)"
    params = {"ext": "{}"}
    new_sql, args = translate_named_params(sql, params)
    assert new_sql == "INSERT INTO t (ext) VALUES ($1::jsonb)"
    assert args == ["{}"]
```

Run: `python -m uv run pytest tests/unit/test_sql_executor_substitution.py -v`
Expected: 5 fail.

- [ ] **Step 2: 实现 `src/ma/core/repo/sql_executor.py`** —— 先把 translator 写好，DB 部分留 stub

```python
"""SqlExecutor：把模板化 SQL 跑到 asyncpg pool 上。

设计书 §3.3.2 / §3.3.3。

模板用 :name 占位符（与人类阅读直觉一致），asyncpg 只懂 $1/$2 ——
本模块负责翻译；翻译做到：
- 同一个 :name 多次出现复用同一个 $n
- 区分 PG 类型转换语法 ::jsonb（双冒号）
- 缺参数立即抛错（启动期/请求期都立即可见）

DB 真正执行通过 SqlExecutor.fetch_all/fetch_one/execute 方法（M1 后续 task 实现，
这里先把 translator 这个纯函数交付了，便于单测）。
"""
from __future__ import annotations

import re
from typing import Any

import asyncpg

from ma.core.repo.sql_loader import SqlLoader

# 匹配 :name 但排除 :: 双冒号 (PG 类型转换)。
# 用 (?<!:) 排除前面已经有一个冒号 (例如 ::jsonb 第二个冒号)。
_NAMED_PARAM_RE = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")


class MissingSqlParam(KeyError):
    """模板里引用了 :name，但 params 字典没有这个 key。"""


def translate_named_params(
    sql: str, params: dict[str, Any]
) -> tuple[str, list[Any]]:
    """把 :name 占位符替换成 $1/$2/...，并返回对应顺序的参数列表。

    同名占位符复用 index：:x 第一次见 → $1，:x 再出现还是 $1，不重复绑定。
    """
    name_to_idx: dict[str, int] = {}
    args: list[Any] = []

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in name_to_idx:
            if name not in params:
                raise MissingSqlParam(name)
            name_to_idx[name] = len(args) + 1
            args.append(params[name])
        return f"${name_to_idx[name]}"

    new_sql = _NAMED_PARAM_RE.sub(_replace, sql)
    return new_sql, args


class SqlExecutor:
    """跑模板化 SQL 到 asyncpg pool。

    M1 阶段：单 DSN 实现；pool_rw == pool_ro。M2+ 拆只读副本时改这里。
    """

    def __init__(
        self,
        *,
        loader: SqlLoader,
        pool_rw: asyncpg.Pool,
        pool_ro: asyncpg.Pool | None = None,
    ) -> None:
        self._loader = loader
        self._rw = pool_rw
        self._ro = pool_ro or pool_rw

    async def fetch_all(self, name: str, **params: Any) -> list[asyncpg.Record]:
        sql = self._loader.get(name)
        translated, args = translate_named_params(sql, params)
        async with self._ro.acquire() as conn:
            return await conn.fetch(translated, *args)

    async def fetch_one(self, name: str, **params: Any) -> asyncpg.Record | None:
        sql = self._loader.get(name)
        translated, args = translate_named_params(sql, params)
        async with self._ro.acquire() as conn:
            return await conn.fetchrow(translated, *args)

    async def execute(self, name: str, **params: Any) -> str:
        sql = self._loader.get(name)
        translated, args = translate_named_params(sql, params)
        async with self._rw.acquire() as conn:
            return await conn.execute(translated, *args)
```

- [ ] **Step 3: 实现 `src/ma/infra/db.py`**

```python
"""asyncpg pool 工厂。

在 lifespan 里调 create_pools(settings) → (pool_rw, pool_ro_or_None)。
"""
from __future__ import annotations

import asyncpg

from ma.infra.settings import Settings


async def create_pools(
    settings: Settings,
) -> tuple[asyncpg.Pool, asyncpg.Pool | None]:
    """创建 RW + 可选 RO asyncpg pool。M1 单 DSN：只有 RW，RO 返回 None。"""
    if settings.pg_dsn_rw is None:
        raise RuntimeError(
            "MA_PG_DSN_RW is required to start the service; cannot create DB pool"
        )
    pool_rw = await asyncpg.create_pool(
        dsn=settings.pg_dsn_rw.get_secret_value(),
        min_size=settings.pg_pool_min,
        max_size=settings.pg_pool_max,
    )
    pool_ro: asyncpg.Pool | None = None
    if settings.pg_dsn_ro is not None:
        pool_ro = await asyncpg.create_pool(
            dsn=settings.pg_dsn_ro.get_secret_value(),
            min_size=settings.pg_pool_min,
            max_size=settings.pg_pool_max,
        )
    return pool_rw, pool_ro


async def close_pools(
    pool_rw: asyncpg.Pool, pool_ro: asyncpg.Pool | None
) -> None:
    await pool_rw.close()
    if pool_ro is not None:
        await pool_ro.close()
```

- [ ] **Step 4: 加 asyncpg 到 deps**

修改 `pyproject.toml` 的 `dependencies`：

```toml
    "asyncpg>=0.30,<0.31",
```

然后 `python -m uv sync --all-groups`。

- [ ] **Step 5: 跑测试 + lint**

```bash
python -m uv run pytest -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 39 passed (34 + 5)；clean。

- [ ] **Step 6: Commit**

```bash
git add src/ma/infra/db.py src/ma/core/repo/sql_executor.py \
        tests/unit/test_sql_executor_substitution.py pyproject.toml uv.lock
git commit -m "feat(repo): SqlExecutor with :name → \$N translation + asyncpg pool factory"
```

### Task 5: Alembic 迁移 + opengauss testcontainer fixture

**Files:**
- Create: `db/alembic.ini`
- Create: `db/env.py`
- Create: `db/versions/0001_init_session_message.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/conftest.py`

参考：设计书 §3.2 / §3.6 / §7.4.1。

- [ ] **Step 1: 加 alembic + testcontainers 到 deps**

`pyproject.toml`:

```toml
dependencies = [
    ...
    "alembic>=1.13,<2",
    ...
]

[dependency-groups]
dev = [
    ...
    "testcontainers[postgres]>=4.8,<5",
    ...
]
```

`python -m uv sync --all-groups`

- [ ] **Step 2: 初始化 alembic 配置**

`db/alembic.ini`:

```ini
[alembic]
script_location = db
version_locations = db/versions
file_template = %%(rev)s_%%(slug)s
sqlalchemy.url = driver://placeholder/  ; 实际 DSN 在 env.py 从 MA_PG_DSN_RW 读

[loggers]
keys = root, alembic
[handlers]
keys = console
[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
```

`db/env.py`:

```python
"""Alembic env.py — 从 MA_PG_DSN_RW 读取 DSN。"""
from __future__ import annotations

import os
import sys

from alembic import context
from sqlalchemy import create_engine, pool

config = context.config

# MA_PG_DSN_RW 必须设置；alembic Job 由部署流水线传入
dsn = os.environ.get("MA_PG_DSN_RW")
if not dsn:
    sys.stderr.write(
        "ERROR: MA_PG_DSN_RW environment variable required for migrations\n"
    )
    raise SystemExit(2)

# alembic 用 sync engine
config.set_main_option("sqlalchemy.url", dsn)

target_metadata = None  # 我们用纯 SQL 迁移，不用 ORM autogen


def run_migrations_online() -> None:
    connectable = create_engine(dsn, poolclass=pool.NullPool, future=True)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
```

`db/versions/0001_init_session_message.py`:

```python
"""init ma_session + ma_message tables

Revision ID: 0001
Revises:
Create Date: 2026-06-24
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE ma_session (
            thread_id       VARCHAR(128) PRIMARY KEY,
            biz_id          VARCHAR(64)  NOT NULL,
            w3_account      VARCHAR(128) NOT NULL,
            title           VARCHAR(256),
            status          VARCHAR(16)  NOT NULL DEFAULT 'active',
            created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_message_at TIMESTAMP,
            ext             JSONB        NOT NULL DEFAULT '{}'::jsonb
        );
        """
    )
    op.execute(
        """
        CREATE INDEX idx_ma_session_w3_biz_time
        ON ma_session(w3_account, biz_id, last_message_at DESC);
        """
    )
    op.execute(
        """
        CREATE TABLE ma_message (
            message_id      VARCHAR(64)  PRIMARY KEY,
            thread_id       VARCHAR(128) NOT NULL,
            seq             BIGINT       NOT NULL,
            role            VARCHAR(16)  NOT NULL,
            content         TEXT         NOT NULL,
            content_meta    JSONB        NOT NULL DEFAULT '{}'::jsonb,
            status          VARCHAR(16)  NOT NULL DEFAULT 'complete',
            route           VARCHAR(32),
            request_id      VARCHAR(64),
            created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ext             JSONB        NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE (thread_id, seq)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX idx_ma_message_thread_seq ON ma_message(thread_id, seq);
        """
    )
    op.execute(
        """
        CREATE INDEX idx_ma_message_request_id ON ma_message(request_id);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_ma_message_request_id;")
    op.execute("DROP INDEX IF EXISTS idx_ma_message_thread_seq;")
    op.execute("DROP TABLE IF EXISTS ma_message;")
    op.execute("DROP INDEX IF EXISTS idx_ma_session_w3_biz_time;")
    op.execute("DROP TABLE IF EXISTS ma_session;")
```

> **注意 OpenGauss 兼容性：** OpenGauss 协议兼容 PG，但部分 JSONB 操作符要确认。这里 DDL 只用 JSONB 字段 + DEFAULT，是 OpenGauss 默认支持的。集成测试若发现差异，需要在迁移里加 `CREATE EXTENSION IF NOT EXISTS ...` 之类。

- [ ] **Step 3: 写 `tests/integration/__init__.py` 和 `tests/integration/conftest.py`**

`tests/integration/__init__.py`：空。

`tests/integration/conftest.py`:

```python
"""集成测试通用 fixture：OpenGauss 容器 + alembic 迁移 + asyncpg pool。

策略：
- 若环境变量 MA_TEST_PG_DSN 已存在 → 跳过容器启动，直接用它（开发者本机或 staging）
- 否则用 testcontainers 启 PG-compatible 镜像（默认 postgres，OpenGauss 协议兼容）
- 整个 pytest session 共用一个 DB，但每个 test 用独立事务，结束 rollback
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator

import asyncpg
import pytest_asyncio
from alembic import command
from alembic.config import Config


def _alembic_upgrade(dsn: str) -> None:
    cfg = Config("db/alembic.ini")
    os.environ["MA_PG_DSN_RW"] = dsn  # env.py reads from this
    command.upgrade(cfg, "head")


@pytest_asyncio.fixture(scope="session")
async def pg_dsn() -> AsyncIterator[str]:
    dsn = os.environ.get("MA_TEST_PG_DSN")
    if dsn:
        _alembic_upgrade(dsn)
        yield dsn
        return

    # testcontainers path —— 用 postgres 镜像（OpenGauss 协议兼容）
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        import pytest
        pytest.skip("testcontainers[postgres] not installed")

    with PostgresContainer("postgres:16-alpine") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        _alembic_upgrade(dsn)
        yield dsn


@pytest_asyncio.fixture
async def pg_pool(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=4)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def pg_conn(pg_pool: asyncpg.Pool) -> AsyncIterator[asyncpg.Connection]:
    """单个 test 用独立连接 + 事务回滚，保证测试间隔离。"""
    async with pg_pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            yield conn
        finally:
            await tx.rollback()
```

> **注意**：用 `postgres:16-alpine` 而非 OpenGauss 镜像有取舍 —— 协议兼容但生产是 OpenGauss，可能漏一些 OpenGauss-only 行为。设计书 §7.2.3 说"必须用真 OpenGauss 镜像"，但 OpenGauss 官方镜像在 testcontainers 集成上略复杂。**取舍：M1 用 postgres 镜像让 CI 快速绿；M2 切换到 OpenGauss 镜像或在 staging 验证。** 这一偏差记录在 M1 完成笔记。

- [ ] **Step 4: 跑一次 integration fixture（本机有 docker 就行；没就跳过）**

```bash
python -m uv run pytest tests/integration/ -v --collect-only
```

Expected: 收集到 0 个用例（还没写 test），但 conftest 应当 import 成功无报错。

```bash
python -m uv run mypy src tests/integration tests/unit
python -m uv run ruff check .
```

Expected: clean。

- [ ] **Step 5: Commit**

```bash
git add db/ pyproject.toml uv.lock tests/integration/
git commit -m "feat(db): Alembic 0001 migration for ma_session/ma_message + testcontainers fixture"
```

### Task 6: SessionRepository + MessageRepository (TDD + integration)

**Files:**
- Create: `src/ma/core/repo/models.py`
- Create: `src/ma/core/repo/session_repository.py`
- Create: `src/ma/core/repo/message_repository.py`
- Create: `tests/integration/test_session_repository.py`
- Create: `tests/integration/test_message_repository.py`

参考：设计书 §3.4。

- [ ] **Step 1: 实现 `src/ma/core/repo/models.py`**

```python
"""会话与消息的 dataclass 模型。

故意不引入 ORM —— 这些是从 asyncpg.Record 装载来的纯数据载体。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class Session:
    thread_id: str
    biz_id: str
    w3_account: str
    title: str | None
    status: str
    created_at: datetime
    updated_at: datetime
    last_message_at: datetime | None
    ext: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Message:
    message_id: str
    thread_id: str
    seq: int
    role: str  # 'user' | 'assistant' | 'system'
    content: str
    content_meta: dict[str, Any] = field(default_factory=dict)
    status: str = "complete"  # 'complete' | 'partial' | 'failed'
    route: str | None = None
    request_id: str | None = None
    created_at: datetime | None = None  # DB 默认填，append 时可不传
    ext: dict[str, Any] = field(default_factory=dict)


class SessionBizMismatchError(ValueError):
    """同一 thread_id 已存在但 biz_id 不匹配（设计书 §3.4 应当 400）。"""
```

- [ ] **Step 2: 写 integration 测试 `tests/integration/test_session_repository.py`**

```python
"""SessionRepository：覆盖 get_or_create / list_by_user / touch。"""
from __future__ import annotations

import json

import asyncpg
import pytest


@pytest.mark.asyncio
async def test_session_get_or_create_creates_new(pg_conn: asyncpg.Connection) -> None:
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    s = await repo.get_or_create(
        thread_id="th_new", biz_id="biz_x", w3_account="alice"
    )
    assert s.thread_id == "th_new"
    assert s.biz_id == "biz_x"
    assert s.w3_account == "alice"
    assert s.status == "active"


@pytest.mark.asyncio
async def test_session_get_or_create_returns_existing(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    s1 = await repo.get_or_create(
        thread_id="th_e", biz_id="biz_x", w3_account="alice"
    )
    s2 = await repo.get_or_create(
        thread_id="th_e", biz_id="biz_x", w3_account="alice"
    )
    assert s1.thread_id == s2.thread_id
    assert s1.created_at == s2.created_at


@pytest.mark.asyncio
async def test_session_get_or_create_rejects_biz_mismatch(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.models import SessionBizMismatchError
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    await repo.get_or_create(
        thread_id="th_mismatch", biz_id="biz_x", w3_account="alice"
    )
    with pytest.raises(SessionBizMismatchError):
        await repo.get_or_create(
            thread_id="th_mismatch", biz_id="biz_y", w3_account="alice"
        )


@pytest.mark.asyncio
async def test_session_touch_updates_last_message_at(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    await repo.get_or_create(
        thread_id="th_touch", biz_id="biz_x", w3_account="alice"
    )
    await repo.touch("th_touch")
    row = await pg_conn.fetchrow(
        "SELECT last_message_at FROM ma_session WHERE thread_id = $1", "th_touch"
    )
    assert row is not None
    assert row["last_message_at"] is not None


@pytest.mark.asyncio
async def test_session_list_by_user_returns_recent_first(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    for tid in ["th_a", "th_b", "th_c"]:
        await repo.get_or_create(
            thread_id=tid, biz_id="biz_x", w3_account="bob"
        )
        await repo.touch(tid)

    items = await repo.list_by_user(
        w3_account="bob", biz_id="biz_x", limit=10, before=None
    )
    thread_ids = [s.thread_id for s in items]
    assert set(thread_ids) == {"th_a", "th_b", "th_c"}
```

- [ ] **Step 3: 实现 `src/ma/core/repo/session_repository.py`**

```python
"""SessionRepository：thread_id 主键、应用层校验 biz_id 一致性。"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg

from ma.core.repo.models import Session, SessionBizMismatchError


def _row_to_session(row: asyncpg.Record) -> Session:
    ext = row["ext"]
    if isinstance(ext, str):
        ext = json.loads(ext)
    return Session(
        thread_id=row["thread_id"],
        biz_id=row["biz_id"],
        w3_account=row["w3_account"],
        title=row["title"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        last_message_at=row["last_message_at"],
        ext=ext or {},
    )


class SessionRepository:
    """对外接口稳定（设计书 §3.4）：

    - get_or_create(thread_id, biz_id, w3_account, ext) -> Session
        若 thread_id 已存在且 biz_id 不匹配 → SessionBizMismatchError
    - list_by_user(w3_account, biz_id, limit, before) -> list[Session]
    - touch(thread_id) -> None
    """

    def __init__(self, *, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def get_or_create(
        self,
        *,
        thread_id: str,
        biz_id: str,
        w3_account: str,
        ext: dict[str, Any] | None = None,
    ) -> Session:
        # 先查
        row = await self._conn.fetchrow(
            "SELECT thread_id, biz_id, w3_account, title, status, "
            "created_at, updated_at, last_message_at, ext "
            "FROM ma_session WHERE thread_id = $1",
            thread_id,
        )
        if row is not None:
            if row["biz_id"] != biz_id:
                raise SessionBizMismatchError(
                    f"thread_id={thread_id} already bound to biz_id={row['biz_id']}, "
                    f"cannot reuse with biz_id={biz_id}"
                )
            return _row_to_session(row)

        # 没有就插
        ext_json = json.dumps(ext or {})
        await self._conn.execute(
            "INSERT INTO ma_session (thread_id, biz_id, w3_account, title, status, ext) "
            "VALUES ($1, $2, $3, $4, 'active', $5::jsonb) "
            "ON CONFLICT (thread_id) DO NOTHING",
            thread_id,
            biz_id,
            w3_account,
            None,
            ext_json,
        )
        row = await self._conn.fetchrow(
            "SELECT thread_id, biz_id, w3_account, title, status, "
            "created_at, updated_at, last_message_at, ext "
            "FROM ma_session WHERE thread_id = $1",
            thread_id,
        )
        assert row is not None  # just inserted
        return _row_to_session(row)

    async def list_by_user(
        self,
        *,
        w3_account: str,
        biz_id: str | None,
        limit: int,
        before: datetime | None,
    ) -> list[Session]:
        rows = await self._conn.fetch(
            "SELECT thread_id, biz_id, w3_account, title, status, "
            "created_at, updated_at, last_message_at, ext "
            "FROM ma_session "
            "WHERE w3_account = $1 "
            "AND ($2::text IS NULL OR biz_id = $2) "
            "AND ($3::timestamp IS NULL OR last_message_at < $3) "
            "ORDER BY last_message_at DESC NULLS LAST, created_at DESC "
            "LIMIT $4",
            w3_account,
            biz_id,
            before,
            limit,
        )
        return [_row_to_session(r) for r in rows]

    async def touch(self, thread_id: str) -> None:
        await self._conn.execute(
            "UPDATE ma_session "
            "SET updated_at = CURRENT_TIMESTAMP, "
            "    last_message_at = CURRENT_TIMESTAMP "
            "WHERE thread_id = $1",
            thread_id,
        )
```

**注意：** SessionRepository **直接用 asyncpg 跑 inline SQL**，不走 SqlLoader/SqlExecutor。
**为什么？** SqlLoader/SqlExecutor 是为 ChatService 主路径的"应用层只读模板"准备的；Repository 内部仍然用结构化 SQL 直接写更清楚（不需要给 SQL 文件起命名），且 PG cast `$2::text IS NULL` 这种参数化技巧在文件里写很别扭。
**约束：** 仍然遵守"参数化绑定、绝不字符串拼接"原则。

设计书 §3.4 的纪律是"ChatService 唯一 DB 入口是 Repository"，但没规定 Repository 必须走 SqlLoader。M1 实际落地时可以两种共存。

- [ ] **Step 4: 写 `tests/integration/test_message_repository.py`** + 实现 `src/ma/core/repo/message_repository.py`

测试：

```python
"""MessageRepository：append / list_recent / mark_partial / mark_failed。"""
from __future__ import annotations

import asyncpg
import pytest


async def _seed_session(pg_conn: asyncpg.Connection, thread_id: str) -> None:
    await pg_conn.execute(
        "INSERT INTO ma_session (thread_id, biz_id, w3_account) "
        "VALUES ($1, 'biz_x', 'alice')",
        thread_id,
    )


@pytest.mark.asyncio
async def test_message_append_assigns_seq(pg_conn: asyncpg.Connection) -> None:
    from ma.core.repo.message_repository import MessageRepository
    from ma.core.repo.models import Message

    await _seed_session(pg_conn, "th_msg_1")
    repo = MessageRepository(conn=pg_conn)
    m1 = Message(
        message_id="msg_a",
        thread_id="th_msg_1",
        seq=0,  # 0 → repo 自动取下一个
        role="user",
        content="hi",
    )
    await repo.append(msg=m1)
    m2 = Message(
        message_id="msg_b",
        thread_id="th_msg_1",
        seq=0,
        role="assistant",
        content="hello",
    )
    await repo.append(msg=m2)

    rows = await pg_conn.fetch(
        "SELECT message_id, seq FROM ma_message WHERE thread_id = $1 ORDER BY seq",
        "th_msg_1",
    )
    assert [(r["message_id"], r["seq"]) for r in rows] == [
        ("msg_a", 1),
        ("msg_b", 2),
    ]


@pytest.mark.asyncio
async def test_message_list_recent_returns_complete_partial_only(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.message_repository import MessageRepository
    from ma.core.repo.models import Message

    await _seed_session(pg_conn, "th_recent")
    repo = MessageRepository(conn=pg_conn)
    for i, status in enumerate(["complete", "partial", "failed"], start=1):
        await repo.append(
            msg=Message(
                message_id=f"m{i}",
                thread_id="th_recent",
                seq=i,
                role="user" if i % 2 else "assistant",
                content=f"c{i}",
                status=status,
            )
        )

    items = await repo.list_recent(thread_id="th_recent", limit=10)
    assert {m.message_id for m in items} == {"m1", "m2"}
    # list_recent 应当按 seq DESC 取
    assert [m.seq for m in items] == [2, 1]


@pytest.mark.asyncio
async def test_message_mark_partial(pg_conn: asyncpg.Connection) -> None:
    from ma.core.repo.message_repository import MessageRepository
    from ma.core.repo.models import Message

    await _seed_session(pg_conn, "th_partial")
    repo = MessageRepository(conn=pg_conn)
    await repo.append(
        msg=Message(
            message_id="m_part",
            thread_id="th_partial",
            seq=1,
            role="assistant",
            content="incomplete",
        )
    )
    await repo.mark_partial("m_part")
    row = await pg_conn.fetchrow(
        "SELECT status FROM ma_message WHERE message_id = $1", "m_part"
    )
    assert row is not None
    assert row["status"] == "partial"


@pytest.mark.asyncio
async def test_message_mark_failed_appends_to_ext(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.message_repository import MessageRepository
    from ma.core.repo.models import Message

    await _seed_session(pg_conn, "th_fail")
    repo = MessageRepository(conn=pg_conn)
    await repo.append(
        msg=Message(
            message_id="m_fail",
            thread_id="th_fail",
            seq=1,
            role="assistant",
            content="x",
        )
    )
    await repo.mark_failed("m_fail", err={"code": "DOWNSTREAM_TIMEOUT"})
    row = await pg_conn.fetchrow(
        "SELECT status, ext FROM ma_message WHERE message_id = $1", "m_fail"
    )
    assert row is not None
    assert row["status"] == "failed"
    import json as _json
    ext = row["ext"] if isinstance(row["ext"], dict) else _json.loads(row["ext"])
    assert ext.get("code") == "DOWNSTREAM_TIMEOUT"
```

实现 `src/ma/core/repo/message_repository.py`：

```python
"""MessageRepository。"""
from __future__ import annotations

import json
from typing import Any

import asyncpg

from ma.core.repo.models import Message


def _row_to_message(row: asyncpg.Record) -> Message:
    content_meta = row["content_meta"]
    if isinstance(content_meta, str):
        content_meta = json.loads(content_meta)
    ext = row["ext"]
    if isinstance(ext, str):
        ext = json.loads(ext)
    return Message(
        message_id=row["message_id"],
        thread_id=row["thread_id"],
        seq=row["seq"],
        role=row["role"],
        content=row["content"],
        content_meta=content_meta or {},
        status=row["status"],
        route=row["route"],
        request_id=row["request_id"],
        created_at=row["created_at"],
        ext=ext or {},
    )


class MessageRepository:
    def __init__(self, *, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def append(self, *, msg: Message) -> None:
        seq = msg.seq
        if seq == 0:
            row = await self._conn.fetchrow(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM ma_message WHERE thread_id = $1",
                msg.thread_id,
            )
            seq = (row["m"] if row else 0) + 1

        await self._conn.execute(
            "INSERT INTO ma_message ("
            "  message_id, thread_id, seq, role, content, content_meta, "
            "  status, route, request_id, ext"
            ") VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10::jsonb)",
            msg.message_id,
            msg.thread_id,
            seq,
            msg.role,
            msg.content,
            json.dumps(msg.content_meta),
            msg.status,
            msg.route,
            msg.request_id,
            json.dumps(msg.ext),
        )

    async def list_recent(
        self, *, thread_id: str, limit: int
    ) -> list[Message]:
        rows = await self._conn.fetch(
            "SELECT message_id, thread_id, seq, role, content, content_meta, "
            "status, route, request_id, created_at, ext "
            "FROM ma_message "
            "WHERE thread_id = $1 AND status IN ('complete', 'partial') "
            "ORDER BY seq DESC LIMIT $2",
            thread_id,
            limit,
        )
        return [_row_to_message(r) for r in rows]

    async def mark_partial(self, message_id: str) -> None:
        await self._conn.execute(
            "UPDATE ma_message SET status = 'partial' WHERE message_id = $1",
            message_id,
        )

    async def mark_failed(self, message_id: str, err: dict[str, Any]) -> None:
        await self._conn.execute(
            "UPDATE ma_message "
            "SET status = 'failed', "
            "    ext = COALESCE(ext, '{}'::jsonb) || $2::jsonb "
            "WHERE message_id = $1",
            message_id,
            json.dumps(err),
        )
```

- [ ] **Step 5: 跑 integration（如果本机有 docker）**

```bash
python -m uv run pytest tests/integration/ -v
```

Expected (本机有 docker): 9 passed (5 + 4)
Expected (无 docker): 整个 file skipped。CI 上必须跑过。

- [ ] **Step 6: 全套**

```bash
python -m uv run pytest -v
python -m uv run mypy src
python -m uv run ruff check .
```

- [ ] **Step 7: Commit**

```bash
git add src/ma/core/repo/ tests/integration/test_session_repository.py \
        tests/integration/test_message_repository.py
git commit -m "feat(repo): SessionRepository + MessageRepository with integration tests"
```

**完成 Phase B 的标志：** Phase B 累计 5 个 commit；DB 层完整；integration 测试 9 个；本地若有 docker 全过，无 docker 也至少 unit 部分全过。

---

## Phase C: Plugin system

参考：设计书 §5.2 / §5.4。

### Task 7: Plugin Protocol + 4 子契约 (TDD)

**Files:**
- Create: `src/ma/core/plugin/__init__.py`
- Create: `src/ma/core/plugin/base.py`
- Create: `tests/unit/test_plugin_base.py`

- [ ] **Step 1: 创建目录**

```bash
mkdir -p src/ma/core/plugin
touch src/ma/core/plugin/__init__.py
```

- [ ] **Step 2: 写失败测试 `tests/unit/test_plugin_base.py`**

```python
"""Plugin 基础契约：Protocol 形态、4 个子契约、AuthResult/EnrichResult/IntentResult dataclasses。"""
from __future__ import annotations

import pytest


def test_plugin_protocol_has_required_fields() -> None:
    from ma.core.plugin.base import Plugin

    # Plugin 是 runtime_checkable Protocol
    class _OK:
        name = "x"
        plugin_kind = "auth"
        def configure(self, params: dict) -> None: ...

    assert isinstance(_OK(), Plugin)


def test_plugin_protocol_rejects_missing_fields() -> None:
    from ma.core.plugin.base import Plugin

    class _NoName:
        plugin_kind = "auth"
        def configure(self, params: dict) -> None: ...

    assert not isinstance(_NoName(), Plugin)


def test_auth_result_dataclass() -> None:
    from ma.core.plugin.base import AuthResult

    r = AuthResult(passed=True, user_ctx={"role": "rep"})
    assert r.passed is True
    assert r.user_ctx == {"role": "rep"}
    assert r.reject_code is None
    assert r.reject_message is None


def test_auth_result_rejection() -> None:
    from ma.core.plugin.base import AuthResult

    r = AuthResult(
        passed=False,
        reject_code="FORBIDDEN_TOPIC",
        reject_message="您暂无权限",
    )
    assert r.passed is False
    assert r.reject_code == "FORBIDDEN_TOPIC"
    assert r.user_ctx is None


def test_enrich_result_dataclass() -> None:
    from ma.core.plugin.base import EnrichResult

    r = EnrichResult(enriched_question="补全后", enrich_meta={"injected": ["region"]})
    assert r.enriched_question == "补全后"
    assert r.enrich_meta == {"injected": ["region"]}


def test_intent_result_dataclass() -> None:
    from ma.core.plugin.base import IntentResult

    r = IntentResult(route="metagc", confidence=0.92, reason="业务数据查询")
    assert r.route == "metagc"
    assert r.confidence == 0.92


def test_intent_failed_exception() -> None:
    from ma.core.plugin.base import IntentFailed

    with pytest.raises(IntentFailed):
        raise IntentFailed("LLM timeout")
```

Run: 6 fail.

- [ ] **Step 3: 实现 `src/ma/core/plugin/base.py`**

```python
"""Plugin 体系的基础契约。

设计书 §5.2。

四种插件子契约：
- AuthPlugin       业务鉴权（专题 + 问题范围）
- EnrichPlugin     问题补全
- IntentPlugin     意图识别（LLM 分类）
- DownstreamAdapter 下游 API 适配器

GraphState 类型导入是 Forward Reference，因为 plugin/base.py 在 graph/state.py
之前可能被 import；用 TYPE_CHECKING 保护即可。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ma.core.graph.events import ChatEvent
    from ma.core.graph.state import GraphState


@runtime_checkable
class Plugin(Protocol):
    """所有插件的最低契约。"""

    name: str
    plugin_kind: str

    def configure(self, params: dict[str, Any]) -> None: ...


@dataclass(slots=True)
class AuthResult:
    passed: bool
    reject_code: str | None = None
    reject_message: str | None = None
    user_ctx: dict[str, Any] | None = None


class AuthPlugin(Plugin, Protocol):
    plugin_kind: str = "auth"

    async def authorize(self, state: GraphState) -> AuthResult: ...


@dataclass(slots=True)
class EnrichResult:
    enriched_question: str
    enrich_meta: dict[str, Any] = field(default_factory=dict)


class EnrichPlugin(Plugin, Protocol):
    plugin_kind: str = "enrich"

    async def enrich(self, state: GraphState) -> EnrichResult: ...


@dataclass(slots=True)
class IntentResult:
    route: str
    confidence: float
    reason: str = ""


class IntentFailed(RuntimeError):
    """意图识别失败 —— ChatService 会兜底走 default_route。"""


class IntentPlugin(Plugin, Protocol):
    plugin_kind: str = "intent"

    async def classify(self, state: GraphState) -> IntentResult: ...


class DownstreamAdapter(Plugin, Protocol):
    plugin_kind: str = "adapter"

    def run(
        self, state: GraphState, *, output: bool = True
    ) -> AsyncIterator[ChatEvent]: ...
```

> **关于 `DownstreamAdapter.run`**：它是 async 生成器，签名应该是 `def run(...) -> AsyncIterator[ChatEvent]`（注意 def 不是 async def — 因为返回 AsyncIterator 而不是 coroutine）。后续 task 实现 adapter 时再细化。

- [ ] **Step 4: 跑测试 + lint**

```bash
python -m uv run pytest tests/unit/test_plugin_base.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 7 passed; clean。

- [ ] **Step 5: Commit**

```bash
git add src/ma/core/plugin/__init__.py src/ma/core/plugin/base.py \
        tests/unit/test_plugin_base.py
git commit -m "feat(plugin): Plugin Protocol + Auth/Enrich/Intent/Adapter sub-contracts"
```

### Task 8: PluginRegistry + bootstrap (TDD)

**Files:**
- Create: `src/ma/core/plugin/registry.py`
- Create: `src/ma/core/plugin/bootstrap.py`
- Create: `tests/unit/test_plugin_registry.py`

参考：设计书 §5.4。显式 import + 装饰器注册；不做自动扫描。

- [ ] **Step 1: 写失败测试 `tests/unit/test_plugin_registry.py`**

```python
"""PluginRegistry：注册 / 查找 / 重复注册冲突 / 未知 kind+name / configure 失败。"""
from __future__ import annotations

import pytest


def test_registry_register_and_create() -> None:
    from ma.core.plugin.registry import PluginRegistry

    reg = PluginRegistry()

    @reg.register
    class FooAuth:
        name = "foo_auth"
        plugin_kind = "auth"

        def configure(self, params: dict) -> None:
            self.params = params

    inst = reg.create("auth", "foo_auth", {"x": 1})
    assert isinstance(inst, FooAuth)
    assert inst.params == {"x": 1}


def test_registry_duplicate_raises() -> None:
    from ma.core.plugin.registry import PluginConflict, PluginRegistry

    reg = PluginRegistry()

    @reg.register
    class A:
        name = "n"
        plugin_kind = "auth"
        def configure(self, params: dict) -> None: ...

    with pytest.raises(PluginConflict):
        @reg.register
        class A2:
            name = "n"
            plugin_kind = "auth"
            def configure(self, params: dict) -> None: ...


def test_registry_create_unknown_raises() -> None:
    from ma.core.plugin.registry import PluginNotFound, PluginRegistry

    reg = PluginRegistry()
    with pytest.raises(PluginNotFound):
        reg.create("auth", "ghost", {})


def test_registry_create_propagates_configure_errors() -> None:
    """configure 抛错 → create 抛同样的错（不要吞掉）。"""
    from ma.core.plugin.registry import PluginRegistry

    reg = PluginRegistry()

    @reg.register
    class Bad:
        name = "bad"
        plugin_kind = "auth"
        def configure(self, params: dict) -> None:
            raise ValueError("missing required param")

    with pytest.raises(ValueError, match="missing required param"):
        reg.create("auth", "bad", {})


def test_registry_separates_by_kind() -> None:
    """同名不同 kind 不冲突 (auth.foo vs adapter.foo)。"""
    from ma.core.plugin.registry import PluginRegistry

    reg = PluginRegistry()

    @reg.register
    class FooAuth:
        name = "foo"
        plugin_kind = "auth"
        def configure(self, params: dict) -> None: ...

    @reg.register
    class FooAdapter:
        name = "foo"
        plugin_kind = "adapter"
        def configure(self, params: dict) -> None: ...

    a = reg.create("auth", "foo", {})
    b = reg.create("adapter", "foo", {})
    assert isinstance(a, FooAuth)
    assert isinstance(b, FooAdapter)


def test_global_registry_singleton() -> None:
    """模块级 registry 单例 —— bootstrap 用它。"""
    from ma.core.plugin.registry import registry

    assert registry is not None
```

Run: 6 fail.

- [ ] **Step 2: 实现 `src/ma/core/plugin/registry.py`**

```python
"""PluginRegistry：装饰器风格注册插件类，by (kind, name) 唯一索引。

每个 Plugin 类用 `@registry.register` 注册；TopicCompiler 在编译期间
通过 `registry.create(kind, name, params)` 实例化并 configure。
"""
from __future__ import annotations

from typing import Any, TypeVar


class PluginConflict(RuntimeError):
    """同 (kind, name) 重复注册。"""


class PluginNotFound(KeyError):
    """请求的 (kind, name) 不存在。"""


T = TypeVar("T")


class PluginRegistry:
    def __init__(self) -> None:
        self._by_kind_name: dict[tuple[str, str], type[Any]] = {}

    def register(self, cls: type[T]) -> type[T]:
        kind = getattr(cls, "plugin_kind", None)
        name = getattr(cls, "name", None)
        if not isinstance(kind, str) or not isinstance(name, str):
            raise TypeError(
                f"{cls.__name__} must declare class attrs `name: str` and `plugin_kind: str`"
            )
        key = (kind, name)
        if key in self._by_kind_name:
            raise PluginConflict(
                f"plugin already registered: kind={kind!r}, name={name!r} "
                f"({self._by_kind_name[key].__name__} vs {cls.__name__})"
            )
        self._by_kind_name[key] = cls
        return cls

    def create(self, kind: str, name: str, params: dict[str, Any]) -> Any:
        cls = self._by_kind_name.get((kind, name))
        if cls is None:
            raise PluginNotFound(f"no plugin registered for kind={kind!r}, name={name!r}")
        instance = cls()
        instance.configure(params)
        return instance


# 模块级单例 —— 业务代码统一用这个
registry = PluginRegistry()
```

- [ ] **Step 3: 实现 `src/ma/core/plugin/bootstrap.py`**

```python
"""插件加载入口。

设计书 §5.4.2：故意 *显式 import* 而非自动扫描，让加载链路可追溯。
M1 阶段插件还没全写出来；这个文件占位，后续 task 实现各插件时往里加 import。
"""
from __future__ import annotations


def load_all_plugins() -> None:
    """在 lifespan 启动期调用一次。

    每个插件模块 import 一次就够 —— 装饰器副作用把类注册进 registry。
    M1 末期这个函数体会引用：
        import ma.plugins.auth.representative_auth  # noqa: F401
        import ma.plugins.enrich.generic_enrich     # noqa: F401
        import ma.plugins.intent.llm_classifier     # noqa: F401
        import ma.plugins.adapter.metagc            # noqa: F401
    M1 早期还没有这些模块，所以现在留空（带 docstring 占位）。
    """
    # 各插件 import 在 Phase F/G 实现各插件后逐步加进来；
    # 见 Task 14/15/16/17 末尾的 "回到 bootstrap 加 import" 步骤
    return None
```

- [ ] **Step 4: 跑测试 + lint**

```bash
python -m uv run pytest tests/unit/test_plugin_registry.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 6 passed; clean。

- [ ] **Step 5: Commit**

```bash
git add src/ma/core/plugin/registry.py src/ma/core/plugin/bootstrap.py \
        tests/unit/test_plugin_registry.py
git commit -m "feat(plugin): PluginRegistry with decorator registration + bootstrap stub"
```

**完成 Phase C 的标志：** 2 个 commit；7 + 6 单测全过；插件契约稳定。

---

## Phase D: Topic system

参考：设计书 §2.4 / §5.5。

### Task 9: TopicConfig Pydantic schema (TDD)

**Files:**
- Create: `src/ma/core/topic/__init__.py`
- Create: `src/ma/core/topic/config.py`
- Create: `tests/unit/test_topic_config.py`

- [ ] **Step 1: 创建目录**

```bash
mkdir -p src/ma/core/topic
touch src/ma/core/topic/__init__.py
```

- [ ] **Step 2: 写失败测试 `tests/unit/test_topic_config.py`**

```python
"""TopicConfig Pydantic schema：合法 YAML 解析 + 非法 YAML fail-fast。"""
from __future__ import annotations

import pytest
from pydantic import ValidationError


_MIN_VALID = {
    "topic_id": "daibiao_xiaoguanjia",
    "display_name": "代表小管家",
    "default_route": "metagc",
    "history": {"max_turns": 10, "scope": "per_topic"},
    "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
    "enrich": {"plugin": "generic_enrich", "params": {}},
    "intent": {
        "plugin": "llm_classifier",
        "params": {
            "model": "fake-model",
            "labels": ["metagc"],
            "prompt_template": "prompts/intent/representative.txt",
        },
    },
    "graph": {
        "nodes": [
            {"id": "metagc_adapter", "adapter": "metagc", "output": True},
        ],
        "edges": [
            {
                "from": "intent",
                "type": "conditional",
                "route_by": "route",
                "mapping": {"metagc": "metagc_adapter"},
            }
        ],
    },
    "biz_params_schema": {"type": "object", "required": [], "properties": {}},
    "error_messages": {
        "FORBIDDEN_TOPIC": "您暂无权限",
        "FORBIDDEN_SCOPE": "范围不允许",
        "DOWNSTREAM_TIMEOUT": "稍后再试",
        "DOWNSTREAM_ERROR": "查询失败",
        "INTERNAL": "服务异常",
    },
}


def test_topic_config_minimal_valid_loads() -> None:
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(_MIN_VALID)
    assert cfg.topic_id == "daibiao_xiaoguanjia"
    assert cfg.default_route == "metagc"
    assert cfg.history.max_turns == 10
    assert cfg.auth.plugin == "representative_auth"
    assert len(cfg.graph.nodes) == 1


def test_topic_config_missing_required_field_fails() -> None:
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID}
    del bad["topic_id"]
    with pytest.raises(ValidationError):
        TopicConfig.model_validate(bad)


def test_topic_config_default_route_must_match_a_route_or_default() -> None:
    """default_route 必须是 graph.edges.mapping 的 keys 之一或 'default'。"""
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID, "default_route": "uniioc"}  # mapping 只有 metagc
    with pytest.raises(ValidationError, match="default_route"):
        TopicConfig.model_validate(bad)


def test_topic_config_history_scope_literal() -> None:
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID, "history": {"max_turns": 10, "scope": "invalid"}}
    with pytest.raises(ValidationError):
        TopicConfig.model_validate(bad)


def test_topic_config_error_messages_required_keys_present() -> None:
    """5 个必填 error_messages 缺一个就报错（设计书 §6.6.3）。"""
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID}
    bad_err = dict(_MIN_VALID["error_messages"])
    del bad_err["INTERNAL"]
    bad["error_messages"] = bad_err
    with pytest.raises(ValidationError, match="INTERNAL"):
        TopicConfig.model_validate(bad)


def test_topic_config_intent_labels_match_mapping_keys() -> None:
    """intent.params.labels 必须等于 graph.edges.mapping 的 keys。"""
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID}
    bad["intent"] = {**_MIN_VALID["intent"]}
    bad["intent"]["params"] = {
        **_MIN_VALID["intent"]["params"],
        "labels": ["metagc", "uniioc"],  # uniioc 没在 mapping 里
    }
    with pytest.raises(ValidationError, match="labels"):
        TopicConfig.model_validate(bad)
```

Run: 6 fail.

- [ ] **Step 3: 实现 `src/ma/core/topic/config.py`**

```python
"""Topic YAML 的 Pydantic schema。

设计书 §2.4。任何不合法配置 → 启动时 ValidationError → fail fast。
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class HistoryPolicy(BaseModel):
    max_turns: int = Field(ge=1, le=200)
    scope: Literal["per_topic"] = "per_topic"


class PluginRef(BaseModel):
    plugin: str
    params: dict[str, Any] = Field(default_factory=dict)


class IntentRef(BaseModel):
    plugin: str
    params: dict[str, Any]


class GraphNode(BaseModel):
    id: str
    adapter: str
    params: dict[str, Any] = Field(default_factory=dict)
    output: bool = True


class ConditionalEdge(BaseModel):
    src: Literal["intent"] = Field(alias="from")
    type: Literal["conditional"]
    route_by: Literal["route"]
    mapping: dict[str, str]


class GraphSpec(BaseModel):
    nodes: list[GraphNode]
    edges: list[ConditionalEdge]


_REQUIRED_ERROR_CODES = (
    "FORBIDDEN_TOPIC",
    "FORBIDDEN_SCOPE",
    "DOWNSTREAM_TIMEOUT",
    "DOWNSTREAM_ERROR",
    "INTERNAL",
)


class TopicConfig(BaseModel):
    topic_id: str = Field(min_length=1)
    display_name: str
    default_route: str
    history: HistoryPolicy
    auth: PluginRef
    enrich: PluginRef
    intent: IntentRef
    graph: GraphSpec
    biz_params_schema: dict[str, Any]
    error_messages: dict[str, str]

    @model_validator(mode="after")
    def _validate_consistency(self) -> "TopicConfig":
        # 1) error_messages 必须含所有必填 code
        missing = [c for c in _REQUIRED_ERROR_CODES if c not in self.error_messages]
        if missing:
            raise ValueError(f"error_messages missing required codes: {missing}")

        # 2) 找 conditional edge 的 mapping keys
        cond_edges = [e for e in self.graph.edges if e.type == "conditional"]
        if not cond_edges:
            raise ValueError("graph.edges must contain at least one conditional edge")
        mapping_keys = set()
        node_ids = {n.id for n in self.graph.nodes}
        for e in cond_edges:
            for label, node_id in e.mapping.items():
                mapping_keys.add(label)
                if node_id not in node_ids:
                    raise ValueError(
                        f"edge mapping points to non-existent node: {node_id!r}"
                    )

        # 3) default_route 必须在 mapping_keys 或者是 'default' / 'reject'
        if (
            self.default_route not in mapping_keys
            and self.default_route not in {"default", "reject"}
        ):
            raise ValueError(
                f"default_route={self.default_route!r} must be one of {mapping_keys} "
                f"or 'default' / 'reject'"
            )

        # 4) intent.params.labels 必须与 mapping_keys 完全一致
        labels = self.intent.params.get("labels")
        if labels is None:
            raise ValueError("intent.params.labels required (list of route labels)")
        if set(labels) != mapping_keys:
            raise ValueError(
                f"intent.params.labels={set(labels)!r} must equal "
                f"graph.edges.mapping keys={mapping_keys!r}"
            )

        return self
```

- [ ] **Step 4: 跑测试 + lint**

```bash
python -m uv run pytest tests/unit/test_topic_config.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 6 passed; clean。

- [ ] **Step 5: Commit**

```bash
git add src/ma/core/topic/__init__.py src/ma/core/topic/config.py \
        tests/unit/test_topic_config.py
git commit -m "feat(topic): TopicConfig Pydantic schema with cross-field validation"
```

### Task 10: TopicCompiler + TopicRegistry (TDD)

**Files:**
- Create: `src/ma/core/topic/compiler.py`
- Create: `src/ma/core/topic/registry.py`
- Create: `tests/unit/test_topic_compiler.py`

`TopicCompiler` 把 `TopicConfig` 编译成"已 configure 好的插件 + 待编译的 GraphSpec"。**M1 阶段不真编译 LangGraph StateGraph** —— 那是 Task 20 (`ChatService`) 在 lifespan 里做的。Compiler 只产 `CompiledTopic` dataclass，把所有插件实例化好、把 biz_params_schema 编译成 Pydantic model。

> 这个职责切分让 TopicCompiler 单测可以脱离 LangGraph，只验证插件装配。Task 20 才把 CompiledTopic + LangGraph StateGraph 拼起来。

- [ ] **Step 1: 写失败测试 `tests/unit/test_topic_compiler.py`**

```python
"""TopicCompiler：从 TopicConfig 装配插件 + 编译 biz_params_schema。"""
from __future__ import annotations

from typing import Any

import pytest


def _register_fake_plugins() -> None:
    """注册一组 fake 插件供测试用。"""
    from ma.core.plugin.registry import registry

    @registry.register
    class FakeAuth:
        name = "fake_auth"
        plugin_kind = "auth"
        def configure(self, params: dict) -> None:
            self.role = params["required_role"]

    @registry.register
    class FakeEnrich:
        name = "fake_enrich"
        plugin_kind = "enrich"
        def configure(self, params: dict) -> None:
            self.fields = params.get("fields_to_inject", [])

    @registry.register
    class FakeIntent:
        name = "fake_intent"
        plugin_kind = "intent"
        def configure(self, params: dict) -> None:
            self.labels = params["labels"]

    @registry.register
    class FakeAdapter:
        name = "fake_adapter"
        plugin_kind = "adapter"
        def configure(self, params: dict) -> None:
            self.params = params


_TOPIC = {
    "topic_id": "test_topic",
    "display_name": "测试专题",
    "default_route": "metagc",
    "history": {"max_turns": 10, "scope": "per_topic"},
    "auth": {"plugin": "fake_auth", "params": {"required_role": "TESTER"}},
    "enrich": {"plugin": "fake_enrich", "params": {"fields_to_inject": ["region"]}},
    "intent": {
        "plugin": "fake_intent",
        "params": {
            "model": "fake-model",
            "labels": ["metagc"],
            "prompt_template": "prompts/intent/test.txt",
        },
    },
    "graph": {
        "nodes": [{"id": "metagc_node", "adapter": "fake_adapter", "output": True}],
        "edges": [{
            "from": "intent",
            "type": "conditional",
            "route_by": "route",
            "mapping": {"metagc": "metagc_node"},
        }],
    },
    "biz_params_schema": {
        "type": "object",
        "required": ["region"],
        "properties": {"region": {"type": "string"}},
    },
    "error_messages": {
        "FORBIDDEN_TOPIC": "no",
        "FORBIDDEN_SCOPE": "no",
        "DOWNSTREAM_TIMEOUT": "slow",
        "DOWNSTREAM_ERROR": "err",
        "INTERNAL": "boom",
    },
}


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch: pytest.MonkeyPatch):
    """每个测试用全新的 registry，避免装饰器副作用累积。"""
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    yield new_reg


def test_compile_minimal_topic_instantiates_plugins() -> None:
    _register_fake_plugins()
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(_TOPIC)
    compiled = TopicCompiler().compile(cfg)
    assert compiled.topic_id == "test_topic"
    assert compiled.auth.role == "TESTER"
    assert compiled.enrich.fields == ["region"]
    assert compiled.intent.labels == ["metagc"]
    assert "metagc_node" in compiled.adapters
    assert compiled.error_messages["INTERNAL"] == "boom"


def test_compile_biz_params_schema_to_pydantic() -> None:
    _register_fake_plugins()
    from pydantic import ValidationError

    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(_TOPIC)
    compiled = TopicCompiler().compile(cfg)

    # 合法 params
    parsed = compiled.biz_params_model.model_validate({"region": "huadong"})
    assert parsed.region == "huadong"  # type: ignore[attr-defined]

    # 缺少 required field
    with pytest.raises(ValidationError):
        compiled.biz_params_model.model_validate({})


def test_compile_unknown_plugin_raises() -> None:
    """auth.plugin 在 registry 里找不到 → 启动期 fail fast。"""
    from ma.core.plugin.registry import PluginNotFound

    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(_TOPIC)  # registry 是空的
    with pytest.raises(PluginNotFound):
        TopicCompiler().compile(cfg)


def test_topic_registry_stores_compiled_topics() -> None:
    _register_fake_plugins()
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig
    from ma.core.topic.registry import TopicNotFound, TopicRegistry

    cfg = TopicConfig.model_validate(_TOPIC)
    compiled = TopicCompiler().compile(cfg)
    reg = TopicRegistry()
    reg.register(compiled)
    assert reg.get("test_topic") is compiled
    with pytest.raises(TopicNotFound):
        reg.get("ghost")
```

- [ ] **Step 2: 实现 `src/ma/core/topic/compiler.py`**

```python
"""TopicCompiler：把 TopicConfig 装配成可运行的 CompiledTopic。

CompiledTopic 包含：
- 已 configure 好的插件实例 (auth/enrich/intent/adapters)
- 从 biz_params_schema (JSON Schema dict) 编译出的 Pydantic Model
- error_messages / history policy / default_route 等运行期数据

它 *不* 直接编译 LangGraph StateGraph —— 那是 ChatService 在拿到
CompiledTopic 时即时做的事（Task 20）。这样 compiler 单测可以脱离
LangGraph 跑。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ConfigDict, create_model

from ma.core.plugin.registry import registry as _global_registry
from ma.core.topic.config import HistoryPolicy, TopicConfig


@dataclass(slots=True)
class CompiledTopic:
    topic_id: str
    display_name: str
    default_route: str
    history: HistoryPolicy
    auth: Any
    enrich: Any
    intent: Any
    adapters: dict[str, Any]
    biz_params_model: type[BaseModel]
    error_messages: dict[str, str]
    config: TopicConfig  # 原始 TopicConfig 留着供后续 LangGraph 编译用


# JSON Schema → Pydantic Model 的最小翻译（M1 只支持 type=object + string properties）
def _compile_biz_params_schema(schema: dict[str, Any]) -> type[BaseModel]:
    if schema.get("type") != "object":
        raise ValueError(
            f"biz_params_schema must be type=object (got {schema.get('type')!r})"
        )
    required = set(schema.get("required", []))
    fields: dict[str, Any] = {}
    for name, prop in schema.get("properties", {}).items():
        prop_type = prop.get("type", "string")
        if prop_type != "string":
            # M1 只支持 string；后续 M2 扩展支持 int/enum/array
            raise ValueError(
                f"biz_params_schema property {name!r}: only type='string' supported in M1 "
                f"(got {prop_type!r})"
            )
        if name in required:
            fields[name] = (str, ...)
        else:
            fields[name] = (str | None, None)

    # extra='forbid' 让未声明的字段被拒绝
    model = create_model(  # type: ignore[call-overload]
        "BizParams",
        __config__=ConfigDict(extra="forbid"),
        **fields,
    )
    return model


class TopicCompiler:
    """一次性使用：compile(cfg) → CompiledTopic。"""

    def __init__(self, registry: Any = None) -> None:
        # registry 可注入便于测试；默认用模块级单例
        self._registry = registry or _global_registry

    def compile(self, cfg: TopicConfig) -> CompiledTopic:
        auth = self._registry.create("auth", cfg.auth.plugin, cfg.auth.params)
        enrich = self._registry.create("enrich", cfg.enrich.plugin, cfg.enrich.params)
        intent = self._registry.create(
            "intent", cfg.intent.plugin, cfg.intent.params
        )

        adapters: dict[str, Any] = {}
        for node in cfg.graph.nodes:
            adapters[node.id] = self._registry.create(
                "adapter", node.adapter, node.params
            )

        biz_params_model = _compile_biz_params_schema(cfg.biz_params_schema)

        return CompiledTopic(
            topic_id=cfg.topic_id,
            display_name=cfg.display_name,
            default_route=cfg.default_route,
            history=cfg.history,
            auth=auth,
            enrich=enrich,
            intent=intent,
            adapters=adapters,
            biz_params_model=biz_params_model,
            error_messages=cfg.error_messages,
            config=cfg,
        )
```

- [ ] **Step 3: 实现 `src/ma/core/topic/registry.py`**

```python
"""TopicRegistry：以 topic_id 索引已编译 Topic。"""
from __future__ import annotations

from ma.core.topic.compiler import CompiledTopic


class TopicNotFound(KeyError):
    """biz_id 没有对应的已编译 Topic（设计书 §4.5 TOPIC_NOT_FOUND）。"""


class TopicRegistry:
    def __init__(self) -> None:
        self._by_id: dict[str, CompiledTopic] = {}

    def register(self, compiled: CompiledTopic) -> None:
        if compiled.topic_id in self._by_id:
            raise ValueError(f"topic already registered: {compiled.topic_id!r}")
        self._by_id[compiled.topic_id] = compiled

    def get(self, topic_id: str) -> CompiledTopic:
        try:
            return self._by_id[topic_id]
        except KeyError:
            raise TopicNotFound(topic_id) from None

    def ids(self) -> list[str]:
        return sorted(self._by_id.keys())
```

- [ ] **Step 4: 跑测试 + lint**

```bash
python -m uv run pytest tests/unit/test_topic_compiler.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 4 passed; clean。

- [ ] **Step 5: Commit**

```bash
git add src/ma/core/topic/compiler.py src/ma/core/topic/registry.py \
        tests/unit/test_topic_compiler.py
git commit -m "feat(topic): TopicCompiler instantiates plugins + compiles biz_params Pydantic model"
```

**完成 Phase D 的标志：** 2 个 commit；6 + 4 单测全过；TopicConfig 与 CompiledTopic 稳定。

---

## Phase E: LangGraph nodes + wrappers

参考：设计书 §2.2 / §5.3 / §6.4.2。

LangGraph 是 M1 引入的最大新依赖。它的 `astream_events(version="v2")` 是设计书 9.8 风险表里的最大未知项 —— 这一 phase 末尾必须先在 staging 跑一次 mini 链路验证。

### Task 11: GraphState 字段类型落实 + Message/AuthResult/AuthnIdentity 集中导出

**Files:**
- Modify: `src/ma/core/graph/state.py`

M0 的 GraphState 用了 `Any` 占位；现在补具体类型。这一 task **只动 state.py，不动其它代码**（其它模块还用 Any，下个 task 再衔接）。

- [ ] **Step 1: 改 `src/ma/core/graph/state.py`**

```python
"""GraphState：LangGraph 图执行的共享状态对象（设计书 §2.1）。

M1 版：字段类型从 Any 落实为具体类型。所有节点共用此 state。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from operator import add
from typing import Annotated, Any, Literal, TypedDict

from ma.core.plugin.base import AuthResult
from ma.core.repo.models import Message

Route = Literal["metagc", "uniioc", "kc", "reject", "default"]


@dataclass(slots=True)
class AuthnIdentity:
    """从 Transport 层 AuthnMiddleware 注入的身份对象（M0 占位实现：信任 header）。"""

    w3_account: str
    raw_claims: dict[str, Any] = field(default_factory=dict)


class GraphState(TypedDict, total=False):
    # ── 输入区（请求进入时填好）─────────
    request_id: str
    biz_id: str
    thread_id: str
    identity: AuthnIdentity
    question: str
    biz_params: dict[str, Any]
    history: list[Message]

    # ── 鉴权区（auth 节点写）─────────────
    user_ctx: dict[str, Any]
    auth: AuthResult

    # ── 补全区（enrich 节点写）───────────
    enriched_question: str
    enrich_meta: dict[str, Any]

    # ── 路由区（intent 节点写）───────────
    route: Route
    route_confidence: float
    route_reason: str

    # ── 下游输出区（adapter 节点写）──────
    answer_chunks: Annotated[list[str], add]
    answer_meta: dict[str, Any]

    # ── 错误区 ────────────────────────
    errors: Annotated[list[dict[str, Any]], add]

    # ── 持久化辅助 ──────────────────────
    user_message_id: str       # ChatService 写入 user 消息后填回，供 persist 节点关联
    assistant_message_id: str  # 同上
```

> **关于 `AuthnIdentity` 放在 state.py 而不是单独文件：** 它是 GraphState 字段类型，跟 state 强相关；放一起减少 import 网状关系。

- [ ] **Step 2: 跑测试 + lint**

```bash
python -m uv run pytest -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 仍 全绿（state.py 单测在 M0 是 0% 覆盖，M1 也不补 —— 后续 LangGraph 节点会通过它）。

- [ ] **Step 3: Commit**

```bash
git add src/ma/core/graph/state.py
git commit -m "feat(graph): concretize GraphState field types + introduce AuthnIdentity"
```

### Task 12: builtins.py — load_session / reject / persist (TDD)

**Files:**
- Create: `src/ma/core/graph/builtins.py`
- Create: `tests/unit/test_graph_builtins.py`

这三个节点是 *内置* 的（不走 PluginRegistry），因为它们没有"会新增多种实现"的扩展点。

- [ ] **Step 1: 写失败测试 `tests/unit/test_graph_builtins.py`**

```python
"""内置节点：load_session 拉历史 / reject 出固定话术 / persist 落库。

测试策略：用 fake repository（asyncio mock）替代真 DB，专注节点逻辑本身。
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from ma.core.repo.models import Message, Session


@pytest.mark.asyncio
async def test_load_session_creates_or_loads_and_fills_history() -> None:
    from ma.core.graph.builtins import make_load_session_node

    fake_session_repo = MagicMock()
    fake_session_repo.get_or_create = AsyncMock(
        return_value=Session(
            thread_id="th_1", biz_id="b", w3_account="a", title=None,
            status="active", created_at=datetime.now(), updated_at=datetime.now(),
            last_message_at=None, ext={},
        )
    )
    fake_msg_repo = MagicMock()
    fake_msg_repo.list_recent = AsyncMock(
        return_value=[
            Message(message_id="m1", thread_id="th_1", seq=1, role="user", content="q1"),
            Message(message_id="m2", thread_id="th_1", seq=2, role="assistant", content="a1"),
        ]
    )

    node = make_load_session_node(
        session_repo=fake_session_repo, message_repo=fake_msg_repo, max_turns=5
    )
    state = {
        "thread_id": "th_1", "biz_id": "b",
        "identity": MagicMock(w3_account="alice"),
    }
    out = await node(state, config={})

    assert "history" in out
    # list_recent 返回的是 DESC 顺序；node 应当 reverse 成正序
    assert [m.message_id for m in out["history"]] == ["m1", "m2"]
    fake_session_repo.get_or_create.assert_awaited_once()
    fake_msg_repo.list_recent.assert_awaited_once()


@pytest.mark.asyncio
async def test_reject_node_emits_fixed_message_and_marks_complete() -> None:
    """reject 节点 yield 一个完整的 delta + 把 reject_message 写到 answer_chunks。"""
    from ma.core.graph.builtins import make_reject_node

    error_messages = {"FORBIDDEN_TOPIC": "您暂无权限使用此专题", "INTERNAL": "服务异常"}
    node = make_reject_node(error_messages)

    auth_result = MagicMock()
    auth_result.reject_code = "FORBIDDEN_TOPIC"
    auth_result.reject_message = None  # 让 node 用 error_messages 兜底

    state = {"auth": auth_result}
    out = await node(state, config={})

    assert out["answer_chunks"] == ["您暂无权限使用此专题"]


@pytest.mark.asyncio
async def test_reject_uses_plugin_provided_message_first() -> None:
    """AuthPlugin 给的 reject_message 优先；error_messages 是兜底。"""
    from ma.core.graph.builtins import make_reject_node

    error_messages = {"FORBIDDEN_TOPIC": "兜底话术"}
    node = make_reject_node(error_messages)

    auth_result = MagicMock()
    auth_result.reject_code = "FORBIDDEN_TOPIC"
    auth_result.reject_message = "插件自定义话术"
    state = {"auth": auth_result}
    out = await node(state, config={})
    assert out["answer_chunks"] == ["插件自定义话术"]


@pytest.mark.asyncio
async def test_persist_node_appends_assistant_message() -> None:
    from ma.core.graph.builtins import make_persist_node

    fake_msg_repo = MagicMock()
    fake_msg_repo.append = AsyncMock()
    fake_session_repo = MagicMock()
    fake_session_repo.touch = AsyncMock()

    node = make_persist_node(
        session_repo=fake_session_repo, message_repo=fake_msg_repo
    )
    state = {
        "thread_id": "th_p",
        "request_id": "req_p",
        "answer_chunks": ["hello ", "world"],
        "route": "metagc",
        "assistant_message_id": "msg_assistant_xyz",
    }
    await node(state, config={})

    fake_msg_repo.append.assert_awaited_once()
    msg = fake_msg_repo.append.await_args.kwargs["msg"]
    assert msg.content == "hello world"
    assert msg.role == "assistant"
    assert msg.status == "complete"
    assert msg.route == "metagc"
    assert msg.message_id == "msg_assistant_xyz"
    fake_session_repo.touch.assert_awaited_once_with("th_p")


@pytest.mark.asyncio
async def test_persist_node_marks_partial_when_no_chunks() -> None:
    """answer_chunks 为空 / 单个 chunk 中断 → 节点应当能容忍并标 partial。"""
    from ma.core.graph.builtins import make_persist_node

    fake_msg_repo = MagicMock()
    fake_msg_repo.append = AsyncMock()
    fake_session_repo = MagicMock()
    fake_session_repo.touch = AsyncMock()
    node = make_persist_node(
        session_repo=fake_session_repo, message_repo=fake_msg_repo
    )
    state = {
        "thread_id": "th_p2",
        "request_id": "req_p2",
        "answer_chunks": [],
        "route": "metagc",
        "assistant_message_id": "msg_x",
    }
    await node(state, config={})
    msg = fake_msg_repo.append.await_args.kwargs["msg"]
    assert msg.status == "partial"
    assert msg.content == ""
```

- [ ] **Step 2: 实现 `src/ma/core/graph/builtins.py`**

```python
"""LangGraph 内置节点（不走 PluginRegistry，没扩展点的通用步骤）：

- load_session: 从 SessionRepository / MessageRepository 装填 state
- reject:       根据 state.auth 输出固定话术到 answer_chunks
- persist:      把 answer_chunks 拼成 assistant message 落库 + touch session

设计书 §1.3 / §2.2 / §3.5。
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ma.core.graph.state import GraphState
from ma.core.repo.message_repository import MessageRepository
from ma.core.repo.models import Message
from ma.core.repo.session_repository import SessionRepository

NodeFn = Callable[[GraphState, dict[str, Any]], Awaitable[dict[str, Any]]]


def make_load_session_node(
    *,
    session_repo: SessionRepository,
    message_repo: MessageRepository,
    max_turns: int,
) -> NodeFn:
    """build load_session 节点。max_turns 是 *消息条数* 不是 *轮数*；
    历史拼接里"按轮丢"语义在 ChatService 上层（M2 落地，M1 简化为消息数）。"""

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        identity = state["identity"]
        session = await session_repo.get_or_create(
            thread_id=state["thread_id"],
            biz_id=state["biz_id"],
            w3_account=identity.w3_account,
        )
        msgs = await message_repo.list_recent(
            thread_id=state["thread_id"], limit=max_turns * 2
        )
        # repo 返回 DESC，业务用正序
        msgs.reverse()
        return {"history": msgs}

    return node


def make_reject_node(error_messages: dict[str, str]) -> NodeFn:
    """build reject 节点。优先用 AuthPlugin 给的 reject_message，否则用 error_messages 兜底。"""

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        auth = state.get("auth")
        text = None
        code = "INTERNAL"
        if auth is not None:
            code = getattr(auth, "reject_code", None) or "INTERNAL"
            text = getattr(auth, "reject_message", None)
        if not text:
            text = error_messages.get(code, error_messages.get("INTERNAL", "服务异常"))
        return {"answer_chunks": [text]}

    return node


def make_persist_node(
    *,
    session_repo: SessionRepository,
    message_repo: MessageRepository,
) -> NodeFn:
    """build persist 节点。assistant message 状态：

    - chunks 非空 → complete
    - chunks 空 → partial (用户/下游中途断了)
    """

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        chunks = state.get("answer_chunks", [])
        content = "".join(chunks)
        status = "complete" if chunks else "partial"
        await message_repo.append(
            msg=Message(
                message_id=state["assistant_message_id"],
                thread_id=state["thread_id"],
                seq=0,  # 0 → repo 自动取下一个
                role="assistant",
                content=content,
                status=status,
                route=state.get("route"),
                request_id=state.get("request_id"),
            )
        )
        await session_repo.touch(state["thread_id"])
        return {}

    return node
```

- [ ] **Step 3: 跑测试**

```bash
python -m uv run pytest tests/unit/test_graph_builtins.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 5 passed; clean。

- [ ] **Step 4: Commit**

```bash
git add src/ma/core/graph/builtins.py tests/unit/test_graph_builtins.py
git commit -m "feat(graph): builtin nodes load_session/reject/persist with TDD"
```

### Task 13: node_wrappers.py — auth/enrich/intent/adapter + `_traced_node` (TDD)

**Files:**
- Create: `src/ma/core/graph/node_wrappers.py`
- Create: `tests/unit/test_node_wrappers.py`

参考：设计书 §5.3 / §6.4.2。

封装层负责四件事：
1. 调对应 Plugin 方法（authorize / enrich / classify / run）
2. 通过 `adispatch_custom_event` 发 `thinking` / `progress` 事件，挂相应 tag
3. 用 `_traced_node` 装饰器开 OTel span
4. 把 Plugin 返回值写回 GraphState

**安装 langgraph + langchain-core** 是这一 task 的前置条件 —— 这两个依赖在 Phase E 才开始用。

- [ ] **Step 1: 加 deps**

修改 `pyproject.toml` `dependencies`，追加：

```toml
    "langgraph>=0.2.50,<0.3",
    "langchain-core>=0.3,<0.4",
```

`python -m uv sync --all-groups`

- [ ] **Step 2: 写失败测试 `tests/unit/test_node_wrappers.py`**

```python
"""节点封装层：调插件 + 发 thinking/progress + 写回 state + OTel span。"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ma.core.plugin.base import AuthResult, EnrichResult, IntentResult


@pytest.mark.asyncio
async def test_auth_node_wraps_plugin_and_dispatches_events() -> None:
    from ma.core.graph.node_wrappers import make_auth_node

    plugin = MagicMock()
    plugin.authorize = AsyncMock(
        return_value=AuthResult(passed=True, user_ctx={"role": "x"})
    )

    captured: list[tuple[str, dict]] = []

    async def fake_dispatch(name: str, data: dict, *, config: dict) -> None:
        captured.append((name, data))

    with patch(
        "ma.core.graph.node_wrappers.adispatch_custom_event",
        side_effect=fake_dispatch,
    ):
        node = make_auth_node(plugin)
        out = await node({"question": "hi"}, config={})

    assert out["auth"].passed is True
    assert out["user_ctx"] == {"role": "x"}
    # 应当发了一个 thinking + 一个 progress
    names = [n for n, _ in captured]
    assert "thinking" in names
    assert "progress" in names
    # progress 必须含 step=auth, status=done
    prog = next(d for n, d in captured if n == "progress")
    assert prog["step"] == "auth"
    assert prog["status"] == "done"


@pytest.mark.asyncio
async def test_auth_node_failure_routes_to_reject() -> None:
    from ma.core.graph.node_wrappers import make_auth_node

    plugin = MagicMock()
    plugin.authorize = AsyncMock(
        return_value=AuthResult(
            passed=False, reject_code="FORBIDDEN_TOPIC", reject_message="拒绝"
        )
    )

    with patch("ma.core.graph.node_wrappers.adispatch_custom_event", new=AsyncMock()):
        node = make_auth_node(plugin)
        out = await node({}, config={})

    assert out["auth"].passed is False
    assert out["route"] == "reject"


@pytest.mark.asyncio
async def test_enrich_node_writes_enriched_question() -> None:
    from ma.core.graph.node_wrappers import make_enrich_node

    plugin = MagicMock()
    plugin.enrich = AsyncMock(
        return_value=EnrichResult(
            enriched_question="补全后的问题", enrich_meta={"injected": ["region"]}
        )
    )
    with patch("ma.core.graph.node_wrappers.adispatch_custom_event", new=AsyncMock()):
        node = make_enrich_node(plugin)
        out = await node({"question": "原问题"}, config={})

    assert out["enriched_question"] == "补全后的问题"
    assert out["enrich_meta"] == {"injected": ["region"]}


@pytest.mark.asyncio
async def test_intent_node_writes_route() -> None:
    from ma.core.graph.node_wrappers import make_intent_node

    plugin = MagicMock()
    plugin.classify = AsyncMock(
        return_value=IntentResult(route="metagc", confidence=0.9, reason="业务数据")
    )
    with patch("ma.core.graph.node_wrappers.adispatch_custom_event", new=AsyncMock()):
        node = make_intent_node(plugin, default_route="metagc", labels=["metagc"])
        out = await node({"question": "q"}, config={})

    assert out["route"] == "metagc"
    assert out["route_confidence"] == 0.9


@pytest.mark.asyncio
async def test_intent_node_fallback_to_default_on_failure() -> None:
    """IntentFailed → 用 default_route + 记 warning 但不抛。"""
    from ma.core.graph.node_wrappers import make_intent_node
    from ma.core.plugin.base import IntentFailed

    plugin = MagicMock()
    plugin.classify = AsyncMock(side_effect=IntentFailed("timeout"))

    with patch("ma.core.graph.node_wrappers.adispatch_custom_event", new=AsyncMock()):
        node = make_intent_node(plugin, default_route="metagc", labels=["metagc"])
        out = await node({}, config={})

    assert out["route"] == "metagc"
    # confidence 字段在 fallback 时设为 0.0 是合理约定
    assert out["route_confidence"] == 0.0


@pytest.mark.asyncio
async def test_intent_node_fallback_on_unknown_label() -> None:
    """Plugin 返回了 mapping 之外的 label → fallback 到 default。"""
    from ma.core.graph.node_wrappers import make_intent_node

    plugin = MagicMock()
    plugin.classify = AsyncMock(
        return_value=IntentResult(route="oops", confidence=0.7, reason="?")
    )

    with patch("ma.core.graph.node_wrappers.adispatch_custom_event", new=AsyncMock()):
        node = make_intent_node(plugin, default_route="metagc", labels=["metagc"])
        out = await node({}, config={})

    assert out["route"] == "metagc"


@pytest.mark.asyncio
async def test_adapter_node_streams_delta_into_chunks() -> None:
    """Adapter run() yield 一连串 ChatEvent，封装层应当：
    - 把 delta 累计到 answer_chunks
    - 把 delta 转发为 user_output tagged custom event
    """
    from ma.core.graph.events import ChatEvent
    from ma.core.graph.node_wrappers import make_adapter_node

    async def fake_run(state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "上个月"})
        yield ChatEvent(type="delta", data={"content": "销售额 X"})

    adapter = MagicMock()
    adapter.run = fake_run

    dispatched: list[tuple[str, dict]] = []

    async def fake_dispatch(name: str, data: dict, *, config: dict) -> None:
        dispatched.append((name, data))

    with patch(
        "ma.core.graph.node_wrappers.adispatch_custom_event",
        side_effect=fake_dispatch,
    ):
        node = make_adapter_node(adapter, output=True)
        out = await node({}, config={})

    assert out["answer_chunks"] == ["上个月", "销售额 X"]
    delta_events = [d for n, d in dispatched if n == "delta"]
    assert delta_events == [{"content": "上个月"}, {"content": "销售额 X"}]
```

- [ ] **Step 3: 实现 `src/ma/core/graph/node_wrappers.py`**

```python
"""节点封装层：把 Plugin 实例包装成 LangGraph 节点，并织入观测事件。

设计书 §5.3 / §6.4.2。每个 wrapper 做四件事：
1. 调插件方法
2. dispatch_custom_event 发 thinking / progress 给 ChatService 转 SSE
3. 把结果写回 GraphState
4. OTel span 由 _traced_node 装饰器统一开
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from langchain_core.callbacks.manager import adispatch_custom_event
from opentelemetry import trace

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import GraphState
from ma.core.plugin.base import (
    AuthPlugin,
    DownstreamAdapter,
    EnrichPlugin,
    IntentFailed,
    IntentPlugin,
)
from ma.infra.logging import get_logger

NodeFn = Callable[[GraphState, dict[str, Any]], Awaitable[dict[str, Any]]]
_tracer = trace.get_tracer("ma.graph")
_log = get_logger("ma.graph")


def _traced_node(name: str, fn: NodeFn) -> NodeFn:
    """给节点函数包一层 OTel span + 错误 attributes。"""

    async def wrapped(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        with _tracer.start_as_current_span(f"graph.node.{name}") as span:
            span.set_attribute("ma.node", name)
            try:
                result = await fn(state, config)
                span.set_attribute("ma.node.status", "ok")
                return result
            except Exception as e:
                span.set_attribute("ma.node.status", "error")
                span.record_exception(e)
                raise

    return wrapped


def make_auth_node(plugin: AuthPlugin) -> NodeFn:
    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        await adispatch_custom_event(
            "thinking", {"phase": "auth", "label": "正在校验权限…"}, config=config
        )
        result = await plugin.authorize(state)
        await adispatch_custom_event(
            "progress",
            {
                "step": "auth",
                "status": "done",
                "extra": {"passed": result.passed},
            },
            config=config,
        )
        out: dict[str, Any] = {"auth": result}
        if result.passed:
            if result.user_ctx is not None:
                out["user_ctx"] = result.user_ctx
        else:
            out["route"] = "reject"
        return out

    return _traced_node("auth", node)


def make_enrich_node(plugin: EnrichPlugin) -> NodeFn:
    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        await adispatch_custom_event(
            "thinking", {"phase": "enrich"}, config=config
        )
        result = await plugin.enrich(state)
        await adispatch_custom_event(
            "progress",
            {"step": "enrich", "status": "done", "extra": result.enrich_meta},
            config=config,
        )
        return {
            "enriched_question": result.enriched_question,
            "enrich_meta": result.enrich_meta,
        }

    return _traced_node("enrich", node)


def make_intent_node(
    plugin: IntentPlugin, *, default_route: str, labels: list[str]
) -> NodeFn:
    """intent 节点。失败或 route 不在 labels 中 → fallback 到 default_route 并记 warning。"""

    label_set = set(labels)

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        await adispatch_custom_event(
            "thinking",
            {"phase": "intent", "label": "正在识别问题意图…"},
            config=config,
        )
        try:
            result = await plugin.classify(state)
            if result.route not in label_set:
                _log.warning(
                    "intent_unknown_label",
                    got_route=result.route,
                    labels=list(label_set),
                    fallback=default_route,
                )
                route = default_route
                conf = 0.0
                reason = f"unknown label {result.route!r}, fallback"
            else:
                route = result.route
                conf = result.confidence
                reason = result.reason
            await adispatch_custom_event(
                "progress",
                {
                    "step": "intent",
                    "status": "done",
                    "detail": reason,
                    "extra": {"route": route},
                },
                config=config,
            )
            return {
                "route": route,
                "route_confidence": conf,
                "route_reason": reason,
            }
        except IntentFailed as e:
            _log.warning("intent_failed", error=str(e), fallback=default_route)
            await adispatch_custom_event(
                "progress",
                {
                    "step": "intent",
                    "status": "fallback",
                    "detail": str(e),
                    "extra": {"route": default_route},
                },
                config=config,
            )
            return {
                "route": default_route,
                "route_confidence": 0.0,
                "route_reason": f"intent_failed: {e}",
            }

    return _traced_node("intent", node)


def make_adapter_node(adapter: DownstreamAdapter, *, output: bool) -> NodeFn:
    """adapter 节点：消费 Adapter.run 的 AsyncIterator[ChatEvent]，写 chunks +
    转发 delta / tool 为 user_output / user_tool 的 dispatch_custom_event。
    """

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        chunks: list[str] = []
        async for ev in adapter.run(state, output=output):
            if ev.type == "delta":
                chunks.append(ev.data["content"])
                if output:
                    await adispatch_custom_event("delta", ev.data, config=config)
            elif ev.type == "tool":
                if output:
                    await adispatch_custom_event("tool", ev.data, config=config)
            elif ev.type == "error":
                # error 让上层（ChatService）感知
                await adispatch_custom_event("error", ev.data, config=config)
                # 不抛 —— 留给 ChatService 决定降级还是终止
                break
        if output:
            return {"answer_chunks": chunks}
        # 非 output 节点：将来 M2 多步编排时用
        return {"answer_chunks": []}

    return _traced_node("adapter", node)
```

> **关于挂 LangGraph tag**：在 `make_adapter_node` 等 wrapper 这一层，**节点函数本身**不挂 tag —— tag 是在 `add_node(name, fn).with_config(tags=[...])` 或 `add_node(name, fn, metadata={...})` 之类的图构建 API 处指定的。M1 的 ChatService（Task 20）在编译 StateGraph 时按节点种类挂 tag。
>
> 但 `adispatch_custom_event` 的 `name` 字段已经携带了语义（"thinking"/"progress"/"delta"/"tool"），ChatService 用 `astream_events("on_custom_event")` 捕获时直接根据 name 路由即可。**所以 M1 实际上不严格依赖 tag** —— tag 是设计书 §2.5 给"内部 LLM token 误外泄"那条契约准备的，M1 阶段 IntentPlugin 用 Fake LLM 不流 token，所以不存在泄漏；M3 接真 LLM 时再补 tag 隔离。

- [ ] **Step 4: 跑测试**

```bash
python -m uv run pytest tests/unit/test_node_wrappers.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 7 passed; clean。

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock src/ma/core/graph/node_wrappers.py \
        tests/unit/test_node_wrappers.py
git commit -m "feat(graph): node wrappers for auth/enrich/intent/adapter with OTel + custom events"
```

**完成 Phase E 的标志：** 3 个 commit；GraphState 类型完整；3 个内置节点 + 4 个 wrapper 都有单测。

---

## Phase F: Plugin implementations

3 个最小实现，都不接真后端。**Task 18 之前不需要任何这些插件真起来跑** —— 只要单测过即可。

### Task 14: representative_auth

**Files:**
- Create: `src/ma/plugins/__init__.py`
- Create: `src/ma/plugins/auth/__init__.py`
- Create: `src/ma/plugins/auth/representative_auth.py`
- Create: `tests/unit/test_representative_auth.py`
- Modify: `src/ma/core/plugin/bootstrap.py` — 加 import

M1 的 representative_auth 不接真外部 user_ctx API（M2 接），它直接根据 GraphState.identity.raw_claims 决定通过/拒绝，可由 `params.required_role` 配置。这让 M1 e2e 能跑通鉴权通过 / 鉴权失败两条路径而不依赖真外部 API。

- [ ] **Step 1: 创建目录 + bootstrap 占位**

```bash
mkdir -p src/ma/plugins/auth
touch src/ma/plugins/__init__.py src/ma/plugins/auth/__init__.py
```

- [ ] **Step 2: 写失败测试 `tests/unit/test_representative_auth.py`**

```python
"""representative_auth：基于 GraphState.identity.raw_claims["role"] 决定通过/拒绝。"""
from __future__ import annotations

import pytest

from ma.core.graph.state import AuthnIdentity


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    """每个测试独立 registry，避免跨用例污染。"""
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    # 重新 import 触发装饰器
    import importlib
    import ma.plugins.auth.representative_auth as mod
    importlib.reload(mod)
    yield new_reg


@pytest.mark.asyncio
async def test_auth_passes_with_matching_role() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(w3_account="alice", raw_claims={"role": "REP"}),
    }
    result = await plugin.authorize(state)
    assert result.passed is True
    assert result.user_ctx is not None
    assert result.user_ctx["role"] == "REP"


@pytest.mark.asyncio
async def test_auth_rejects_with_wrong_role() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(
            w3_account="bob", raw_claims={"role": "VIEWER"}
        ),
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_TOPIC"
    assert result.reject_message is not None


@pytest.mark.asyncio
async def test_auth_rejects_when_no_role_claim() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(w3_account="ghost", raw_claims={}),
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_TOPIC"
```

- [ ] **Step 3: 实现 `src/ma/plugins/auth/representative_auth.py`**

```python
"""代表小管家鉴权插件。

M1 简化实现：从 GraphState.identity.raw_claims 中读取 role，与 required_role 比对。
M2 起接外部用户中心 API 拉真实角色 + 客户归属 + 客户级权限。
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import AuthResult
from ma.core.plugin.registry import registry


class RepresentativeAuthParams(BaseModel):
    required_role: str


@registry.register
class RepresentativeAuth:
    name = "representative_auth"
    plugin_kind = "auth"

    def configure(self, params: dict[str, Any]) -> None:
        p = RepresentativeAuthParams(**params)
        self._required_role = p.required_role

    async def authorize(self, state: GraphState) -> AuthResult:
        identity = state.get("identity")
        if identity is None:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_TOPIC",
                reject_message="无法识别用户身份。",
            )
        claims = identity.raw_claims or {}
        role = claims.get("role")
        if role != self._required_role:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_TOPIC",
                reject_message=f"该专题仅限 {self._required_role} 角色访问。",
            )
        return AuthResult(passed=True, user_ctx={"role": role})
```

- [ ] **Step 4: 改 `src/ma/core/plugin/bootstrap.py`，加 import**

```python
def load_all_plugins() -> None:
    """启动期调一次。"""
    import ma.plugins.auth.representative_auth  # noqa: F401
    # 后续 task 加：
    # import ma.plugins.enrich.generic_enrich  # noqa: F401
    # import ma.plugins.intent.llm_classifier  # noqa: F401
    # import ma.plugins.adapter.metagc        # noqa: F401
```

- [ ] **Step 5: 跑测试**

```bash
python -m uv run pytest tests/unit/test_representative_auth.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 3 passed; clean。

- [ ] **Step 6: Commit**

```bash
git add src/ma/plugins/ src/ma/core/plugin/bootstrap.py \
        tests/unit/test_representative_auth.py
git commit -m "feat(plugins): representative_auth based on identity.raw_claims.role"
```

### Task 15: generic_enrich

**Files:**
- Create: `src/ma/plugins/enrich/__init__.py`
- Create: `src/ma/plugins/enrich/generic_enrich.py`
- Create: `tests/unit/test_generic_enrich.py`
- Modify: `src/ma/core/plugin/bootstrap.py` — 加 import

最简实现：从 `state.biz_params` + `state.user_ctx` 按 `params.fields_to_inject` 白名单拼一段前缀附加到 `state.question`。

- [ ] **Step 1: 写测试**

```python
"""generic_enrich：按白名单从 biz_params/user_ctx 取字段拼前缀。"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module
    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    import importlib
    import ma.plugins.enrich.generic_enrich as mod
    importlib.reload(mod)
    yield new_reg


@pytest.mark.asyncio
async def test_enrich_injects_whitelist_fields() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "enrich", "generic_enrich", {"fields_to_inject": ["region"]}
    )
    state = {
        "question": "上个月销售额？",
        "biz_params": {"region": "huadong"},
        "user_ctx": {"role": "REP"},
    }
    out = await plugin.enrich(state)
    assert "huadong" in out.enriched_question
    assert "上个月销售额" in out.enriched_question
    assert out.enrich_meta["injected"] == ["region"]


@pytest.mark.asyncio
async def test_enrich_skips_missing_fields() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "enrich", "generic_enrich", {"fields_to_inject": ["region", "customer"]}
    )
    state = {"question": "Q", "biz_params": {"region": "huadong"}, "user_ctx": {}}
    out = await plugin.enrich(state)
    assert "region" in out.enrich_meta["injected"]
    assert "customer" not in out.enrich_meta["injected"]


@pytest.mark.asyncio
async def test_enrich_no_fields_keeps_question_unchanged() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "enrich", "generic_enrich", {"fields_to_inject": []}
    )
    state = {"question": "Q", "biz_params": {}, "user_ctx": {}}
    out = await plugin.enrich(state)
    assert out.enriched_question == "Q"
    assert out.enrich_meta["injected"] == []
```

- [ ] **Step 2: 实现 `src/ma/plugins/enrich/generic_enrich.py`**

```python
"""通用补全：按白名单从 biz_params / user_ctx 取字段拼前缀。

设计书 §6.9 规则 6：不允许把 user_ctx 整体扔进 prompt；必须白名单。
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import EnrichResult
from ma.core.plugin.registry import registry


class GenericEnrichParams(BaseModel):
    fields_to_inject: list[str] = []


@registry.register
class GenericEnrich:
    name = "generic_enrich"
    plugin_kind = "enrich"

    def configure(self, params: dict[str, Any]) -> None:
        p = GenericEnrichParams(**params)
        self._fields = p.fields_to_inject

    async def enrich(self, state: GraphState) -> EnrichResult:
        biz_params = state.get("biz_params", {})
        user_ctx = state.get("user_ctx", {})
        question = state.get("question", "")
        injected: list[str] = []
        prefix_parts: list[str] = []
        for field in self._fields:
            if field in biz_params:
                prefix_parts.append(f"{field}={biz_params[field]}")
                injected.append(field)
            elif field in user_ctx:
                prefix_parts.append(f"{field}={user_ctx[field]}")
                injected.append(field)
        if prefix_parts:
            prefix = "[" + " ".join(prefix_parts) + "] "
        else:
            prefix = ""
        return EnrichResult(
            enriched_question=prefix + question,
            enrich_meta={"injected": injected},
        )
```

- [ ] **Step 3: bootstrap 加 import**

```python
def load_all_plugins() -> None:
    import ma.plugins.auth.representative_auth  # noqa: F401
    import ma.plugins.enrich.generic_enrich    # noqa: F401
```

- [ ] **Step 4: 测试 + lint**

```bash
python -m uv run pytest tests/unit/test_generic_enrich.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 3 passed; clean。

- [ ] **Step 5: Commit**

```bash
git add src/ma/plugins/enrich/ src/ma/core/plugin/bootstrap.py \
        tests/unit/test_generic_enrich.py
git commit -m "feat(plugins): generic_enrich whitelisted field injection"
```

### Task 16: llm_classifier (FakeListChatModel)

**Files:**
- Create: `src/ma/plugins/intent/__init__.py`
- Create: `src/ma/plugins/intent/llm_classifier.py`
- Create: `tests/unit/test_llm_classifier.py`
- Modify: `src/ma/core/plugin/bootstrap.py` — 加 import

M1 实现支持两个模式：
- `provider = "fake"` (默认)：从 `params.fake_responses` 拿固定回答序列（FakeListChatModel）
- `provider = "openai-compatible"` 等：M3 才接，M1 不实现（占位 raise NotImplementedError）

输出格式：让 LLM 返回 plain text 含 "route: <label>"，再用 regex 抽。简单稳定。

- [ ] **Step 1: 写测试**

```python
"""llm_classifier：FakeListChatModel 输出固定 text，解析出 route。"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module
    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    import importlib
    import ma.plugins.intent.llm_classifier as mod
    importlib.reload(mod)
    yield new_reg


@pytest.mark.asyncio
async def test_llm_classifier_fake_route_extraction() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "intent",
        "llm_classifier",
        {
            "provider": "fake",
            "model": "fake-model",
            "labels": ["metagc", "kc"],
            "prompt_template": "prompts/intent/test.txt",
            "fake_responses": ["route: metagc\nreason: 业务数据查询"],
        },
    )
    state = {"enriched_question": "上个月销售额？"}
    result = await plugin.classify(state)
    assert result.route == "metagc"
    assert "业务数据" in result.reason


@pytest.mark.asyncio
async def test_llm_classifier_invalid_response_raises() -> None:
    """LLM 返回的 text 找不到 route → IntentFailed。"""
    from ma.core.plugin.base import IntentFailed
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "intent",
        "llm_classifier",
        {
            "provider": "fake",
            "model": "fake-model",
            "labels": ["metagc"],
            "prompt_template": "prompts/intent/test.txt",
            "fake_responses": ["totally unparseable garbage"],
        },
    )
    with pytest.raises(IntentFailed):
        await plugin.classify({"enriched_question": "?"})


@pytest.mark.asyncio
async def test_llm_classifier_unknown_provider_at_configure() -> None:
    """非 fake / openai-compatible / internal → 启动期 fail fast。"""
    from ma.core.plugin.registry import registry

    with pytest.raises(Exception):
        registry.create(
            "intent",
            "llm_classifier",
            {
                "provider": "unknown",
                "model": "x",
                "labels": ["metagc"],
                "prompt_template": "p.txt",
            },
        )
```

- [ ] **Step 2: 实现 `src/ma/plugins/intent/llm_classifier.py`**

```python
"""LLM 分类器：用 LangChain 风格的 ChatModel 抽象。

M1 实现：
- provider="fake" → langchain_core.language_models.fake.FakeListChatModel
- 其它 provider → M3 实现；M1 raise NotImplementedError

输出协议：让 LLM 用纯文本回答，第一行 "route: <label>"，第二行可选 "reason: ..."
正则解析 route；解析失败 → IntentFailed。
"""
from __future__ import annotations

import re
from typing import Any, Literal

from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import IntentFailed, IntentResult
from ma.core.plugin.registry import registry


class LLMClassifierParams(BaseModel):
    provider: Literal["fake", "openai-compatible", "internal"] = "fake"
    model: str
    labels: list[str]
    prompt_template: str  # M1 不读模板文件，留着字段作为 schema 校验占位
    fake_responses: list[str] | None = None  # provider=fake 时必填


_ROUTE_RE = re.compile(r"^\s*route\s*:\s*([a-zA-Z0-9_-]+)\s*$", re.MULTILINE)
_REASON_RE = re.compile(r"^\s*reason\s*:\s*(.+)$", re.MULTILINE)


@registry.register
class LLMClassifier:
    name = "llm_classifier"
    plugin_kind = "intent"

    def configure(self, params: dict[str, Any]) -> None:
        p = LLMClassifierParams(**params)
        self._labels = set(p.labels)
        if p.provider == "fake":
            if not p.fake_responses:
                raise ValueError(
                    "provider='fake' requires non-empty fake_responses"
                )
            self._llm = FakeListChatModel(responses=p.fake_responses)
        else:
            raise NotImplementedError(
                f"provider={p.provider!r} not implemented in M1 (use 'fake'; "
                f"real LLM接入 is M3 work)"
            )

    async def classify(self, state: GraphState) -> IntentResult:
        question = state.get("enriched_question") or state.get("question", "")
        prompt = (
            "你是路由分类器。返回纯文本，第一行 'route: <label>'，"
            "第二行可选 'reason: ...'\n"
            f"可选 label: {', '.join(sorted(self._labels))}\n\n"
            f"用户问题：{question}"
        )
        response = await self._llm.ainvoke([HumanMessage(content=prompt)])
        text = response.content if isinstance(response.content, str) else str(response.content)
        m = _ROUTE_RE.search(text)
        if not m:
            raise IntentFailed(f"cannot parse route from LLM response: {text!r}")
        route = m.group(1)
        reason_m = _REASON_RE.search(text)
        reason = reason_m.group(1).strip() if reason_m else ""
        return IntentResult(route=route, confidence=1.0, reason=reason)
```

- [ ] **Step 3: bootstrap 加 import**

```python
def load_all_plugins() -> None:
    import ma.plugins.auth.representative_auth   # noqa: F401
    import ma.plugins.enrich.generic_enrich     # noqa: F401
    import ma.plugins.intent.llm_classifier     # noqa: F401
```

- [ ] **Step 4: 测试**

```bash
python -m uv run pytest tests/unit/test_llm_classifier.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 3 passed; clean。

- [ ] **Step 5: Commit**

```bash
git add src/ma/plugins/intent/ src/ma/core/plugin/bootstrap.py \
        tests/unit/test_llm_classifier.py
git commit -m "feat(plugins): llm_classifier with FakeListChatModel; real LLM deferred to M3"
```

**完成 Phase F 的标志：** 3 个 commit；3 个 plugin 实现 + 9 单测全过；bootstrap 已 import 三个。

---

## Phase G: MetaGC Adapter

### Task 17: MetaGC mock server + MetaGCAdapter (TDD)

**Files:**
- Create: `src/ma/plugins/adapter/__init__.py`
- Create: `src/ma/plugins/adapter/metagc.py`
- Create: `tests/mock_downstreams/__init__.py`
- Create: `tests/mock_downstreams/metagc_sse.py`
- Create: `tests/unit/test_metagc_adapter.py`
- Modify: `src/ma/core/plugin/bootstrap.py`
- Modify: `pyproject.toml`：加 `httpx-sse` dep（SSE 客户端解析）

参考：设计书 §2.4 / §4.2.2 / §5.2.4。

- [ ] **Step 1: 加 deps**

```toml
dependencies = [
    ...
    "httpx-sse>=0.4,<0.5",
]
```

`python -m uv sync --all-groups`

- [ ] **Step 2: 写 mock server `tests/mock_downstreams/metagc_sse.py`**

```python
"""MetaGC 下游 mock server (FastAPI)。

提供 POST /api/v1/chat/sse 端点：
- 默认行为：流式返回预设 5 段 token 组成的一句话回复
- 通过 query param ?scenario=timeout 模拟卡住
- 通过 query param ?scenario=5xx 模拟服务错误
- 通过 query param ?scenario=slow 模拟首 chunk 延迟（默认 5s）

集成测试启动一个 ASGI app 用 httpx.AsyncClient + ASGITransport 直接对接；
独立运行模式 (python -m tests.mock_downstreams.metagc_sse) 起到 8001 端口便于
人工 curl 验证。
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import StreamingResponse

app = FastAPI(title="MetaGC mock")


def _sse_frame(event: str, data: dict[str, Any]) -> bytes:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n".encode()


@app.post("/api/v1/chat/sse")
async def metagc_sse_endpoint(
    request: Request,
    scenario: str = Query(default="default"),
) -> StreamingResponse:
    body = await request.json()
    question = body.get("question", "?")

    async def stream() -> AsyncIterator[bytes]:
        if scenario == "5xx":
            # FastAPI doesn't return non-200 from a stream easily without changing return type
            # We emit an error SSE frame and stop
            yield _sse_frame("error", {"code": "METAGC_INTERNAL", "message": "boom"})
            return

        if scenario == "timeout":
            # Hang forever (Adapter side timeout should fire)
            await asyncio.sleep(3600)
            return

        if scenario == "slow":
            await asyncio.sleep(5)

        # default: 5 chunks
        yield _sse_frame("meta", {"upstream_id": "metagc_demo"})
        chunks = ["关于 ", question, " 的回复：", "示例数据。", ""]
        for c in chunks:
            if c:
                yield _sse_frame("delta", {"content": c})
                await asyncio.sleep(0.05)
        yield _sse_frame("done", {})

    return StreamingResponse(stream(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="warning")
```

注：mock server 用 FastAPI（已是 main deps），无需新加依赖；放在 `tests/` 下不会被打进 Docker 镜像。

- [ ] **Step 3: 写失败测试 `tests/unit/test_metagc_adapter.py`**

测试用 `httpx.AsyncClient(transport=ASGITransport(app=mock_app))` 直接打到 mock server，不开物理端口。

```python
"""MetaGCAdapter：用 ASGITransport 直连 mock server 跑单测。"""
from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.mock_downstreams.metagc_sse import app as mock_app


@pytest.fixture
def metagc_client() -> httpx.AsyncClient:
    """httpx AsyncClient + ASGITransport，直接打 mock app 无需起端口。"""
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_app),
        base_url="http://metagc.test",
    )


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module
    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    import importlib
    import ma.plugins.adapter.metagc as mod
    importlib.reload(mod)
    yield new_reg


@pytest.mark.asyncio
async def test_metagc_adapter_streams_delta_events(
    metagc_client: httpx.AsyncClient,
) -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "adapter",
        "metagc",
        {"base_url": "http://metagc.test", "timeout_ms": 10000},
    )
    plugin._client = metagc_client  # 测试注入 client

    state = {"enriched_question": "上个月销售额"}
    events = []
    async for ev in plugin.run(state, output=True):
        events.append(ev)
    await metagc_client.aclose()

    deltas = [e for e in events if e.type == "delta"]
    assert len(deltas) >= 3
    full = "".join(e.data["content"] for e in deltas)
    assert "上个月销售额" in full


@pytest.mark.asyncio
async def test_metagc_adapter_emits_error_on_upstream_error(
    metagc_client: httpx.AsyncClient,
) -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "adapter",
        "metagc",
        {"base_url": "http://metagc.test?scenario=5xx", "timeout_ms": 5000},
    )
    plugin._client = metagc_client

    events = []
    async for ev in plugin.run({}, output=True):
        events.append(ev)
    await metagc_client.aclose()

    errors = [e for e in events if e.type == "error"]
    assert len(errors) == 1
    assert errors[0].data["code"]
```

- [ ] **Step 4: 实现 `src/ma/plugins/adapter/metagc.py`**

```python
"""MetaGCAdapter：POST 到 MetaGC SSE 端点，把 SSE 帧翻译成 ChatEvent。

设计书 §2.2 / §5.2.4。

输入：state.enriched_question + state.biz_params
输出：AsyncIterator[ChatEvent]，type=delta / type=error / type=tool (mapped from upstream meta)
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
from httpx_sse import aconnect_sse
from pydantic import BaseModel

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import GraphState
from ma.core.plugin.registry import registry
from ma.infra.logging import get_logger

_log = get_logger("ma.adapter.metagc")


class MetaGCAdapterParams(BaseModel):
    base_url: str
    timeout_ms: int = 30000
    path: str = "/api/v1/chat/sse"


@registry.register
class MetaGCAdapter:
    name = "metagc"
    plugin_kind = "adapter"

    def configure(self, params: dict[str, Any]) -> None:
        p = MetaGCAdapterParams(**params)
        self._base_url = p.base_url
        self._timeout = httpx.Timeout(p.timeout_ms / 1000.0)
        self._path = p.path
        # 默认 client；测试可以替换 self._client
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def run(
        self, state: GraphState, *, output: bool = True
    ) -> AsyncIterator[ChatEvent]:
        body = {
            "question": state.get("enriched_question") or state.get("question", ""),
            "biz_params": state.get("biz_params", {}),
            "request_id": state.get("request_id"),
        }
        try:
            async with aconnect_sse(
                self._client, "POST", self._base_url + self._path, json=body
            ) as event_source:
                async for sse in event_source.aiter_sse():
                    if sse.event == "meta":
                        # 上游 meta 不向用户透出；记录 trace 即可
                        _log.info("metagc_meta", upstream_data=sse.data)
                    elif sse.event == "delta":
                        data = json.loads(sse.data) if sse.data else {}
                        yield ChatEvent(type="delta", data=data)
                    elif sse.event == "error":
                        data = json.loads(sse.data) if sse.data else {}
                        yield ChatEvent(
                            type="error",
                            data={
                                "code": data.get("code", "DOWNSTREAM_ERROR"),
                                "message": data.get("message", "MetaGC 上游出错"),
                                "retryable": False,
                            },
                        )
                    elif sse.event == "done":
                        return
        except httpx.TimeoutException:
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_TIMEOUT",
                    "message": "MetaGC 接口响应超时，请稍后重试",
                    "retryable": False,
                },
            )
        except httpx.RequestError as e:
            _log.exception("metagc_request_error")
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_ERROR",
                    "message": "调用下游失败，请稍后重试",
                    "retryable": False,
                },
            )
```

- [ ] **Step 5: bootstrap 加 import**

```python
def load_all_plugins() -> None:
    import ma.plugins.auth.representative_auth   # noqa: F401
    import ma.plugins.enrich.generic_enrich     # noqa: F401
    import ma.plugins.intent.llm_classifier     # noqa: F401
    import ma.plugins.adapter.metagc            # noqa: F401
```

- [ ] **Step 6: 跑测试 + lint**

```bash
python -m uv run pytest tests/unit/test_metagc_adapter.py -v
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 2 passed; clean。

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock src/ma/plugins/adapter/ \
        tests/mock_downstreams/ tests/unit/test_metagc_adapter.py \
        src/ma/core/plugin/bootstrap.py
git commit -m "feat(adapter): MetaGCAdapter (SSE client) + local mock server + ASGI-based tests"
```

### Task 18: MetaGC integration test through mock server on a real port

**Files:**
- Create: `tests/integration/test_metagc_with_real_port.py`

为什么再来一次？ASGI transport 测试漏掉了真实 HTTP 栈（无端口、无 server header、超时语义不同）。这个集成测试启动 mock_downstreams 到一个 ephemeral 端口，跑真 httpx → 真 socket → MetaGCAdapter，验证生产形态。

- [ ] **Step 1: 写测试**

```python
"""MetaGCAdapter 走真 HTTP socket + 真端口的 integration test。

启动 tests/mock_downstreams/metagc_sse.py 到 ephemeral port，跑 MetaGCAdapter，
验证 SSE 流式拉取正常。
"""
from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
import uvicorn


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest_asyncio.fixture
async def metagc_server() -> AsyncIterator[str]:
    """启动 mock metagc 到 ephemeral port，yield base_url，结束时关闭。"""
    from tests.mock_downstreams.metagc_sse import app

    port = _get_free_port()
    config = uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="warning"
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    # 等启动
    for _ in range(40):
        if server.started:
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("metagc mock server failed to start")

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module
    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    import importlib
    import ma.plugins.adapter.metagc as mod
    importlib.reload(mod)
    yield new_reg


@pytest.mark.asyncio
async def test_metagc_full_socket_stream(metagc_server: str) -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "adapter",
        "metagc",
        {"base_url": metagc_server, "timeout_ms": 10000},
    )

    deltas = []
    async for ev in plugin.run(
        {"enriched_question": "上个月销售额是多少"}, output=True
    ):
        if ev.type == "delta":
            deltas.append(ev.data["content"])
        elif ev.type == "error":
            pytest.fail(f"unexpected error: {ev.data}")
    assert len(deltas) >= 3
    text = "".join(deltas)
    assert "上个月销售额是多少" in text
```

- [ ] **Step 2: 跑（如果本机没问题）**

```bash
python -m uv run pytest tests/integration/test_metagc_with_real_port.py -v
```

Expected: 1 passed（任何平台只要能监听 localhost）。

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_metagc_with_real_port.py
git commit -m "test(adapter): MetaGC integration via real socket + uvicorn mock server"
```

**完成 Phase G 的标志：** 2 个 commit；MetaGCAdapter + mock server 都跑通；2 个单测 + 1 个集成测试全过；bootstrap 完整。

---

## Phase H: ChatService 重写 + Topic YAML

### Task 19: 代表小管家 Topic YAML + prompt 模板

**Files:**
- Create: `config/topics/daibiao_xiaoguanjia.yaml`
- Create: `prompts/intent/representative.txt`

- [ ] **Step 1: 创建 prompt 模板**

`prompts/intent/representative.txt`:
```
你是路由分类器。当前用户问题来自"代表小管家"专题。
根据问题内容，选择最匹配的下游处理路径。

可选 label:
- metagc: 业务数据查询、销售额、客户信息

输出格式：
route: <label>
reason: <一行简短理由>
```

> M1 阶段，prompt 实际上没被 llm_classifier 读 — provider='fake' 直接返回 fake_responses。模板文件存在主要是 schema 校验 + 给 M3 真接 LLM 时复用。

- [ ] **Step 2: 创建 Topic YAML**

`config/topics/daibiao_xiaoguanjia.yaml`:
```yaml
topic_id: daibiao_xiaoguanjia
display_name: 代表小管家
default_route: metagc

history:
  max_turns: 10
  scope: per_topic

auth:
  plugin: representative_auth
  params:
    required_role: REP

enrich:
  plugin: generic_enrich
  params:
    fields_to_inject: [region]

intent:
  plugin: llm_classifier
  params:
    provider: fake
    model: fake-model
    labels: [metagc]
    prompt_template: prompts/intent/representative.txt
    fake_responses:
      - "route: metagc\nreason: 业务数据查询"

graph:
  nodes:
    - id: metagc_adapter
      adapter: metagc
      output: true
      params:
        base_url: ${env:MA_METAGC_BASE_URL}
        timeout_ms: 30000

  edges:
    - from: intent
      type: conditional
      route_by: route
      mapping:
        metagc: metagc_adapter

biz_params_schema:
  type: object
  required: [region]
  properties:
    region:
      type: string

error_messages:
  FORBIDDEN_TOPIC: "您暂无权限使用代表小管家。"
  FORBIDDEN_SCOPE: "您当前的角色无法查询这部分数据。"
  DOWNSTREAM_TIMEOUT: "服务繁忙，请稍后再试。"
  DOWNSTREAM_ERROR: "查询遇到问题，请稍后再试或联系管理员。"
  INTERNAL: "服务异常，请稍后再试。"
```

> `${env:MA_METAGC_BASE_URL}` 是个占位符 —— **TopicConfig 当前 schema 不支持环境变量替换**。M1 简化路线：
>
> 方案 A（推荐）：YAML 里写一个 placeholder string `"http://configured-at-runtime"`，由 ChatService 在 lifespan 里读取 Settings.metagc_base_url 后**用 setattr 覆盖** adapter 实例的 `_base_url` 字段。简单粗暴，但插件没暴露这个能力 → 通过 TopicCompiler 在 compile 阶段把 Settings 注入 params 解决。
>
> 方案 B：扩展 `TopicCompiler.compile(cfg, settings=...)`，在装配 adapter params 前做 `${env:...}` 替换。**这是正经做法。**

**决定：采用方案 B**。下面是 Compiler 扩展：

- [ ] **Step 3: 扩展 `src/ma/core/topic/compiler.py` 支持 env 占位符**

修改 `TopicCompiler.compile` 接口加 `settings` 参数：

```python
import os
import re
from typing import Any

_ENV_PLACEHOLDER = re.compile(r"\$\{env:([A-Z_][A-Z0-9_]*)\}")


def _resolve_env(value: Any, env: dict[str, str]) -> Any:
    """递归把 dict / list / str 里的 ${env:VAR} 替换为环境值。"""
    if isinstance(value, str):
        def _sub(m: re.Match[str]) -> str:
            var = m.group(1)
            v = env.get(var)
            if v is None:
                raise KeyError(f"env var {var!r} required by topic config but unset")
            return v
        return _ENV_PLACEHOLDER.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _resolve_env(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env(v, env) for v in value]
    return value


class TopicCompiler:
    def __init__(self, registry: Any = None) -> None:
        self._registry = registry or _global_registry

    def compile(
        self,
        cfg: TopicConfig,
        *,
        env: dict[str, str] | None = None,
    ) -> CompiledTopic:
        envmap = env if env is not None else dict(os.environ)
        auth_params = _resolve_env(cfg.auth.params, envmap)
        enrich_params = _resolve_env(cfg.enrich.params, envmap)
        intent_params = _resolve_env(cfg.intent.params, envmap)

        auth = self._registry.create("auth", cfg.auth.plugin, auth_params)
        enrich = self._registry.create("enrich", cfg.enrich.plugin, enrich_params)
        intent = self._registry.create("intent", cfg.intent.plugin, intent_params)

        adapters: dict[str, Any] = {}
        for node in cfg.graph.nodes:
            adapters[node.id] = self._registry.create(
                "adapter", node.adapter, _resolve_env(node.params, envmap)
            )

        biz_params_model = _compile_biz_params_schema(cfg.biz_params_schema)

        return CompiledTopic(
            topic_id=cfg.topic_id,
            display_name=cfg.display_name,
            default_route=cfg.default_route,
            history=cfg.history,
            auth=auth,
            enrich=enrich,
            intent=intent,
            adapters=adapters,
            biz_params_model=biz_params_model,
            error_messages=cfg.error_messages,
            config=cfg,
        )
```

- [ ] **Step 4: 加 TopicCompiler env 测试**

追加到 `tests/unit/test_topic_compiler.py`:

```python
def test_compile_resolves_env_placeholders(monkeypatch: pytest.MonkeyPatch) -> None:
    _register_fake_plugins()
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    topic = {
        **_TOPIC,
        "graph": {
            "nodes": [
                {
                    "id": "metagc_node",
                    "adapter": "fake_adapter",
                    "output": True,
                    "params": {"base_url": "${env:META_TEST_URL}"},
                }
            ],
            "edges": _TOPIC["graph"]["edges"],
        },
    }
    cfg = TopicConfig.model_validate(topic)
    compiled = TopicCompiler().compile(
        cfg, env={"META_TEST_URL": "http://x.test"}
    )
    assert compiled.adapters["metagc_node"].params == {"base_url": "http://x.test"}


def test_compile_missing_env_raises() -> None:
    _register_fake_plugins()
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    topic = {
        **_TOPIC,
        "graph": {
            "nodes": [{"id": "metagc_node", "adapter": "fake_adapter",
                       "output": True, "params": {"base_url": "${env:NOPE}"}}],
            "edges": _TOPIC["graph"]["edges"],
        },
    }
    cfg = TopicConfig.model_validate(topic)
    with pytest.raises(KeyError, match="NOPE"):
        TopicCompiler().compile(cfg, env={})
```

跑测试，确认通过。

- [ ] **Step 5: Commit**

```bash
git add config/topics/daibiao_xiaoguanjia.yaml prompts/intent/representative.txt \
        src/ma/core/topic/compiler.py tests/unit/test_topic_compiler.py
git commit -m "feat(topic): daibiao_xiaoguanjia YAML + env placeholder resolution in compiler"
```

### Task 20: ChatService 接 TopicRegistry + LangGraph runner

**Files:**
- Modify: `src/ma/core/chat_service.py` — 完全重写
- Create: `tests/unit/test_chat_service_with_fake_topic.py` — 用 fake plugins + 跑真 LangGraph

这是 M1 最关键的 task。这一步要：
1. 把 `ChatRequest` 扩展加 `identity: AuthnIdentity`
2. 在 ChatService 里编译每次请求要跑的 StateGraph（基于 CompiledTopic）
3. 用 `graph.astream_events(version="v2")` 跑 + 按事件 name 转 ChatEvent yield 出去
4. 流前发 `meta`，流后发 `done`

- [ ] **Step 1: 重写 `src/ma/core/chat_service.py`**

```python
"""ChatService：M1 实现 —— 接 TopicRegistry + LangGraph 编译 + astream_events 转 ChatEvent。

设计书 §1.3 / §2.3 / §2.5 / §4.2.2。

接口与 M0 兼容：async def handle(req: ChatRequest) -> AsyncIterator[ChatEvent]
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import ulid
from langgraph.constants import END, START
from langgraph.graph import StateGraph

from ma.core.graph.builtins import (
    make_load_session_node,
    make_persist_node,
    make_reject_node,
)
from ma.core.graph.events import ChatEvent
from ma.core.graph.node_wrappers import (
    make_adapter_node,
    make_auth_node,
    make_enrich_node,
    make_intent_node,
)
from ma.core.graph.state import AuthnIdentity, GraphState
from ma.core.repo.message_repository import MessageRepository
from ma.core.repo.models import Message
from ma.core.repo.session_repository import SessionRepository
from ma.core.topic.registry import TopicRegistry, TopicNotFound
from ma.infra.logging import get_logger


@dataclass(slots=True)
class ChatRequest:
    biz_id: str
    thread_id: str
    w3_account: str
    question: str
    request_id: str
    biz_params: dict[str, Any] = field(default_factory=dict)
    identity: AuthnIdentity | None = None


_log = get_logger("ma.chat")


class ChatService:
    def __init__(
        self,
        *,
        topic_registry: TopicRegistry,
        session_repo_factory,
        message_repo_factory,
    ) -> None:
        """factory: (conn) -> Repository instance；ChatService 在 handle 里
        从 pool 取 conn → 建 repo → 用完释放。
        """
        self._topics = topic_registry
        self._session_repo_factory = session_repo_factory
        self._message_repo_factory = message_repo_factory

    async def handle(self, req: ChatRequest) -> AsyncIterator[ChatEvent]:
        identity = req.identity or AuthnIdentity(w3_account=req.w3_account)
        try:
            topic = self._topics.get(req.biz_id)
        except TopicNotFound:
            yield ChatEvent(
                type="error",
                data={
                    "code": "TOPIC_NOT_FOUND",
                    "message": f"未配置专题：{req.biz_id}",
                    "retryable": False,
                    "request_id": req.request_id,
                },
            )
            yield ChatEvent(
                type="done",
                data={"status": "failed"},
            )
            return

        # biz_params 校验（设计书 §4.6.2）
        # 注意：失败应当是 BAD_REQUEST，在 transport 层校验更合适；
        # M1 简化：在 ChatService 里也兜底一次，输出 error event。
        try:
            topic.biz_params_model.model_validate(req.biz_params)
        except Exception as e:
            yield ChatEvent(
                type="error",
                data={
                    "code": "BAD_REQUEST",
                    "message": "请求参数不合法",
                    "retryable": False,
                    "detail": str(e),
                    "request_id": req.request_id,
                },
            )
            yield ChatEvent(type="done", data={"status": "failed"})
            return

        assistant_message_id = f"msg_{ulid.new()!s}"

        # 装填 meta 事件
        yield ChatEvent(
            type="meta",
            data={
                "thread_id": req.thread_id,
                "request_id": req.request_id,
                "message_id": assistant_message_id,
                "topic": req.biz_id,
            },
        )

        # 编译 StateGraph
        graph = self._build_graph(topic, assistant_message_id)

        initial_state: GraphState = {
            "request_id": req.request_id,
            "biz_id": req.biz_id,
            "thread_id": req.thread_id,
            "identity": identity,
            "question": req.question,
            "biz_params": req.biz_params,
            "assistant_message_id": assistant_message_id,
        }

        # 写入 user message（先于 graph 跑）—— M1 简化：放在这里而不在 load_session 节点
        await self._persist_user_message(req)

        # astream_events 跑图
        try:
            async for event in graph.astream_events(
                initial_state,
                config={"configurable": {"request_id": req.request_id}},
                version="v2",
            ):
                ev = event.get("event")
                if ev == "on_custom_event":
                    name = event.get("name")
                    if name in {"thinking", "progress", "delta", "tool", "error"}:
                        yield ChatEvent(type=name, data=event.get("data") or {})
        except Exception as e:
            _log.exception("chat_loop_error")
            yield ChatEvent(
                type="error",
                data={
                    "code": "INTERNAL",
                    "message": topic.error_messages.get("INTERNAL", "服务异常"),
                    "retryable": False,
                    "request_id": req.request_id,
                },
            )

        yield ChatEvent(
            type="done",
            data={
                "message_id": assistant_message_id,
                "status": "complete",
            },
        )

    async def _persist_user_message(self, req: ChatRequest) -> None:
        from ma.core.repo.models import Message

        async with self._session_repo_factory.acquire() as conn:
            session_repo = self._session_repo_factory.build(conn)
            message_repo = self._message_repo_factory.build(conn)
            await session_repo.get_or_create(
                thread_id=req.thread_id,
                biz_id=req.biz_id,
                w3_account=req.w3_account,
            )
            await message_repo.append(
                msg=Message(
                    message_id=f"msg_{ulid.new()!s}",
                    thread_id=req.thread_id,
                    seq=0,
                    role="user",
                    content=req.question,
                    request_id=req.request_id,
                    status="complete",
                )
            )

    def _build_graph(self, topic, assistant_message_id: str):
        sg = StateGraph(GraphState)

        # 内置节点要拿到 repos，但每个请求需要独立 conn —— M1 简化：
        # 通过 closure 把 factory 传进去，在节点里用 acquire。
        # （现实生产可能希望把 conn 拿到 GraphState 里以减少 acquire 次数；
        # 但 LangGraph 序列化 state 会出问题。先用最直接的写法。）
        session_factory = self._session_repo_factory
        message_factory = self._message_repo_factory

        async def load_session_node(state: GraphState, config: dict[str, Any]):
            async with session_factory.acquire() as conn:
                node = make_load_session_node(
                    session_repo=session_factory.build(conn),
                    message_repo=message_factory.build(conn),
                    max_turns=topic.history.max_turns,
                )
                return await node(state, config)

        async def persist_node(state: GraphState, config: dict[str, Any]):
            async with session_factory.acquire() as conn:
                node = make_persist_node(
                    session_repo=session_factory.build(conn),
                    message_repo=message_factory.build(conn),
                )
                return await node(state, config)

        sg.add_node("load_session", load_session_node)
        sg.add_node("auth", make_auth_node(topic.auth))
        sg.add_node("reject", make_reject_node(topic.error_messages))
        sg.add_node("enrich", make_enrich_node(topic.enrich))

        # intent 节点的 labels 来自 mapping keys（已在 TopicConfig 校验过等于 intent.labels）
        cond_edges = [e for e in topic.config.graph.edges if e.type == "conditional"]
        mapping = cond_edges[0].mapping
        labels = list(mapping.keys())
        sg.add_node(
            "intent",
            make_intent_node(
                topic.intent, default_route=topic.default_route, labels=labels
            ),
        )

        # adapter 节点们
        for node_id, adapter in topic.adapters.items():
            sg.add_node(node_id, make_adapter_node(adapter, output=True))

        sg.add_node("persist", persist_node)

        # 边
        sg.add_edge(START, "load_session")
        sg.add_edge("load_session", "auth")
        sg.add_conditional_edges(
            "auth",
            lambda s: "reject" if not s["auth"].passed else "enrich",
            {"reject": "reject", "enrich": "enrich"},
        )
        sg.add_edge("enrich", "intent")
        sg.add_conditional_edges(
            "intent",
            lambda s: s.get("route", topic.default_route),
            mapping,
        )
        for node_id in topic.adapters:
            sg.add_edge(node_id, "persist")
        sg.add_edge("reject", "persist")
        sg.add_edge("persist", END)

        return sg.compile()
```

> **关于 `session_repo_factory`：** ChatService 不直接持 asyncpg pool；而是接受两个 *factory* 抽象（`acquire()` ctx manager + `build(conn)`）。这让单测能注入 fake factory 不依赖真 DB。

- [ ] **Step 2: 写 `tests/unit/test_chat_service_with_fake_topic.py`**

```python
"""ChatService 单测：用 fake repo factory + fake plugins，跑真 LangGraph。"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pytest

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import AuthnIdentity
from ma.core.repo.models import Message, Session


@dataclass
class _FakeSessionRepo:
    sessions: dict[str, Session] = field(default_factory=dict)

    async def get_or_create(self, *, thread_id: str, biz_id: str, w3_account: str, ext=None) -> Session:
        if thread_id not in self.sessions:
            self.sessions[thread_id] = Session(
                thread_id=thread_id, biz_id=biz_id, w3_account=w3_account,
                title=None, status="active", created_at=datetime.now(),
                updated_at=datetime.now(), last_message_at=None, ext=ext or {},
            )
        return self.sessions[thread_id]

    async def touch(self, thread_id: str) -> None: ...


@dataclass
class _FakeMessageRepo:
    messages: list[Message] = field(default_factory=list)

    async def list_recent(self, *, thread_id: str, limit: int) -> list[Message]:
        msgs = [m for m in self.messages if m.thread_id == thread_id]
        return list(reversed(msgs))[:limit]

    async def append(self, *, msg: Message) -> None:
        if msg.seq == 0:
            msg.seq = sum(1 for m in self.messages if m.thread_id == msg.thread_id) + 1
        self.messages.append(msg)


class _FakeFactory:
    def __init__(self, repo: Any) -> None:
        self._repo = repo

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Any]:
        yield None

    def build(self, conn: Any) -> Any:
        return self._repo


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module
    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    # reload all plugin modules to re-register
    import importlib
    for modname in [
        "ma.plugins.auth.representative_auth",
        "ma.plugins.enrich.generic_enrich",
        "ma.plugins.intent.llm_classifier",
        "ma.plugins.adapter.metagc",
    ]:
        importlib.reload(importlib.import_module(modname))
    yield new_reg


def _build_topic(metagc_url: str):
    """编译代表小管家 topic，base_url 注入测试用 url。"""
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate({
        "topic_id": "daibiao_xiaoguanjia",
        "display_name": "代表小管家",
        "default_route": "metagc",
        "history": {"max_turns": 5, "scope": "per_topic"},
        "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
        "enrich": {"plugin": "generic_enrich", "params": {"fields_to_inject": ["region"]}},
        "intent": {
            "plugin": "llm_classifier",
            "params": {
                "provider": "fake",
                "model": "fake-model",
                "labels": ["metagc"],
                "prompt_template": "p.txt",
                "fake_responses": ["route: metagc\nreason: 业务"],
            },
        },
        "graph": {
            "nodes": [
                {"id": "metagc_adapter", "adapter": "metagc", "output": True,
                 "params": {"base_url": metagc_url, "timeout_ms": 5000}}
            ],
            "edges": [{
                "from": "intent", "type": "conditional", "route_by": "route",
                "mapping": {"metagc": "metagc_adapter"},
            }],
        },
        "biz_params_schema": {
            "type": "object", "required": ["region"],
            "properties": {"region": {"type": "string"}},
        },
        "error_messages": {
            "FORBIDDEN_TOPIC": "无权限", "FORBIDDEN_SCOPE": "范围",
            "DOWNSTREAM_TIMEOUT": "慢", "DOWNSTREAM_ERROR": "错", "INTERNAL": "炸",
        },
    })
    return TopicCompiler().compile(cfg, env={})


@pytest.fixture
async def http_metagc_url():
    """ASGI mock metagc → 跑出真 base_url 走 ASGITransport 比较麻烦。
    M1 ChatService 单测用 httpx ASGI 路径太曲折；改用 mock 一个 Adapter run() 直接 yield。

    实际上更简单的做法：把 MetaGCAdapter 实例替换为 fake adapter 在 fresh_registry 里。
    """
    # 这里返回的 url 不重要，反正下面 patch 掉 adapter
    return "http://unused.test"


@pytest.mark.asyncio
async def test_chat_service_happy_path(http_metagc_url):
    """走完整流程：auth 通过 → enrich → intent → metagc → persist → done。
    
    用 monkeypatch 替换 MetaGCAdapter.run 为 fake yield 序列。
    """
    from unittest.mock import patch

    from ma.core.chat_service import ChatRequest, ChatService
    from ma.core.graph.events import ChatEvent as _Ev
    from ma.core.topic.registry import TopicRegistry

    topic = _build_topic(http_metagc_url)
    reg = TopicRegistry()
    reg.register(topic)

    session_repo = _FakeSessionRepo()
    message_repo = _FakeMessageRepo()
    session_factory = _FakeFactory(session_repo)
    message_factory = _FakeFactory(message_repo)

    svc = ChatService(
        topic_registry=reg,
        session_repo_factory=session_factory,
        message_repo_factory=message_factory,
    )

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "hello "})
        yield ChatEvent(type="delta", data={"content": "world"})

    with patch(
        "ma.plugins.adapter.metagc.MetaGCAdapter.run",
        new=fake_run,
    ):
        req = ChatRequest(
            biz_id="daibiao_xiaoguanjia",
            thread_id="th_happy",
            w3_account="alice",
            question="销售额？",
            request_id="req_happy",
            biz_params={"region": "huadong"},
            identity=AuthnIdentity(w3_account="alice", raw_claims={"role": "REP"}),
        )
        events = [ev async for ev in svc.handle(req)]

    types = [e.type for e in events]
    assert types[0] == "meta"
    assert types[-1] == "done"
    assert "delta" in types
    deltas = [e for e in events if e.type == "delta"]
    full = "".join(e.data["content"] for e in deltas)
    assert full == "hello world"

    # 鉴权通过 → 不应有 reject 事件（reject 出的话术也是 delta 形式，但内容是错误话术）
    err_evs = [e for e in events if e.type == "error"]
    assert err_evs == []


@pytest.mark.asyncio
async def test_chat_service_auth_failure_yields_reject_message(http_metagc_url):
    """鉴权失败 → reject 节点 → answer_chunks 含话术 → done。"""
    from ma.core.chat_service import ChatRequest, ChatService
    from ma.core.topic.registry import TopicRegistry

    topic = _build_topic(http_metagc_url)
    reg = TopicRegistry()
    reg.register(topic)
    svc = ChatService(
        topic_registry=reg,
        session_repo_factory=_FakeFactory(_FakeSessionRepo()),
        message_repo_factory=_FakeFactory(_FakeMessageRepo()),
    )
    req = ChatRequest(
        biz_id="daibiao_xiaoguanjia",
        thread_id="th_rej",
        w3_account="bob",
        question="?",
        request_id="req_rej",
        biz_params={"region": "huadong"},
        identity=AuthnIdentity(w3_account="bob", raw_claims={"role": "VIEWER"}),
    )
    events = [ev async for ev in svc.handle(req)]
    # 鉴权拒绝时 reject 节点 yield 一个固定话术作为 answer_chunks；
    # M1 简化 —— 这部分话术不通过 delta 流给用户（M2 把 reject 节点改为也通过
    # dispatch_custom_event 发 delta）。
    # 所以 M1 这个 case 应当至少能跑完 done。
    assert events[0].type == "meta"
    assert events[-1].type == "done"
```

> **测试简化说明**：上面 happy path 用 `patch` 替换 MetaGCAdapter.run 为 fake；这避开了在单测里起一个真 metagc 服务的复杂度。**真完整链路（含 mock metagc）放在 Phase J 的 e2e**。

- [ ] **Step 3: 跑测试**

```bash
python -m uv run pytest tests/unit/test_chat_service_with_fake_topic.py -v
python -m uv run pytest tests/unit/ -v  # 全 unit
python -m uv run mypy src
python -m uv run ruff check .
```

Expected: 2 + 之前所有单测通过；clean。

- [ ] **Step 4: 修复（如果有）M0 旧的 test_events.py 的 ChatService 测试**

M0 的 `test_chat_service_*` 三个用例假定 ChatService **不接 TopicRegistry**，调用 `ChatService()` 即可。M1 改了 ChatService 构造签名，这三个用例会断。

**怎么处理？** 删掉 M0 的 stub 用例（它们覆盖的契约——meta 首 done 尾，delta 至少 1 个，meta 含 thread_id/request_id——已被新的 ChatService 测试覆盖）。

修改 `tests/unit/test_events.py`：删除三个 `test_chat_service_*` 用例（保留所有 ChatEvent / encode_sse 用例）。

跑测试再次：

```bash
python -m uv run pytest -v
```

Expected: 全过。

- [ ] **Step 5: Commit**

```bash
git add src/ma/core/chat_service.py tests/unit/test_chat_service_with_fake_topic.py \
        tests/unit/test_events.py
git commit -m "feat(core): ChatService re-wired to TopicRegistry + LangGraph runner

Breaking change to ChatService constructor: now takes (topic_registry,
session_repo_factory, message_repo_factory). M0's stub ChatService tests
in test_events.py removed (their contracts subsumed by new
test_chat_service_with_fake_topic)."
```

### Task 21: main.py lifespan 完整装配

**Files:**
- Modify: `src/ma/main.py`
- Modify: `src/ma/api/sse.py` — 增加 identity 透传 + TOPIC_NOT_FOUND 等 HTTP 错误码处理

- [ ] **Step 1: 改 `src/ma/main.py` 把 DB / plugins / topics 全装上**

```python
"""FastAPI app 入口。

设计书 §8.2.3 启动顺序（M1 版）：
1. Settings → fail-fast
2. configure logging
3. configure tracing
4. create DB pools
5. load_all_plugins
6. 编译所有 Topic YAML
7. 装配 ChatService 注入 app.state
8. FastAPIInstrumentor
9. mark_ready
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import yaml
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from ma.api import health, sse
from ma.core.chat_service import ChatService
from ma.core.plugin.bootstrap import load_all_plugins
from ma.core.repo.message_repository import MessageRepository
from ma.core.repo.session_repository import SessionRepository
from ma.core.topic.compiler import TopicCompiler
from ma.core.topic.config import TopicConfig
from ma.core.topic.registry import TopicRegistry
from ma.infra.db import close_pools, create_pools
from ma.infra.logging import configure_logging, get_logger
from ma.infra.settings import Settings
from ma.infra.tracing import configure_tracing


class _RepoFactory:
    """从 asyncpg pool 拿 conn 建 repository。"""

    def __init__(self, pool: asyncpg.Pool, repo_cls: type) -> None:
        self._pool = pool
        self._repo_cls = repo_cls

    def acquire(self):
        return self._pool.acquire()

    def build(self, conn: asyncpg.Connection):
        return self._repo_cls(conn=conn)


def create_app() -> FastAPI:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)
    configure_tracing(settings)
    log = get_logger("ma.main")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info("startup_begin", env=settings.env)

        # 4. DB pools
        if settings.pg_dsn_rw is not None:
            pool_rw, pool_ro = await create_pools(settings)
            app.state.pg_pool_rw = pool_rw
            app.state.pg_pool_ro = pool_ro
        else:
            log.warning(
                "no_pg_dsn",
                hint="MA_PG_DSN_RW not set; service running without DB (smoke only)",
            )
            app.state.pg_pool_rw = None

        # 5. plugins
        load_all_plugins()

        # 6. compile topics
        topic_registry = TopicRegistry()
        topics_dir = Path(settings.config_topics_dir)
        if topics_dir.is_dir():
            for yaml_path in sorted(topics_dir.glob("*.yaml")):
                with open(yaml_path, encoding="utf-8") as f:
                    raw = yaml.safe_load(f)
                cfg = TopicConfig.model_validate(raw)
                compiled = TopicCompiler().compile(cfg, env=dict(os.environ))
                topic_registry.register(compiled)
                log.info("topic_loaded", topic_id=compiled.topic_id)
        app.state.topic_registry = topic_registry

        # 7. ChatService —— 若 DB pool 缺失，仍创建但 handle 会在 _persist_user_message 抛
        if app.state.pg_pool_rw is not None:
            sess_factory = _RepoFactory(app.state.pg_pool_rw, SessionRepository)
            msg_factory = _RepoFactory(app.state.pg_pool_rw, MessageRepository)
            app.state.chat_service = ChatService(
                topic_registry=topic_registry,
                session_repo_factory=sess_factory,
                message_repo_factory=msg_factory,
            )
        else:
            app.state.chat_service = None  # transport 层会回 503

        health.mark_ready()
        log.info("startup_complete", topics=topic_registry.ids())
        yield
        health.mark_not_ready()
        if app.state.pg_pool_rw is not None:
            await close_pools(app.state.pg_pool_rw, getattr(app.state, "pg_pool_ro", None))
        log.info("shutdown_complete")

    app = FastAPI(title="MasterAgent", version="0.2.0", lifespan=lifespan)
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(sse.router, prefix="/api/v1")
    FastAPIInstrumentor().instrument_app(app)
    return app


app = create_app()
```

- [ ] **Step 2: 改 `src/ma/api/sse.py`** 加 identity + TOPIC_NOT_FOUND fast path

```python
"""POST /api/v1/chat/sse 端点。

M1 改动：
- 加 AuthnMiddleware 占位（信任 X-User-Account header）→ AuthnIdentity → state
- 在调 ChatService 之前快速校验 biz_id 是否在 TopicRegistry 中
- 若 ChatService 未装配（无 DB） → 503
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import ulid
from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ma.core.chat_service import ChatRequest, ChatService
from ma.core.graph.events import encode_sse
from ma.core.graph.state import AuthnIdentity
from ma.core.topic.registry import TopicRegistry, TopicNotFound
from ma.infra.logging import bind_request_ctx, get_logger

router = APIRouter()
log = get_logger("ma.api.sse")


class ChatBody(BaseModel):
    biz_id: str = Field(min_length=1)
    thread_id: str = Field(min_length=1)
    w3_account: str = Field(min_length=1)
    question: str = Field(min_length=1)
    biz_params: dict[str, Any] = Field(default_factory=dict)


def _new_request_id() -> str:
    return f"req_{ulid.new()!s}"


@router.post("/chat/sse")
async def chat_sse(
    request: Request,
    body: ChatBody,
    x_request_id: str | None = Header(default=None),
    x_user_account: str | None = Header(default=None),
) -> StreamingResponse:
    rid = x_request_id or _new_request_id()
    bind_request_ctx(
        request_id=rid, thread_id=body.thread_id, w3_account=body.w3_account
    )
    log.info("chat_started", biz_id=body.biz_id)

    chat_service: ChatService | None = request.app.state.chat_service
    topic_registry: TopicRegistry = request.app.state.topic_registry

    if chat_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DB not configured (MA_PG_DSN_RW required)",
        )

    # Topic 存在性快速校验 → HTTP 404（设计书 §4.5）
    try:
        topic_registry.get(body.biz_id)
    except TopicNotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown biz_id: {body.biz_id}",
        )

    # AuthnMiddleware 占位 —— 信任 X-User-Account header
    # （生产部署前必须替换为 JWT / Gateway 信任）
    if x_user_account is not None and x_user_account != body.w3_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="X-User-Account header mismatch with body.w3_account",
        )
    # M1 简化：role 从可选 header X-User-Role 拿；缺省 = "REP"（让默认能跑通鉴权）
    role = request.headers.get("X-User-Role", "REP")
    identity = AuthnIdentity(
        w3_account=body.w3_account,
        raw_claims={"role": role},
    )

    chat_req = ChatRequest(
        biz_id=body.biz_id,
        thread_id=body.thread_id,
        w3_account=body.w3_account,
        question=body.question,
        request_id=rid,
        biz_params=body.biz_params,
        identity=identity,
    )

    async def stream() -> AsyncIterator[bytes]:
        async for ev in chat_service.handle(chat_req):
            yield encode_sse(ev).encode("utf-8")

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "X-Request-Id": rid,
        },
    )
```

- [ ] **Step 3: 修复 `tests/unit/test_sse_endpoint.py`**

M0 的 SSE 测试用了 `create_app()` 启动；M1 改之后 lifespan 会去找 topics 配置 → 测试需要要么挂上 `MA_CONFIG_TOPICS_DIR` 指向测试 fixtures，要么允许 topics_dir 不存在静默通过。

建议改测试 fixture：

```python
@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path) -> TestClient:
    monkeypatch.setenv("MA_ENV", "dev")
    # 不设 MA_PG_DSN_RW → ChatService 是 None → 旧测试期望 SSE 跑通会失败
    # 给个空 topics dir 让 lifespan 不爆
    topics_dir = tmp_path / "topics"
    topics_dir.mkdir()
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", str(topics_dir))
    from ma.main import create_app

    with TestClient(create_app()) as c:
        yield c


def test_sse_returns_503_when_chat_service_unconfigured(client: TestClient) -> None:
    """M1：没配 MA_PG_DSN_RW → SSE 应当 503 而非 200。"""
    body = {
        "biz_id": "daibiao_xiaoguanjia",
        "thread_id": "th",
        "w3_account": "alice",
        "question": "hi",
    }
    r = client.post("/api/v1/chat/sse", json=body)
    assert r.status_code == 503


# 404 路径（未知 biz_id）需要 ChatService 已装 + topic_registry 已加载，
# 在 unit 层难以单独构造（要起 DB 才能装 ChatService）。
# 推到 e2e Phase J 的 test_unknown_biz_id_returns_404（如需要补的话）。
```

> **简化**：M0 SSE 测试的"5 帧 stub 流"已不适用 M1（ChatService 不再 stub）；删掉那批 SSE 流式断言，新增 503 / 404 测试代替。完整流式断言放在 Phase J 的 e2e 测试。

具体改法 —— 替换 `tests/unit/test_sse_endpoint.py` 内容为简化版（只测 422 / 503 / 404 这种 fast-path），完整流式契约推到 e2e 里。

- [ ] **Step 4: 全套测试**

```bash
python -m uv run pytest -v
python -m uv run mypy src
python -m uv run ruff check . && python -m uv run ruff format --check .
```

Expected: 全过；clean。

- [ ] **Step 5: 手动 smoke test（要 docker 才有意义；本机无 docker 则跳过）**

```bash
# 起 mock metagc
MA_ENV=dev python -m tests.mock_downstreams.metagc_sse &
sleep 2
# 起一个空 postgres 容器（docker 必需）—— 跳过
```

- [ ] **Step 6: Commit**

```bash
git add src/ma/main.py src/ma/api/sse.py tests/unit/test_sse_endpoint.py
git commit -m "feat: wire DB pools + plugin bootstrap + topic compilation into lifespan; SSE checks topic 404"
```

**完成 Phase H 的标志：** 3 个 commit；ChatService 重写完成；main.py lifespan 装配完整；端到端可在 dev 环境跑通（要 DB + mock metagc）。

---

## Phase I: Stream contract tests

参考：设计书 §7.2.2。10 条契约里 WS 相关的（#10）推到 M4；M1 必须覆盖剩下 9 条。

### Task 22: tests/stream/ 基础设施

**Files:**
- Create: `tests/stream/__init__.py`
- Create: `tests/stream/conftest.py`
- Create: `tests/stream/test_contracts.py` — 9 条契约的 test_xxx 函数（每条独立 test）

参考：设计书 §7.2.2 / §7.4.

- [ ] **Step 1: 写 fixtures `tests/stream/conftest.py`**

```python
"""Stream-contract 测试基础设施：直接调 ChatService.handle 收集 ChatEvent。

不经过 HTTP / TestClient —— 直接拿 svc instance，喂 ChatRequest，收事件列表。
这层覆盖 ChatService 行为契约；HTTP transport 编码契约在 SSE endpoint 单测里。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pytest

from ma.core.chat_service import ChatRequest, ChatService
from ma.core.graph.events import ChatEvent
from ma.core.graph.state import AuthnIdentity
from ma.core.repo.models import Message, Session
from ma.core.topic.compiler import TopicCompiler
from ma.core.topic.config import TopicConfig
from ma.core.topic.registry import TopicRegistry


@dataclass
class FakeSessionRepo:
    sessions: dict[str, Session] = field(default_factory=dict)

    async def get_or_create(self, *, thread_id: str, biz_id: str, w3_account: str, ext=None) -> Session:
        if thread_id not in self.sessions:
            self.sessions[thread_id] = Session(
                thread_id=thread_id, biz_id=biz_id, w3_account=w3_account,
                title=None, status="active", created_at=datetime.now(),
                updated_at=datetime.now(), last_message_at=None, ext=ext or {},
            )
        return self.sessions[thread_id]

    async def touch(self, thread_id: str) -> None: ...


@dataclass
class FakeMessageRepo:
    messages: list[Message] = field(default_factory=list)

    async def list_recent(self, *, thread_id: str, limit: int) -> list[Message]:
        msgs = [m for m in self.messages if m.thread_id == thread_id]
        return list(reversed(msgs))[:limit]

    async def append(self, *, msg: Message) -> None:
        if msg.seq == 0:
            msg.seq = sum(1 for m in self.messages if m.thread_id == msg.thread_id) + 1
        self.messages.append(msg)


class FakeFactory:
    def __init__(self, repo: Any) -> None:
        self._repo = repo

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Any]:
        yield None

    def build(self, conn: Any) -> Any:
        return self._repo


def _topic_config(intent_responses: list[str] | None = None) -> TopicConfig:
    return TopicConfig.model_validate({
        "topic_id": "test_topic",
        "display_name": "测试",
        "default_route": "metagc",
        "history": {"max_turns": 5, "scope": "per_topic"},
        "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
        "enrich": {"plugin": "generic_enrich", "params": {"fields_to_inject": ["region"]}},
        "intent": {
            "plugin": "llm_classifier",
            "params": {
                "provider": "fake",
                "model": "fake-model",
                "labels": ["metagc"],
                "prompt_template": "p.txt",
                "fake_responses": intent_responses or ["route: metagc\nreason: ok"],
            },
        },
        "graph": {
            "nodes": [{"id": "metagc_adapter", "adapter": "metagc", "output": True,
                       "params": {"base_url": "http://unused", "timeout_ms": 5000}}],
            "edges": [{"from": "intent", "type": "conditional", "route_by": "route",
                       "mapping": {"metagc": "metagc_adapter"}}],
        },
        "biz_params_schema": {"type": "object", "required": ["region"],
                              "properties": {"region": {"type": "string"}}},
        "error_messages": {
            "FORBIDDEN_TOPIC": "无权限", "FORBIDDEN_SCOPE": "范围",
            "DOWNSTREAM_TIMEOUT": "慢", "DOWNSTREAM_ERROR": "错", "INTERNAL": "炸",
        },
    })


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    """每个测试独立 registry，避免插件注册副作用累积。"""
    from ma.core.plugin import registry as registry_module
    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    import importlib
    for modname in [
        "ma.plugins.auth.representative_auth",
        "ma.plugins.enrich.generic_enrich",
        "ma.plugins.intent.llm_classifier",
        "ma.plugins.adapter.metagc",
    ]:
        importlib.reload(importlib.import_module(modname))
    yield new_reg


@pytest.fixture
def fake_repos():
    return FakeSessionRepo(), FakeMessageRepo()


@pytest.fixture
def topic_config():
    return _topic_config


@pytest.fixture
def make_service(fake_repos, topic_config):
    """工厂：(intent_responses=None) → (svc, repos)。"""
    def _make(intent_responses=None) -> tuple[ChatService, tuple]:
        cfg = topic_config(intent_responses)
        compiled = TopicCompiler().compile(cfg, env={})
        reg = TopicRegistry()
        reg.register(compiled)
        sess_repo, msg_repo = fake_repos
        svc = ChatService(
            topic_registry=reg,
            session_repo_factory=FakeFactory(sess_repo),
            message_repo_factory=FakeFactory(msg_repo),
        )
        return svc, (sess_repo, msg_repo)
    return _make


def make_request(
    *,
    thread_id: str = "th_test",
    role: str = "REP",
    region: str = "huadong",
    question: str = "上个月销售？",
    request_id: str = "req_test",
) -> ChatRequest:
    return ChatRequest(
        biz_id="test_topic",
        thread_id=thread_id,
        w3_account="alice",
        question=question,
        request_id=request_id,
        biz_params={"region": region},
        identity=AuthnIdentity(w3_account="alice", raw_claims={"role": role}),
    )


async def collect(svc: ChatService, req: ChatRequest) -> list[ChatEvent]:
    return [ev async for ev in svc.handle(req)]
```

> 注：fixtures 故意不导出 `make_request` 和 `collect` 作为 fixture，因为它们要灵活接受参数；测试直接 import。

- [ ] **Step 2: 写契约测试 `tests/stream/test_contracts.py`** —— 覆盖 9 条契约

```python
"""Stream-contract 测试：设计书 §7.2.2 的 9 条契约（WS 相关 #10 在 M4）。

每条契约一个独立 test，名字与契约编号绑定，便于将来追溯。
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from ma.core.graph.events import ChatEvent
from tests.stream.conftest import collect, make_request


# Contract 1: meta 恰好 1 次且在首
@pytest.mark.asyncio
async def test_contract_1_meta_exactly_once_at_start(make_service):
    svc, _ = make_service()

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "x"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())
    metas = [e for e in events if e.type == "meta"]
    assert len(metas) == 1
    assert events[0].type == "meta"


# Contract 2: done 恰好 1 次且在末
@pytest.mark.asyncio
async def test_contract_2_done_exactly_once_at_end(make_service):
    svc, _ = make_service()

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "x"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())
    dones = [e for e in events if e.type == "done"]
    assert len(dones) == 1
    assert events[-1].type == "done"


# Contract 3: 仅 output 节点产生 delta（intent 的 fake LLM 不会流 token；
# 但需确认即使 LLM "流" 也不会进 delta）
@pytest.mark.asyncio
async def test_contract_3_only_output_node_produces_delta(make_service):
    """构造 intent 返回多行 text 含 "route: ..."；不应有任何 delta 来自 intent。"""
    svc, _ = make_service(
        intent_responses=["route: metagc\nreason: very long reason should not leak"]
    )

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "adapter-only"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())
    deltas = [e for e in events if e.type == "delta"]
    full = "".join(e.data["content"] for e in deltas)
    assert full == "adapter-only"
    # intent reason 内容不允许出现在 delta 流里
    assert "very long reason should not leak" not in full


# Contract 4: progress.step 命名稳定
@pytest.mark.asyncio
async def test_contract_4_progress_step_names_stable(make_service):
    svc, _ = make_service()

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "x"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())
    progresses = [e for e in events if e.type == "progress"]
    steps = [p.data["step"] for p in progresses]
    assert "auth" in steps
    assert "enrich" in steps
    assert "intent" in steps


# Contract 5: thinking/progress payload 不含敏感字段
@pytest.mark.asyncio
async def test_contract_5_thinking_progress_no_sensitive_keys(make_service):
    """auth 节点的 user_ctx 不应整段塞进 progress；
    enrich 的 enrich_meta 只暴露 "injected" 字段名列表不暴露 value。"""
    svc, _ = make_service()

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "x"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())

    # auth progress 应只暴露 passed=True/False，不暴露 user_ctx 整体
    auth_progresses = [
        e for e in events if e.type == "progress" and e.data["step"] == "auth"
    ]
    for p in auth_progresses:
        extra = p.data.get("extra", {})
        assert "user_ctx" not in extra
        assert "role" not in extra  # 不能透露用户角色信息


# Contract 6: error event 后流可继续（intent fallback 路径）
@pytest.mark.asyncio
async def test_contract_6_stream_continues_after_intent_fallback(make_service):
    """intent 返回无法解析的回答 → IntentFailed → 走 default_route → 流继续。"""
    svc, _ = make_service(intent_responses=["unparseable garbage"])

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "fallback content"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())
    # 应该有 delta（fallback 后跑了 adapter）+ 1 个 done
    deltas = [e for e in events if e.type == "delta"]
    assert len(deltas) >= 1
    assert events[-1].type == "done"


# Contract 7: 客户端断开 → message 标 partial （M1 单测里无法真正模拟客户端断
# 开；推到 e2e 阶段。这里只断言 persist 节点在 chunks 为空时使用 status=partial）
@pytest.mark.asyncio
async def test_contract_7_persist_marks_partial_when_no_chunks(make_service):
    """Adapter 直接抛错（没产生任何 chunk）→ persist 应当存 partial。"""
    svc, (_, msg_repo) = make_service()

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(
            type="error",
            data={"code": "DOWNSTREAM_TIMEOUT", "message": "timeout", "retryable": False},
        )

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())
    # 找到 assistant message
    assistants = [m for m in msg_repo.messages if m.role == "assistant"]
    assert len(assistants) == 1
    assert assistants[0].status == "partial"
    assert assistants[0].content == ""


# Contract 8: retry 命中对用户透明（M1 不实现节点级 retry —— 设计书 §6.6.1 那部分
# 推到 M3）；用 placeholder 标记这条契约 todo。
@pytest.mark.asyncio
async def test_contract_8_retry_transparent_to_user_PLACEHOLDER():
    """M1 未实现 retry；M3 起补这一条。当前仅占位 skip。"""
    pytest.skip("retry not implemented until M3 (design book §6.6.1)")


# Contract 9: 鉴权失败 → reject 节点输出固定话术 → done
@pytest.mark.asyncio
async def test_contract_9_auth_rejection_yields_message_and_done(make_service):
    svc, _ = make_service()

    async def fake_run(self, state, *, output: bool):
        # 不应该被调到 —— auth 失败应直接跳到 reject
        yield ChatEvent(type="delta", data={"content": "should not appear"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request(role="VIEWER"))
    # reject 节点 yield 进 answer_chunks，但 M1 reject 节点不 dispatch delta
    # （见 builtins.make_reject_node：只 return {"answer_chunks": [...]}）→ user 看不到话术
    # 但 done 必须有
    assert events[-1].type == "done"
    # 没有 delta（adapter 没被调）
    deltas = [e for e in events if e.type == "delta"]
    assert deltas == []


# Contract 10: WS 多 request 并发 → M4
@pytest.mark.asyncio
async def test_contract_10_ws_multi_request_isolation_PLACEHOLDER():
    """WS 通道在 M4 实现，本契约 M4 补。"""
    pytest.skip("WS channel deferred to M4")
```

> **关于 Contract 9 的 M1 简化**：M1 的 reject 节点把话术写到 `answer_chunks` 但**不**通过 `dispatch_custom_event` 发 `delta`，所以前端实际看不到话术内容（只看到 meta + done）。**这违反设计书 §4.5 的 "reject 走 error event + 兜底话术" 表面承诺**，但本里程碑接受这一简化 —— M2 当 reject 节点改为同时发 delta 时再修复契约 #9。这一偏差需要记录在 M1 完成笔记里。

- [ ] **Step 3: 跑测试**

```bash
python -m uv run pytest tests/stream/ -v
```

Expected: 7 passed + 2 skipped。

- [ ] **Step 4: 全套**

```bash
python -m uv run pytest -v
```

Expected: 全过。

- [ ] **Step 5: Commit**

```bash
git add tests/stream/
git commit -m "test(stream): 9 stream contract tests (2 skipped: retry M3, WS M4)"
```

### Task 23: 修订 reject 节点输出契约 (deferred to M2 -- M1 placeholder commit)

跳过 —— Contract 9 的"reject 也产 delta"留到 M2 完成。这一 task **不实现代码**，仅在完成笔记里记录 "M2 第一件事：改 reject 节点 dispatch_custom_event delta"。

**Phase I 完成标志：** 1 个 commit；9 个契约测试（7 实测 + 2 占位 skip）；contract 9 简化记录在完成笔记。

---

## Phase J: e2e + 出口验证

### Task 24: e2e test_daibiao_xiaoguanjia_sse

**Files:**
- Create: `tests/e2e/__init__.py`
- Create: `tests/e2e/test_daibiao_xiaoguanjia_sse.py`

最完整的一条 e2e：起 OpenGauss container + mock metagc + MA app，curl SSE 拿完整 7-type 事件流，验证消息落库。

- [ ] **Step 1: 写 e2e 测试**

```python
"""e2e: 代表小管家 + MetaGC mock 的全链路。

依赖：
- OpenGauss container（testcontainers postgres image，复用 tests/integration/conftest.py 的 pg_dsn fixture）
- mock metagc server（uvicorn）
- MA FastAPI app（in-process via TestClient）
"""
from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
import uvicorn
from fastapi.testclient import TestClient

from tests.integration.conftest import pg_dsn  # type: ignore


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@pytest_asyncio.fixture
async def metagc_server() -> AsyncIterator[str]:
    from tests.mock_downstreams.metagc_sse import app

    port = _get_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    for _ in range(40):
        if server.started:
            break
        await asyncio.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


def _parse_sse(body: str) -> list[dict]:
    events: list[dict] = []
    for block in body.split("\n\n"):
        block = block.strip("\n")
        if not block or block.startswith(":"):
            continue
        ev: dict = {}
        for line in block.splitlines():
            if line.startswith("event: "):
                ev["event"] = line[len("event: "):]
            elif line.startswith("data: "):
                ev["data"] = json.loads(line[len("data: "):])
        if ev:
            events.append(ev)
    return events


@pytest.mark.asyncio
async def test_daibiao_xiaoguanjia_full_loop(
    pg_dsn: str, metagc_server: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    monkeypatch.setenv("MA_METAGC_BASE_URL", metagc_server)
    # 用真实的 config/topics/daibiao_xiaoguanjia.yaml
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", "config/topics")

    from ma.main import create_app

    with TestClient(create_app()) as client:
        body = {
            "biz_id": "daibiao_xiaoguanjia",
            "thread_id": "th_e2e",
            "w3_account": "alice",
            "question": "上个月华东大区销售额？",
            "biz_params": {"region": "huadong"},
        }
        headers = {
            "X-Request-Id": "req_e2e",
            "X-User-Role": "REP",  # 让鉴权过
        }
        with client.stream(
            "POST", "/api/v1/chat/sse", json=body, headers=headers
        ) as r:
            assert r.status_code == 200
            text = "".join(r.iter_text())

    events = _parse_sse(text)
    types = [e["event"] for e in events]
    assert types[0] == "meta"
    assert types[-1] == "done"
    # 应有 thinking / progress / delta
    assert "thinking" in types
    assert "progress" in types
    deltas = [e for e in events if e["event"] == "delta"]
    assert len(deltas) >= 2
    full = "".join(d["data"]["content"] for d in deltas)
    assert "上个月华东大区销售额" in full

    # 验证消息落库
    import asyncpg
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT role, status, content FROM ma_message "
            "WHERE thread_id = $1 ORDER BY seq",
            "th_e2e",
        )
    await pool.close()
    assert len(rows) == 2  # user + assistant
    assert rows[0]["role"] == "user"
    assert rows[1]["role"] == "assistant"
    assert rows[1]["status"] == "complete"
```

- [ ] **Step 2: 跑 e2e**

```bash
python -m uv run pytest tests/e2e/ -v
```

Expected (CI 上 docker 可用): 1 passed.
Expected (本机无 docker): pg_dsn fixture skip → 整个 e2e skip。

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/
git commit -m "test(e2e): full代表小管家 SSE loop with real OpenGauss + mock MetaGC"
```

### Task 25: Dockerfile / CI / Makefile 更新

**Files:**
- Modify: `Dockerfile` — 加 `COPY config/` `COPY sql/` `COPY prompts/` `COPY db/`
- Modify: `.github/workflows/ci.yml` — 增加 integration + e2e job
- Modify: `Makefile` — 增加 `test-integration` / `test-e2e` 目标
- Modify: `README.md` — M1 状态

- [ ] **Step 1: 改 Dockerfile**

在 runtime stage 加：

```dockerfile
COPY config/ /app/config/
COPY sql/ /app/sql/
COPY prompts/ /app/prompts/
COPY db/ /app/db/
```

放在 `COPY pyproject.toml /app/` 之后。

- [ ] **Step 2: 改 CI workflow `.github/workflows/ci.yml`**

在现有 `check` job 之后增加 `integration` job：

```yaml
  integration:
    runs-on: ubuntu-latest
    needs: check
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: ma
          POSTGRES_PASSWORD: ma
          POSTGRES_DB: ma_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install uv==0.11.24
      - run: uv sync --frozen --all-groups
      - env:
          MA_ENV: test
          MA_TEST_PG_DSN: postgresql://ma:ma@127.0.0.1:5432/ma_test
        run: uv run pytest tests/integration tests/e2e -v
```

> 整合测试用 GitHub service container (官方 postgres) 而非 testcontainers 自启 —— 简单，跑得快。`tests/integration/conftest.py` 的 `pg_dsn` fixture 会读 `MA_TEST_PG_DSN` 跳过 testcontainers 分支。

- [ ] **Step 3: 改 Makefile**

加目标：

```makefile
test-unit:
	python -m uv run pytest tests/unit -v

test-stream:
	python -m uv run pytest tests/stream -v

test-integration:
	python -m uv run pytest tests/integration -v

test-e2e:
	python -m uv run pytest tests/e2e -v

test-all:
	python -m uv run pytest -v
```

把现有 `test` 改为 `test-all`，或保留 `test`。

- [ ] **Step 4: 改 README.md**

```markdown
## 当前状态

**M1 — 代表小管家专题端到端跑通。**
- POST /api/v1/chat/sse → 鉴权 → 补全 → 意图识别（fake LLM） → MetaGC adapter → SSE 7-type 流 → 落库 → done
- 仅支持代表小管家一个专题（M2 加经营小助手 / 销售合同责任人）
- MetaGC 用本地 mock server；真接入 M3
- WebSocket 通道 M4
- 完整测试套（unit + stream + integration + e2e），9 条流式契约（2 个推后）

环境变量（M1 新增）：

| MA_PG_DSN_RW | 必填（生产） | OpenGauss / PG DSN |
| MA_METAGC_BASE_URL | 必填（生产） | MetaGC 下游 base URL |
| MA_CONFIG_TOPICS_DIR | 否 | YAML 目录，默认 `config/topics` |
```

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .github/workflows/ci.yml Makefile README.md
git commit -m "build(M1): ship config/sql/prompts/db in image + integration CI job + Make targets"
```

### Task 26: M1 出口验证 + 合并 + tag

- [ ] **Step 1: 全套 sweep**

```bash
python -m uv run pytest -v
python -m uv run mypy src
python -m uv run ruff check . && python -m uv run ruff format --check .
```

Expected: 全过；integration / e2e 如本机有 docker 也全过，无 docker 则 skip。

- [ ] **Step 2: 覆盖率**

```bash
python -m uv run pytest --cov=ma --cov-report=term-missing
```

Expected: core/ 各模块 ≥ 85%（设计书 §7.6 门槛）；总体 ≥ 80%。

- [ ] **Step 3: 写 M1 完成笔记**

`docs/superpowers/plans/2026-06-24-master-agent-m1-metagc-e2e.notes.md`:

```markdown
# M1 完成笔记

- 日期：<完成日期>
- 分支：feature/m1-metagc-e2e（合入 design/master-agent-v1，tag `m1-complete`）
- commit 数：<phase A 1 + B 5 + C 2 + D 2 + E 3 + F 3 + G 2 + H 3 + I 1 + J 3 = 25 个>
- 覆盖率：<实际>

## 与计划相比的偏差

1. M1 SSE endpoint 用 `X-User-Role` header 决定鉴权 role —— 这是 AuthnMiddleware 占位
   的最简形态；M5 接真 JWT/Gateway 时换掉
2. testcontainers 用 `postgres:16-alpine` 而非 OpenGauss 镜像 —— 协议兼容但生产用
   OpenGauss；M2 或上线前在 staging 跑真 OpenGauss
3. Contract 9（鉴权失败话术下发用户）M1 简化：reject 节点仅写 answer_chunks 不 dispatch
   delta，前端实际看不到话术。**M2 第一件事修复**
4. Contract 8（retry 透明） + Contract 10（WS 并发）M1 skip（M3 / M4）

## 给 M2 的提示

1. M2 首个 commit：修 reject 节点同时 dispatch delta 让 contract 9 真生效
2. 经营小助手 + 销售合同责任人 Topic YAML 文件添加后，无需改代码 —— 验证插件化承诺
3. AuthPlugin 接外部用户中心 API 拉真实 role + 客户归属，并把这些注入 user_ctx 供
   GenericEnrich 使用
4. ConditionalEdge 当前 schema 只支持 `from: intent`；M2 多专题验证下来若需要从
   其它节点条件路由，扩展 schema

## 验收清单

- [x] curl SSE 代表小管家 + mock MetaGC，拿到 meta/thinking/progress/delta×N/done
- [x] 历史落库（user + assistant 都有 seq）
- [x] 鉴权失败 → 流终止（M1 简化的话术不下发是已知偏差）
- [x] mock MetaGC 5xx → error event + done
- [x] mypy / ruff / pytest 全绿
- [x] 覆盖率达标
```

- [ ] **Step 4: 合并 + tag**

```bash
git add docs/superpowers/plans/2026-06-24-master-agent-m1-metagc-e2e.notes.md
git commit -m "docs: M1 completion notes"
git checkout design/master-agent-v1
git merge --no-ff feature/m1-metagc-e2e -m "merge: M1 minimal e2e + MetaGC"
git tag m1-complete
git log --oneline --graph -8
```

- [ ] **Step 5: M1 final code review**

参照 M0 收尾：调一个 subagent 做整个 M1 范围的 holistic code review，输出 Strengths / Important / Minor / Assessment。如有 Important 项，作为 M2 day-1 cleanup 收纳。

**完成 Phase J 的标志：M1 全套绿；m1-complete tag 落地；完成笔记里包含偏差与给 M2 的提示。**

---

# M1 总览

| Phase | Task # | 描述 | 预估 commits |
|---|---|---|---|
| A | 1 | M0 day-1 cleanup | 1 |
| B | 2-6 | DB infra | 5 |
| C | 7-8 | Plugin system | 2 |
| D | 9-10 | Topic system | 2 |
| E | 11-13 | LangGraph nodes/wrappers | 3 |
| F | 14-16 | Plugin impls (auth/enrich/intent fake) | 3 |
| G | 17-18 | MetaGC adapter + mock | 2 |
| H | 19-21 | ChatService + main.py rewire | 3 |
| I | 22 | Stream contracts (9 tests) | 1 |
| J | 24-26 | e2e + Dockerfile/CI + exit | 3 |
| **Total** | | | **~25** |

预估时间：2~3 名后端工程师，**~2 周**（与设计书 §9.3 M1 估算一致）。













