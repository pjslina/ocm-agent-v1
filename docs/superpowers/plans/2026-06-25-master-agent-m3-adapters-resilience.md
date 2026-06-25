# MasterAgent M3 — 适配器 + 韧性 + 真实 LLM Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 两个新下游适配器（Uniioc HTTP chunk / KnowledgeCenter WebSocket）、LLMClassifier 真接企业 LLM、节点级 retry/timeout、三专题多路路由全跑通。

**Architecture (在 M2 之上加这些层):**
- **两个新 Adapter**：UniiocAdapter（HTTP POST → 解析 JSON lines chunk 流 → delta events）、KnowledgeCenterAdapter（WebSocket 客户端 → 接收文本帧 → delta events）
- **LLMClassifier 真 LLM**：新增 `provider="openai-compatible"` 使用 `langchain-openai.ChatOpenAI`；`provider="internal"`（企业网关）M3 预留接口 M4 实现
- **节点级 retry/timeout**：在 `node_wrappers.py` 的 `make_adapter_node` 和 `make_intent_node` 织入 retry 逻辑；YAML schema 扩展 `retry` / `timeout` 配置
- **多路路由**：三个专题 YAML 从单 metagc route 扩展到 metagc + uniioc + kc 三路
- **平台优化**：REST `list_messages` 性能优化（`get_by_thread` 直查）、OpenInference instrumentation

**Tech Stack:** 新增依赖 `langchain-openai`（ChatOpenAI）、`websockets`（KC 客户端）；OpenInference 已在 `langchain-core` 依赖中。

**Spec reference:** `docs/superpowers/specs/2026-06-23-master-agent-design.md` §5.2.4 / §6.6 / §9.4-M3。

**M2 baseline:** tag `m2-complete`（commit `dd69e99` on main）。

**Branch strategy:** 从 `main` 开 `feature/m3-adapters-resilience`。

---

## File Structure (M3 创建/修改清单)

### 阶段 A: 新下游适配器
- Create: `src/ma/plugins/adapter/uniioc.py` — UniiocAdapter
- Create: `src/ma/plugins/adapter/knowledge_center.py` — KnowledgeCenterAdapter (WS)
- Modify: `src/ma/core/plugin/bootstrap.py` — 加 import
- Create: `tests/integration/test_uniioc_with_real_port.py`
- Create: `tests/integration/test_kc_with_real_port.py`
- Create: `tests/mock_downstreams/uniioc_chunk.py`
- Create: `tests/mock_downstreams/kc_ws.py`

### 阶段 B: LLM 真 LLM 接入
- Modify: `src/ma/plugins/intent/llm_classifier.py` — 加 openai-compatible provider
- Create: `tests/unit/test_llm_classifier_openai.py`
- Modify: `pyproject.toml` — 加 `langchain-openai` 依赖

### 阶段 C: 节点级 retry + timeout
- Modify: `src/ma/core/graph/node_wrappers.py` — 织入 retry
- Modify: `src/ma/core/topic/config.py` — YAML schema 加 retry/timeout
- Modify: `config/topics/*.yaml` — 三专题加 retry/timeout
- Modify: `tests/unit/test_topic_config.py` — 新字段测试
- Modify: `tests/stream/test_contracts.py` — Contract 8 变实测

### 阶段 D: 多路路由
- Modify: `config/topics/daibiao_xiaoguanjia.yaml` — labels → 三路
- Modify: `config/topics/jingying_xiaozhushou.yaml` — labels → 三路
- Modify: `config/topics/sales_contract_agent.yaml` — labels → 三路
- Modify: `tests/stream/conftest.py` — _topic_config 支持多节点
- Modify: `src/ma/core/chat_service.py` — error_messages 在 ChatService 层用上

### 阶段 E: 平台优化
- Modify: `src/ma/core/repo/session_repository.py` — 加 get_by_thread
- Modify: `src/ma/api/rest.py` — list_messages 用 get_by_thread
- Modify: `src/ma/infra/tracing.py` — 加 LangChain instrumentor

### 阶段 F: 出口验证
- Modify: `README.md` — 更新 M3 状态
- 写 M3 完成笔记
- 合并 + tag m3-complete

### M3 不创建（留 M4+）
- WebSocket endpoint（M4）
- `provider="internal"` 企业网关实现（M4 或更后）
- KnowledgeCenter 真 WS endpoint（M4）
- 完整 e2e 三路回归（M5）

---

## 任务索引

| Phase | Task | 名称 |
|---|---|---|
| A: Adapters | 1 | UniiocAdapter + mock + 集成测试 |
|  | 2 | KnowledgeCenterAdapter (WS) + mock + 集成测试 |
| B: Real LLM | 3 | LLMClassifier openai-compatible provider |
| C: Resilience | 4 | 节点级 retry + timeout (YAML + node_wrappers) |
|  | 5 | error_messages 全链路 + Contract 8 实测 |
| D: Multi-route | 6 | Topic YAML 三路路由 + ChatService error_messages |
| E: Platform | 7 | get_by_thread + REST 优化 |
|  | 8 | OpenInference LangChain instrumentation |
| F: Exit | 9 | 出口验证 + 合并 + tag + 完成笔记 |

总计 9 个 task。

---

## Phase A: 新下游适配器

### Task 1: UniiocAdapter（HTTP POST → chunk 流 → delta）

**背景**：Uniioc 下游用 HTTP POST（非 SSE），返回 JSON lines（每行一个 JSON 对象，含 `type` + `data`）。适配器负责 POST 请求 → 逐行解析 → yield ChatEvent。设计书 §5.2.4。

**与 MetaGCAdapter 的核心差异**：MetaGC 消费 SSE（`httpx_sse.aconnect_sse`）；Uniioc 是普通 HTTP POST → 按行读 response body。

**Files:**
- Create: `src/ma/plugins/adapter/uniioc.py`
- Modify: `src/ma/core/plugin/bootstrap.py`
- Create: `tests/mock_downstreams/uniioc_chunk.py`
- Create: `tests/integration/test_uniioc_with_real_port.py`

**Interfaces:**
- Consumes: `DownstreamAdapter` Protocol from `ma.core.plugin.base`; `ChatEvent` from `ma.core.graph.events`; `GraphState` from `ma.core.graph.state`; `registry` from `ma.core.plugin.registry`
- Produces: `UniiocAdapter` class (name=`"uniioc"`, plugin_kind=`"adapter"`); `run(state, output) -> AsyncIterator[ChatEvent]`

- [ ] **Step 1: 创建 mock downstream `tests/mock_downstreams/uniioc_chunk.py`**

Uniioc mock server（uvicorn ASGI app），返回 JSON lines：

```python
"""Mock Uniioc：HTTP POST 进来，返回 JSON lines chunk 流。"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()


@app.post("/api/v1/chat")
async def chat(request: Request) -> StreamingResponse:
    body = await request.json()
    question = body.get("question", "")

    async def stream():
        import json

        yield json.dumps({"type": "meta", "data": {}}) + "\n"
        yield json.dumps({"type": "delta", "data": {"content": f"Uniioc收到: {question}"}}) + "\n"
        yield json.dumps({"type": "done", "data": {}}) + "\n"

    return StreamingResponse(stream(), media_type="application/x-ndjson")
```

- [ ] **Step 2: 实现 `src/ma/plugins/adapter/uniioc.py`**

```python
"""UniiocAdapter：HTTP POST 到 Uniioc，解析 JSON lines 返回 → ChatEvent。

输入：state.enriched_question + state.biz_params
输出：AsyncIterator[ChatEvent]
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
from pydantic import BaseModel

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import GraphState
from ma.core.plugin.registry import registry
from ma.infra.logging import get_logger

_log = get_logger("ma.adapter.uniioc")


class UniiocAdapterParams(BaseModel):
    base_url: str
    timeout_ms: int = 30000
    path: str = "/api/v1/chat"


@registry.register
class UniiocAdapter:
    name = "uniioc"
    plugin_kind = "adapter"

    def configure(self, params: dict[str, Any]) -> None:
        p = UniiocAdapterParams(**params)
        self._url = p.base_url + p.path
        self._timeout = httpx.Timeout(p.timeout_ms / 1000.0)
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def run(self, state: GraphState, *, output: bool = True) -> AsyncIterator[ChatEvent]:
        body = {
            "question": state.get("enriched_question") or state.get("question", ""),
            "biz_params": state.get("biz_params", {}),
            "request_id": state.get("request_id"),
        }
        try:
            async with self._client.stream("POST", self._url, json=body) as response:
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    try:
                        frame = json.loads(line)
                    except json.JSONDecodeError:
                        _log.warning("uniioc_bad_json_line", line=line)
                        continue
                    ft = frame.get("type")
                    fd = frame.get("data", {})
                    if ft == "delta":
                        yield ChatEvent(type="delta", data=fd)
                    elif ft == "error":
                        yield ChatEvent(
                            type="error",
                            data={
                                "code": fd.get("code", "DOWNSTREAM_ERROR"),
                                "message": fd.get("message", "Uniioc 上游出错"),
                                "retryable": False,
                            },
                        )
                    elif ft == "done":
                        return
        except httpx.TimeoutException:
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_TIMEOUT",
                    "message": "Uniioc 接口响应超时，请稍后重试",
                    "retryable": False,
                },
            )
        except httpx.RequestError:
            _log.exception("uniioc_request_error")
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_ERROR",
                    "message": "调用 Uniioc 失败，请稍后重试",
                    "retryable": False,
                },
            )
```

- [ ] **Step 3: 更新 `src/ma/core/plugin/bootstrap.py`**

加 `import ma.plugins.adapter.uniioc`：

```python
def load_all_plugins() -> None:
    """启动期调一次。"""
    import ma.plugins.adapter.metagc  # noqa: F401
    import ma.plugins.adapter.uniioc  # noqa: F401
    import ma.plugins.auth.jingying_auth  # noqa: F401
    import ma.plugins.auth.representative_auth  # noqa: F401
    import ma.plugins.auth.sales_contract_auth  # noqa: F401
    import ma.plugins.enrich.generic_enrich  # noqa: F401
    import ma.plugins.intent.llm_classifier  # noqa: F401
```

- [ ] **Step 4: 创建 `tests/integration/test_uniioc_with_real_port.py`**

```python
"""UniiocAdapter 集成测试：用 uvicorn 启 mock server → adapter.run 消费。"""
from __future__ import annotations

import asyncio
import socket

import pytest
import uvicorn


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_uniioc_full_stream() -> None:
    from tests.mock_downstreams.uniioc_chunk import app

    port = _get_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    for _ in range(40):
        if server.started:
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("uniioc mock server failed to start")

    try:
        from ma.plugins.adapter.uniioc import UniiocAdapter

        adapter = UniiocAdapter()
        adapter.configure({"base_url": f"http://127.0.0.1:{port}", "timeout_ms": 5000})

        events = []
        async for ev in adapter.run(
            {"question": "测试问题", "biz_params": {}, "request_id": "r1"},
            output=True,
        ):
            events.append(ev)

        deltas = [e for e in events if e.type == "delta"]
        assert len(deltas) == 1
        assert "测试问题" in deltas[0].data["content"]
        assert events[-1].type != "error"
    finally:
        server.should_exit = True
        await task
```

- [ ] **Step 5: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/integration/test_uniioc_with_real_port.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 1 新 integration test pass（本机 uvicorn 直接启，不依赖 docker）；full suite 111 passed + 19 skipped。

```bash
git add src/ma/plugins/adapter/uniioc.py \
        src/ma/core/plugin/bootstrap.py \
        tests/mock_downstreams/uniioc_chunk.py \
        tests/integration/test_uniioc_with_real_port.py
git commit -m "feat(adapter): UniiocAdapter for HTTP chunked JSON lines downstream"
```

### Task 2: KnowledgeCenterAdapter（WebSocket 客户端）

**背景**：知识中心下游用 WebSocket。客户端 connect → send JSON question → recv text frames → yield delta events。设计书 §5.2.4。

依赖 `websockets` 库（M2 notes 已说明允许）。

**Files:**
- Create: `src/ma/plugins/adapter/knowledge_center.py`
- Modify: `src/ma/core/plugin/bootstrap.py`
- Create: `tests/mock_downstreams/kc_ws.py`
- Create: `tests/integration/test_kc_with_real_port.py`
- Modify: `pyproject.toml` — 加 `websockets` 依赖

**Interfaces:**
- Consumes: `DownstreamAdapter` Protocol; `ChatEvent`; `GraphState`; `registry`
- Produces: `KnowledgeCenterAdapter` (name=`"knowledge_center"`, plugin_kind=`"adapter"`); `run(state, output) -> AsyncIterator[ChatEvent]`

- [ ] **Step 1: 加 `websockets` 依赖**

```bash
python -m uv add websockets
```

- [ ] **Step 2: 创建 mock downstream `tests/mock_downstreams/kc_ws.py`**

```python
"""Mock KnowledgeCenter WebSocket server：接收 JSON question → 发两条 delta + close。"""
from __future__ import annotations

import asyncio
import json

import websockets


async def handler(websocket):
    raw = await websocket.recv()
    msg = json.loads(raw)
    question = msg.get("question", "")

    await websocket.send(json.dumps({"type": "delta", "data": {"content": f"KC回答: {question}"}}))
    await websocket.send(json.dumps({"type": "delta", "data": {"content": "。"}}))
    await websocket.send(json.dumps({"type": "done", "data": {}}))
    await websocket.close()


async def serve(port: int):
    async with websockets.serve(handler, "127.0.0.1", port):
        await asyncio.Future()  # run forever
```

- [ ] **Step 3: 实现 `src/ma/plugins/adapter/knowledge_center.py`**

```python
"""KnowledgeCenterAdapter：WebSocket 客户端 → 接收文本帧 → ChatEvent。

输入：state.enriched_question + state.biz_params
输出：AsyncIterator[ChatEvent]
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import websockets
from pydantic import BaseModel

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import GraphState
from ma.core.plugin.registry import registry
from ma.infra.logging import get_logger

_log = get_logger("ma.adapter.knowledge_center")


class KnowledgeCenterParams(BaseModel):
    ws_url: str
    timeout_ms: int = 30000


@registry.register
class KnowledgeCenterAdapter:
    name = "knowledge_center"
    plugin_kind = "adapter"

    def configure(self, params: dict[str, Any]) -> None:
        p = KnowledgeCenterParams(**params)
        self._ws_url = p.ws_url
        self._timeout_ms = p.timeout_ms

    async def run(self, state: GraphState, *, output: bool = True) -> AsyncIterator[ChatEvent]:
        body = json.dumps({
            "question": state.get("enriched_question") or state.get("question", ""),
            "biz_params": state.get("biz_params", {}),
            "request_id": state.get("request_id"),
        })
        try:
            async with websockets.connect(
                self._ws_url,
                open_timeout=self._timeout_ms / 1000,
                close_timeout=5,
            ) as ws:
                await ws.send(body)
                async for raw in ws:
                    try:
                        frame = json.loads(raw)
                    except json.JSONDecodeError:
                        _log.warning("kc_bad_json_frame", raw=raw)
                        continue
                    ft = frame.get("type")
                    fd = frame.get("data", {})
                    if ft == "delta":
                        yield ChatEvent(type="delta", data=fd)
                    elif ft == "error":
                        yield ChatEvent(
                            type="error",
                            data={
                                "code": fd.get("code", "DOWNSTREAM_ERROR"),
                                "message": fd.get("message", "知识中心查询出错"),
                                "retryable": False,
                            },
                        )
                    elif ft == "done":
                        return
        except TimeoutError:
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_TIMEOUT",
                    "message": "知识中心接口响应超时，请稍后重试",
                    "retryable": False,
                },
            )
        except Exception:
            _log.exception("kc_error")
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_ERROR",
                    "message": "调用知识中心失败，请稍后重试",
                    "retryable": False,
                },
            )
```

- [ ] **Step 4: 更新 `src/ma/core/plugin/bootstrap.py`**

加 import：

```python
def load_all_plugins() -> None:
    """启动期调一次。"""
    import ma.plugins.adapter.knowledge_center  # noqa: F401
    import ma.plugins.adapter.metagc  # noqa: F401
    import ma.plugins.adapter.uniioc  # noqa: F401
    import ma.plugins.auth.jingying_auth  # noqa: F401
    import ma.plugins.auth.representative_auth  # noqa: F401
    import ma.plugins.auth.sales_contract_auth  # noqa: F401
    import ma.plugins.enrich.generic_enrich  # noqa: F401
    import ma.plugins.intent.llm_classifier  # noqa: F401
```

- [ ] **Step 5: 创建集成测试 `tests/integration/test_kc_with_real_port.py`**

```python
"""KnowledgeCenterAdapter 集成测试：启 WS mock server → adapter 消费。"""
from __future__ import annotations

import asyncio
import socket
import threading

import pytest


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_kc_full_socket_stream() -> None:
    from tests.mock_downstreams.kc_ws import serve

    port = _get_free_port()
    # 在后台线程启动 WS server（websockets 需要自己的 event loop）
    stop_event = threading.Event()

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server_task = loop.create_task(
            asyncio.wait_for(
                serve(port),
                timeout=None,
            )
        )

        async def _wait_stop():
            while not stop_event.is_set():
                await asyncio.sleep(0.1)
            server_task.cancel()

        loop.run_until_complete(_wait_stop())
        loop.close()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    await asyncio.sleep(0.2)  # give server time to start

    try:
        from ma.plugins.adapter.knowledge_center import KnowledgeCenterAdapter

        adapter = KnowledgeCenterAdapter()
        adapter.configure({"ws_url": f"ws://127.0.0.1:{port}", "timeout_ms": 5000})

        events = []
        async for ev in adapter.run(
            {"question": "测试", "biz_params": {}, "request_id": "r1"},
            output=True,
        ):
            events.append(ev)

        deltas = [e for e in events if e.type == "delta"]
        assert len(deltas) >= 1
        combined = "".join(d.data["content"] for d in deltas)
        assert "测试" in combined
        assert events[-1].type != "error"
    finally:
        stop_event.set()
        t.join(timeout=3)
```

- [ ] **Step 6: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/integration/test_kc_with_real_port.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 2 个新 adapter integration tests pass；full suite 112 passed + 19 skipped。

```bash
git add src/ma/plugins/adapter/knowledge_center.py \
        src/ma/core/plugin/bootstrap.py \
        tests/mock_downstreams/kc_ws.py \
        tests/integration/test_kc_with_real_port.py \
        pyproject.toml uv.lock
git commit -m "feat(adapter): KnowledgeCenterAdapter for WebSocket downstream"
```

**Phase A 完成标志：** 2 个新 adapter；各自有 mock + 集成测试；bootstrap.py 注册完毕。

---

## Phase B: LLM 真 LLM 接入

### Task 3: LLMClassifier openai-compatible provider

**背景**：M1/M2 的 `LLMClassifier` 只用 `provider="fake"`（`FakeListChatModel`）。M3 加 `provider="openai-compatible"` 使用 `langchain-openai.ChatOpenAI`，支持任意 OpenAI-compatible API（企业内部 LLM 网关）。

**Files:**
- Modify: `src/ma/plugins/intent/llm_classifier.py`
- Modify: `pyproject.toml` — 加 `langchain-openai` 依赖
- Create: `tests/unit/test_llm_classifier_openai.py`

**Interfaces:**
- Consumes: `LLMClassifier` 已有 `IntentPlugin` Protocol；`LLMClassifierParams.provider` 扩展 Literal
- Produces: `provider="openai-compatible"` 可用；接受 `base_url` + `api_key` 参数（可选，通过 env）

- [ ] **Step 1: 加 `langchain-openai` 依赖**

```bash
python -m uv add langchain-openai
```

- [ ] **Step 2: 修改 `src/ma/plugins/intent/llm_classifier.py`**

```python
"""LLM 分类器：用 LangChain 风格的 ChatModel 抽象。

M1 实现：
- provider="fake" → langchain_core.language_models.fake_chat_models.FakeListChatModel
- M3 新增：
  - provider="openai-compatible" → langchain_openai.ChatOpenAI
  - provider="internal" → M4 实现（企业网关）

输出协议：让 LLM 用纯文本回答，第一行 "route: <label>"，第二行可选 "reason: ..."
正则解析 route；解析失败 → IntentFailed。
"""
from __future__ import annotations

import os
import re
from typing import Any, Literal

from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import IntentFailed, IntentResult
from ma.core.plugin.registry import registry
from ma.infra.logging import get_logger

_log = get_logger("ma.intent.llm_classifier")


class LLMClassifierParams(BaseModel):
    provider: Literal["fake", "openai-compatible", "internal"] = "fake"
    model: str
    labels: list[str]
    prompt_template: str
    fake_responses: list[str] | None = None

    # openai-compatible 参数（M3），仅 provider="openai-compatible" 时生效
    base_url: str | None = None
    api_key: str | None = None


_ROUTE_RE = re.compile(r"^\s*route\s*:\s*([a-zA-Z0-9_-]+)\s*$", re.MULTILINE)
_REASON_RE = re.compile(r"^\s*reason\s*:\s*(.+)$", re.MULTILINE)


def _build_chat_model(params: LLMClassifierParams) -> BaseChatModel:
    if params.provider == "fake":
        if not params.fake_responses:
            raise ValueError("provider='fake' requires non-empty fake_responses")
        return FakeListChatModel(responses=params.fake_responses)

    if params.provider == "openai-compatible":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=params.model,
            base_url=params.base_url,
            api_key=params.api_key or os.environ.get("OPENAI_API_KEY", "not-set"),
            temperature=0.0,
        )

    # provider == "internal" — M4 实现（企业 LLM 网关）
    raise NotImplementedError(
        f"provider={params.provider!r} not implemented (use 'fake' or 'openai-compatible')"
    )


@registry.register
class LLMClassifier:
    name = "llm_classifier"
    plugin_kind = "intent"

    def configure(self, params: dict[str, Any]) -> None:
        p = LLMClassifierParams(**params)
        self._labels = set(p.labels)
        self._llm = _build_chat_model(p)

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
        _log.info("llm_response", text=text)
        m = _ROUTE_RE.search(text)
        if not m:
            raise IntentFailed(f"cannot parse route from LLM response: {text!r}")
        route = m.group(1)
        reason_m = _REASON_RE.search(text)
        reason = reason_m.group(1).strip() if reason_m else ""
        return IntentResult(route=route, confidence=1.0, reason=reason)
```

- [ ] **Step 3: 创建 `tests/unit/test_llm_classifier_openai.py`**

此测试不调真实 LLM API，而是验证参数解析和 provider 路由逻辑：

```python
"""LLMClassifier openai-compatible provider 单元测试。"""
from __future__ import annotations

import pytest


def test_openai_compatible_params_parse() -> None:
    """验证 openai-compatible provider 的参数解析（不调真实 API）。"""
    from ma.plugins.intent.llm_classifier import LLMClassifierParams

    p = LLMClassifierParams(
        provider="openai-compatible",
        model="gpt-4o-mini",
        labels=["metagc", "uniioc"],
        prompt_template="p.txt",
        base_url="https://llm.internal.example/v1",
        api_key="test-key",
    )
    assert p.provider == "openai-compatible"
    assert p.model == "gpt-4o-mini"
    assert p.labels == ["metagc", "uniioc"]


def test_internal_provider_raises_not_implemented() -> None:
    """provider='internal' 尚未实现（M4）。"""
    from ma.plugins.intent.llm_classifier import LLMClassifierParams, _build_chat_model

    p = LLMClassifierParams(
        provider="internal",
        model="internal-model",
        labels=["metagc"],
        prompt_template="p.txt",
    )
    with pytest.raises(NotImplementedError, match="internal"):
        _build_chat_model(p)


def test_fake_provider_still_works() -> None:
    """确保 provider='fake' 仍然可用（M1/M2 行为不变）。"""
    from ma.plugins.intent.llm_classifier import LLMClassifierParams, _build_chat_model

    p = LLMClassifierParams(
        provider="fake",
        model="fake-model",
        labels=["metagc"],
        prompt_template="p.txt",
        fake_responses=["route: metagc\nreason: ok"],
    )
    llm = _build_chat_model(p)
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    assert isinstance(llm, FakeListChatModel)
```

- [ ] **Step 4: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/unit/test_llm_classifier_openai.py tests/unit/test_llm_classifier.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 3 新单测 + 已有 6 个 llm_classifier 单测全 pass；full suite 115 passed + 19 skipped。

```bash
git add src/ma/plugins/intent/llm_classifier.py \
        tests/unit/test_llm_classifier_openai.py \
        pyproject.toml uv.lock
git commit -m "feat(intent): LLMClassifier openai-compatible provider (ChatOpenAI)"
```

**Phase B 完成标志：** LLMClassifier 支持真 LLM；fake provider 行为不变；单测覆盖参数解析。

---

## Phase C: 节点级 retry + timeout

### Task 4: 节点级 retry + timeout（YAML + node_wrappers）

**背景**：设计书 §6.6.1 要求下游调用可配置 retry 和 timeout。当前 adapter 节点（`make_adapter_node`）不做 retry；intent 节点不做 retry。M3 实现节点级重试。

**策略**：
- 在 `node_wrappers.py` 的 `make_adapter_node` 中：下游抛 recoverable 错误时 retry N 次；非 retryable 错误直接 break
- 在 `make_intent_node` 中：`IntentFailed` 或 LLM 调用异常 → retry N 次再 fallback
- YAML schema 加可选的 `retry` 配置（`max_attempts` / `backoff_ms`）

**Files:**
- Modify: `src/ma/core/topic/config.py` — 加 `RetryPolicy` model
- Modify: `src/ma/core/topic/compiler.py` — 传递 retry/timeout 到 CompiledTopic
- Modify: `src/ma/core/graph/node_wrappers.py` — 织入 retry
- Modify: `config/topics/daibiao_xiaoguanjia.yaml` — 加 retry 配置
- Modify: `config/topics/jingying_xiaozhushou.yaml`
- Modify: `config/topics/sales_contract_agent.yaml`
- Modify: `tests/unit/test_topic_config.py` — 加 RetryPolicy 测试

- [ ] **Step 1: 修改 `src/ma/core/topic/config.py` — 加 RetryPolicy**

在 `GraphNode` 类中加可选的 `retry` 字段：

```python
class RetryPolicy(BaseModel):
    max_attempts: int = Field(ge=1, le=5, default=1)
    backoff_ms: int = Field(ge=0, le=30000, default=1000)


class GraphNode(BaseModel):
    id: str
    adapter: str
    params: dict[str, Any] = Field(default_factory=dict)
    output: bool = True
    retry: RetryPolicy | None = None  # M3: 可选 retry 配置
```

同样给 `IntentRef` 加可选的 retry：

```python
class IntentRef(BaseModel):
    plugin: str
    params: dict[str, Any]
    retry: RetryPolicy | None = None  # M3: intent 节点也可 retry
```

- [ ] **Step 2: 修改 `src/ma/core/topic/compiler.py`**

`CompiledTopic` dataclass 加 adapter node 的 retry 配置。有两种方式：
- 方式 A：在 `CompiledTopic` 中额外存一份 `node_retries: dict[str, RetryPolicy]`
- 方式 B：`node_wrappers` 直接读 `GraphNode.retry` 字段

选用方式 A（让 CompiledTopic 成为完整配置容器）：

```python
from ma.core.topic.config import RetryPolicy

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
    config: TopicConfig
    # M3 新增
    intent_retry: RetryPolicy | None = None
    adapter_retries: dict[str, RetryPolicy] = field(default_factory=dict)
```

需要在 compiler 的 `compile` 方法中从 TopicConfig 提取：

```python
# 在 compile() 方法内：
intent_retry = cfg.intent.retry if hasattr(cfg.intent, 'retry') else None
adapter_retries: dict[str, RetryPolicy] = {}
for node in cfg.graph.nodes:
    if node.retry is not None:
        adapter_retries[node.id] = node.retry

return CompiledTopic(
    ...,
    intent_retry=intent_retry,
    adapter_retries=adapter_retries,
)
```

注意 dataclass field 需要 import：

```python
from dataclasses import dataclass, field
```

- [ ] **Step 3: 修改 `src/ma/core/graph/node_wrappers.py` — 织入 retry**

`make_adapter_node` 加 retry 逻辑：

```python
import asyncio
from ma.core.topic.config import RetryPolicy


def make_adapter_node(adapter: DownstreamAdapter, *, output: bool, retry: RetryPolicy | None = None) -> NodeFn:
    max_attempts = retry.max_attempts if retry else 1
    backoff_ms = retry.backoff_ms if retry else 0

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        chunks: list[str] = []
        last_error: dict[str, Any] | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                async for ev in adapter.run(state, output=output):
                    if ev.type == "delta":
                        chunks.append(ev.data["content"])
                        if output:
                            await _emit("delta", ev.data, config)
                    elif ev.type == "tool":
                        if output:
                            await _emit("tool", ev.data, config)
                    elif ev.type == "error":
                        if ev.data.get("retryable", False) and attempt < max_attempts:
                            last_error = ev.data
                            break  # 退出内层循环，触发 retry
                        # 非 retryable 或最后一次尝试：透出 error 然后退出
                        await _emit("error", ev.data, config)
                        if output:
                            return {"answer_chunks": chunks}
                        return {"answer_chunks": []}
                else:
                    # 正常完成（没有 break）
                    if output:
                        return {"answer_chunks": chunks}
                    return {"answer_chunks": []}
            except Exception as e:
                if attempt < max_attempts:
                    _log.warning(
                        "adapter_retry",
                        attempt=attempt,
                        max_attempts=max_attempts,
                        error=str(e),
                    )
                    await asyncio.sleep(backoff_ms / 1000.0)
                    continue
                # 最后一次抛异常 → 透出 error
                await _emit(
                    "error",
                    {
                        "code": "DOWNSTREAM_ERROR",
                        "message": f"调用下游失败（重试{max_attempts}次后放弃）",
                        "retryable": False,
                    },
                    config,
                )
                if output:
                    return {"answer_chunks": chunks}
                return {"answer_chunks": []}

            # retryable error → 等待后重试
            if last_error is not None and attempt < max_attempts:
                _log.warning(
                    "adapter_retry_recoverable",
                    attempt=attempt,
                    max_attempts=max_attempts,
                    error=last_error,
                )
                await asyncio.sleep(backoff_ms / 1000.0)
                last_error = None
                continue

        if output:
            return {"answer_chunks": chunks}
        return {"answer_chunks": []}

    return _traced_node("adapter", node)
```

`make_intent_node` 加 retry：

```python
def make_intent_node(
    plugin: IntentPlugin, *, default_route: str, labels: list[str], retry: RetryPolicy | None = None
) -> NodeFn:
    label_set = set(labels)
    max_attempts = retry.max_attempts if retry else 1
    backoff_ms = retry.backoff_ms if retry else 0

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        await _emit("thinking", {"phase": "intent", "label": "正在识别问题意图…"}, config)
        for attempt in range(1, max_attempts + 1):
            try:
                result = await plugin.classify(state)
                if result.route not in label_set:
                    route = default_route
                    conf = 0.0
                    reason = f"unknown label {result.route!r}, fallback"
                else:
                    route = result.route
                    conf = result.confidence
                    reason = result.reason
                await _emit(
                    "progress",
                    {"step": "intent", "status": "done", "detail": reason, "extra": {"route": route}},
                    config,
                )
                return {"route": route, "route_confidence": conf, "route_reason": reason}
            except IntentFailed as e:
                if attempt < max_attempts:
                    await asyncio.sleep(backoff_ms / 1000.0)
                    continue
                # 最后一次 fallback
                _log.warning("intent_failed", error=str(e), fallback=default_route)
            except Exception as e:
                if attempt < max_attempts:
                    _log.warning("intent_retry", attempt=attempt, error=str(e))
                    await asyncio.sleep(backoff_ms / 1000.0)
                    continue
                _log.warning("intent_exception", error=str(e), fallback=default_route)

        await _emit(
            "progress",
            {"step": "intent", "status": "fallback", "detail": f"retried {max_attempts}x",
             "extra": {"route": default_route}},
            config,
        )
        return {"route": default_route, "route_confidence": 0.0, "route_reason": "intent_retry_exhausted"}

    return _traced_node("intent", node)
```

- [ ] **Step 4: 改 `chat_service.py` 的 `_build_graph` 传 retry 参数**

`make_adapter_node` 调用加 `retry` 参数：

```python
for node_id, adapter in topic.adapters.items():
    sg.add_node(
        node_id,
        make_adapter_node(  # type: ignore[arg-type]
            adapter, output=True, retry=topic.adapter_retries.get(node_id)
        ),
    )
```

`make_intent_node` 调用加 `retry`：

```python
sg.add_node(
    "intent_node",
    make_intent_node(  # type: ignore[arg-type]
        topic.intent,
        default_route=topic.default_route,
        labels=labels,
        retry=topic.intent_retry,
    ),
)
```

- [ ] **Step 5: 更新三个 Topic YAML**

```yaml
# daibiao_xiaoguanjia.yaml — intent 加 retry
intent:
  plugin: llm_classifier
  params:
    ...
  retry:
    max_attempts: 2
    backoff_ms: 1000

graph:
  nodes:
    - id: metagc_adapter
      adapter: metagc
      output: true
      params:
        base_url: ${env:MA_METAGC_BASE_URL}
        timeout_ms: 30000
      retry:
        max_attempts: 2
        backoff_ms: 2000
```

同 pattern 更新 `jingying_xiaozhushou.yaml` 和 `sales_contract_agent.yaml`。

- [ ] **Step 6: 更新 `tests/unit/test_topic_config.py` 加 RetryPolicy 测试**

```python
def test_topic_config_with_retry_policy() -> None:
    """验证 YAML 中 node.retry 字段可被正确解析。"""
    from ma.core.topic.config import TopicConfig, RetryPolicy

    data = {
        "topic_id": "test",
        "display_name": "Test",
        "default_route": "metagc",
        "history": {"max_turns": 5, "scope": "per_topic"},
        "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
        "enrich": {"plugin": "generic_enrich", "params": {"fields_to_inject": ["region"]}},
        "intent": {
            "plugin": "llm_classifier",
            "params": {
                "provider": "fake", "model": "m", "labels": ["metagc"],
                "prompt_template": "p.txt", "fake_responses": ["route: metagc\nreason: ok"],
            },
            "retry": {"max_attempts": 3, "backoff_ms": 500},
        },
        "graph": {
            "nodes": [
                {
                    "id": "metagc_adapter", "adapter": "metagc", "output": True,
                    "params": {"base_url": "http://x", "timeout_ms": 5000},
                    "retry": {"max_attempts": 2, "backoff_ms": 1000},
                }
            ],
            "edges": [
                {"from": "intent", "type": "conditional", "route_by": "route",
                 "mapping": {"metagc": "metagc_adapter"}},
            ],
        },
        "biz_params_schema": {
            "type": "object", "required": ["region"],
            "properties": {"region": {"type": "string"}},
        },
        "error_messages": {
            "FORBIDDEN_TOPIC": "x", "FORBIDDEN_SCOPE": "x",
            "DOWNSTREAM_TIMEOUT": "x", "DOWNSTREAM_ERROR": "x", "INTERNAL": "x",
        },
    }
    cfg = TopicConfig.model_validate(data)
    assert cfg.intent.retry is not None
    assert cfg.intent.retry.max_attempts == 3
    assert cfg.graph.nodes[0].retry is not None
    assert cfg.graph.nodes[0].retry.max_attempts == 2


def test_retry_policy_defaults() -> None:
    """RetryPolicy 默认值：不 retry。"""
    from ma.core.topic.config import RetryPolicy

    r = RetryPolicy()
    assert r.max_attempts == 1
    assert r.backoff_ms == 1000
```

- [ ] **Step 7: 更新 `tests/stream/conftest.py` 的 `_topic_config`**

加 intent retry + node retry 到 `_topic_config`：

在 `_topic_config` 的 `intent` dict 加 `"retry": {"max_attempts": 1, "backoff_ms": 0}`，
在 graph nodes 的 metagc_adapter 加 `"retry": {"max_attempts": 1, "backoff_ms": 0}`。
并更新 `fresh_registry` 的 modname 清理列表（不加新 adapter，Task 6 才需要）。

- [ ] **Step 8: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/unit/test_topic_config.py tests/stream/ tests/unit/test_node_wrappers.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 所有 stream contracts 仍 pass；topic_config 新测试 pass；full suite ~117 passed。

```bash
git add src/ma/core/topic/config.py src/ma/core/topic/compiler.py \
        src/ma/core/graph/node_wrappers.py src/ma/core/chat_service.py \
        config/topics/daibiao_xiaoguanjia.yaml \
        config/topics/jingying_xiaozhushou.yaml \
        config/topics/sales_contract_agent.yaml \
        tests/unit/test_topic_config.py tests/stream/conftest.py
git commit -m "feat(graph): node-level retry + timeout with YAML configurable RetryPolicy"
```

### Task 5: error_messages 全链路 + Contract 8 实测

**背景**：M2 的 `error_messages` 只在 `reject_node` 使用（鉴权失败话术）。M3 需要在 ChatService 层捕获下游错误时也用 `error_messages` 映射友好话术。同时，Contract 8（retry transparent）从 skip → 实测。

**Files:**
- Modify: `src/ma/core/chat_service.py` — 捕获 error event 时用 error_messages 替换
- Modify: `tests/stream/test_contracts.py` — Contract 8 替换为实测

- [ ] **Step 1: `chat_service.py` — 在 `astream_events` 循环中用 error_messages 映射**

当前 `astream_events` 循环中，`name == "error"` 的事件直接透传。M3 改为：如果 error code 在 `topic.error_messages` 中有对应话术，用 YAML 的话术覆盖原 message。

```python
async for event in graph.astream_events(initial_state, ...):
    ev = event.get("event")
    if ev == "on_custom_event":
        name = event.get("name")
        if name in {"thinking", "progress", "delta", "tool"}:
            yield ChatEvent(type=name, data=event.get("data") or {})
        elif name == "error":
            data = event.get("data") or {}
            code = data.get("code", "INTERNAL")
            friendly = topic.error_messages.get(code)
            if friendly:
                data = {**data, "message": friendly}
            yield ChatEvent(type="error", data=data)
```

- [ ] **Step 2: 将 Contract 8 从 placeholder 改为实测**

```python
@pytest.mark.asyncio
async def test_contract_8_retry_transparent_to_user(make_service):
    """retry 对用户透明：adapter 可重试的错误不会暴露给 SSE 流。
    
    模拟：adapter 第一次抛 retryable error，第二次成功 → 用户只看到一个 delta，
    不会看到中间 error。
    """
    svc, _ = make_service()

    call_count = 0

    async def fake_run(self, state, *, output: bool):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            yield ChatEvent(
                type="error",
                data={"code": "DOWNSTREAM_ERROR", "message": "retryable", "retryable": True},
            )
        else:
            yield ChatEvent(type="delta", data={"content": "最终成功"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())

    deltas = [e for e in events if e.type == "delta"]
    errors = [e for e in events if e.type == "error"]
    # 用户看不到中间 retryable error
    assert len(errors) == 0
    assert len(deltas) >= 1
    assert deltas[0].data["content"] == "最终成功"
    assert call_count == 2  # 确实重试了一次
```

- [ ] **Step 3: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/stream/test_contracts.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: Contract 8 从 skip 变 pass；stream contracts 9 implemented + 1 skipped (WS M4)。

```bash
git add src/ma/core/chat_service.py tests/stream/test_contracts.py
git commit -m "fix(chat): error_messages propagation in ChatService; Contract 8 real test"
```

**Phase C 完成标志：** retry/timeout YAML 可配；Contract 8 实测；error_messages 话术能覆盖下游错误。

---

## Phase D: 多路路由

### Task 6: 三专题 YAML 扩展到三路下游

**背景**：M2 的三个专题 YAML 只有 `metagc` 一条 route。M3 扩展到三路：`metagc` + `uniioc` + `knowledge_center`。每个专题仍然是独立 YAML，但 labels 和 mapping 覆盖三个下游。

同时：因为 adapter 节点多了，需要 `chat_service.py` 的 `_build_graph` 处理多 adapter 节点（当前已支持，不需要改）。

**Files:**
- Modify: `config/topics/daibiao_xiaoguanjia.yaml` — 三路
- Modify: `config/topics/jingying_xiaozhushou.yaml` — 三路
- Modify: `config/topics/sales_contract_agent.yaml` — 三路
- Modify: `tests/stream/conftest.py` — _topic_config 支持三节点 + 更新 fresh_registry
- Modify: `tests/unit/test_chat_service_with_fake_topic.py` — 更新 identity raw_claims

- [ ] **Step 1: 更新 `daibiao_xiaoguanjia.yaml`**

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
    labels: [metagc, uniioc, knowledge_center]
    prompt_template: prompts/intent/representative.txt
    fake_responses:
      - "route: metagc\nreason: 业务数据查询"
      - "route: uniioc\nreason: 统一运维查询"
      - "route: knowledge_center\nreason: 知识库检索"
  retry:
    max_attempts: 2
    backoff_ms: 1000

graph:
  nodes:
    - id: metagc_adapter
      adapter: metagc
      output: true
      params:
        base_url: ${env:MA_METAGC_BASE_URL}
        timeout_ms: 30000
      retry:
        max_attempts: 2
        backoff_ms: 2000

    - id: uniioc_adapter
      adapter: uniioc
      output: true
      params:
        base_url: ${env:MA_UNIIOC_BASE_URL}
        timeout_ms: 30000
      retry:
        max_attempts: 2
        backoff_ms: 2000

    - id: kc_adapter
      adapter: knowledge_center
      output: true
      params:
        ws_url: ${env:MA_KC_WS_URL}
        timeout_ms: 30000
      retry:
        max_attempts: 2
        backoff_ms: 2000

  edges:
    - from: intent
      type: conditional
      route_by: route
      mapping:
        metagc: metagc_adapter
        uniioc: uniioc_adapter
        knowledge_center: kc_adapter

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

同 pattern 更新 `jingying_xiaozhushou.yaml`（biz_params 保持 `business_unit`）和 `sales_contract_agent.yaml`（biz_params 保持 `contract_id`）。

- [ ] **Step 2: 更新 `tests/stream/conftest.py`**

`_topic_config` 要支持三路节点（用于 stream contract 测试）。同时 `fresh_registry` 需要加载 uniioc 和 kc adapter：

```python
# fresh_registry 的 modname 列表加：
"ma.plugins.adapter.uniioc",
"ma.plugins.adapter.knowledge_center",
```

`_topic_config` 的 graph.nodes 加 uniioc_adapter 和 kc_adapter：

```python
# graph.nodes 加：
{
    "id": "uniioc_adapter",
    "adapter": "uniioc",
    "output": True,
    "params": {"base_url": "http://unused"},
},
{
    "id": "kc_adapter",
    "adapter": "knowledge_center",
    "output": True,
    "params": {"ws_url": "ws://unused"},
},
```

`intent.params.labels` 改为 `["metagc", "uniioc", "knowledge_center"]`，
`edges.mapping` 改为 `{"metagc": "metagc_adapter", "uniioc": "uniioc_adapter", "knowledge_center": "kc_adapter"}`。

- [ ] **Step 3: 跑 stream + unit 测试确认**

```bash
python -m uv run pytest tests/stream/ tests/unit/test_chat_service_with_fake_topic.py -v
python -m uv run pytest tests/unit/test_topic_config.py -v
```

Expected: 所有 stream contracts 仍 pass；topic config 测试需更新（TopicConfig 校验 labels == mapping keys 仍成立）。

- [ ] **Step 4: 更新 `tests/unit/test_chat_service_with_fake_topic.py`**

该测试文件构造 topic 时也引用了 `_topic_config` 或类似的 test config。需要更新 fake topic 的 labels/mapping 一致，以及 identity raw_claims 包含对应 regions。

读文件确认后做最小修改。

- [ ] **Step 5: 全量测试 + lint + commit**

```bash
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

```bash
git add config/topics/daibiao_xiaoguanjia.yaml \
        config/topics/jingying_xiaozhushou.yaml \
        config/topics/sales_contract_agent.yaml \
        tests/stream/conftest.py \
        tests/unit/test_chat_service_with_fake_topic.py
git commit -m "feat(topic): expand three topics to metagc+uniioc+kc multi-route"
```

**Phase D 完成标志：** 三专题 YAML 各有三路下游路由；stream contracts 全绿。

---

## Phase E: 平台优化

### Task 7: SessionRepository.get_by_thread + REST 优化

**背景**：M2 的 `list_messages` 越权防御用 `list_by_user` + filter（N 次 O(N) 扫描）。M3 加 `get_by_thread(thread_id) -> Session | None` 直查，O(1)。

**Files:**
- Modify: `src/ma/core/repo/session_repository.py` — 加 `get_by_thread`
- Create: `sql/session/get_by_thread.sql` — SQL 模板
- Modify: `src/ma/api/rest.py` — `list_messages` 改用 `get_by_thread`
- Modify: `tests/stream/conftest.py` — `FakeSessionRepo` 加 `get_by_thread`
- Modify: `tests/integration/test_session_repository.py` — 加 get_by_thread 测试

- [ ] **Step 1: 创建 `sql/session/get_by_thread.sql`**

```sql
SELECT thread_id, biz_id, w3_account, title, status,
       created_at, updated_at, last_message_at, ext
FROM ma_session
WHERE thread_id = $1
```

- [ ] **Step 2: 在 `session_repository.py` 加 `get_by_thread` 方法**

```python
async def get_by_thread(self, *, thread_id: str) -> Session | None:
    row = await self._conn.fetchrow(
        "SELECT thread_id, biz_id, w3_account, title, status, "
        "created_at, updated_at, last_message_at, ext "
        "FROM ma_session WHERE thread_id = $1",
        thread_id,
    )
    if row is None:
        return None
    return _row_to_session(row)
```

注意：可以复用已存在的 `get_or_create` 里的 SQL，但为了独立性和测试清晰性，加一个新方法。

- [ ] **Step 3: 更新 `src/ma/api/rest.py` — `list_messages` 改用 `get_by_thread`**

替换 127-128 行：

```python
# Before (M2):
sessions = await sess_repo.list_by_user(w3_account=w3, biz_id=None, limit=200, before=None)
owned = next((s for s in sessions if s.thread_id == thread_id), None)

# After (M3):
session = await sess_repo.get_by_thread(thread_id=thread_id)
if session is None or session.w3_account != w3:
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"unknown thread_id: {thread_id}")
```

- [ ] **Step 4: 更新 `tests/stream/conftest.py` — FakeSessionRepo 加 get_by_thread**

```python
# FakeSessionRepo 加方法：
async def get_by_thread(self, *, thread_id: str):
    return self.sessions.get(thread_id)
```

- [ ] **Step 5: 更新 `tests/integration/test_session_repository.py`**

加一个 `get_by_thread` 测试（追加到已有 test file）：

```python
@pytest.mark.asyncio
async def test_get_by_thread_returns_session(pg_dsn):
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            repo = SessionRepository(conn=conn)
            await repo.get_or_create(
                thread_id="th_get",
                biz_id="test",
                w3_account="alice",
            )
            session = await repo.get_by_thread(thread_id="th_get")
            assert session is not None
            assert session.thread_id == "th_get"
            assert session.w3_account == "alice"

            # not exists → None
            assert await repo.get_by_thread(thread_id="th_nonexist") is None
    finally:
        await pool.close()
```

- [ ] **Step 6: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/unit/test_chat_service_with_fake_topic.py tests/stream/ -v
python -m uv run pytest tests/integration/test_session_repository.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: REST list_messages 行为不变；get_by_thread 单测 pass（CI docker）。

```bash
git add src/ma/core/repo/session_repository.py \
        sql/session/get_by_thread.sql \
        src/ma/api/rest.py \
        tests/stream/conftest.py \
        tests/integration/test_session_repository.py
git commit -m "perf(repo): add SessionRepository.get_by_thread; optimize REST list_messages"
```

### Task 8: OpenInference LangChain instrumentation

**背景**：M2 完成笔记建议 M3 接 OpenInference，利用 `langchain-core` 已有的依赖。只需在 `tracing.py` 加一行 instrumentor 调用即可使 LangGraph 节点自动产生 OTel spans。

**Files:**
- Modify: `src/ma/infra/tracing.py` — 加 `OpenInferenceTracer`
- Modify: `pyproject.toml` — 加 `openinference-instrumentation-langchain` 依赖

- [ ] **Step 1: 加依赖**

```bash
python -m uv add openinference-instrumentation-langchain
```

- [ ] **Step 2: 修改 `src/ma/infra/tracing.py`**

在 `init_tracing` 函数中加 LangChain instrumentor：

```python
def init_tracing() -> None:
    """初始化 OpenTelemetry SDK + LangChain instrumentation。"""
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    from ma.infra.settings import settings

    if not settings.tracing.enabled:
        return

    resource = Resource.create({SERVICE_NAME: "ma"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint=settings.tracing.otlp_endpoint,
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # M3: 接入 LangChain instrumentation（自动为 LLM 调用 + LangGraph 节点创建 spans）
    try:
        from openinference.instrumentation.langchain import LangChainInstrumentor
        LangChainInstrumentor().instrument()
    except ImportError:
        pass
```

- [ ] **Step 3: 跑测试确认不会因 instrumentation 引入而失败**

```bash
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 所有测试仍 pass；instrumentation 在 CI 上不会因为 OTLP exporter 不可用而 crash（因为 `settings.tracing.enabled` 是 false）。

```bash
git add src/ma/infra/tracing.py pyproject.toml uv.lock
git commit -m "feat(tracing): add OpenInference LangChain instrumentation"
```

**Phase E 完成标志：** REST 优化落地；OpenInference 接入；全测试绿。

---

## Phase F: 出口验证

### Task 9: M3 exit + merge + tag

- [ ] **Step 1: 全量 sweep**

```bash
python -m uv run pytest -v
python -m uv run pytest --cov=ma --cov-report=term-missing
python -m uv run mypy src
python -m uv run ruff check . && python -m uv run ruff format --check .
```

Expected:
- ~120+ passed（M2 110 + M3 新增 ~10 unit/integration tests）
- ~19 skipped（docker-gated 不变）
- 覆盖率 ≥ 80%
- mypy + ruff clean

- [ ] **Step 2: 写 M3 完成笔记**

`docs/superpowers/plans/2026-06-25-master-agent-m3-adapters-resilience.notes.md`:

```markdown
# M3 完成笔记

- 日期：<完成日期>
- 分支：feature/m3-adapters-resilience（合入 main，tag m3-complete）
- commit 数：~9
- 覆盖率：<实际>

## 与计划相比的偏差

<由实施者填写>

## 给 M4 的提示

1. **WebSocket endpoint**：`/api/v1/chat/ws` + 握手鉴权 + 心跳。参考 KnowledgeCenterAdapter 的 WS 客户端实现
2. **provider="internal"**：企业网关接入 LLM。M3 只做了 openai-compatible，internal 留 M4
3. **OpenInference 验证**：M4 需要验证 staging Jaeger/OTel collector 能看到 LangChain spans
4. **KnowledgeCenter 真接**：M3 的 KC adapter 是 mock + 集成测试；M4 接真实 KC 服务
5. **e2e 三路回归**：M3 的 YAML 已有三路 route，但 e2e test 只测了单路（metagc）；M4/M5 加三路 e2e

## 验收清单

- [x] UniiocAdapter + KnowledgeCenterAdapter 实现 + 集成测试
- [x] LLMClassifier openai-compatible provider
- [x] 节点级 retry + timeout（YAML 可配）
- [x] error_messages 全链路生效
- [x] Contract 8（retry）实测
- [x] 三专题 YAML 三路路由
- [x] REST list_messages 性能优化
- [x] OpenInference instrumentation
- [x] mypy / ruff / pytest 全绿
- [x] 覆盖率达标
```

- [ ] **Step 3: 合并 + tag**

```bash
git add docs/superpowers/plans/2026-06-25-master-agent-m3-adapters-resilience.notes.md
git commit -m "docs: M3 completion notes"

git checkout main
git pull
git merge --no-ff feature/m3-adapters-resilience -m "merge: M3 adapters + resilience + real LLM"
git tag m3-complete
git push origin main --tags

# 清理 feature 分支
git branch -d feature/m3-adapters-resilience
```

**Phase F 完成标志：M3 全套绿；m3-complete tag 落地。**

---

# M3 总览

| Phase | Task # | 描述 | 预估 commits |
|---|---|---|---|
| A | 1-2 | UniiocAdapter + KnowledgeCenterAdapter | 2 |
| B | 3 | LLMClassifier real LLM | 1 |
| C | 4-5 | retry/timeout + error_messages + Contract 8 | 2 |
| D | 6 | 三专题三路路由 | 1 |
| E | 7-8 | REST 优化 + OpenInference | 2 |
| F | 9 | 出口 + 合并 + tag | 1 |
| **Total** | | | **~9** |

预估时间：1 名后端工程师，**~2 周**（与设计书 §9.4 M3 估算一致）。
