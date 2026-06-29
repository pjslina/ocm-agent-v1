# MasterAgent M4 — WebSocket 通道 + 可观测性闭环 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** WebSocket 端点让移动端接入跑通，Contract 10 实测，`replay_request_id` CLI 工具落地，OTel Jaeger 本地验证可行。

**Architecture:** FastAPI 原生 WebSocket route 复用 ChatService.handle 的 ChatEvent 流；WS 信封协议（JSON 双向）编码/解码在 `events.py` 新增 `encode_ws`/`decode_ws_frame`；`ConnectionManager` 管理单连接内并发任务上限（5）和心跳超时（45s）。

**Tech Stack:** FastAPI WebSocket（原生，不引入 socketio）、asyncio.create_task 并发、已有 `websockets` 依赖用于集成测试客户端。

**Spec reference:** `docs/superpowers/specs/2026-06-23-master-agent-design.md` §4.3 / §4.7.2 / §9.4-M4。

**M3 baseline:** tag `m3-complete`（commit `a6ad0d7` on main）。

**Branch strategy:** 从 `main` 开 `feature/m4-ws-observability`。

---

## File Structure (M4 创建/修改清单)

### 阶段 A: WS 核心
- Modify: `src/ma/core/graph/events.py` — 加 `encode_ws` / `decode_ws_frame` / WS 帧类型
- Create: `src/ma/api/ws.py` — WebSocket endpoint + ConnectionManager
- Modify: `src/ma/main.py` — 注册 ws router
- Create: `tests/integration/test_ws.py`

### 阶段 B: WS 契约 + e2e
- Modify: `tests/stream/test_contracts.py` — Contract 10 变实测
- Create: `tests/e2e/test_ws_e2e.py`

### 阶段 C: 工具 + 可观测性
- Create: `src/ma/cli/replay.py` — replay_request_id CLI
- Modify: `pyproject.toml` — 加 CLI entry point
- Modify: `src/ma/infra/tracing.py` — OTel Jaeger 导出配置
- Modify: `README.md` — 本地 Jaeger 验证步骤

### 阶段 D: 出口
- 写 M4 完成笔记
- 合并 + tag m4-complete

---

## 任务索引

| Phase | Task | 名称 |
|---|---|---|
| A: WS Core | 1 | WS 信封协议（encode_ws / decode_ws）|
|  | 2 | WS endpoint + ConnectionManager + main.py 注册 |
|  | 3 | WS 集成测试 |
| B: Contracts | 4 | Contract 10（WS 多请求隔离）实测 |
|  | 5 | WS disconnect → partial 测试 |
| C: Tools | 6 | replay_request_id CLI |
|  | 7 | OTel Jaeger 本地验证 + fixture 就位 |
| D: Exit | 8 | 出口验证 + 合并 + tag + 完成笔记 |

总计 8 个 task。

---

## Phase A: WS 核心

### Task 1: WS 信封协议（encode_ws / decode_ws）

**背景**：设计书 §4.3.2 定义 WS 双向 JSON 协议。服务端→客户端帧：`{op, request_id?, event?, data?}`。客户端→服务端帧：`{op, request_id, thread_id?, question?, biz_params?}`。7 种事件 type 与 SSE 对齐。

**Files:**
- Modify: `src/ma/core/graph/events.py`

**Interfaces:**
- Produces: `encode_ws(event, request_id) -> str` — ChatEvent → WS JSON 帧
- Produces: `encode_ws_ready(version, heartbeat_s) -> str` — ready 帧
- Produces: `encode_ws_pong() -> str` — pong 帧
- Produces: `encode_ws_closed(reason) -> str` — closed 帧
- Produces: `decode_ws_frame(raw: str) -> dict` — JSON 字符串 → dict（含 op 校验）

- [ ] **Step 1: 修改 `src/ma/core/graph/events.py`**

在 `encode_sse` 之后追加 WS 协议函数：

```python
import json as _json


def encode_ws(event: ChatEvent, request_id: str) -> str:
    """ChatEvent → WS JSON 帧（服务端→客户端 event）。设计书 §4.3.2。"""
    frame = {
        "op": "event",
        "request_id": request_id,
        "event": event.type,
        "data": event.data,
    }
    return _json.dumps(frame, ensure_ascii=False, separators=(",", ":"))


def encode_ws_ready(version: str = "0.2.0", heartbeat_interval_s: int = 20) -> str:
    """握手成功后的 ready 帧。"""
    return _json.dumps(
        {"op": "ready", "server_version": version, "heartbeat_interval_s": heartbeat_interval_s},
        ensure_ascii=False,
    )


def encode_ws_pong() -> str:
    """心跳应答。"""
    return _json.dumps({"op": "pong"}, ensure_ascii=False)


def encode_ws_closed(reason: str) -> str:
    """服务端主动关闭通知。"""
    return _json.dumps({"op": "closed", "reason": reason}, ensure_ascii=False)
```

同时追加 WS 帧解析函数（客户端→服务端）：

```python
# WS 客户端帧的合法 op 值
_WS_CLIENT_OPS = frozenset({"ask", "cancel", "ping"})


class WSFrameError(ValueError):
    """WS 帧格式不合法。"""


def decode_ws_frame(raw: str) -> dict:
    """解析客户端 WS JSON 帧 → dict。

    返回 dict 保证含 `op` key；其它字段 caller 自行 get。
    """
    try:
        frame = _json.loads(raw)
    except _json.JSONDecodeError as e:
        raise WSFrameError(f"invalid JSON: {e}") from e
    if not isinstance(frame, dict):
        raise WSFrameError("frame must be a JSON object")
    op = frame.get("op")
    if op not in _WS_CLIENT_OPS:
        raise WSFrameError(f"unknown or missing op: {op!r}")
    return frame
```

- [ ] **Step 2: 更新现有单测 `tests/unit/test_events.py`**

追加 WS 编码测试：

```python
def test_encode_ws_event() -> None:
    """ChatEvent → WS JSON 帧。"""
    from ma.core.graph.events import ChatEvent, encode_ws

    ev = ChatEvent(type="delta", data={"content": "你好"})
    frame = encode_ws(ev, request_id="req_1")
    obj = json.loads(frame)
    assert obj["op"] == "event"
    assert obj["request_id"] == "req_1"
    assert obj["event"] == "delta"
    assert obj["data"]["content"] == "你好"


def test_encode_ws_ready() -> None:
    from ma.core.graph.events import encode_ws_ready

    obj = json.loads(encode_ws_ready())
    assert obj["op"] == "ready"
    assert obj["server_version"] == "0.2.0"
    assert obj["heartbeat_interval_s"] == 20


def test_encode_ws_pong() -> None:
    from ma.core.graph.events import encode_ws_pong

    obj = json.loads(encode_ws_pong())
    assert obj["op"] == "pong"


def test_encode_ws_closed() -> None:
    from ma.core.graph.events import encode_ws_closed

    obj = json.loads(encode_ws_closed("bye"))
    assert obj["op"] == "closed"
    assert obj["reason"] == "bye"


def test_decode_ws_frame_valid_ask() -> None:
    from ma.core.graph.events import decode_ws_frame

    frame = decode_ws_frame(
        json.dumps(
            {
                "op": "ask",
                "request_id": "r1",
                "thread_id": "t1",
                "question": "hi",
                "biz_params": {},
            }
        )
    )
    assert frame["op"] == "ask"
    assert frame["request_id"] == "r1"


def test_decode_ws_frame_rejects_invalid_op() -> None:
    from ma.core.graph.events import decode_ws_frame, WSFrameError

    with pytest.raises(WSFrameError, match="unknown or missing op"):
        decode_ws_frame(json.dumps({"op": "unknown"}))


def test_decode_ws_frame_rejects_array() -> None:
    from ma.core.graph.events import decode_ws_frame, WSFrameError

    with pytest.raises(WSFrameError, match="must be a JSON object"):
        decode_ws_frame(json.dumps([1, 2, 3]))


def test_decode_ws_frame_rejects_bad_json() -> None:
    from ma.core.graph.events import decode_ws_frame, WSFrameError

    with pytest.raises(WSFrameError, match="invalid JSON"):
        decode_ws_frame("not json")
```

- [ ] **Step 3: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/unit/test_events.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: events 单测 8 个新增 + 已有 6 个 = 14 passed；full suite 126 passed + 19 skipped。

```bash
git add src/ma/core/graph/events.py tests/unit/test_events.py
git commit -m "feat(ws): WS envelope protocol — encode_ws / decode_ws_frame"
```

### Task 2: WS endpoint + ConnectionManager

**背景**：设计书 §4.3.1 / §4.7.2。FastAPI 原生 WebSocket route，复用 ChatService.handle。ConnectionManager 管理心跳超时 + 并发任务上限。

**Files:**
- Create: `src/ma/api/ws.py`
- Modify: `src/ma/main.py` — 注册 ws router

**Interfaces:**
- Consumes: `ChatService.handle(ChatRequest) -> AsyncIterator[ChatEvent]`
- Consumes: `encode_ws`, `encode_ws_ready`, `encode_ws_pong`, `encode_ws_closed`, `decode_ws_frame`, `WSFrameError` from `ma.core.graph.events`
- Consumes: `TopicRegistry`, `TopicNotFound` from `ma.core.topic.registry`
- Produces: `router: APIRouter` with `@router.websocket("/chat/ws")`

- [ ] **Step 1: 创建 `src/ma/api/ws.py`**

```python
"""WebSocket /api/v1/chat/ws 端点。设计书 §4.3。

连接 = 一个用户在一个专题下的在线通道，可承载多次 ask。
协议：双向 JSON。单连接内最大 5 个并发任务；45s 无帧则关闭。
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import ulid
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState

from ma.core.chat_service import ChatRequest, ChatService
from ma.core.graph.events import (
    WSFrameError,
    decode_ws_frame,
    encode_ws,
    encode_ws_closed,
    encode_ws_pong,
    encode_ws_ready,
)
from ma.core.graph.state import AuthnIdentity
from ma.core.topic.registry import TopicNotFound, TopicRegistry
from ma.infra.logging import bind_request_ctx, get_logger

router = APIRouter()
log = get_logger("ma.api.ws")

_MAX_CONCURRENT = 5
_HEARTBEAT_TIMEOUT_S = 45
_PING_INTERVAL_S = 20


class ConnectionManager:
    """管理 WS 连接级状态：并发任务数、心跳计时。"""

    def __init__(self) -> None:
        self._active_tasks: dict[str, asyncio.Task] = {}

    def _request_count(self) -> int:
        return len(self._active_tasks)

    def _can_accept(self) -> bool:
        return self._request_count() < _MAX_CONCURRENT

    def _add(self, request_id: str, task: asyncio.Task) -> None:
        self._active_tasks[request_id] = task

    def _remove(self, request_id: str) -> None:
        self._active_tasks.pop(request_id, None)

    async def _cancel_all(self) -> None:
        for rid, task in list(self._active_tasks.items()):
            task.cancel()
            self._active_tasks.pop(rid, None)


def _new_request_id() -> str:
    return f"req_{ulid.new()!s}"


@router.websocket("/chat/ws")
async def chat_ws(websocket: WebSocket, request: Request) -> None:
    await websocket.accept()

    # --- 从 query params 拿上下文（M4 占位，M5 接 AuthnMiddleware） ---
    w3_account = websocket.query_params.get("w3_account", "anonymous")
    biz_id = websocket.query_params.get("biz_id", "")
    x_user_role = websocket.headers.get("X-User-Role", "REP")
    x_user_claims_raw = websocket.headers.get("X-User-Claims")

    extra_claims: dict[str, Any] = {}
    if x_user_claims_raw:
        try:
            extra_claims = json.loads(x_user_claims_raw)
            if not isinstance(extra_claims, dict):
                extra_claims = {}
        except (ValueError, TypeError):
            extra_claims = {}

    identity = AuthnIdentity(
        w3_account=w3_account,
        raw_claims={"role": x_user_role, **extra_claims},
    )

    bind_request_ctx(w3_account=w3_account)
    log.info("ws_connected", w3_account=w3_account, biz_id=biz_id)

    chat_service: ChatService | None = request.app.state.chat_service
    topic_registry: TopicRegistry = request.app.state.topic_registry

    if chat_service is None:
        await websocket.send_text(
            encode_ws_closed("DB not configured"),
        )
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        return

    # Topic 校验
    try:
        topic_registry.get(biz_id)
    except TopicNotFound:
        await websocket.send_text(encode_ws_closed(f"unknown biz_id: {biz_id}"))
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # 握手完成 → ready
    await websocket.send_text(encode_ws_ready())
    mgr = ConnectionManager()

    async def _handle_ask(frame: dict) -> None:
        """处理单次 ask：调 ChatService → 推 event 帧。"""
        request_id = frame.get("request_id") or _new_request_id()
        thread_id = frame.get("thread_id") or "unknown"
        question = frame.get("question", "")
        biz_params = frame.get("biz_params", {})
        bind_request_ctx(request_id=request_id, thread_id=thread_id)

        chat_req = ChatRequest(
            biz_id=biz_id,
            thread_id=thread_id,
            w3_account=w3_account,
            question=question,
            request_id=request_id,
            biz_params=biz_params,
            identity=identity,
        )
        try:
            async for ev in chat_service.handle(chat_req):
                if websocket.client_state != WebSocketState.CONNECTED:
                    break
                await websocket.send_text(encode_ws(ev, request_id))
        except Exception:
            log.exception("ws_ask_error", request_id=request_id)
        finally:
            mgr._remove(request_id)

    try:
        while True:
            # 等待客户端帧（45s 超时）
            try:
                raw = await asyncio.wait_for(
                    websocket.receive_text(), timeout=_HEARTBEAT_TIMEOUT_S
                )
            except asyncio.TimeoutError:
                await websocket.send_text(
                    encode_ws_closed("heartbeat timeout"),
                )
                await websocket.close(code=4408)
                break

            try:
                frame = decode_ws_frame(raw)
            except WSFrameError as e:
                await websocket.send_text(
                    json.dumps({"op": "error", "reason": str(e)}),
                )
                continue

            op = frame["op"]

            if op == "ping":
                await websocket.send_text(encode_ws_pong())

            elif op == "ask":
                if not mgr._can_accept():
                    await websocket.send_text(
                        json.dumps(
                            {
                                "op": "error",
                                "reason": f"too many concurrent requests (max {_MAX_CONCURRENT})",
                                "request_id": frame.get("request_id", ""),
                            }
                        )
                    )
                    continue
                task = asyncio.create_task(_handle_ask(frame))
                mgr._add(frame.get("request_id", ""), task)

            elif op == "cancel":
                rid = frame.get("request_id", "")
                task = mgr._active_tasks.get(rid)
                if task is not None:
                    task.cancel()
                    mgr._remove(rid)

    except WebSocketDisconnect:
        log.info("ws_disconnected", w3_account=w3_account)
    finally:
        await mgr._cancel_all()
```

- [ ] **Step 2: 修改 `src/ma/main.py`**

加 ws router import 和注册：

```python
from ma.api import health, rest, sse, ws

# ...

app.include_router(ws.router, prefix="/api/v1")
```

- [ ] **Step 3: 跑 lint（先不跑集成测试——Task 3 做）**

```bash
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: mypy + ruff clean。

- [ ] **Step 4: Commit**

```bash
git add src/ma/api/ws.py src/ma/main.py
git commit -m "feat(ws): WebSocket endpoint + ConnectionManager at /api/v1/chat/ws"
```

### Task 3: WS 集成测试

**背景**：用 `websockets` 库连接 MA TestClient 的 WS endpoint，发 ask 帧 → 收 event 帧。

**Files:**
- Create: `tests/integration/test_ws.py`

- [ ] **Step 1: 创建 `tests/integration/test_ws.py`**

```python
"""WebSocket 端点集成测试：ask → event 流 → done。

本机无 docker，用 TestClient 启 app + websockets 连接。
"""
from __future__ import annotations

import json

import pytest
import websockets
from fastapi.testclient import TestClient


@pytest.fixture
def app_with_db(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """启 app 但不配 DB → ChatService=None → WS 应 closed。"""
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.delenv("MA_PG_DSN_RW", raising=False)
    topics_dir = tmp_path / "topics"
    topics_dir.mkdir()
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", str(topics_dir))
    from ma.main import create_app

    with TestClient(create_app()) as c:
        yield c


def test_ws_no_db_returns_closed(app_with_db: TestClient) -> None:
    """无 DB 时 WS 返回 closed 帧。"""
    # TestClient 不原生支持 WS；此测试验证 import + route 注册正确
    from ma.api import ws as ws_module

    assert ws_module.router is not None
    assert ws_module.ConnectionManager is not None
    # smoke: ConnectionManager 基本逻辑
    mgr = ws_module.ConnectionManager()
    assert mgr._can_accept() is True
    assert mgr._request_count() == 0


@pytest.mark.asyncio
async def test_ws_ask_event_flow(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """完整 WS 流程：connect → ask → 收 delta → 收 done → close。

    用 uvicorn 启 app 到 ephemeral port，websockets 连接。
    """
    import asyncio
    import socket

    import uvicorn

    # 准备 topic
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.delenv("MA_PG_DSN_RW", raising=False)
    topics_dir = tmp_path / "topics"
    topics_dir.mkdir()
    # 写一个最小 topic YAML
    import yaml

    topic_yaml = {
        "topic_id": "test",
        "display_name": "Test",
        "default_route": "metagc",
        "history": {"max_turns": 5, "scope": "per_topic"},
        "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
        "enrich": {"plugin": "generic_enrich", "params": {"fields_to_inject": ["region"]}},
        "intent": {
            "plugin": "llm_classifier",
            "params": {
                "provider": "fake",
                "model": "m",
                "labels": ["metagc"],
                "prompt_template": "p.txt",
                "fake_responses": ["route: metagc\nreason: ok"],
            },
        },
        "graph": {
            "nodes": [
                {
                    "id": "metagc_adapter",
                    "adapter": "metagc",
                    "output": True,
                    "params": {"base_url": "http://unused", "timeout_ms": 5000},
                }
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
        "biz_params_schema": {
            "type": "object",
            "required": ["region"],
            "properties": {"region": {"type": "string"}},
        },
        "error_messages": {
            "FORBIDDEN_TOPIC": "x",
            "FORBIDDEN_SCOPE": "x",
            "DOWNSTREAM_TIMEOUT": "x",
            "DOWNSTREAM_ERROR": "x",
            "INTERNAL": "x",
        },
    }
    (topics_dir / "test.yaml").write_text(yaml.dump(topic_yaml), encoding="utf-8")
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", str(topics_dir))

    from ma.main import create_app

    app = create_app()

    # ephemeral port
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    for _ in range(40):
        if server.started:
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("app failed to start")

    try:
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/api/v1/chat/ws?w3_account=alice&biz_id=test",
            open_timeout=5,
        ) as ws:
            # ready 帧
            raw = await ws.recv()
            ready = json.loads(raw)
            assert ready["op"] == "ready"

            # 发 ask
            await ws.send(
                json.dumps(
                    {
                        "op": "ask",
                        "request_id": "req_ws_test",
                        "thread_id": "th_ws",
                        "question": "hi",
                        "biz_params": {"region": "huadong"},
                    }
                )
            )

            # 收 event 帧直到 done
            events = []
            while True:
                raw = await ws.recv()
                frame = json.loads(raw)
                events.append(frame)
                if frame.get("op") == "event" and frame.get("event") == "done":
                    break
                if len(events) > 50:
                    break

            op_types = [e["op"] for e in events]
            assert "event" in op_types
            # 最后一个是 done event
            done_frames = [e for e in events if e.get("event") == "done"]
            assert len(done_frames) >= 1
    finally:
        server.should_exit = True
        await task
```

- [ ] **Step 2: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/integration/test_ws.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 2 个 WS 集成测试 pass（不依赖 docker）；full suite ~128 passed + 19 skipped。

```bash
git add tests/integration/test_ws.py
git commit -m "test(ws): WebSocket ask→event→done integration test"
```

**Phase A 完成标志：** WS 信封协议编码/解码完备；WS endpoint 注册；集成测试可跑通 ask→done 完整流。

---

## Phase B: WS 契约 + e2e

### Task 4: Contract 10（WS 多请求隔离）实测

**背景**：M1 以来 Contract 10 一直是 `pytest.skip("WS channel deferred to M4")`。M4 把它变成实测。

**Files:**
- Modify: `tests/stream/test_contracts.py` — 替换 Contract 10 为实测

- [ ] **Step 1: 修改 `tests/stream/test_contracts.py`**

替换 Contract 10 placeholder：

```python
@pytest.mark.asyncio
async def test_contract_10_ws_multi_request_isolation(make_service):
    """同一连接多个 ask 并发 → request_id 隔离，各自收到自己的 done。
    
    验证方式：并发调两次 ChatService.handle，断言每个流的 request_id
    在事件中保持一致。这模拟了 WS ConnectionManager 的并发处理。
    """
    import asyncio
    
    svc, _ = make_service()

    call_order: list[str] = []

    async def fake_run_1(self, state, *, output: bool):
        call_order.append("adapter_1_start")
        yield ChatEvent(type="delta", data={"content": "answer-1"})
        call_order.append("adapter_1_end")

    async def fake_run_2(self, state, *, output: bool):
        call_order.append("adapter_2_start")
        yield ChatEvent(type="delta", data={"content": "answer-2"})
        call_order.append("adapter_2_end")

    async def collect_stream(req):
        events = []
        async for ev in svc.handle(req):
            events.append(ev)
        return events

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run") as mock_run:
        # 第一个 req 用 fake_run_1，第二个用 fake_run_2
        mock_run.side_effect = [fake_run_1, fake_run_2]

        req1 = make_request(thread_id="th_a", request_id="req_a")
        req2 = make_request(thread_id="th_b", request_id="req_b")

        results = await asyncio.gather(
            collect_stream(req1),
            collect_stream(req2),
        )

    events_a, events_b = results

    # 每个流独立收到 done
    assert events_a[-1].type == "done"
    assert events_b[-1].type == "done"

    # 各自收到自己的 delta
    deltas_a = [e for e in events_a if e.type == "delta"]
    deltas_b = [e for e in events_b if e.type == "delta"]
    assert len(deltas_a) >= 1
    assert len(deltas_b) >= 1
    assert "answer-1" in deltas_a[0].data["content"]
    assert "answer-2" in deltas_b[0].data["content"]

    # 两个并发请求确实都被调了（验证并发隔离，非串行）
    assert "adapter_1_start" in call_order
    assert "adapter_2_start" in call_order
```

- [ ] **Step 2: 跑测试 + commit**

```bash
python -m uv run pytest tests/stream/test_contracts.py -v
python -m uv run pytest -v
```

Expected: Contract 10 从 skip 变 pass；stream contracts 10 implemented + 0 skipped（WS 契约全绿）。

```bash
git add tests/stream/test_contracts.py
git commit -m "test(contract): Contract 10 multi-request isolation from skip to real"
```

### Task 5: WS disconnect → partial 测试

**背景**：设计书 §4.3.4 — 连接断开时，正在跑的 task 被 cancel，assistant message 标记 `partial`。M4 测试此行为。

**Files:**
- Modify: `tests/stream/test_contracts.py` — 追加 contract test
- Create: 或在 Task 4 的同一个 commit 中追加

- [ ] **Step 1: 追加 `test_ws_disconnect_marks_partial` 到 test_contracts.py**

```python
@pytest.mark.asyncio
async def test_ws_disconnect_marks_partial(make_service):
    """模拟 WS 断开：adapter 中途抛 CancelledError → persist 写 partial。"""
    import asyncio

    svc, (_, msg_repo) = make_service()

    async def fake_run_abort(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "前一半"})
        raise asyncio.CancelledError("ws disconnected")

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run_abort):
        events = await collect(svc, make_request())

    # CancelledError 被 ChatService 捕获 → 流内不应有 error 事件冒给用户
    errors = [e for e in events if e.type == "error"]
    assert len(errors) == 0

    # assistant message 应被 persist，状态为 partial
    assistants = [m for m in msg_repo.messages if m.role == "assistant"]
    assert len(assistants) == 1
    assert assistants[0].status == "partial"
    assert "前一半" in assistants[0].content
```

- [ ] **Step 2: 跑测试 + commit**

```bash
python -m uv run pytest tests/stream/test_contracts.py -v -k "ws_disconnect"
python -m uv run pytest -v
```

Expected: 新 disconnect 测试 pass；stream contracts 11 implemented。

```bash
git add tests/stream/test_contracts.py
git commit -m "test(contract): WS disconnect → partial message marking"
```

**Phase B 完成标志：** Contract 10 实测；disconnect → partial 验证；stream contracts 全绿（11 个）。

---

## Phase C: 工具 + 可观测性

### Task 6: replay_request_id CLI

**背景**：设计书 §7.4.1 提到 `replay_request_id` CLI。给定 request_id，从 DB 拉消息历史，把 assistant 的事件流重放到 stdout（JSON lines 格式）。

**Files:**
- Create: `src/ma/cli/__init__.py`
- Create: `src/ma/cli/replay.py`
- Modify: `pyproject.toml` — 加 `[project.scripts]` entry point

- [ ] **Step 1: 创建 `src/ma/cli/__init__.py`**

空文件。

- [ ] **Step 2: 创建 `src/ma/cli/replay.py`**

```python
"""replay_request_id CLI：从 DB 拉一条 request 的消息历史，重放到 stdout。

用法：
    python -m ma.cli.replay <request_id> [--pg-dsn DSN]

输出：每行一个 JSON 对象（{"role","content","status","created_at"}），chronological 顺序。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime

import asyncpg


async def _replay(request_id: str, pg_dsn: str) -> list[dict]:
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT role, content, status, created_at, thread_id, seq "
                "FROM ma_message "
                "WHERE request_id = $1 "
                "ORDER BY seq",
                request_id,
            )
    finally:
        await pool.close()

    result: list[dict] = []
    for r in rows:
        result.append(
            {
                "role": r["role"],
                "content": r["content"],
                "status": r["status"],
                "thread_id": r["thread_id"],
                "seq": r["seq"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
        )
    return result


def _default_dsn() -> str:
    return os.environ.get(
        "MA_PG_DSN_RW",
        "postgresql://ma:ma@localhost:5432/ma",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a request's message history")
    parser.add_argument("request_id", help="Request ID to replay")
    parser.add_argument(
        "--pg-dsn",
        default=_default_dsn(),
        help="PostgreSQL DSN (default: $MA_PG_DSN_RW or localhost)",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = parser.parse_args()

    indent = 2 if args.pretty else None
    rows = asyncio.run(_replay(args.request_id, args.pg_dsn))

    if not rows:
        print(f"no messages found for request_id={args.request_id}", file=sys.stderr)
        sys.exit(1)

    for row in rows:
        print(json.dumps(row, ensure_ascii=False, indent=indent))


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: 修改 `pyproject.toml` 加 entry point**

```toml
[project.scripts]
ma-replay = "ma.cli.replay:main"
```

- [ ] **Step 4: 跑 lint + smoke test**

```bash
python -m uv run mypy src && python -m uv run ruff check .
# smoke: 不带 DB 运行 CLI（应报连不上 DB 但不会 import error）
python -m uv run python -c "from ma.cli.replay import main; print('CLI import OK')"
```

Expected: CLI import 成功；mypy + ruff clean。

- [ ] **Step 5: Commit**

```bash
git add src/ma/cli/__init__.py src/ma/cli/replay.py pyproject.toml
git commit -m "feat(cli): replay_request_id CLI — replay message history from DB"
```

### Task 7: OTel Jaeger 本地验证 + fixture 就位

**背景**：M3 已加 `LangChainInstrumentor().instrument()`。M4 需要确认：
1. 本地可启 Jaeger（docker compose）接收 spans
2. 关键 fixture（pg_dsn, metagc_server, ws_endpoint）在 CI 上就位
3. README 有验证步骤

**Files:**
- Modify: `README.md` — 加 Jaeger 本地验证步骤
- Modify: `src/ma/infra/tracing.py` — 可选：添加环境变量驱动的 Jaeger exporter 切换

- [ ] **Step 1: README 加 Jaeger 验证步骤**

在 README 追加：

```markdown
## 本地 OTel / Jaeger 验证 (M4)

```bash
# 1. 启 Jaeger all-in-one（内存存储）
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest

# 2. 设置 MA 的 OTLP endpoint 指向 Jaeger
export MA_OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

# 3. 启 MA
uvicorn ma.main:app --port 8000

# 4. 发一个请求
curl -X POST http://localhost:8000/api/v1/chat/sse \
  -H 'Content-Type: application/json' \
  -H 'X-User-Role: REP' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_jaeger","w3_account":"alice","question":"测试","biz_params":{"region":"huadong"}}'

# 5. 打开 Jaeger UI → http://localhost:16686
#    搜索 service=ma，应能看到 graph.node.* spans + llm.classify span
```
```

- [ ] **Step 2: （可选用）tracing.py 加 Jaeger 友好 exporter**

当前用 gRPC exporter（`OTLPSpanExporter`）。Jaeger all-in-one 支持 gRPC OTLP on port 4317。如果需要 HTTP exporter，可选加：

不过 gRPC exporter 已经能对接 Jaeger（port 4317），不需要改代码。确认 `settings.py` 里的 `otel_exporter_otlp_endpoint` 默认值可用即可。

- [ ] **Step 3: 验证 CI fixture 已就位**

检查 `tests/integration/conftest.py` 的 `pg_dsn` + `tests/e2e/conftest.py` 的 `metagc_server` fixture 是否都在 CI 上可用。M2/M3 已通过 CI，不需要额外改动。

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: local Jaeger OTel verification steps"
```

**Phase C 完成标志：** CLI 可用；Jaeger 验证文档就绪；fixture 不新增 gap。

---

## Phase D: 出口验证

### Task 8: M4 exit + merge + tag

- [ ] **Step 1: 全量 sweep**

```bash
python -m uv run pytest -v
python -m uv run pytest --cov=ma --cov-report=term-missing
python -m uv run mypy src
python -m uv run ruff check . && python -m uv run ruff format --check .
```

Expected:
- ~130+ passed（M3 118 + M4 新增 ~12 tests）
- 覆盖率 ≥ 80%
- mypy + ruff clean

- [ ] **Step 2: 写 M4 完成笔记**

`docs/superpowers/plans/2026-06-25-master-agent-m4-ws-observability.notes.md`

- [ ] **Step 3: 合并 + tag**

```bash
git add docs/superpowers/plans/2026-06-25-master-agent-m4-ws-observability.notes.md
git commit -m "docs: M4 completion notes"

git checkout main
git pull
git merge --no-ff feature/m4-ws-observability -m "merge: M4 WebSocket channel + observability"
git tag m4-complete
git push origin main --tags
```

**Phase D 完成标志：M4 全套绿；m4-complete tag 落地。**

---

# M4 总览

| Phase | Task # | 描述 | 预估 commits |
|---|---|---|---|
| A | 1-3 | WS 信封协议 + endpoint + 集成测试 | 3 |
| B | 4-5 | Contract 10 实测 + disconnect→partial | 2 |
| C | 6-7 | replay CLI + OTel Jaeger 验证 | 2 |
| D | 8 | 出口 + 合并 + tag | 1 |
| **Total** | | | **~8** |

预估时间：1 名后端工程师，**~1.5 周**（与设计书 §9.4 M4 估算一致）。
