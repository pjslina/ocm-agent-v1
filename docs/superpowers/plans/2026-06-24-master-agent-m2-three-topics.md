# MasterAgent M2 — 历史 + 鉴权 + 三专题完备 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 三个专题（代表小管家 / 经营小助手 / 销售合同责任人）端到端跑通；历史拼接按"整轮一起丢"语义；鉴权分专题 + 问题范围两层；REST 历史查询端点带越权防御。

**Architecture (在 M1 之上加这些层):**
- **Day-1 cleanup**：修 reject 节点 dispatch delta（M1 Contract 9 偏差）；`SessionBizMismatchError` → HTTP 400；历史拼接整轮一起丢
- **三专题三套 AuthPlugin**：`representative_auth` 调整为 M2 形态；新增 `jingying_auth`、`sales_contract_auth`；每个含 role + biz_params 范围校验
- **三专题三个 Topic YAML**：经营小助手 / 销售合同责任人；都路由到 MetaGC（M3 接其它下游）
- **REST endpoints**：`/api/v1/sessions` + `/api/v1/sessions/{thread_id}/messages`；w3_account 从 `X-User-Account` header 取（与 SSE 一致）；越权防御（thread_id.w3_account == identity.w3_account）
- **追问场景 e2e**：连续两次问答验证历史拼接
- **Stream contract 9 修复**：reject 真 dispatch delta，前端能看到话术

**Tech Stack:** 不引入新依赖；全部基于 M1 已有抽象。

**Spec reference:** `docs/superpowers/specs/2026-06-23-master-agent-design.md` §3.5 / §4.4 / §4.5 / §9.4-M2。

**M1 baseline:** tag `m1-complete`（commit `afa749e`）。

**Branch strategy:** 当前 `design/master-agent-v1`（M1 合入）。M2 在新分支 `feature/m2-three-topics` 开发。

**Locked design decisions** (来自 brainstorming 阶段)：
- **三独立 AuthPlugin**：representative_auth / jingying_auth / sales_contract_auth，各自含 role + 范围校验
- **问题范围鉴权数据源**：仅用 `identity.raw_claims` + `biz_params`，不接外部用户中心
- **REST 历史端点的 w3**：从 `X-User-Account` header 取（与 SSE endpoint 一致）
- **下游**：三专题都走 M1 已有 MetaGC mock；不引入新 mock

---

## File Structure (M2 创建/修改清单)

### 阶段 A: M1 day-1 cleanup
- Modify: `src/ma/core/graph/builtins.py` — reject 节点 dispatch delta
- Modify: `src/ma/api/sse.py` 或 `src/ma/core/chat_service.py` — `SessionBizMismatchError` → HTTP 400 / error event
- Modify: `src/ma/core/graph/builtins.py` 中 `make_load_session_node` — 整轮一起丢
- Modify: `tests/stream/test_contracts.py` — contract 9 改为正向断言

### 阶段 B: 第二、三个 AuthPlugin + 范围鉴权扩展
- Modify: `src/ma/plugins/auth/representative_auth.py` — 加范围鉴权（region 校验）
- Create: `src/ma/plugins/auth/jingying_auth.py`
- Create: `src/ma/plugins/auth/sales_contract_auth.py`
- Modify: `src/ma/core/plugin/bootstrap.py` — 加 import
- Create: `tests/unit/test_jingying_auth.py`
- Create: `tests/unit/test_sales_contract_auth.py`
- Modify: `tests/unit/test_representative_auth.py` — 加范围鉴权测试

### 阶段 C: 第二、三个 Topic YAML + intent prompt
- Create: `config/topics/jingying_xiaozhushou.yaml`
- Create: `config/topics/sales_contract_agent.yaml`
- Create: `prompts/intent/jingying.txt`
- Create: `prompts/intent/sales_contract.txt`

### 阶段 D: REST endpoints + 越权防御
- Create: `src/ma/api/rest.py` — `/sessions` + `/sessions/{id}/messages`
- Modify: `src/ma/main.py` — 注册 rest router；扩 ChatService 暴露 repo factories 给 REST
- Modify: `src/ma/core/chat_service.py` — 暴露 factory 属性供 rest 用，或者另写一个 `HistoryService`
- Create: `tests/integration/test_rest_history.py`

### 阶段 E: e2e 三专题 + 追问
- Create: `tests/e2e/test_jingying_xiaozhushou_sse.py`
- Create: `tests/e2e/test_sales_contract_sse.py`
- Create: `tests/e2e/test_followup_question.py` — 追问场景

### 阶段 F: 出口验证
- Modify: `README.md` — 更新 M2 状态
- 写 M2 完成笔记
- 合并 + tag m2-complete

### M2 不创建（留 M3+）
- `UniiocAdapter` / `KnowledgeCenterAdapter`（M3）
- 真接 LLM（M3）
- WebSocket endpoint（M4）
- 真接外部用户中心 API（M3 或更后）

---

## 任务索引

| Phase | Task | 名称 |
|---|---|---|
| A: M1 day-1 cleanup | 1 | reject 节点 dispatch delta（修复 Contract 9）|
|  | 2 | SessionBizMismatchError → HTTP 400 |
|  | 3 | 历史拼接"整轮一起丢"（修 load_session 节点）|
| B: 三套 AuthPlugin | 4 | representative_auth 扩展为含范围鉴权 |
|  | 5 | jingying_auth + 单测 |
|  | 6 | sales_contract_auth + 单测 |
| C: Topic YAML | 7 | jingying_xiaozhushou.yaml + intent prompt |
|  | 8 | sales_contract_agent.yaml + intent prompt |
| D: REST endpoints | 9 | `/sessions` + `/sessions/{id}/messages` + 越权防御 |
|  | 10 | REST integration tests |
| E: e2e 三专题 + 追问 | 11 | e2e 经营小助手 |
|  | 12 | e2e 销售合同责任人 |
|  | 13 | e2e 追问场景（历史拼接验证） |
| F: 出口 | 14 | 出口验证 + 合并 + tag + 完成笔记 |

总计 14 个 task，比 M1 少一半（M1 是 26 个）。

---

(以下章节将分阶段展开。)

---

## Phase A: M1 day-1 cleanup

M1 完成笔记列出 3 项遗留：reject 不下发话术、历史按消息数而非轮数、`SessionBizMismatchError` 没映射 HTTP。本 phase 一次性扫清。

### Task 1: reject 节点 dispatch delta（修复 Contract 9）

**Files:**
- Modify: `src/ma/core/graph/builtins.py` — `make_reject_node` 改为通过 `dispatch_custom_event` 发 delta
- Modify: `tests/unit/test_graph_builtins.py` — 加 dispatch 断言
- Modify: `tests/stream/test_contracts.py` — Contract 9 改为正向断言

**背景**：M1 的 `make_reject_node` 只把固定话术写到 `answer_chunks`，不通过 dispatch_custom_event 发 delta。LangGraph `astream_events` 看不到，ChatService 也不会推 SSE delta 给前端。结果是鉴权失败时用户只看到 meta + done，没话术 —— 设计书 §4.5 承诺打折。

**修复**：reject 节点显式调 `adispatch_custom_event("delta", ...)`，保持 answer_chunks 写入（用于 persist 节点拼内容）。

- [ ] **Step 1: 切到 M2 工作分支**

```bash
cd E:/work/source/python/ocm-agent-v1
git checkout design/master-agent-v1
git pull
git checkout -b feature/m2-three-topics
git status
```

- [ ] **Step 2: 改 `src/ma/core/graph/builtins.py` — `make_reject_node`**

Reject 节点签名保持 `make_reject_node(error_messages)`，但内部加 dispatch：

```python
def make_reject_node(error_messages: dict[str, str]) -> NodeFn:
    """build reject 节点。优先用 AuthPlugin 给的 reject_message，否则用 error_messages 兜底。

    M2 改动：除了写 answer_chunks（供 persist 节点拼），还通过 dispatch_custom_event
    发一条 delta 事件，让前端真的看到话术。
    """
    from langchain_core.callbacks.manager import adispatch_custom_event

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        auth = state.get("auth")
        text = None
        code = "INTERNAL"
        if auth is not None:
            code = getattr(auth, "reject_code", None) or "INTERNAL"
            text = getattr(auth, "reject_message", None)
        if not text:
            text = error_messages.get(code, error_messages.get("INTERNAL", "服务异常"))
        # 让前端看到话术
        await adispatch_custom_event("delta", {"content": text}, config=config)
        return {"answer_chunks": [text]}

    return node
```

- [ ] **Step 3: 更新 `tests/unit/test_graph_builtins.py`**

读现有文件，找到 `test_reject_node_emits_fixed_message_and_marks_complete` 和 `test_reject_uses_plugin_provided_message_first`。它们调用 `await node(state, config={})`，但现在 node 会调用 `adispatch_custom_event` —— 在没有 callback manager 的情况下这个调用会无声 fail 或抛错。

需要包一层 `patch`：

```python
@pytest.mark.asyncio
async def test_reject_node_emits_fixed_message_and_marks_complete() -> None:
    """reject 节点把 reject_message 写到 answer_chunks 并 dispatch delta。"""
    from unittest.mock import patch
    from ma.core.graph.builtins import make_reject_node

    error_messages = {"FORBIDDEN_TOPIC": "您暂无权限使用此专题", "INTERNAL": "服务异常"}
    node = make_reject_node(error_messages)

    auth_result = MagicMock()
    auth_result.reject_code = "FORBIDDEN_TOPIC"
    auth_result.reject_message = None

    state = {"auth": auth_result}
    captured: list[tuple[str, dict]] = []

    async def fake_dispatch(name: str, data: dict, *, config: dict) -> None:
        captured.append((name, data))

    with patch(
        "langchain_core.callbacks.manager.adispatch_custom_event",
        side_effect=fake_dispatch,
    ):
        out = await node(state, config={})

    assert out["answer_chunks"] == ["您暂无权限使用此专题"]
    # M2 新增断言：确实发了一条 delta
    assert ("delta", {"content": "您暂无权限使用此专题"}) in captured
```

同理改 `test_reject_uses_plugin_provided_message_first`，包 patch 并断言 delta 内容是插件话术。

- [ ] **Step 4: 修 `tests/stream/test_contracts.py` Contract 9**

读现有文件，找到 `test_contract_9_auth_rejection_yields_done`。改为正向断言：

```python
@pytest.mark.asyncio
async def test_contract_9_auth_rejection_yields_done(make_service):
    """鉴权失败 → reject 节点 dispatch delta + done。M2 修复（M1 简化已撤销）。"""
    svc, _ = make_service()

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "should not appear"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request(role="VIEWER"))

    assert events[-1].type == "done"
    deltas = [e for e in events if e.type == "delta"]
    # M2 改动：reject 节点真的下发了话术 delta
    assert len(deltas) == 1
    # 内容是 error_messages 里 FORBIDDEN_TOPIC 配的话术（"无权限"——见 conftest._topic_config）
    # 或 AuthPlugin 自己给的话术（"该专题仅限 REP 角色访问。"）—— 后者优先
    assert "REP" in deltas[0].data["content"] or "无权限" in deltas[0].data["content"]
    # adapter.run 不应被调到（auth 失败 → reject → persist → END）
    assert "should not appear" not in deltas[0].data["content"]
```

- [ ] **Step 5: 跑测试**

```bash
python -m uv run pytest tests/unit/test_graph_builtins.py tests/stream/test_contracts.py -v
```

Expected:
- 5 builtin tests pass（含 2 个含 dispatch 断言）
- Contract 9 改为正向，pass

Then full suite:
```bash
python -m uv run pytest -v
```
Expected: 95 + 0 (我们没加测试，只是改了断言) = 95 passed + 10 skipped + 1 skipped（contract 10 仍 skipped；contract 8 仍 skipped）= **95 passed + 12 skipped → 仍 95 passed + 12 skipped**（其中 contract 9 之前 placeholder 现在变成实测，反向之前 11 skipped 中本来就有 contract 8/10，原来是 9 实测 placeholder + 现在变正测）

Actually：M1 末态 contract 9 应该已经实测（没 placeholder），所以 M2 修后还是 95 + 12。**verify by counting**：

- M1 末态 stream contract: 8 实测 + 2 skipped（contract 8 + contract 10）
- M2 改后 stream contract: 仍 8 实测 + 2 skipped（contract 9 内容变了但仍实测）

Expected full: **95 passed + 12 skipped**。

- [ ] **Step 6: Lint + commit**

```bash
python -m uv run mypy src && python -m uv run ruff check . && python -m uv run ruff format --check .
git add src/ma/core/graph/builtins.py tests/unit/test_graph_builtins.py tests/stream/test_contracts.py
git commit -m "fix(graph): reject node dispatches delta so rejection message reaches client

Resolves M1 Contract 9 simplification (see M1 notes §M2 day-1)."
```

### Task 2: SessionBizMismatchError → HTTP 400

**Files:**
- Modify: `src/ma/core/chat_service.py` — 在 `_persist_user_message` 捕获 `SessionBizMismatchError` 转 ChatEvent(error, BAD_REQUEST)
- Modify: `tests/stream/test_contracts.py` 或新增 contract test — 验证 biz_id 不匹配场景

**背景**：M1 完成笔记记的第 3 项 — 如果同一 thread_id 用两个 biz_id 请求，`SessionRepository.get_or_create` 会抛 `SessionBizMismatchError`，目前会冒成 `INTERNAL` error。设计书 §3.4 应该是 HTTP 400。

**修复方式**：M2 在 ChatService.handle 里捕获并转换。**不**通过 HTTPException —— 因为 ChatService 是 generator，HTTPException 在 generator 里抛会被 FastAPI 误处理；应该 yield 一个 ChatEvent(error, BAD_REQUEST) 后 yield done。

- [ ] **Step 1: 改 `src/ma/core/chat_service.py`**

在 `_persist_user_message` 里捕获 `SessionBizMismatchError`。但这个方法目前签名是 `async def _persist_user_message(self, req)` 不 yield；调用点在 handle 主体里。需要重构为让 handle 处理。

具体做法：在 handle 的 `await self._persist_user_message(req)` 那一行包 try/except：

```python
from ma.core.repo.models import SessionBizMismatchError

# ...在 handle 方法内，meta yield 之后、build_graph 之前：

try:
    await self._persist_user_message(req)
except SessionBizMismatchError as e:
    yield ChatEvent(
        type="error",
        data={
            "code": "BAD_REQUEST",
            "message": "thread_id 已绑定到其它专题，无法跨专题复用",
            "retryable": False,
            "detail": str(e),
            "request_id": req.request_id,
        },
    )
    yield ChatEvent(type="done", data={"status": "failed"})
    return
```

- [ ] **Step 2: 加一个 stream contract test 验证此行为**

追加到 `tests/stream/test_contracts.py`:

```python
@pytest.mark.asyncio
async def test_session_biz_mismatch_yields_bad_request(make_service):
    """同一 thread_id 用两个 biz_id 请求 → BAD_REQUEST error + done。"""
    svc, (sess_repo, _) = make_service()

    # 先预填一个 session_repo 里的 session，绑到不同 biz_id
    from datetime import datetime
    from ma.core.repo.models import Session
    sess_repo.sessions["th_conflict"] = Session(
        thread_id="th_conflict",
        biz_id="some_other_topic",
        w3_account="alice",
        title=None,
        status="active",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        last_message_at=None,
        ext={},
    )

    events = await collect(svc, make_request(thread_id="th_conflict"))
    error_evs = [e for e in events if e.type == "error"]
    assert len(error_evs) == 1
    assert error_evs[0].data["code"] == "BAD_REQUEST"
    assert events[-1].type == "done"
```

- [ ] **Step 3: 跑测试 + lint + commit**

```bash
python -m uv run pytest tests/stream/test_contracts.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check . && python -m uv run ruff format --check .
```

Expected: 9 stream contract tests pass + 2 skipped；full suite 96 passed + 12 skipped。

```bash
git add src/ma/core/chat_service.py tests/stream/test_contracts.py
git commit -m "fix(chat): catch SessionBizMismatchError → BAD_REQUEST error event

Resolves M1 notes §M2 day-1 cleanup item 3."
```

### Task 3: 历史拼接"整轮一起丢"

**Files:**
- Modify: `src/ma/core/graph/builtins.py` — `make_load_session_node` 调整拼接语义
- Modify: `tests/unit/test_graph_builtins.py` — 加"按轮丢"测试

**背景**：设计书 §3.5 规定"如果历史超过 LLM context 限制，最早的整轮先丢"。M1 简化为按消息数：`max_turns * 2` 条消息。M2 要落地整轮语义。

"轮"定义：相邻的 user + assistant 一对算一轮。半轮（只有 user 无 assistant，或反之）算不完整。

**规则**：
- 从 list_recent 拿到的消息（DESC）反向为正序
- 从正序末尾开始按轮（user → assistant）拼，最多 max_turns 轮
- 若拼到一半发现某轮缺 assistant（如最末尾的 user 还没回答），跳过该轮
- 若超过 max_turns 限制，从最早的整轮开始丢

**简化**：M2 实现 "按 max_turns 取最近 N 轮完整对话"，丢半轮逻辑。

- [ ] **Step 1: 改 `src/ma/core/graph/builtins.py` — `make_load_session_node`**

```python
def make_load_session_node(
    *,
    session_repo: SessionRepository,
    message_repo: MessageRepository,
    max_turns: int,
) -> NodeFn:
    """build load_session 节点。

    历史拼接（M2 落地）：按"轮"取，最近 max_turns 轮完整对话。
    一轮 = user + assistant 一对相邻消息。半轮（只有 user 无 assistant，
    或只有 assistant 无 user）跳过。

    设计书 §3.5。
    """

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        identity = state["identity"]
        session = await session_repo.get_or_create(
            thread_id=state["thread_id"],
            biz_id=state["biz_id"],
            w3_account=identity.w3_account,
        )
        # 取足够多的消息：max_turns * 2 + 一些 buffer 应对半轮过滤
        raw = await message_repo.list_recent(
            thread_id=state["thread_id"], limit=max_turns * 4
        )
        raw.reverse()  # 正序

        # 按轮拼：从前往后扫描，识别 user + assistant 对
        turns: list[list] = []  # list of [user_msg, assistant_msg]
        i = 0
        while i < len(raw) - 1:
            if raw[i].role == "user" and raw[i + 1].role == "assistant":
                turns.append([raw[i], raw[i + 1]])
                i += 2
            else:
                # 半轮：跳过
                i += 1

        # 保留最近 max_turns 轮
        kept = turns[-max_turns:] if len(turns) > max_turns else turns
        history: list = []
        for turn in kept:
            history.extend(turn)

        return {"history": history}

    return node
```

- [ ] **Step 2: 加测试到 `tests/unit/test_graph_builtins.py`**

追加到现有文件末尾：

```python
@pytest.mark.asyncio
async def test_load_session_keeps_only_complete_turns() -> None:
    """半轮（缺 assistant 或缺 user）会被丢；完整轮保留。"""
    from datetime import datetime
    from ma.core.graph.builtins import make_load_session_node
    from ma.core.repo.models import Message, Session

    fake_session_repo = MagicMock()
    fake_session_repo.get_or_create = AsyncMock(
        return_value=Session(
            thread_id="th_1", biz_id="b", w3_account="a", title=None,
            status="active", created_at=datetime.now(), updated_at=datetime.now(),
            last_message_at=None, ext={},
        )
    )
    fake_msg_repo = MagicMock()
    # 模拟历史：u-a / u-a / u (半轮，最新)
    # repo 返回 DESC 顺序
    fake_msg_repo.list_recent = AsyncMock(
        return_value=[
            Message(message_id="m5", thread_id="th_1", seq=5, role="user", content="q3"),
            Message(message_id="m4", thread_id="th_1", seq=4, role="assistant", content="a2"),
            Message(message_id="m3", thread_id="th_1", seq=3, role="user", content="q2"),
            Message(message_id="m2", thread_id="th_1", seq=2, role="assistant", content="a1"),
            Message(message_id="m1", thread_id="th_1", seq=1, role="user", content="q1"),
        ]
    )
    node = make_load_session_node(
        session_repo=fake_session_repo, message_repo=fake_msg_repo, max_turns=5
    )
    out = await node(
        {"thread_id": "th_1", "biz_id": "b", "identity": MagicMock(w3_account="a")},
        config={},
    )
    # 应当只保留两对完整轮（m1-m2 + m3-m4），m5 半轮丢
    assert [m.message_id for m in out["history"]] == ["m1", "m2", "m3", "m4"]


@pytest.mark.asyncio
async def test_load_session_caps_to_max_turns() -> None:
    """超过 max_turns 轮：保留最近 N 轮。"""
    from datetime import datetime
    from ma.core.graph.builtins import make_load_session_node
    from ma.core.repo.models import Message, Session

    fake_session_repo = MagicMock()
    fake_session_repo.get_or_create = AsyncMock(
        return_value=Session(
            thread_id="th_1", biz_id="b", w3_account="a", title=None,
            status="active", created_at=datetime.now(), updated_at=datetime.now(),
            last_message_at=None, ext={},
        )
    )
    fake_msg_repo = MagicMock()
    # 4 轮完整对话；max_turns=2 → 保留最近 2 轮
    fake_msg_repo.list_recent = AsyncMock(
        return_value=[
            Message(message_id=f"m{i}", thread_id="th_1", seq=i,
                    role="assistant" if i % 2 == 0 else "user",
                    content=f"c{i}")
            for i in range(8, 0, -1)
        ]
    )
    node = make_load_session_node(
        session_repo=fake_session_repo, message_repo=fake_msg_repo, max_turns=2
    )
    out = await node(
        {"thread_id": "th_1", "biz_id": "b", "identity": MagicMock(w3_account="a")},
        config={},
    )
    # 4 轮，保留最近 2 轮 = 最后 4 条消息（m5, m6, m7, m8）
    assert [m.message_id for m in out["history"]] == ["m5", "m6", "m7", "m8"]
```

- [ ] **Step 3: 跑测试 + commit**

```bash
python -m uv run pytest tests/unit/test_graph_builtins.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 7 builtin tests pass（5 + 2）；full suite 98 passed。

```bash
git add src/ma/core/graph/builtins.py tests/unit/test_graph_builtins.py
git commit -m "fix(graph): load_session keeps complete turns only (entire turn drop)

Resolves M1 notes §M2 day-1 cleanup item 2 (design book §3.5)."
```

**Phase A 完成标志：** 3 个 commit；M1 day-1 cleanup 三项全修；测试套 98 passed + 12 skipped。

---

## Phase B: 三套 AuthPlugin（专题 + 范围鉴权）

每个 AuthPlugin 两层校验：
1. **专题级**：identity.raw_claims.role 必须在 plugin params 的 allowed_roles 集合里
2. **范围级**：根据 biz_params 内容判断当前问题是否在用户授权范围内

三个专题各有差异：

| 专题 | role 校验 | 范围校验（来自 biz_params + claims）|
|---|---|---|
| 代表小管家 | role == "REP" | claims.regions 必须包含 biz_params.region |
| 经营小助手 | role in {"MANAGER", "REP"} | claims.business_units 必须包含 biz_params.business_unit |
| 销售合同责任人 | role == "SALES_OWNER" | claims.contract_ids 必须包含 biz_params.contract_id |

### Task 4: representative_auth 扩展含范围鉴权

**Files:**
- Modify: `src/ma/plugins/auth/representative_auth.py`
- Modify: `tests/unit/test_representative_auth.py`

- [ ] **Step 1: 改 `src/ma/plugins/auth/representative_auth.py`**

加范围鉴权（region 必须在 user 的 claims.regions 列表里）：

```python
"""代表小管家鉴权插件（M2）。

两层校验：
1. role == required_role (M1 已有)
2. biz_params.region 必须在 identity.raw_claims.regions 列表内（M2 新增）

M3 起接外部用户中心 API 拉真实 role + regions + 客户归属。
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

        # 1) 专题级：role 校验
        role = claims.get("role")
        if role != self._required_role:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_TOPIC",
                reject_message=f"该专题仅限 {self._required_role} 角色访问。",
            )

        # 2) 范围级：region 必须在 user 的 allowed regions 内
        biz_params = state.get("biz_params", {})
        region = biz_params.get("region")
        allowed_regions = claims.get("regions", [])
        if region is None:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message="请求缺少 region 参数。",
            )
        if region not in allowed_regions:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message=f"您无权访问 {region} 大区数据。",
            )

        return AuthResult(
            passed=True,
            user_ctx={"role": role, "regions": allowed_regions, "region": region},
        )
```

- [ ] **Step 2: 改 `tests/unit/test_representative_auth.py`**

原有 3 个测试已经覆盖了 role 校验。需要：
1. 改原有测试，让 happy path 也传 regions claim
2. 加 2 个范围鉴权测试

读现有文件后做改动。新加：

```python
@pytest.mark.asyncio
async def test_auth_rejects_when_region_not_in_allowed() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "REP", "regions": ["huadong", "huabei"]},
        ),
        "biz_params": {"region": "huanan"},  # not in claims.regions
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_SCOPE"


@pytest.mark.asyncio
async def test_auth_rejects_when_biz_params_missing_region() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "REP", "regions": ["huadong"]},
        ),
        "biz_params": {},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_SCOPE"


@pytest.mark.asyncio
async def test_auth_passes_with_region_in_allowed() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "REP", "regions": ["huadong"]},
        ),
        "biz_params": {"region": "huadong"},
    }
    result = await plugin.authorize(state)
    assert result.passed is True
    assert result.user_ctx["region"] == "huadong"
    assert result.user_ctx["regions"] == ["huadong"]
```

并且修改原有的 `test_auth_passes_with_matching_role` 让它适配新逻辑（加 regions claim + biz_params region）：

```python
@pytest.mark.asyncio
async def test_auth_passes_with_matching_role() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "REP", "regions": ["huadong"]},
        ),
        "biz_params": {"region": "huadong"},
    }
    result = await plugin.authorize(state)
    assert result.passed is True
    assert result.user_ctx is not None
    assert result.user_ctx["role"] == "REP"
```

另外两个测试（rejects wrong role / no role claim）可保留，但需要 biz_params 防止 region 检查先报错 —— wrong role 的 case 应当在 role 校验时就 reject，不需要走到 region 校验。验证测试还过即可。

- [ ] **Step 3: 跑测试 + sweep**

```bash
python -m uv run pytest tests/unit/test_representative_auth.py -v
python -m uv run pytest tests/stream/ tests/unit/test_chat_service_with_fake_topic.py -v
```

注意：`tests/stream/conftest.py` 里的 `make_request` 默认传 `region="huadong"`，但 fixture topic config 里的 `representative_auth` 现在期望 `regions` claim 也有 `huadong`。要修 `make_request` 的 identity，让 raw_claims 加上 `regions=["huadong"]`：

```python
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
        identity=AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": role, "regions": [region]},  # M2 加 regions
        ),
    )
```

同理修 `tests/unit/test_chat_service_with_fake_topic.py` 里的 identity 构造，让 raw_claims 含 `regions`。

跑 stream contract + chat service tests 应当全过。

Full sweep:
```bash
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 98 + 3 新测试 = 101 passed + 12 skipped；mypy + ruff clean。

- [ ] **Step 4: Commit**

```bash
git add src/ma/plugins/auth/representative_auth.py \
        tests/unit/test_representative_auth.py \
        tests/stream/conftest.py \
        tests/unit/test_chat_service_with_fake_topic.py
git commit -m "feat(auth): representative_auth adds region-scope check (claims.regions ∋ biz_params.region)"
```

### Task 5: jingying_auth + 单测

**Files:**
- Create: `src/ma/plugins/auth/jingying_auth.py`
- Create: `tests/unit/test_jingying_auth.py`
- Modify: `src/ma/core/plugin/bootstrap.py` — 加 import

经营小助手鉴权：role 必须在 `allowed_roles` 集合里（MANAGER 或 REP）；biz_params 里有 `business_unit` 字段，必须在 user 的 `business_units` claim 列表里。

- [ ] **Step 1: 写测试 `tests/unit/test_jingying_auth.py`**

```python
"""jingying_auth：经营小助手鉴权（role + business_unit 范围）。"""
from __future__ import annotations

import importlib
import sys

import pytest

from ma.core.graph.state import AuthnIdentity


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module
    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    sys.modules.pop("ma.plugins.auth.jingying_auth", None)
    importlib.import_module("ma.plugins.auth.jingying_auth")
    yield new_reg


@pytest.mark.asyncio
async def test_jingying_passes_with_allowed_role_and_unit() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "jingying_auth",
        {"allowed_roles": ["MANAGER", "REP"]},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "MANAGER", "business_units": ["bu_a", "bu_b"]},
        ),
        "biz_params": {"business_unit": "bu_a"},
    }
    result = await plugin.authorize(state)
    assert result.passed is True
    assert result.user_ctx["business_unit"] == "bu_a"


@pytest.mark.asyncio
async def test_jingying_rejects_role_not_in_allowed() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "jingying_auth",
        {"allowed_roles": ["MANAGER", "REP"]},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="bob",
            raw_claims={"role": "VIEWER", "business_units": ["bu_a"]},
        ),
        "biz_params": {"business_unit": "bu_a"},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_TOPIC"


@pytest.mark.asyncio
async def test_jingying_rejects_business_unit_not_authorized() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "jingying_auth",
        {"allowed_roles": ["MANAGER"]},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "MANAGER", "business_units": ["bu_a"]},
        ),
        "biz_params": {"business_unit": "bu_z"},  # not in claims.business_units
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_SCOPE"
```

- [ ] **Step 2: 实现 `src/ma/plugins/auth/jingying_auth.py`**

```python
"""经营小助手鉴权插件（M2）。

两层校验：
1. role 在 allowed_roles 集合内
2. biz_params.business_unit 必须在 identity.raw_claims.business_units 列表内
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import AuthResult
from ma.core.plugin.registry import registry


class JingyingAuthParams(BaseModel):
    allowed_roles: list[str]


@registry.register
class JingyingAuth:
    name = "jingying_auth"
    plugin_kind = "auth"

    def configure(self, params: dict[str, Any]) -> None:
        p = JingyingAuthParams(**params)
        self._allowed_roles = set(p.allowed_roles)

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
        if role not in self._allowed_roles:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_TOPIC",
                reject_message=(
                    f"该专题仅限 {'/'.join(sorted(self._allowed_roles))} 角色访问。"
                ),
            )

        biz_params = state.get("biz_params", {})
        business_unit = biz_params.get("business_unit")
        allowed_units = claims.get("business_units", [])
        if business_unit is None:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message="请求缺少 business_unit 参数。",
            )
        if business_unit not in allowed_units:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message=f"您无权访问 {business_unit} 业务单元数据。",
            )

        return AuthResult(
            passed=True,
            user_ctx={
                "role": role,
                "business_units": allowed_units,
                "business_unit": business_unit,
            },
        )
```

- [ ] **Step 3: 改 `src/ma/core/plugin/bootstrap.py`，加 import**

```python
def load_all_plugins() -> None:
    """启动期调一次。"""
    import ma.plugins.adapter.metagc  # noqa: F401
    import ma.plugins.auth.jingying_auth  # noqa: F401
    import ma.plugins.auth.representative_auth  # noqa: F401
    import ma.plugins.enrich.generic_enrich  # noqa: F401
    import ma.plugins.intent.llm_classifier  # noqa: F401
```

注意 import 已按字母序。

- [ ] **Step 4: 跑测试 + commit**

```bash
python -m uv run pytest tests/unit/test_jingying_auth.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 3 jingying tests pass；full suite 104 passed + 12 skipped。

```bash
git add src/ma/plugins/auth/jingying_auth.py \
        src/ma/core/plugin/bootstrap.py \
        tests/unit/test_jingying_auth.py
git commit -m "feat(auth): jingying_auth with role + business_unit scope checks"
```

### Task 6: sales_contract_auth + 单测

**Files:**
- Create: `src/ma/plugins/auth/sales_contract_auth.py`
- Create: `tests/unit/test_sales_contract_auth.py`
- Modify: `src/ma/core/plugin/bootstrap.py` — 加 import

销售合同责任人鉴权：role == "SALES_OWNER"；biz_params 里有 `contract_id`，必须在 user 的 `contract_ids` claim 列表里。

- [ ] **Step 1: 写测试 `tests/unit/test_sales_contract_auth.py`**

```python
"""sales_contract_auth：销售合同责任人鉴权（role + contract_id 范围）。"""
from __future__ import annotations

import importlib
import sys

import pytest

from ma.core.graph.state import AuthnIdentity


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module
    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    sys.modules.pop("ma.plugins.auth.sales_contract_auth", None)
    importlib.import_module("ma.plugins.auth.sales_contract_auth")
    yield new_reg


@pytest.mark.asyncio
async def test_sales_contract_passes_with_owner_role_and_contract() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "sales_contract_auth",
        {"required_role": "SALES_OWNER"},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "SALES_OWNER", "contract_ids": ["C001", "C002"]},
        ),
        "biz_params": {"contract_id": "C001"},
    }
    result = await plugin.authorize(state)
    assert result.passed is True
    assert result.user_ctx["contract_id"] == "C001"


@pytest.mark.asyncio
async def test_sales_contract_rejects_wrong_role() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "sales_contract_auth",
        {"required_role": "SALES_OWNER"},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "REP", "contract_ids": ["C001"]},
        ),
        "biz_params": {"contract_id": "C001"},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_TOPIC"


@pytest.mark.asyncio
async def test_sales_contract_rejects_contract_not_owned() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "sales_contract_auth",
        {"required_role": "SALES_OWNER"},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "SALES_OWNER", "contract_ids": ["C001"]},
        ),
        "biz_params": {"contract_id": "C999"},  # not in claims
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_SCOPE"
```

- [ ] **Step 2: 实现 `src/ma/plugins/auth/sales_contract_auth.py`**

```python
"""销售合同责任人鉴权插件（M2）。

两层校验：
1. role == required_role（SALES_OWNER）
2. biz_params.contract_id 必须在 identity.raw_claims.contract_ids 列表内
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import AuthResult
from ma.core.plugin.registry import registry


class SalesContractAuthParams(BaseModel):
    required_role: str


@registry.register
class SalesContractAuth:
    name = "sales_contract_auth"
    plugin_kind = "auth"

    def configure(self, params: dict[str, Any]) -> None:
        p = SalesContractAuthParams(**params)
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

        biz_params = state.get("biz_params", {})
        contract_id = biz_params.get("contract_id")
        owned_contracts = claims.get("contract_ids", [])
        if contract_id is None:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message="请求缺少 contract_id 参数。",
            )
        if contract_id not in owned_contracts:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message=f"您不是 {contract_id} 合同的责任人。",
            )

        return AuthResult(
            passed=True,
            user_ctx={
                "role": role,
                "contract_ids": owned_contracts,
                "contract_id": contract_id,
            },
        )
```

- [ ] **Step 3: 更新 bootstrap.py**

```python
def load_all_plugins() -> None:
    """启动期调一次。"""
    import ma.plugins.adapter.metagc  # noqa: F401
    import ma.plugins.auth.jingying_auth  # noqa: F401
    import ma.plugins.auth.representative_auth  # noqa: F401
    import ma.plugins.auth.sales_contract_auth  # noqa: F401
    import ma.plugins.enrich.generic_enrich  # noqa: F401
    import ma.plugins.intent.llm_classifier  # noqa: F401
```

- [ ] **Step 4: 跑测试 + commit**

```bash
python -m uv run pytest tests/unit/test_sales_contract_auth.py -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 3 sales_contract tests pass；full suite 107 passed + 12 skipped。

```bash
git add src/ma/plugins/auth/sales_contract_auth.py \
        src/ma/core/plugin/bootstrap.py \
        tests/unit/test_sales_contract_auth.py
git commit -m "feat(auth): sales_contract_auth with role + contract_id ownership checks"
```

**Phase B 完成标志：** 3 个 commit；三个 AuthPlugin 完整；9 个 plugin 测试新增/扩展全过；full suite 107 + 12。

---

## Phase C: 第二、三个 Topic YAML

### Task 7: 经营小助手 Topic YAML + intent prompt

**Files:**
- Create: `config/topics/jingying_xiaozhushou.yaml`
- Create: `prompts/intent/jingying.txt`

- [ ] **Step 1: 创建 `prompts/intent/jingying.txt`**

```
你是路由分类器。当前用户问题来自"经营小助手"专题。
根据问题内容，选择最匹配的下游处理路径。

可选 label:
- metagc: 业务数据查询、销售额、客户分布、业务单元绩效

输出格式：
route: <label>
reason: <一行简短理由>
```

- [ ] **Step 2: 创建 `config/topics/jingying_xiaozhushou.yaml`**

```yaml
topic_id: jingying_xiaozhushou
display_name: 经营小助手
default_route: metagc

history:
  max_turns: 10
  scope: per_topic

auth:
  plugin: jingying_auth
  params:
    allowed_roles: [MANAGER, REP]

enrich:
  plugin: generic_enrich
  params:
    fields_to_inject: [business_unit]

intent:
  plugin: llm_classifier
  params:
    provider: fake
    model: fake-model
    labels: [metagc]
    prompt_template: prompts/intent/jingying.txt
    fake_responses:
      - "route: metagc\nreason: 经营数据查询"

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
  required: [business_unit]
  properties:
    business_unit:
      type: string

error_messages:
  FORBIDDEN_TOPIC: "您暂无权限使用经营小助手。"
  FORBIDDEN_SCOPE: "您当前的角色无法查询该业务单元数据。"
  DOWNSTREAM_TIMEOUT: "服务繁忙，请稍后再试。"
  DOWNSTREAM_ERROR: "查询遇到问题，请稍后再试或联系管理员。"
  INTERNAL: "服务异常，请稍后再试。"
```

- [ ] **Step 3: 启动 smoke test**

```bash
MA_METAGC_BASE_URL=http://example.test python -m uv run python -c "
import os
from pathlib import Path
import yaml
from ma.core.plugin.bootstrap import load_all_plugins
from ma.core.topic.config import TopicConfig
from ma.core.topic.compiler import TopicCompiler

load_all_plugins()
data = yaml.safe_load(Path('config/topics/jingying_xiaozhushou.yaml').read_text(encoding='utf-8'))
cfg = TopicConfig.model_validate(data)
compiled = TopicCompiler().compile(cfg, env=dict(os.environ))
print('compiled:', compiled.topic_id, 'auth_kind:', type(compiled.auth).__name__)
"
```

Expected: `compiled: jingying_xiaozhushou auth_kind: JingyingAuth`.

- [ ] **Step 4: 跑测试 + commit**

```bash
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 107 + 12 unchanged。

```bash
git add config/topics/jingying_xiaozhushou.yaml prompts/intent/jingying.txt
git commit -m "feat(topic): jingying_xiaozhushou YAML + intent prompt"
```

### Task 8: 销售合同责任人 Topic YAML + intent prompt

**Files:**
- Create: `config/topics/sales_contract_agent.yaml`
- Create: `prompts/intent/sales_contract.txt`

- [ ] **Step 1: 创建 `prompts/intent/sales_contract.txt`**

```
你是路由分类器。当前用户问题来自"销售合同责任人"专题。
根据问题内容，选择最匹配的下游处理路径。

可选 label:
- metagc: 合同条款查询、客户履约、付款进度

输出格式：
route: <label>
reason: <一行简短理由>
```

- [ ] **Step 2: 创建 `config/topics/sales_contract_agent.yaml`**

```yaml
topic_id: sales_contract_agent
display_name: 销售合同责任人 Agent
default_route: metagc

history:
  max_turns: 10
  scope: per_topic

auth:
  plugin: sales_contract_auth
  params:
    required_role: SALES_OWNER

enrich:
  plugin: generic_enrich
  params:
    fields_to_inject: [contract_id]

intent:
  plugin: llm_classifier
  params:
    provider: fake
    model: fake-model
    labels: [metagc]
    prompt_template: prompts/intent/sales_contract.txt
    fake_responses:
      - "route: metagc\nreason: 合同信息查询"

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
  required: [contract_id]
  properties:
    contract_id:
      type: string

error_messages:
  FORBIDDEN_TOPIC: "您暂无权限使用销售合同责任人 Agent。"
  FORBIDDEN_SCOPE: "您不是该合同的责任人。"
  DOWNSTREAM_TIMEOUT: "服务繁忙，请稍后再试。"
  DOWNSTREAM_ERROR: "查询遇到问题，请稍后再试或联系管理员。"
  INTERNAL: "服务异常，请稍后再试。"
```

- [ ] **Step 3: Smoke + commit**

```bash
MA_METAGC_BASE_URL=http://example.test python -m uv run python -c "
import os
from pathlib import Path
import yaml
from ma.core.plugin.bootstrap import load_all_plugins
from ma.core.topic.config import TopicConfig
from ma.core.topic.compiler import TopicCompiler

load_all_plugins()
data = yaml.safe_load(Path('config/topics/sales_contract_agent.yaml').read_text(encoding='utf-8'))
cfg = TopicConfig.model_validate(data)
compiled = TopicCompiler().compile(cfg, env=dict(os.environ))
print('compiled:', compiled.topic_id, 'auth_kind:', type(compiled.auth).__name__)
"
```

Expected: `compiled: sales_contract_agent auth_kind: SalesContractAuth`.

```bash
python -m uv run pytest -v
git add config/topics/sales_contract_agent.yaml prompts/intent/sales_contract.txt
git commit -m "feat(topic): sales_contract_agent YAML + intent prompt"
```

**Phase C 完成标志：** 2 个 commit；三个专题 YAML 都能编译。

---

## Phase D: REST endpoints + 越权防御

### Task 9: `/sessions` + `/sessions/{id}/messages` + 越权防御

**Files:**
- Create: `src/ma/api/rest.py`
- Modify: `src/ma/main.py` — 注册 rest router；attach repos to app.state
- Modify: `src/ma/core/chat_service.py` 或新加 `src/ma/core/history_service.py` — 把 repo factory 暴露给 REST

设计书 §4.4 规定：
- `GET /api/v1/sessions` — 列出用户的会话；w3 从 `X-User-Account` header 取（不从 query），可选 `biz_id` 过滤
- `GET /api/v1/sessions/{thread_id}/messages` — 列出某会话消息；越权防御：thread_id 的 w3_account 必须 == identity.w3_account

我选用最小侵入做法：让 `main.py` 在 `app.state` 上挂 `session_repo_factory` + `message_repo_factory`，REST handler 从那里拿。

- [ ] **Step 1: 改 `src/ma/main.py`**

在 `create_app` 的 lifespan 里，加这一句（在 ChatService 装配之后、mark_ready 之前）：

```python
        # M2: 把 factory 也挂到 app.state，供 REST history 端点使用
        if app.state.pg_pool_rw is not None:
            app.state.session_repo_factory = sess_factory
            app.state.message_repo_factory = msg_factory
        else:
            app.state.session_repo_factory = None
            app.state.message_repo_factory = None
```

并在文件顶部 import：

```python
from ma.api import health, rest, sse
```

并在路由注册处加：

```python
    app.include_router(rest.router, prefix="/api/v1")
```

- [ ] **Step 2: 创建 `src/ma/api/rest.py`**

```python
"""REST endpoints：历史查询。

设计书 §4.4：
- GET /sessions —— 列出用户会话，w3_account 从 X-User-Account header 取
- GET /sessions/{thread_id}/messages —— 列出某会话消息，含越权防御
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

from ma.core.repo.models import Message, Session
from ma.infra.logging import bind_request_ctx, get_logger

router = APIRouter()
log = get_logger("ma.api.rest")


def _require_w3_account(x_user_account: str | None) -> str:
    if not x_user_account:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-User-Account header required",
        )
    return x_user_account


def _serialize_session(s: Session) -> dict[str, Any]:
    return {
        "thread_id": s.thread_id,
        "biz_id": s.biz_id,
        "w3_account": s.w3_account,
        "title": s.title,
        "status": s.status,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
        "last_message_at": s.last_message_at.isoformat() if s.last_message_at else None,
    }


def _serialize_message(m: Message) -> dict[str, Any]:
    return {
        "message_id": m.message_id,
        "thread_id": m.thread_id,
        "seq": m.seq,
        "role": m.role,
        "content": m.content,
        "status": m.status,
        "route": m.route,
        "created_at": m.created_at.isoformat() if m.created_at else None,
    }


@router.get("/sessions")
async def list_sessions(
    request: Request,
    biz_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    before: str | None = Query(default=None),
    x_user_account: str | None = Header(default=None),
) -> dict[str, Any]:
    """列出用户的会话。

    w3_account 强制从 header 取（设计书 §4.4：避免越权）。
    """
    w3 = _require_w3_account(x_user_account)
    bind_request_ctx(w3_account=w3)
    log.info("list_sessions", biz_id=biz_id, limit=limit)

    factory = request.app.state.session_repo_factory
    if factory is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DB not configured",
        )

    before_dt: datetime | None = None
    if before:
        try:
            before_dt = datetime.fromisoformat(before)
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"invalid 'before' (must be ISO 8601): {e}",
            ) from e

    async with factory.acquire() as conn:
        repo = factory.build(conn)
        items = await repo.list_by_user(
            w3_account=w3,
            biz_id=biz_id,
            limit=limit,
            before=before_dt,
        )
    return {"items": [_serialize_session(s) for s in items]}


@router.get("/sessions/{thread_id}/messages")
async def list_messages(
    request: Request,
    thread_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    x_user_account: str | None = Header(default=None),
) -> dict[str, Any]:
    """列出某会话的消息。

    越权防御：thread_id 必须属于当前 w3_account，否则 404（不 403 —— 不泄漏存在性）。
    """
    w3 = _require_w3_account(x_user_account)
    bind_request_ctx(w3_account=w3, thread_id=thread_id)
    log.info("list_messages", thread_id=thread_id, limit=limit)

    sess_factory = request.app.state.session_repo_factory
    msg_factory = request.app.state.message_repo_factory
    if sess_factory is None or msg_factory is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DB not configured",
        )

    async with sess_factory.acquire() as conn:
        sess_repo = sess_factory.build(conn)
        # 拿 session 验证归属
        sessions = await sess_repo.list_by_user(
            w3_account=w3, biz_id=None, limit=200, before=None
        )
        owned = next((s for s in sessions if s.thread_id == thread_id), None)
        if owned is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown thread_id: {thread_id}",
            )

        msg_repo = msg_factory.build(conn)
        items = await msg_repo.list_recent(thread_id=thread_id, limit=limit)

    # list_recent 返回 DESC；REST 也按 DESC 给（最新在前），前端可自行 reverse
    return {
        "thread_id": thread_id,
        "items": [_serialize_message(m) for m in items],
    }
```

> **越权防御策略**：拿 sessions list 验证归属 —— 简单但不高效（N 次会话场景下查 200 条筛一条）。生产可加 `repo.get(thread_id)` 方法直接查并比 w3。M2 这里简化；M3 起优化。

- [ ] **Step 3: 跑 lint + 起服务做一次手动 smoke**

```bash
python -m uv run mypy src && python -m uv run ruff check .
```

启动 server smoke test 先跳过（要 DB；放到 Task 10 integration test 里做）。

- [ ] **Step 4: Commit**

```bash
git add src/ma/api/rest.py src/ma/main.py
git commit -m "feat(api): REST history endpoints with header-based w3 + ownership check"
```

### Task 10: REST integration tests

**Files:**
- Create: `tests/integration/test_rest_history.py`

- [ ] **Step 1: 写测试**

```python
"""REST 历史端点 integration：复用 pg_dsn 容器 + TestClient 启 MA app。"""
from __future__ import annotations

import asyncpg
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app_with_pg(monkeypatch: pytest.MonkeyPatch, pg_dsn: str, tmp_path):
    """启 app 接到真 OpenGauss 容器；topics 目录为空，只测历史端点。"""
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    topics_dir = tmp_path / "topics"
    topics_dir.mkdir()
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", str(topics_dir))
    from ma.main import create_app
    with TestClient(create_app()) as c:
        yield c


async def _seed_session_and_messages(pg_dsn: str, *, thread_id: str, w3: str, biz_id: str) -> None:
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO ma_session (thread_id, biz_id, w3_account) "
                "VALUES ($1, $2, $3) "
                "ON CONFLICT (thread_id) DO NOTHING",
                thread_id, biz_id, w3,
            )
            await conn.execute(
                "UPDATE ma_session SET last_message_at = CURRENT_TIMESTAMP WHERE thread_id = $1",
                thread_id,
            )
            for i, (role, content) in enumerate(
                [("user", "q1"), ("assistant", "a1")], start=1
            ):
                await conn.execute(
                    "INSERT INTO ma_message (message_id, thread_id, seq, role, content) "
                    "VALUES ($1, $2, $3, $4, $5)",
                    f"msg_{thread_id}_{i}", thread_id, i, role, content,
                )
    finally:
        await pool.close()


def test_list_sessions_requires_w3_header(app_with_pg: TestClient) -> None:
    r = app_with_pg.get("/api/v1/sessions")
    assert r.status_code == 401


def test_list_sessions_returns_user_sessions(app_with_pg: TestClient, pg_dsn: str) -> None:
    import asyncio
    asyncio.run(_seed_session_and_messages(pg_dsn, thread_id="th_a", w3="alice", biz_id="biz_x"))
    asyncio.run(_seed_session_and_messages(pg_dsn, thread_id="th_b", w3="bob", biz_id="biz_x"))

    r = app_with_pg.get(
        "/api/v1/sessions",
        headers={"X-User-Account": "alice"},
    )
    assert r.status_code == 200
    items = r.json()["items"]
    thread_ids = [s["thread_id"] for s in items]
    assert "th_a" in thread_ids
    assert "th_b" not in thread_ids  # 不能看到 bob 的


def test_list_messages_owned_thread(app_with_pg: TestClient, pg_dsn: str) -> None:
    import asyncio
    asyncio.run(_seed_session_and_messages(pg_dsn, thread_id="th_msgs", w3="alice", biz_id="biz_x"))

    r = app_with_pg.get(
        "/api/v1/sessions/th_msgs/messages",
        headers={"X-User-Account": "alice"},
    )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 2
    roles = {m["role"] for m in items}
    assert roles == {"user", "assistant"}


def test_list_messages_others_thread_returns_404(
    app_with_pg: TestClient, pg_dsn: str
) -> None:
    """越权防御：bob 不能看 alice 的 thread。"""
    import asyncio
    asyncio.run(_seed_session_and_messages(pg_dsn, thread_id="th_secret", w3="alice", biz_id="biz_x"))

    r = app_with_pg.get(
        "/api/v1/sessions/th_secret/messages",
        headers={"X-User-Account": "bob"},
    )
    assert r.status_code == 404
```

- [ ] **Step 2: 跑测试**

```bash
python -m uv run pytest tests/integration/test_rest_history.py -v
```

Expected：本机无 docker 时 4 个 skip；CI 有 docker 时 4 passed。

```bash
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: full suite 107 passed + 16 skipped（12 + 4 new docker-gated）。

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_rest_history.py
git commit -m "test(rest): integration tests for /sessions and /sessions/{id}/messages"
```

**Phase D 完成标志：** 2 个 commit；REST endpoints 全实现 + 越权防御；4 integration tests 全过（CI 上）。

---

## Phase E: e2e 三专题 + 追问

### Task 11: e2e 经营小助手

**Files:**
- Create: `tests/e2e/test_jingying_xiaozhushou_sse.py`

类似 M1.J.24 的 `test_daibiao_xiaoguanjia_sse.py`，但 biz_id / biz_params / role / claims 适配经营小助手。

- [ ] **Step 1: 写测试**

```python
"""e2e: 经营小助手 + MetaGC mock 全链路。"""
from __future__ import annotations

import json

import asyncpg
import pytest
from fastapi.testclient import TestClient


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
async def test_jingying_xiaozhushou_full_loop(
    pg_dsn: str, metagc_server: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    monkeypatch.setenv("MA_METAGC_BASE_URL", metagc_server)
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", "config/topics")

    from ma.main import create_app

    with TestClient(create_app()) as client:
        body = {
            "biz_id": "jingying_xiaozhushou",
            "thread_id": "th_jy_e2e",
            "w3_account": "manager_alice",
            "question": "bu_a 上月业绩怎样？",
            "biz_params": {"business_unit": "bu_a"},
        }
        # X-User-Role + 自定义 claims via header 较难表达；M2 这里假定 sse.py
        # 把 role 从 header 取，但 business_units claim 也得给。
        # M2 简化：sse endpoint 已经接受 X-User-Role；但 business_units claim
        # 没法从 header 单独传。临时方案：扩 sse.py 接受 X-User-Claims 为 JSON。
        # 见 Task 11.A 备注。
        headers = {
            "X-Request-Id": "req_jy_e2e",
            "X-User-Account": "manager_alice",
            "X-User-Role": "MANAGER",
            "X-User-Claims": json.dumps({"business_units": ["bu_a"]}),
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
    deltas = [e for e in events if e["event"] == "delta"]
    assert len(deltas) >= 2

    # DB 验证：用户消息 + 助手消息都落库
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT role, status FROM ma_message WHERE thread_id = $1 ORDER BY seq",
                "th_jy_e2e",
            )
    finally:
        await pool.close()
    assert [r["role"] for r in rows] == ["user", "assistant"]
    assert rows[1]["status"] == "complete"
```

#### Task 11.A 备注 — `sse.py` 需扩展 X-User-Claims

测试假定 `sse.py` 接受 `X-User-Claims` header（JSON 字符串）作为额外 claims。这要求修 `src/ma/api/sse.py`，把 claims 字典里加 header 解析的内容。

修改 sse.py 内的 identity 构造：

```python
import json as _json

# ... in chat_sse handler:
role = x_user_role or "REP"
extra_claims: dict[str, Any] = {}
x_user_claims = request.headers.get("X-User-Claims")
if x_user_claims:
    try:
        extra_claims = _json.loads(x_user_claims)
        if not isinstance(extra_claims, dict):
            raise ValueError("X-User-Claims must be JSON object")
    except (ValueError, TypeError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid X-User-Claims: {e}",
        ) from e
identity = AuthnIdentity(
    w3_account=body.w3_account,
    raw_claims={"role": role, **extra_claims},
)
```

同时同步更新 M1.J.24 的 `test_daibiao_xiaoguanjia_sse.py` —— 它当时假定 role 就够；M2 加了 regions claim 后需要传 `X-User-Claims='{"regions":["huadong"]}'`。

- [ ] **Step 2: 改 `src/ma/api/sse.py`**

按上面备注的代码改 identity 构造。

- [ ] **Step 3: 改 `tests/e2e/test_daibiao_xiaoguanjia_sse.py`**

```python
        headers = {
            "X-Request-Id": "req_e2e",
            "X-User-Role": "REP",
            "X-User-Account": "alice",
            "X-User-Claims": json.dumps({"regions": ["huadong"]}),
        }
```

并加 `import json` 顶部。

- [ ] **Step 4: 跑测试 + commit**

```bash
python -m uv run pytest tests/e2e/ -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: 本机 2 e2e skip (docker 不可)；CI 上 passed。

```bash
git add src/ma/api/sse.py \
        tests/e2e/test_jingying_xiaozhushou_sse.py \
        tests/e2e/test_daibiao_xiaoguanjia_sse.py
git commit -m "feat(api): support X-User-Claims header; e2e for jingying_xiaozhushou"
```

### Task 12: e2e 销售合同责任人

**Files:**
- Create: `tests/e2e/test_sales_contract_sse.py`

- [ ] **Step 1: 写测试**

```python
"""e2e: 销售合同责任人 Agent + MetaGC mock 全链路。"""
from __future__ import annotations

import json

import asyncpg
import pytest
from fastapi.testclient import TestClient


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
async def test_sales_contract_full_loop(
    pg_dsn: str, metagc_server: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    monkeypatch.setenv("MA_METAGC_BASE_URL", metagc_server)
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", "config/topics")

    from ma.main import create_app

    with TestClient(create_app()) as client:
        body = {
            "biz_id": "sales_contract_agent",
            "thread_id": "th_sc_e2e",
            "w3_account": "owner_alice",
            "question": "合同 C001 付款进度？",
            "biz_params": {"contract_id": "C001"},
        }
        headers = {
            "X-Request-Id": "req_sc_e2e",
            "X-User-Account": "owner_alice",
            "X-User-Role": "SALES_OWNER",
            "X-User-Claims": json.dumps({"contract_ids": ["C001", "C002"]}),
        }
        with client.stream(
            "POST", "/api/v1/chat/sse", json=body, headers=headers
        ) as r:
            assert r.status_code == 200
            text = "".join(r.iter_text())

    events = _parse_sse(text)
    assert events[0]["event"] == "meta"
    assert events[-1]["event"] == "done"
    deltas = [e for e in events if e["event"] == "delta"]
    assert len(deltas) >= 2

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT role, status FROM ma_message WHERE thread_id = $1 ORDER BY seq",
                "th_sc_e2e",
            )
    finally:
        await pool.close()
    assert [r["role"] for r in rows] == ["user", "assistant"]
```

- [ ] **Step 2: 跑测试 + commit**

```bash
python -m uv run pytest tests/e2e/ -v
git add tests/e2e/test_sales_contract_sse.py
git commit -m "test(e2e): sales_contract_agent full loop"
```

### Task 13: e2e 追问场景

**Files:**
- Create: `tests/e2e/test_followup_question.py`

验证：同一 thread_id 连发两次问答，第二次能看到第一次的历史。

- [ ] **Step 1: 写测试**

```python
"""e2e 追问：同一 thread 第二次问答历史会被加载到 GraphState。

验证方式：用 MetaGCAdapter 真的 mock；mock server 在请求 body 里看到 history
（实际上 M1 的 MetaGCAdapter 没把 history 传给下游，只传 question + biz_params）。

M2 简化验证：第二次问答完成后，DB 里有 4 条 message（2 轮 user/assistant）。
真正的 history 拼接到 LLM context 的验证留到 M3（接真 LLM 后能看 prompt）。
"""
from __future__ import annotations

import json

import asyncpg
import pytest
from fastapi.testclient import TestClient


def _read_stream(client: TestClient, body: dict, headers: dict) -> str:
    with client.stream("POST", "/api/v1/chat/sse", json=body, headers=headers) as r:
        assert r.status_code == 200
        return "".join(r.iter_text())


@pytest.mark.asyncio
async def test_followup_persists_two_turns(
    pg_dsn: str, metagc_server: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    monkeypatch.setenv("MA_METAGC_BASE_URL", metagc_server)
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", "config/topics")

    from ma.main import create_app

    with TestClient(create_app()) as client:
        thread_id = "th_followup"
        common_headers = {
            "X-User-Account": "alice",
            "X-User-Role": "REP",
            "X-User-Claims": json.dumps({"regions": ["huadong"]}),
        }
        # 第一轮
        _read_stream(
            client,
            {
                "biz_id": "daibiao_xiaoguanjia",
                "thread_id": thread_id,
                "w3_account": "alice",
                "question": "第一个问题",
                "biz_params": {"region": "huadong"},
            },
            {"X-Request-Id": "req_fu_1", **common_headers},
        )
        # 第二轮
        _read_stream(
            client,
            {
                "biz_id": "daibiao_xiaoguanjia",
                "thread_id": thread_id,
                "w3_account": "alice",
                "question": "第二个问题（追问）",
                "biz_params": {"region": "huadong"},
            },
            {"X-Request-Id": "req_fu_2", **common_headers},
        )

    # DB 验证：4 条消息（u/a/u/a）
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT seq, role, content FROM ma_message "
                "WHERE thread_id = $1 ORDER BY seq",
                thread_id,
            )
    finally:
        await pool.close()
    assert len(rows) == 4
    assert [r["role"] for r in rows] == ["user", "assistant", "user", "assistant"]
    assert rows[0]["content"] == "第一个问题"
    assert rows[2]["content"] == "第二个问题（追问）"
```

- [ ] **Step 2: 跑测试 + commit**

```bash
python -m uv run pytest tests/e2e/ -v
python -m uv run pytest -v
python -m uv run mypy src && python -m uv run ruff check .
```

Expected: full suite (107 + 12) + 3 new e2e tests = 107 passed + 15 skipped（12 + 3 e2e docker-gated）。

```bash
git add tests/e2e/test_followup_question.py
git commit -m "test(e2e): followup question persists two complete turns"
```

**Phase E 完成标志：** 3 个 commit；两个新 e2e + 一个追问 e2e；总 e2e 4 个（M1 1 + M2 3）。

---

## Phase F: 出口验证 + 合并 + tag

### Task 14: M2 exit + merge + tag

- [ ] **Step 1: 全套 sweep**

```bash
python -m uv run pytest -v
python -m uv run pytest --cov=ma --cov-report=term-missing
python -m uv run mypy src
python -m uv run ruff check . && python -m uv run ruff format --check .
```

Expected:
- 107 passed + 15 skipped（M1 12 + M2 3 e2e + M2 4 rest integration = 17，但实际只新增 docker-gated 7 个，所以总 skipped = 19? 算下：M1 末态 12 skipped；M2 加了 4 rest integration + 3 e2e = 7 新；总 = 19。**实际数字依测试本身的 docker 可用判断**。CI 上 docker 可用时这些跑通；本机仍 skip）
- 覆盖率 ≥ 80%
- 全 lint/type clean

- [ ] **Step 2: 写 M2 完成笔记** `docs/superpowers/plans/2026-06-24-master-agent-m2-three-topics.notes.md`

```markdown
# M2 完成笔记

- 日期：<完成日期>
- 分支：feature/m2-three-topics（合入 design/master-agent-v1，tag m2-complete）
- commit 数：~14
- 覆盖率：<实际>

## 与计划相比的偏差

<由实施者填写>

## 给 M3 的提示

1. **OpenInference 接入**：M3 接真 LLM 时，`langchain-core` 已在依赖里，加 `openinference-instrumentation-langchain` + 在 `tracing.py` 加 instrumentor 调用即可
2. **节点级 retry / timeout**：M3 必须实现（Contract 8 当前 skipped）。设计书 §6.6.1
3. **UniiocAdapter / KnowledgeCenterAdapter**：参考 MetaGCAdapter 实现；KC 是 WebSocket，需要 `websockets` 依赖
4. **多步编排**：YAML schema 已支持多 output 节点，M3 起可以试 "MetaGC → Uniioc 链式" 场景
5. **越权防御优化**：`list_messages` 当前用 `list_by_user` + filter，N+1 风险；M3 加 `repo.get_by_thread(thread_id) -> Session | None` 直查

## 验收清单

- [x] 三专题端到端跑通（curl SSE 三种 biz_id）
- [x] 追问场景验证（4 条消息按 u/a/u/a 落库）
- [x] REST `/sessions` 和 `/sessions/{id}/messages` 含越权防御
- [x] Contract 9（鉴权失败话术下发）真生效
- [x] mypy / ruff / pytest 全绿
- [x] 覆盖率达标
```

- [ ] **Step 3: 合并 + tag**

```bash
git add docs/superpowers/plans/2026-06-24-master-agent-m2-three-topics.notes.md
git commit -m "docs: M2 completion notes"

git checkout design/master-agent-v1
git merge --no-ff feature/m2-three-topics -m "merge: M2 three topics + REST history + followup"
git tag m2-complete
git log --oneline --graph -8
```

**Phase F 完成标志：M2 全套绿；m2-complete tag 落地；完成笔记记录偏差与给 M3 的提示。**

---

# M2 总览

| Phase | Task # | 描述 | 预估 commits |
|---|---|---|---|
| A | 1-3 | M1 day-1 cleanup (reject delta / biz mismatch / 整轮丢) | 3 |
| B | 4-6 | 三套 AuthPlugin | 3 |
| C | 7-8 | 两个新 Topic YAML | 2 |
| D | 9-10 | REST endpoints + integration | 2 |
| E | 11-13 | 三 e2e（含追问） | 3 |
| F | 14 | 出口 + 合并 + tag | 1 |
| **Total** | | | **~14** |

预估时间：2~3 名后端工程师，**~2 周**（与设计书 §9.3 M2 估算一致）。






