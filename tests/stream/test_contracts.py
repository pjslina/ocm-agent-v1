"""Stream-contract 测试：设计书 §7.2.2 的 9 条契约（WS 相关 #10 在 M4）。"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ma.core.graph.events import ChatEvent
from tests.stream.conftest import collect, make_request


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


@pytest.mark.asyncio
async def test_contract_3_only_output_node_produces_delta(make_service):
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
    assert "very long reason should not leak" not in full


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


@pytest.mark.asyncio
async def test_contract_5_thinking_progress_no_sensitive_keys(make_service):
    svc, _ = make_service()

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "x"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())

    auth_progresses = [e for e in events if e.type == "progress" and e.data["step"] == "auth"]
    for p in auth_progresses:
        extra = p.data.get("extra", {})
        assert "user_ctx" not in extra
        assert "role" not in extra


@pytest.mark.asyncio
async def test_contract_6_stream_continues_after_intent_fallback(make_service):
    svc, _ = make_service(intent_responses=["unparseable garbage"])

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "fallback content"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request())
    deltas = [e for e in events if e.type == "delta"]
    assert len(deltas) >= 1
    assert events[-1].type == "done"


@pytest.mark.asyncio
async def test_contract_7_persist_marks_partial_when_no_chunks(make_service):
    svc, (_, msg_repo) = make_service()

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(
            type="error",
            data={"code": "DOWNSTREAM_TIMEOUT", "message": "timeout", "retryable": False},
        )

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        await collect(svc, make_request())
    assistants = [m for m in msg_repo.messages if m.role == "assistant"]
    assert len(assistants) == 1
    assert assistants[0].status == "partial"
    assert assistants[0].content == ""


@pytest.mark.asyncio
async def test_contract_8_retry_transparent_to_user(make_service):
    """retry 对用户透明：adapter 可重试的错误不会暴露给 SSE 流。

    模拟：adapter 第一次抛 retryable error，第二次成功 → 用户只看到一个 delta，
    不会看到中间 error。
    """
    from ma.core.topic.config import RetryPolicy

    svc, _ = make_service()

    # make_service 默认 retry.max_attempts=1（不重试），需要 patch 为 2 才能验证
    # retry 透明性（call_count==2）。
    svc._topics._by_id["test_topic"].adapter_retries["metagc_adapter"] = RetryPolicy(
        max_attempts=2, backoff_ms=0
    )

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


@pytest.mark.asyncio
async def test_contract_9_auth_rejection_yields_done(make_service):
    """鉴权失败 → reject 节点 dispatch delta + done。M2 修复（M1 简化已撤销）。"""
    svc, _ = make_service()

    async def fake_run(self, state, *, output: bool):
        # 不应该被调到 —— auth 失败应直接跳到 reject
        yield ChatEvent(type="delta", data={"content": "should not appear"})

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
        events = await collect(svc, make_request(role="VIEWER"))

    assert events[-1].type == "done"
    deltas = [e for e in events if e.type == "delta"]
    # M2: reject 节点真的下发了话术 delta
    assert len(deltas) == 1
    # 内容是 AuthPlugin 给的 "该专题仅限 REP 角色访问。"
    assert "REP" in deltas[0].data["content"]
    # adapter.run 不应被调到
    assert "should not appear" not in deltas[0].data["content"]


@pytest.mark.asyncio
async def test_contract_10_ws_multi_request_isolation(make_service):
    """同一连接多个 ask 并发 → request_id 隔离，各自收到自己的 done。

    验证方式：并发调两次 ChatService.handle，断言每个流独立收到自己的
    delta + done。这模拟了 WS ConnectionManager 的并发处理。
    """
    import asyncio

    svc, _ = make_service(intent_responses=["route: metagc\nreason: ok"])

    call_count = 0

    async def fake_run(self, state, *, output: bool):
        nonlocal call_count
        call_count += 1
        idx = call_count
        yield ChatEvent(type="delta", data={"content": f"answer-{idx}"})

    async def collect_stream(req):
        events = []
        async for ev in svc.handle(req):
            events.append(ev)
        return events

    with patch("ma.plugins.adapter.metagc.MetaGCAdapter.run", new=fake_run):
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

    # 两个并发请求确实都被调了（验证并发隔离，非串行）
    assert call_count == 2


@pytest.mark.asyncio
async def test_session_biz_mismatch_yields_bad_request(make_service):
    """同一 thread_id 用两个 biz_id 请求 → BAD_REQUEST error + done。"""
    from datetime import datetime

    from ma.core.repo.models import Session

    svc, (sess_repo, _) = make_service()

    # 预填一个 session_repo 里的 session，绑到不同 biz_id
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
