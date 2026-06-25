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
async def test_contract_8_retry_transparent_to_user_PLACEHOLDER():
    pytest.skip("retry not implemented until M3 (design book §6.6.1)")


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
async def test_contract_10_ws_multi_request_isolation_PLACEHOLDER():
    pytest.skip("WS channel deferred to M4")


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
