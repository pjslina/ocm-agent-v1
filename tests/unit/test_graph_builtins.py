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
            thread_id="th_1",
            biz_id="b",
            w3_account="a",
            title=None,
            status="active",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_message_at=None,
            ext={},
        )
    )
    fake_msg_repo = MagicMock()
    fake_msg_repo.list_recent = AsyncMock(
        return_value=[
            Message(message_id="m2", thread_id="th_1", seq=2, role="assistant", content="a1"),
            Message(message_id="m1", thread_id="th_1", seq=1, role="user", content="q1"),
        ]
    )

    node = make_load_session_node(
        session_repo=fake_session_repo, message_repo=fake_msg_repo, max_turns=5
    )
    state = {
        "thread_id": "th_1",
        "biz_id": "b",
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
    """reject 节点把 reject_message 写到 answer_chunks 并 dispatch delta。"""
    from unittest.mock import patch

    from ma.core.graph.builtins import make_reject_node

    error_messages = {"FORBIDDEN_TOPIC": "您暂无权限使用此专题", "INTERNAL": "服务异常"}
    node = make_reject_node(error_messages)

    auth_result = MagicMock()
    auth_result.reject_code = "FORBIDDEN_TOPIC"
    auth_result.reject_message = None  # 让 node 用 error_messages 兜底

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
    assert ("delta", {"content": "您暂无权限使用此专题"}) in captured


@pytest.mark.asyncio
async def test_reject_uses_plugin_provided_message_first() -> None:
    """AuthPlugin 给的 reject_message 优先；error_messages 是兜底。dispatch 内容应当是插件话术。"""
    from unittest.mock import patch

    from ma.core.graph.builtins import make_reject_node

    error_messages = {"FORBIDDEN_TOPIC": "兜底话术"}
    node = make_reject_node(error_messages)

    auth_result = MagicMock()
    auth_result.reject_code = "FORBIDDEN_TOPIC"
    auth_result.reject_message = "插件自定义话术"
    state = {"auth": auth_result}
    captured: list[tuple[str, dict]] = []

    async def fake_dispatch(name: str, data: dict, *, config: dict) -> None:
        captured.append((name, data))

    with patch(
        "langchain_core.callbacks.manager.adispatch_custom_event",
        side_effect=fake_dispatch,
    ):
        out = await node(state, config={})

    assert out["answer_chunks"] == ["插件自定义话术"]
    assert ("delta", {"content": "插件自定义话术"}) in captured


@pytest.mark.asyncio
async def test_persist_node_appends_assistant_message() -> None:
    from ma.core.graph.builtins import make_persist_node

    fake_msg_repo = MagicMock()
    fake_msg_repo.append = AsyncMock()
    fake_session_repo = MagicMock()
    fake_session_repo.touch = AsyncMock()

    node = make_persist_node(session_repo=fake_session_repo, message_repo=fake_msg_repo)
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
    node = make_persist_node(session_repo=fake_session_repo, message_repo=fake_msg_repo)
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
