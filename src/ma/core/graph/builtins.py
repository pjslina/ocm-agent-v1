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
        await session_repo.get_or_create(
            thread_id=state["thread_id"],
            biz_id=state["biz_id"],
            w3_account=identity.w3_account,
        )
        msgs = await message_repo.list_recent(thread_id=state["thread_id"], limit=max_turns * 2)
        # repo 返回 DESC，业务用正序
        msgs.reverse()
        return {"history": msgs}

    return node


def make_reject_node(error_messages: dict[str, str]) -> NodeFn:
    """build reject 节点。优先用 AuthPlugin 给的 reject_message，否则用 error_messages 兜底。"""

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        auth = state.get("auth")
        text: str | None = None
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
