"""ChatService：M1 实现 —— 接 TopicRegistry + LangGraph 编译 + astream_events 转 ChatEvent。

设计书 §1.3 / §2.3 / §2.5 / §4.2.2。

接口与 M0 兼容：async def handle(req: ChatRequest) -> AsyncIterator[ChatEvent]
"""

from __future__ import annotations

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
from ma.core.repo.models import Message, SessionBizMismatchError
from ma.core.topic.registry import TopicNotFound, TopicRegistry
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
    """每次 handle() 调用编译一个 StateGraph 并跑它。

    构造时需要 factory 抽象（acquire() ctx manager + build(conn)）来支持
    单测用 fake repo 注入。生产用 _RepoFactory 包 asyncpg pool。
    """

    def __init__(
        self,
        *,
        topic_registry: TopicRegistry,
        session_repo_factory: Any,
        message_repo_factory: Any,
    ) -> None:
        self._topics = topic_registry
        self._session_repo_factory = session_repo_factory
        self._message_repo_factory = message_repo_factory

    async def handle(self, req: ChatRequest) -> AsyncIterator[ChatEvent]:
        identity = req.identity or AuthnIdentity(w3_account=req.w3_account)

        # 1) Topic lookup
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
            yield ChatEvent(type="done", data={"status": "failed"})
            return

        # 2) biz_params 校验（设计书 §4.6.2 fast-path）
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

        # 3) meta 事件
        yield ChatEvent(
            type="meta",
            data={
                "thread_id": req.thread_id,
                "request_id": req.request_id,
                "message_id": assistant_message_id,
                "topic": req.biz_id,
            },
        )

        # 4) 持久化 user message（先于 graph 跑）
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

        # 5) 编译 StateGraph 并跑
        graph = self._build_graph(topic)

        initial_state: GraphState = {
            "request_id": req.request_id,
            "biz_id": req.biz_id,
            "thread_id": req.thread_id,
            "identity": identity,
            "question": req.question,
            "biz_params": req.biz_params,
            "assistant_message_id": assistant_message_id,
        }

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
        except Exception:
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

    def _build_graph(self, topic: Any) -> Any:
        sg = StateGraph(GraphState)

        session_factory = self._session_repo_factory
        message_factory = self._message_repo_factory

        async def load_session_node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
            async with session_factory.acquire() as conn:
                node = make_load_session_node(
                    session_repo=session_factory.build(conn),
                    message_repo=message_factory.build(conn),
                    max_turns=topic.history.max_turns,
                )
                return await node(state, config)

        async def persist_node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
            async with session_factory.acquire() as conn:
                node = make_persist_node(
                    session_repo=session_factory.build(conn),
                    message_repo=message_factory.build(conn),
                )
                return await node(state, config)

        # 节点名不能与 GraphState 的 TypedDict 字段同名（langgraph 把 state key 与节点名共享命名空间），
        # 所以 auth/enrich/intent 节点统一加 `_node` 后缀。adapter 节点名由 topic YAML 自带，
        # 设计上不会与 state field 撞名（state field 不允许 mapping 出现）。
        # langgraph 的 add_node typing 没覆盖 (state, config) 2 参签名（运行时支持），
        # 暂用 type: ignore[arg-type] 跨过；node_wrappers 单测已经验证了运行时行为。
        sg.add_node("load_session", load_session_node)  # type: ignore[arg-type]
        sg.add_node("auth_node", make_auth_node(topic.auth))  # type: ignore[arg-type]
        sg.add_node("reject_node", make_reject_node(topic.error_messages))  # type: ignore[arg-type]
        sg.add_node("enrich_node", make_enrich_node(topic.enrich))  # type: ignore[arg-type]

        # intent labels 来自 mapping keys（TopicConfig 已校验 labels == mapping keys）
        cond_edges = [e for e in topic.config.graph.edges if e.type == "conditional"]
        mapping = cond_edges[0].mapping
        labels = list(mapping.keys())
        sg.add_node(
            "intent_node",
            make_intent_node(  # type: ignore[arg-type]
                topic.intent,
                default_route=topic.default_route,
                labels=labels,
                retry=topic.intent_retry,
            ),
        )

        # adapter 节点们
        for node_id, adapter in topic.adapters.items():
            sg.add_node(
                node_id,
                make_adapter_node(  # type: ignore[arg-type]
                    adapter, output=True, retry=topic.adapter_retries.get(node_id)
                ),
            )

        sg.add_node("persist_node", persist_node)  # type: ignore[arg-type]

        # 边
        sg.add_edge(START, "load_session")
        sg.add_edge("load_session", "auth_node")
        sg.add_conditional_edges(
            "auth_node",
            lambda s: "reject" if not s["auth"].passed else "enrich",
            {"reject": "reject_node", "enrich": "enrich_node"},
        )
        sg.add_edge("enrich_node", "intent_node")
        sg.add_conditional_edges(
            "intent_node",
            lambda s: s.get("route", topic.default_route),
            mapping,
        )
        for node_id in topic.adapters:
            sg.add_edge(node_id, "persist_node")
        sg.add_edge("reject_node", "persist_node")
        sg.add_edge("persist_node", END)

        return sg.compile()
