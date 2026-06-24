"""节点封装层：把 Plugin 实例包装成 LangGraph 节点，并织入观测事件。

设计书 §5.3 / §6.4.2。每个 wrapper 做四件事：
1. 调插件方法
2. dispatch_custom_event 发 thinking / progress 给 ChatService 转 SSE
3. 把结果写回 GraphState
4. OTel span 由 _traced_node 装饰器统一开
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast

from langchain_core.callbacks.manager import adispatch_custom_event
from langchain_core.runnables import RunnableConfig
from opentelemetry import trace

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


async def _emit(name: str, data: dict[str, Any], config: dict[str, Any]) -> None:
    """LangChain 的 adispatch_custom_event 期望 RunnableConfig (TypedDict)。
    LangGraph 节点收到的 config 是同结构 dict，做一次 cast 即可。"""
    await adispatch_custom_event(name, data, config=cast(RunnableConfig, config))


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
        await _emit("thinking", {"phase": "auth", "label": "正在校验权限…"}, config)
        result = await plugin.authorize(state)
        await _emit(
            "progress",
            {
                "step": "auth",
                "status": "done",
                "extra": {"passed": result.passed},
            },
            config,
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
        await _emit("thinking", {"phase": "enrich"}, config)
        result = await plugin.enrich(state)
        await _emit(
            "progress",
            {"step": "enrich", "status": "done", "extra": result.enrich_meta},
            config,
        )
        return {
            "enriched_question": result.enriched_question,
            "enrich_meta": result.enrich_meta,
        }

    return _traced_node("enrich", node)


def make_intent_node(plugin: IntentPlugin, *, default_route: str, labels: list[str]) -> NodeFn:
    label_set = set(labels)

    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        await _emit(
            "thinking",
            {"phase": "intent", "label": "正在识别问题意图…"},
            config,
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
            await _emit(
                "progress",
                {
                    "step": "intent",
                    "status": "done",
                    "detail": reason,
                    "extra": {"route": route},
                },
                config,
            )
            return {
                "route": route,
                "route_confidence": conf,
                "route_reason": reason,
            }
        except IntentFailed as e:
            _log.warning("intent_failed", error=str(e), fallback=default_route)
            await _emit(
                "progress",
                {
                    "step": "intent",
                    "status": "fallback",
                    "detail": str(e),
                    "extra": {"route": default_route},
                },
                config,
            )
            return {
                "route": default_route,
                "route_confidence": 0.0,
                "route_reason": f"intent_failed: {e}",
            }

    return _traced_node("intent", node)


def make_adapter_node(adapter: DownstreamAdapter, *, output: bool) -> NodeFn:
    async def node(state: GraphState, config: dict[str, Any]) -> dict[str, Any]:
        chunks: list[str] = []
        async for ev in adapter.run(state, output=output):
            if ev.type == "delta":
                chunks.append(ev.data["content"])
                if output:
                    await _emit("delta", ev.data, config)
            elif ev.type == "tool":
                if output:
                    await _emit("tool", ev.data, config)
            elif ev.type == "error":
                await _emit("error", ev.data, config)
                break
        if output:
            return {"answer_chunks": chunks}
        return {"answer_chunks": []}

    return _traced_node("adapter", node)
