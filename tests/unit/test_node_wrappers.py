"""节点封装层：调插件 + 发 thinking/progress + 写回 state + OTel span。"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ma.core.plugin.base import AuthResult, EnrichResult, IntentResult


@pytest.mark.asyncio
async def test_auth_node_wraps_plugin_and_dispatches_events() -> None:
    from ma.core.graph.node_wrappers import make_auth_node

    plugin = MagicMock()
    plugin.authorize = AsyncMock(return_value=AuthResult(passed=True, user_ctx={"role": "x"}))

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
    names = [n for n, _ in captured]
    assert "thinking" in names
    assert "progress" in names
    prog = next(d for n, d in captured if n == "progress")
    assert prog["step"] == "auth"
    assert prog["status"] == "done"


@pytest.mark.asyncio
async def test_auth_node_failure_routes_to_reject() -> None:
    from ma.core.graph.node_wrappers import make_auth_node

    plugin = MagicMock()
    plugin.authorize = AsyncMock(
        return_value=AuthResult(passed=False, reject_code="FORBIDDEN_TOPIC", reject_message="拒绝")
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
    from ma.core.graph.node_wrappers import make_intent_node
    from ma.core.plugin.base import IntentFailed

    plugin = MagicMock()
    plugin.classify = AsyncMock(side_effect=IntentFailed("timeout"))

    with patch("ma.core.graph.node_wrappers.adispatch_custom_event", new=AsyncMock()):
        node = make_intent_node(plugin, default_route="metagc", labels=["metagc"])
        out = await node({}, config={})

    assert out["route"] == "metagc"
    assert out["route_confidence"] == 0.0


@pytest.mark.asyncio
async def test_intent_node_fallback_on_unknown_label() -> None:
    from ma.core.graph.node_wrappers import make_intent_node

    plugin = MagicMock()
    plugin.classify = AsyncMock(return_value=IntentResult(route="oops", confidence=0.7, reason="?"))

    with patch("ma.core.graph.node_wrappers.adispatch_custom_event", new=AsyncMock()):
        node = make_intent_node(plugin, default_route="metagc", labels=["metagc"])
        out = await node({}, config={})

    assert out["route"] == "metagc"


@pytest.mark.asyncio
async def test_adapter_node_streams_delta_into_chunks() -> None:
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
