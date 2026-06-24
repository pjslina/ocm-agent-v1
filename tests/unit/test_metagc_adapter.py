"""MetaGCAdapter：用 ASGITransport 直连 mock server 跑单测。"""

from __future__ import annotations

import importlib
import sys

import httpx
import pytest

from tests.mock_downstreams.metagc_sse import app as mock_app


@pytest.fixture
def metagc_client() -> httpx.AsyncClient:
    """httpx AsyncClient + ASGITransport，直接打 mock app 无需起端口。"""
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_app),
        base_url="http://metagc.test",
    )


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    sys.modules.pop("ma.plugins.adapter.metagc", None)
    importlib.import_module("ma.plugins.adapter.metagc")
    yield new_reg


@pytest.mark.asyncio
async def test_metagc_adapter_streams_delta_events(
    metagc_client: httpx.AsyncClient,
) -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "adapter",
        "metagc",
        {"base_url": "http://metagc.test", "timeout_ms": 10000},
    )
    plugin._client = metagc_client  # 测试注入 client

    state = {"enriched_question": "上个月销售额"}
    events = []
    async for ev in plugin.run(state, output=True):
        events.append(ev)
    await metagc_client.aclose()

    deltas = [e for e in events if e.type == "delta"]
    assert len(deltas) >= 3
    full = "".join(e.data["content"] for e in deltas)
    assert "上个月销售额" in full


@pytest.mark.asyncio
async def test_metagc_adapter_emits_error_on_upstream_error(
    metagc_client: httpx.AsyncClient,
) -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "adapter",
        "metagc",
        {
            "base_url": "http://metagc.test",
            "timeout_ms": 5000,
            "path": "/api/v1/chat/sse?scenario=5xx",
        },
    )
    plugin._client = metagc_client

    events = []
    async for ev in plugin.run({}, output=True):
        events.append(ev)
    await metagc_client.aclose()

    errors = [e for e in events if e.type == "error"]
    assert len(errors) == 1
    assert errors[0].data["code"]
