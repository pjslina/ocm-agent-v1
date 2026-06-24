"""MetaGCAdapter 走真 HTTP socket + 真端口的 integration test。

启动 tests/mock_downstreams/metagc_sse.py 到 ephemeral port，跑 MetaGCAdapter，
验证 SSE 流式拉取正常。
"""

from __future__ import annotations

import asyncio
import importlib
import socket
import sys
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
import uvicorn


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


@pytest_asyncio.fixture
async def metagc_server() -> AsyncIterator[str]:
    """启动 mock metagc 到 ephemeral port，yield base_url，结束时关闭。"""
    from tests.mock_downstreams.metagc_sse import app

    port = _get_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    # 等启动
    for _ in range(40):
        if server.started:
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("metagc mock server failed to start")

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    sys.modules.pop("ma.plugins.adapter.metagc", None)
    importlib.import_module("ma.plugins.adapter.metagc")
    yield new_reg


@pytest.mark.asyncio
async def test_metagc_full_socket_stream(metagc_server: str) -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "adapter",
        "metagc",
        {"base_url": metagc_server, "timeout_ms": 10000},
    )

    deltas = []
    async for ev in plugin.run({"enriched_question": "上个月销售额是多少"}, output=True):
        if ev.type == "delta":
            deltas.append(ev.data["content"])
        elif ev.type == "error":
            pytest.fail(f"unexpected error: {ev.data}")
    assert len(deltas) >= 3
    text = "".join(deltas)
    assert "上个月销售额是多少" in text
