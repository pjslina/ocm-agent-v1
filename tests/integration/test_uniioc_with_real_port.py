"""UniiocAdapter 集成测试：用 uvicorn 启 mock server → adapter.run 消费。"""

from __future__ import annotations

import asyncio
import socket

import pytest
import uvicorn


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_uniioc_full_stream() -> None:
    from tests.mock_downstreams.uniioc_chunk import app

    port = _get_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    for _ in range(40):
        if server.started:
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("uniioc mock server failed to start")

    try:
        from ma.plugins.adapter.uniioc import UniiocAdapter

        adapter = UniiocAdapter()
        adapter.configure({"base_url": f"http://127.0.0.1:{port}", "timeout_ms": 5000})

        events = []
        async for ev in adapter.run(
            {"question": "测试问题", "biz_params": {}, "request_id": "r1"},
            output=True,
        ):
            events.append(ev)

        deltas = [e for e in events if e.type == "delta"]
        assert len(deltas) == 1
        assert "测试问题" in deltas[0].data["content"]
        assert events[-1].type != "error"
    finally:
        server.should_exit = True
        await task
