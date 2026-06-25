"""KnowledgeCenterAdapter 集成测试：启 WS mock server → adapter 消费。"""
from __future__ import annotations

import asyncio
import socket
import threading

import pytest


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_kc_full_socket_stream() -> None:
    from tests.mock_downstreams.kc_ws import serve

    port = _get_free_port()
    # 在后台线程启动 WS server（websockets 需要自己的 event loop）
    stop_event = threading.Event()

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server_task = loop.create_task(
            asyncio.wait_for(
                serve(port),
                timeout=None,
            )
        )

        async def _wait_stop():
            await asyncio.to_thread(stop_event.wait)
            server_task.cancel()

        loop.run_until_complete(_wait_stop())
        loop.close()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    await asyncio.sleep(0.2)  # give server time to start

    try:
        from ma.plugins.adapter.knowledge_center import KnowledgeCenterAdapter

        adapter = KnowledgeCenterAdapter()
        adapter.configure({"ws_url": f"ws://127.0.0.1:{port}", "timeout_ms": 5000})

        events = []
        async for ev in adapter.run(
            {"question": "测试", "biz_params": {}, "request_id": "r1"},
            output=True,
        ):
            events.append(ev)

        deltas = [e for e in events if e.type == "delta"]
        assert len(deltas) >= 1
        combined = "".join(d.data["content"] for d in deltas)
        assert "测试" in combined
        assert events[-1].type != "error"
    finally:
        stop_event.set()
        t.join(timeout=3)
