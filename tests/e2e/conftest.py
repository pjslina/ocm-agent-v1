"""e2e fixtures: 复用 integration 的 pg_dsn + 增加 metagc_server (uvicorn) fixture。"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator

import pytest_asyncio
import uvicorn

# 复用 integration 的 pg_dsn fixture（pytest 不会跨 sibling dir 自动继承 conftest，
# 但通过显式 import 把它注册到本目录的 conftest 上）
from tests.integration.conftest import pg_dsn  # noqa: F401  (used as fixture)


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


@pytest_asyncio.fixture
async def metagc_server() -> AsyncIterator[str]:
    """启动 mock metagc 到 ephemeral port。"""
    from tests.mock_downstreams.metagc_sse import app

    port = _get_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

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
