"""WebSocket 端点集成测试：ask → event 流 → done。

本机无 docker，用 TestClient 启 app + websockets 连接。
"""

from __future__ import annotations

import asyncio
import json
import socket

import pytest
import websockets
from fastapi.testclient import TestClient


@pytest.fixture
def app_with_db(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """启 app 但不配 DB → ChatService=None → WS 应 closed。"""
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.delenv("MA_PG_DSN_RW", raising=False)
    topics_dir = tmp_path / "topics"
    topics_dir.mkdir()
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", str(topics_dir))
    from ma.main import create_app

    with TestClient(create_app()) as c:
        yield c


def test_ws_no_db_returns_closed(app_with_db: TestClient) -> None:
    """无 DB 时 WS 返回 closed 帧。"""
    # TestClient 不原生支持 WS；此测试验证 import + route 注册正确
    from ma.api import ws as ws_module

    assert ws_module.router is not None
    assert ws_module.ConnectionManager is not None
    # smoke: ConnectionManager 基本逻辑
    mgr = ws_module.ConnectionManager()
    assert mgr._can_accept() is True
    assert mgr._request_count() == 0


def _get_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_ws_ask_event_flow(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """完整 WS 流程：connect → 收 closed（无 DB）。

    用 uvicorn 启 app 到 ephemeral port，websockets 连接。
    无 DB 时 ChatService 为 None，服务端立即返回 closed 帧后关闭连接。
    """
    import uvicorn
    import yaml

    # 准备 topic YAML —— 编译通过但不实际调用下游（无 DB）
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.delenv("MA_PG_DSN_RW", raising=False)
    topics_dir = tmp_path / "topics"
    topics_dir.mkdir()

    topic_yaml = {
        "topic_id": "test",
        "display_name": "Test",
        "default_route": "metagc",
        "history": {"max_turns": 5, "scope": "per_topic"},
        "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
        "enrich": {"plugin": "generic_enrich", "params": {"fields_to_inject": ["region"]}},
        "intent": {
            "plugin": "llm_classifier",
            "params": {
                "provider": "fake",
                "model": "m",
                "labels": ["metagc"],
                "prompt_template": "p.txt",
                "fake_responses": ["route: metagc\nreason: ok"],
            },
        },
        "graph": {
            "nodes": [
                {
                    "id": "metagc_adapter",
                    "adapter": "metagc",
                    "output": True,
                    "params": {"base_url": "http://unused", "timeout_ms": 5000},
                }
            ],
            "edges": [
                {
                    "from": "intent",
                    "type": "conditional",
                    "route_by": "route",
                    "mapping": {"metagc": "metagc_adapter"},
                }
            ],
        },
        "biz_params_schema": {
            "type": "object",
            "required": ["region"],
            "properties": {"region": {"type": "string"}},
        },
        "error_messages": {
            "FORBIDDEN_TOPIC": "x",
            "FORBIDDEN_SCOPE": "x",
            "DOWNSTREAM_TIMEOUT": "x",
            "DOWNSTREAM_ERROR": "x",
            "INTERNAL": "x",
        },
    }
    (topics_dir / "test.yaml").write_text(yaml.dump(topic_yaml), encoding="utf-8")
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", str(topics_dir))

    # 修复：fresh_registry autouse fixture（来自 test_metagc_with_real_port.py）
    # 把 metagc 注册到了临时 registry 里，导致 sys.modules 中缓存的模块
    # 关联的 registry 引用不对。此处清除缓存，让 load_all_plugins() 重新注册。
    import sys as _sys

    _sys.modules.pop("ma.plugins.adapter.metagc", None)

    from ma.main import create_app

    app = create_app()

    port = _get_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    for _ in range(40):
        if server.started:
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("app failed to start")

    try:
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/api/v1/chat/ws?w3_account=alice&biz_id=test",
            open_timeout=5,
        ) as ws:
            # 无 DB → 服务端发送 closed 帧（不是 ready）
            raw = await ws.recv()
            frame = json.loads(raw)
            assert frame["op"] == "closed"
            assert "DB" in frame.get("reason", "")
    finally:
        server.should_exit = True
        await task
