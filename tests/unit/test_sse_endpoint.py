"""SSE endpoint M1 fast-path tests：

M0 时代的 stub-stream 5 帧断言不再适用（ChatService 真接 LangGraph + DB）。
完整流式契约推到 stream/ 与 e2e/。这里只测 fast-path：422 / 503。

完整的流式 SSE 行为由 e2e 测试覆盖（M1.J.24）。
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path) -> TestClient:
    """启 app 但不配 DB → ChatService 为 None；topics 目录为空。

    用来覆盖 422（pydantic 校验）和 503（ChatService 未装配）。
    """
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.delenv("MA_PG_DSN_RW", raising=False)
    topics_dir = tmp_path / "topics"
    topics_dir.mkdir()
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", str(topics_dir))
    from ma.main import create_app

    with TestClient(create_app()) as c:
        yield c


def test_sse_returns_422_for_missing_question(client: TestClient) -> None:
    """Pydantic 校验在 ChatService 之前 → 422。"""
    body = {
        "biz_id": "daibiao_xiaoguanjia",
        "thread_id": "th",
        "w3_account": "alice",
        # missing question
    }
    r = client.post("/api/v1/chat/sse", json=body)
    assert r.status_code == 422


def test_sse_returns_503_when_chat_service_unconfigured(client: TestClient) -> None:
    """没配 MA_PG_DSN_RW → ChatService=None → SSE 应当 503。"""
    body = {
        "biz_id": "daibiao_xiaoguanjia",
        "thread_id": "th",
        "w3_account": "alice",
        "question": "hi",
    }
    r = client.post("/api/v1/chat/sse", json=body)
    assert r.status_code == 503
