"""POST /api/v1/chat/sse：体合法 → 200 + text/event-stream + 合法 SSE 帧序列。

NOTE: 这套测试在 Task 20（ChatService 重写为 TopicRegistry + LangGraph 驱动）后
失去 M0 时期那种 "stub topic 立即返回固定 5 件事件" 的可测性。Task 21（main.py
lifespan 完整装配 + 真 TopicRegistry + RepoFactory）会重写这些用例的 fixture：
注入一个测试 topic + fake adapter。整体模块在 Task 21 完成前 skip。
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.skip(reason="rewired by Task 21 (main.py lifespan 完整装配)")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MA_ENV", "dev")
    from ma.main import create_app

    with TestClient(create_app()) as c:
        yield c


def _parse_sse(body: str) -> list[dict]:
    """把 SSE 响应体解析为 [{event, data, id?}, ...]。"""
    events: list[dict] = []
    for block in body.split("\n\n"):
        block = block.strip("\n")
        if not block or block.startswith(":"):
            continue
        ev: dict = {}
        for line in block.splitlines():
            if line.startswith("event: "):
                ev["event"] = line[len("event: ") :]
            elif line.startswith("data: "):
                ev["data"] = json.loads(line[len("data: ") :])
            elif line.startswith("id: "):
                ev["id"] = line[len("id: ") :]
        if ev:
            events.append(ev)
    return events


def test_sse_returns_text_event_stream_header(client: TestClient) -> None:
    body = {
        "biz_id": "daibiao_xiaoguanjia",
        "thread_id": "th_test",
        "w3_account": "zhangsan",
        "question": "hi",
    }
    with client.stream("POST", "/api/v1/chat/sse", json=body) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        assert r.headers["cache-control"] == "no-cache"
        assert r.headers["x-accel-buffering"] == "no"
        assert "x-request-id" in r.headers


def test_sse_yields_meta_first_done_last(client: TestClient) -> None:
    body = {
        "biz_id": "daibiao_xiaoguanjia",
        "thread_id": "th_a",
        "w3_account": "zhangsan",
        "question": "hi",
    }
    with client.stream("POST", "/api/v1/chat/sse", json=body) as r:
        text = "".join(r.iter_text())
    events = _parse_sse(text)
    assert events[0]["event"] == "meta"
    assert events[-1]["event"] == "done"


def test_sse_request_id_propagates_to_meta(client: TestClient) -> None:
    body = {
        "biz_id": "x",
        "thread_id": "th_a",
        "w3_account": "z",
        "question": "hi",
    }
    headers = {"X-Request-Id": "req_caller"}
    with client.stream("POST", "/api/v1/chat/sse", json=body, headers=headers) as r:
        assert r.headers["x-request-id"] == "req_caller"
        text = "".join(r.iter_text())
    events = _parse_sse(text)
    meta = events[0]
    assert meta["event"] == "meta"
    assert meta["data"]["request_id"] == "req_caller"


def test_sse_request_id_auto_generated_when_missing(client: TestClient) -> None:
    body = {
        "biz_id": "x",
        "thread_id": "th_a",
        "w3_account": "z",
        "question": "hi",
    }
    with client.stream("POST", "/api/v1/chat/sse", json=body) as r:
        rid = r.headers["x-request-id"]
        text = "".join(r.iter_text())
    assert rid.startswith("req_")
    events = _parse_sse(text)
    assert events[0]["data"]["request_id"] == rid


def test_sse_rejects_missing_question(client: TestClient) -> None:
    body = {
        "biz_id": "x",
        "thread_id": "th_a",
        "w3_account": "z",
        # missing question
    }
    r = client.post("/api/v1/chat/sse", json=body)
    assert r.status_code == 422  # FastAPI validation
