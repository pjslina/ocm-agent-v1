"""e2e: 代表小管家 + MetaGC mock 的全链路。

依赖：
- OpenGauss container（testcontainers postgres image）—— 复用 pg_dsn fixture
- mock metagc server（uvicorn）—— metagc_server fixture
- MA FastAPI app（in-process via TestClient）

本机无 docker 时整个 file skipped；CI 跑全套。
"""

from __future__ import annotations

import json

import asyncpg
import pytest
from fastapi.testclient import TestClient


def _parse_sse(body: str) -> list[dict]:
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
        if ev:
            events.append(ev)
    return events


@pytest.mark.asyncio
async def test_daibiao_xiaoguanjia_full_loop(
    pg_dsn: str, metagc_server: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    monkeypatch.setenv("MA_METAGC_BASE_URL", metagc_server)
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", "config/topics")

    from ma.main import create_app

    with TestClient(create_app()) as client:
        body = {
            "biz_id": "daibiao_xiaoguanjia",
            "thread_id": "th_e2e",
            "w3_account": "alice",
            "question": "上个月华东大区销售额？",
            "biz_params": {"region": "huadong"},
        }
        headers = {
            "X-Request-Id": "req_e2e",
            "X-User-Account": "alice",
            "X-User-Role": "REP",
            "X-User-Claims": json.dumps({"regions": ["huadong"]}),
        }
        with client.stream("POST", "/api/v1/chat/sse", json=body, headers=headers) as r:
            assert r.status_code == 200
            text = "".join(r.iter_text())

    events = _parse_sse(text)
    types = [e["event"] for e in events]
    assert types[0] == "meta"
    assert types[-1] == "done"
    assert "thinking" in types
    assert "progress" in types
    deltas = [e for e in events if e["event"] == "delta"]
    assert len(deltas) >= 2
    full = "".join(d["data"]["content"] for d in deltas)
    assert "上个月华东大区销售额" in full

    # 验证消息落库
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT role, status, content FROM ma_message " "WHERE thread_id = $1 ORDER BY seq",
                "th_e2e",
            )
    finally:
        await pool.close()

    assert len(rows) == 2
    assert rows[0]["role"] == "user"
    assert rows[1]["role"] == "assistant"
    assert rows[1]["status"] == "complete"
