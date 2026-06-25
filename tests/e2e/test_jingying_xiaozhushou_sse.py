"""e2e: 经营小助手 + MetaGC mock 全链路。

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
async def test_jingying_xiaozhushou_full_loop(
    pg_dsn: str, metagc_server: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    monkeypatch.setenv("MA_METAGC_BASE_URL", metagc_server)
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", "config/topics")

    from ma.main import create_app

    with TestClient(create_app()) as client:
        body = {
            "biz_id": "jingying_xiaozhushou",
            "thread_id": "th_jy_e2e",
            "w3_account": "manager_alice",
            "question": "bu_a 上月业绩怎样？",
            "biz_params": {"business_unit": "bu_a"},
        }
        headers = {
            "X-Request-Id": "req_jy_e2e",
            "X-User-Account": "manager_alice",
            "X-User-Role": "MANAGER",
            "X-User-Claims": json.dumps({"business_units": ["bu_a"]}),
        }
        with client.stream("POST", "/api/v1/chat/sse", json=body, headers=headers) as r:
            assert r.status_code == 200
            text = "".join(r.iter_text())

    events = _parse_sse(text)
    types = [e["event"] for e in events]
    assert types[0] == "meta"
    assert types[-1] == "done"
    deltas = [e for e in events if e["event"] == "delta"]
    assert len(deltas) >= 2

    # DB 验证：用户消息 + 助手消息都落库
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT role, status FROM ma_message WHERE thread_id = $1 ORDER BY seq",
                "th_jy_e2e",
            )
    finally:
        await pool.close()
    assert [r["role"] for r in rows] == ["user", "assistant"]
    assert rows[1]["status"] == "complete"
