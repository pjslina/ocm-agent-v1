"""e2e 追问：同一 thread 第二次问答历史会被加载到 GraphState。

验证方式：同一 thread_id 连发两次问答，完成后 DB 里有 4 条 message（两轮 u/a/u/a）。
真正的 history 拼接到 LLM context 的验证留到 M3（接真 LLM 后能看 prompt）。

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


def _read_stream(client: TestClient, body: dict, headers: dict) -> str:
    with client.stream("POST", "/api/v1/chat/sse", json=body, headers=headers) as r:
        assert r.status_code == 200
        return "".join(r.iter_text())


@pytest.mark.asyncio
async def test_followup_persists_two_turns(
    pg_dsn: str, metagc_server: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    monkeypatch.setenv("MA_METAGC_BASE_URL", metagc_server)
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", "config/topics")

    from ma.main import create_app

    with TestClient(create_app()) as client:
        thread_id = "th_followup"
        common_headers = {
            "X-User-Account": "alice",
            "X-User-Role": "REP",
            "X-User-Claims": json.dumps({"regions": ["huadong"]}),
        }
        # 第一轮
        _read_stream(
            client,
            {
                "biz_id": "daibiao_xiaoguanjia",
                "thread_id": thread_id,
                "w3_account": "alice",
                "question": "第一个问题",
                "biz_params": {"region": "huadong"},
            },
            {"X-Request-Id": "req_fu_1", **common_headers},
        )
        # 第二轮（追问）
        _read_stream(
            client,
            {
                "biz_id": "daibiao_xiaoguanjia",
                "thread_id": thread_id,
                "w3_account": "alice",
                "question": "第二个问题（追问）",
                "biz_params": {"region": "huadong"},
            },
            {"X-Request-Id": "req_fu_2", **common_headers},
        )

    # DB 验证：4 条消息（u/a/u/a）
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT seq, role, content FROM ma_message " "WHERE thread_id = $1 ORDER BY seq",
                thread_id,
            )
    finally:
        await pool.close()
    assert len(rows) == 4
    assert [r["role"] for r in rows] == ["user", "assistant", "user", "assistant"]
    assert rows[0]["content"] == "第一个问题"
    assert rows[2]["content"] == "第二个问题（追问）"
