"""REST 历史端点 integration：复用 pg_dsn 容器 + TestClient 启 MA app。"""

from __future__ import annotations

import asyncpg
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app_with_pg(monkeypatch: pytest.MonkeyPatch, pg_dsn: str, tmp_path):
    """启 app 接到真 OpenGauss 容器；topics 目录为空，只测历史端点。"""
    monkeypatch.setenv("MA_ENV", "test")
    monkeypatch.setenv("MA_PG_DSN_RW", pg_dsn)
    topics_dir = tmp_path / "topics"
    topics_dir.mkdir()
    monkeypatch.setenv("MA_CONFIG_TOPICS_DIR", str(topics_dir))
    from ma.main import create_app

    with TestClient(create_app()) as c:
        yield c


async def _seed_session_and_messages(pg_dsn: str, *, thread_id: str, w3: str, biz_id: str) -> None:
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO ma_session (thread_id, biz_id, w3_account) "
                "VALUES ($1, $2, $3) "
                "ON CONFLICT (thread_id) DO NOTHING",
                thread_id,
                biz_id,
                w3,
            )
            await conn.execute(
                "UPDATE ma_session SET last_message_at = CURRENT_TIMESTAMP " "WHERE thread_id = $1",
                thread_id,
            )
            for i, (role, content) in enumerate([("user", "q1"), ("assistant", "a1")], start=1):
                await conn.execute(
                    "INSERT INTO ma_message (message_id, thread_id, seq, role, content) "
                    "VALUES ($1, $2, $3, $4, $5)",
                    f"msg_{thread_id}_{i}",
                    thread_id,
                    i,
                    role,
                    content,
                )
    finally:
        await pool.close()


def test_list_sessions_requires_w3_header(app_with_pg: TestClient) -> None:
    r = app_with_pg.get("/api/v1/sessions")
    assert r.status_code == 401


def test_list_sessions_returns_user_sessions(app_with_pg: TestClient, pg_dsn: str) -> None:
    import asyncio

    asyncio.run(_seed_session_and_messages(pg_dsn, thread_id="th_a", w3="alice", biz_id="biz_x"))
    asyncio.run(_seed_session_and_messages(pg_dsn, thread_id="th_b", w3="bob", biz_id="biz_x"))

    r = app_with_pg.get(
        "/api/v1/sessions",
        headers={"X-User-Account": "alice"},
    )
    assert r.status_code == 200
    items = r.json()["items"]
    thread_ids = [s["thread_id"] for s in items]
    assert "th_a" in thread_ids
    assert "th_b" not in thread_ids  # 不能看到 bob 的


def test_list_messages_owned_thread(app_with_pg: TestClient, pg_dsn: str) -> None:
    import asyncio

    asyncio.run(_seed_session_and_messages(pg_dsn, thread_id="th_msgs", w3="alice", biz_id="biz_x"))

    r = app_with_pg.get(
        "/api/v1/sessions/th_msgs/messages",
        headers={"X-User-Account": "alice"},
    )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 2
    roles = {m["role"] for m in items}
    assert roles == {"user", "assistant"}


def test_list_messages_others_thread_returns_404(app_with_pg: TestClient, pg_dsn: str) -> None:
    """越权防御：bob 不能看 alice 的 thread。"""
    import asyncio

    asyncio.run(
        _seed_session_and_messages(pg_dsn, thread_id="th_secret", w3="alice", biz_id="biz_x")
    )

    r = app_with_pg.get(
        "/api/v1/sessions/th_secret/messages",
        headers={"X-User-Account": "bob"},
    )
    assert r.status_code == 404
