"""MessageRepository：append / list_recent / mark_partial / mark_failed。"""

from __future__ import annotations

import asyncpg
import pytest


async def _seed_session(pg_conn: asyncpg.Connection, thread_id: str) -> None:
    await pg_conn.execute(
        "INSERT INTO ma_session (thread_id, biz_id, w3_account) " "VALUES ($1, 'biz_x', 'alice')",
        thread_id,
    )


@pytest.mark.asyncio
async def test_message_append_assigns_seq(pg_conn: asyncpg.Connection) -> None:
    from ma.core.repo.message_repository import MessageRepository
    from ma.core.repo.models import Message

    await _seed_session(pg_conn, "th_msg_1")
    repo = MessageRepository(conn=pg_conn)
    m1 = Message(
        message_id="msg_a",
        thread_id="th_msg_1",
        seq=0,
        role="user",
        content="hi",
    )
    await repo.append(msg=m1)
    m2 = Message(
        message_id="msg_b",
        thread_id="th_msg_1",
        seq=0,
        role="assistant",
        content="hello",
    )
    await repo.append(msg=m2)

    rows = await pg_conn.fetch(
        "SELECT message_id, seq FROM ma_message WHERE thread_id = $1 ORDER BY seq",
        "th_msg_1",
    )
    assert [(r["message_id"], r["seq"]) for r in rows] == [
        ("msg_a", 1),
        ("msg_b", 2),
    ]


@pytest.mark.asyncio
async def test_message_list_recent_returns_complete_partial_only(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.message_repository import MessageRepository
    from ma.core.repo.models import Message

    await _seed_session(pg_conn, "th_recent")
    repo = MessageRepository(conn=pg_conn)
    for i, status in enumerate(["complete", "partial", "failed"], start=1):
        await repo.append(
            msg=Message(
                message_id=f"m{i}",
                thread_id="th_recent",
                seq=i,
                role="user" if i % 2 else "assistant",
                content=f"c{i}",
                status=status,
            )
        )

    items = await repo.list_recent(thread_id="th_recent", limit=10)
    assert {m.message_id for m in items} == {"m1", "m2"}
    assert [m.seq for m in items] == [2, 1]


@pytest.mark.asyncio
async def test_message_mark_partial(pg_conn: asyncpg.Connection) -> None:
    from ma.core.repo.message_repository import MessageRepository
    from ma.core.repo.models import Message

    await _seed_session(pg_conn, "th_partial")
    repo = MessageRepository(conn=pg_conn)
    await repo.append(
        msg=Message(
            message_id="m_part",
            thread_id="th_partial",
            seq=1,
            role="assistant",
            content="incomplete",
        )
    )
    await repo.mark_partial("m_part")
    row = await pg_conn.fetchrow("SELECT status FROM ma_message WHERE message_id = $1", "m_part")
    assert row is not None
    assert row["status"] == "partial"


@pytest.mark.asyncio
async def test_message_mark_failed_appends_to_ext(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.message_repository import MessageRepository
    from ma.core.repo.models import Message

    await _seed_session(pg_conn, "th_fail")
    repo = MessageRepository(conn=pg_conn)
    await repo.append(
        msg=Message(
            message_id="m_fail",
            thread_id="th_fail",
            seq=1,
            role="assistant",
            content="x",
        )
    )
    await repo.mark_failed("m_fail", err={"code": "DOWNSTREAM_TIMEOUT"})
    row = await pg_conn.fetchrow(
        "SELECT status, ext FROM ma_message WHERE message_id = $1", "m_fail"
    )
    assert row is not None
    assert row["status"] == "failed"
    import json as _json

    ext = row["ext"] if isinstance(row["ext"], dict) else _json.loads(row["ext"])
    assert ext.get("code") == "DOWNSTREAM_TIMEOUT"
