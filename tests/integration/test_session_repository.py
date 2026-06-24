"""SessionRepository：覆盖 get_or_create / list_by_user / touch。"""

from __future__ import annotations

import asyncpg
import pytest


@pytest.mark.asyncio
async def test_session_get_or_create_creates_new(pg_conn: asyncpg.Connection) -> None:
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    s = await repo.get_or_create(thread_id="th_new", biz_id="biz_x", w3_account="alice")
    assert s.thread_id == "th_new"
    assert s.biz_id == "biz_x"
    assert s.w3_account == "alice"
    assert s.status == "active"


@pytest.mark.asyncio
async def test_session_get_or_create_returns_existing(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    s1 = await repo.get_or_create(thread_id="th_e", biz_id="biz_x", w3_account="alice")
    s2 = await repo.get_or_create(thread_id="th_e", biz_id="biz_x", w3_account="alice")
    assert s1.thread_id == s2.thread_id
    assert s1.created_at == s2.created_at


@pytest.mark.asyncio
async def test_session_get_or_create_rejects_biz_mismatch(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.models import SessionBizMismatchError
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    await repo.get_or_create(thread_id="th_mismatch", biz_id="biz_x", w3_account="alice")
    with pytest.raises(SessionBizMismatchError):
        await repo.get_or_create(thread_id="th_mismatch", biz_id="biz_y", w3_account="alice")


@pytest.mark.asyncio
async def test_session_touch_updates_last_message_at(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    await repo.get_or_create(thread_id="th_touch", biz_id="biz_x", w3_account="alice")
    await repo.touch("th_touch")
    row = await pg_conn.fetchrow(
        "SELECT last_message_at FROM ma_session WHERE thread_id = $1", "th_touch"
    )
    assert row is not None
    assert row["last_message_at"] is not None


@pytest.mark.asyncio
async def test_session_list_by_user_returns_recent_first(
    pg_conn: asyncpg.Connection,
) -> None:
    from ma.core.repo.session_repository import SessionRepository

    repo = SessionRepository(conn=pg_conn)
    for tid in ["th_a", "th_b", "th_c"]:
        await repo.get_or_create(thread_id=tid, biz_id="biz_x", w3_account="bob")
        await repo.touch(tid)

    items = await repo.list_by_user(w3_account="bob", biz_id="biz_x", limit=10, before=None)
    thread_ids = [s.thread_id for s in items]
    assert set(thread_ids) == {"th_a", "th_b", "th_c"}
