"""SessionRepository：thread_id 主键、应用层校验 biz_id 一致性。"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg

from ma.core.repo.models import Session, SessionBizMismatchError


def _row_to_session(row: asyncpg.Record) -> Session:
    ext = row["ext"]
    if isinstance(ext, str):
        ext = json.loads(ext)
    return Session(
        thread_id=row["thread_id"],
        biz_id=row["biz_id"],
        w3_account=row["w3_account"],
        title=row["title"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        last_message_at=row["last_message_at"],
        ext=ext or {},
    )


class SessionRepository:
    """对外接口稳定（设计书 §3.4）：

    - get_or_create(thread_id, biz_id, w3_account, ext) -> Session
        若 thread_id 已存在且 biz_id 不匹配 → SessionBizMismatchError
    - list_by_user(w3_account, biz_id, limit, before) -> list[Session]
    - touch(thread_id) -> None
    """

    def __init__(self, *, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def get_or_create(
        self,
        *,
        thread_id: str,
        biz_id: str,
        w3_account: str,
        ext: dict[str, Any] | None = None,
    ) -> Session:
        row = await self._conn.fetchrow(
            "SELECT thread_id, biz_id, w3_account, title, status, "
            "created_at, updated_at, last_message_at, ext "
            "FROM ma_session WHERE thread_id = $1",
            thread_id,
        )
        if row is not None:
            if row["biz_id"] != biz_id:
                raise SessionBizMismatchError(
                    f"thread_id={thread_id} already bound to biz_id={row['biz_id']}, "
                    f"cannot reuse with biz_id={biz_id}"
                )
            return _row_to_session(row)

        ext_json = json.dumps(ext or {})
        await self._conn.execute(
            "INSERT INTO ma_session (thread_id, biz_id, w3_account, title, status, ext) "
            "VALUES ($1, $2, $3, $4, 'active', $5::jsonb) "
            "ON CONFLICT (thread_id) DO NOTHING",
            thread_id,
            biz_id,
            w3_account,
            None,
            ext_json,
        )
        row = await self._conn.fetchrow(
            "SELECT thread_id, biz_id, w3_account, title, status, "
            "created_at, updated_at, last_message_at, ext "
            "FROM ma_session WHERE thread_id = $1",
            thread_id,
        )
        assert row is not None  # just inserted
        return _row_to_session(row)

    async def list_by_user(
        self,
        *,
        w3_account: str,
        biz_id: str | None,
        limit: int,
        before: datetime | None,
    ) -> list[Session]:
        rows = await self._conn.fetch(
            "SELECT thread_id, biz_id, w3_account, title, status, "
            "created_at, updated_at, last_message_at, ext "
            "FROM ma_session "
            "WHERE w3_account = $1 "
            "AND ($2::text IS NULL OR biz_id = $2) "
            "AND ($3::timestamp IS NULL OR last_message_at < $3) "
            "ORDER BY last_message_at DESC NULLS LAST, created_at DESC "
            "LIMIT $4",
            w3_account,
            biz_id,
            before,
            limit,
        )
        return [_row_to_session(r) for r in rows]

    async def touch(self, thread_id: str) -> None:
        await self._conn.execute(
            "UPDATE ma_session "
            "SET updated_at = CURRENT_TIMESTAMP, "
            "    last_message_at = CURRENT_TIMESTAMP "
            "WHERE thread_id = $1",
            thread_id,
        )
