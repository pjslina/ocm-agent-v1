"""MessageRepository。"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from ma.core.repo.models import Message


def _row_to_message(row: asyncpg.Record) -> Message:
    content_meta = row["content_meta"]
    if isinstance(content_meta, str):
        content_meta = json.loads(content_meta)
    ext = row["ext"]
    if isinstance(ext, str):
        ext = json.loads(ext)
    return Message(
        message_id=row["message_id"],
        thread_id=row["thread_id"],
        seq=row["seq"],
        role=row["role"],
        content=row["content"],
        content_meta=content_meta or {},
        status=row["status"],
        route=row["route"],
        request_id=row["request_id"],
        created_at=row["created_at"],
        ext=ext or {},
    )


class MessageRepository:
    def __init__(self, *, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def append(self, *, msg: Message) -> None:
        seq = msg.seq
        if seq == 0:
            row = await self._conn.fetchrow(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM ma_message WHERE thread_id = $1",
                msg.thread_id,
            )
            seq = (row["m"] if row else 0) + 1

        await self._conn.execute(
            "INSERT INTO ma_message ("
            "  message_id, thread_id, seq, role, content, content_meta, "
            "  status, route, request_id, ext"
            ") VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10::jsonb)",
            msg.message_id,
            msg.thread_id,
            seq,
            msg.role,
            msg.content,
            json.dumps(msg.content_meta),
            msg.status,
            msg.route,
            msg.request_id,
            json.dumps(msg.ext),
        )

    async def list_recent(self, *, thread_id: str, limit: int) -> list[Message]:
        rows = await self._conn.fetch(
            "SELECT message_id, thread_id, seq, role, content, content_meta, "
            "status, route, request_id, created_at, ext "
            "FROM ma_message "
            "WHERE thread_id = $1 AND status IN ('complete', 'partial') "
            "ORDER BY seq DESC LIMIT $2",
            thread_id,
            limit,
        )
        return [_row_to_message(r) for r in rows]

    async def mark_partial(self, message_id: str) -> None:
        await self._conn.execute(
            "UPDATE ma_message SET status = 'partial' WHERE message_id = $1",
            message_id,
        )

    async def mark_failed(self, message_id: str, err: dict[str, Any]) -> None:
        await self._conn.execute(
            "UPDATE ma_message "
            "SET status = 'failed', "
            "    ext = COALESCE(ext, '{}'::jsonb) || $2::jsonb "
            "WHERE message_id = $1",
            message_id,
            json.dumps(err),
        )
