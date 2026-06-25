"""replay_request_id CLI：从 DB 拉一条 request 的消息历史，重放到 stdout。

用法：
    python -m ma.cli.replay <request_id> [--pg-dsn DSN]

输出：每行一个 JSON 对象（{"role","content","status","created_at"}），chronological 顺序。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

import asyncpg


async def _replay(request_id: str, pg_dsn: str) -> list[dict[str, Any]]:
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT role, content, status, created_at, thread_id, seq "
                "FROM ma_message "
                "WHERE request_id = $1 "
                "ORDER BY seq",
                request_id,
            )
    finally:
        await pool.close()

    result: list[dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "role": r["role"],
                "content": r["content"],
                "status": r["status"],
                "thread_id": r["thread_id"],
                "seq": r["seq"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
        )
    return result


def _default_dsn() -> str:
    return os.environ.get(
        "MA_PG_DSN_RW",
        "postgresql://ma:ma@localhost:5432/ma",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a request's message history")
    parser.add_argument("request_id", help="Request ID to replay")
    parser.add_argument(
        "--pg-dsn",
        default=_default_dsn(),
        help="PostgreSQL DSN (default: $MA_PG_DSN_RW or localhost)",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = parser.parse_args()

    indent = 2 if args.pretty else None
    rows = asyncio.run(_replay(args.request_id, args.pg_dsn))

    if not rows:
        print(f"no messages found for request_id={args.request_id}", file=sys.stderr)
        sys.exit(1)

    for row in rows:
        print(json.dumps(row, ensure_ascii=False, indent=indent))


if __name__ == "__main__":
    main()
