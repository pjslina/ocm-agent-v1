"""asyncpg pool 工厂。

在 lifespan 里调 create_pools(settings) → (pool_rw, pool_ro_or_None)。
"""

from __future__ import annotations

import asyncpg

from ma.infra.settings import Settings


async def create_pools(
    settings: Settings,
) -> tuple[asyncpg.Pool, asyncpg.Pool | None]:
    """创建 RW + 可选 RO asyncpg pool。M1 单 DSN：只有 RW，RO 返回 None。"""
    if settings.pg_dsn_rw is None:
        raise RuntimeError("MA_PG_DSN_RW is required to start the service; cannot create DB pool")
    pool_rw = await asyncpg.create_pool(
        dsn=settings.pg_dsn_rw.get_secret_value(),
        min_size=settings.pg_pool_min,
        max_size=settings.pg_pool_max,
    )
    pool_ro: asyncpg.Pool | None = None
    if settings.pg_dsn_ro is not None:
        pool_ro = await asyncpg.create_pool(
            dsn=settings.pg_dsn_ro.get_secret_value(),
            min_size=settings.pg_pool_min,
            max_size=settings.pg_pool_max,
        )
    return pool_rw, pool_ro


async def close_pools(pool_rw: asyncpg.Pool, pool_ro: asyncpg.Pool | None) -> None:
    await pool_rw.close()
    if pool_ro is not None:
        await pool_ro.close()
