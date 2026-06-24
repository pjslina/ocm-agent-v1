"""集成测试通用 fixture：OpenGauss 容器 + alembic 迁移 + asyncpg pool。

策略：
- 若环境变量 MA_TEST_PG_DSN 已存在 → 跳过容器启动，直接用它（开发者本机或 staging）
- 否则用 testcontainers 启 PG-compatible 镜像（默认 postgres，OpenGauss 协议兼容）
- 整个 pytest session 共用一个 DB，但每个 test 用独立事务，结束 rollback
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config


def _alembic_upgrade(dsn: str) -> None:
    # alembic 的 sync DSN 与 asyncpg 同（postgresql://...），但需要 sqlalchemy driver
    sa_dsn = dsn.replace("postgresql://", "postgresql+psycopg2://", 1) if "+" not in dsn else dsn
    cfg = Config("db/alembic.ini")
    os.environ["MA_PG_DSN_RW"] = sa_dsn
    command.upgrade(cfg, "head")


@pytest_asyncio.fixture(scope="session")
async def pg_dsn() -> AsyncIterator[str]:
    dsn = os.environ.get("MA_TEST_PG_DSN")
    if dsn:
        _alembic_upgrade(dsn)
        yield dsn
        return

    # testcontainers path —— 用 postgres 镜像（OpenGauss 协议兼容）
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        pytest.skip("testcontainers[postgres] not installed")

    try:
        pg_container = PostgresContainer("postgres:16-alpine")
        pg_container.start()
    except Exception as e:
        pytest.skip(f"docker not available: {e}")
        return
    try:
        sync_dsn = pg_container.get_connection_url()
        # asyncpg 不需要 sqlalchemy 的 driver 前缀
        async_dsn = sync_dsn.replace("postgresql+psycopg2", "postgresql")
        _alembic_upgrade(async_dsn)
        yield async_dsn
    finally:
        pg_container.stop()


@pytest_asyncio.fixture
async def pg_pool(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=4)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def pg_conn(pg_pool: asyncpg.Pool) -> AsyncIterator[asyncpg.Connection]:
    """单个 test 用独立连接 + 事务回滚，保证测试间隔离。"""
    async with pg_pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            yield conn
        finally:
            await tx.rollback()
