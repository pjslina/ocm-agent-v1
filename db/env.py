"""Alembic env.py — 从 MA_PG_DSN_RW 读取 DSN。"""

from __future__ import annotations

import os
import sys

from alembic import context
from sqlalchemy import create_engine, pool

config = context.config

# MA_PG_DSN_RW 必须设置；alembic Job 由部署流水线传入
dsn = os.environ.get("MA_PG_DSN_RW")
if not dsn:
    sys.stderr.write("ERROR: MA_PG_DSN_RW environment variable required for migrations\n")
    raise SystemExit(2)

# alembic 用 sync engine
config.set_main_option("sqlalchemy.url", dsn)

target_metadata = None  # 我们用纯 SQL 迁移，不用 ORM autogen


def run_migrations_online() -> None:
    connectable = create_engine(dsn, poolclass=pool.NullPool, future=True)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
