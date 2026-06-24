"""init ma_session + ma_message tables

Revision ID: 0001
Revises:
Create Date: 2026-06-24
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa  # noqa: F401
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE ma_session (
            thread_id       VARCHAR(128) PRIMARY KEY,
            biz_id          VARCHAR(64)  NOT NULL,
            w3_account      VARCHAR(128) NOT NULL,
            title           VARCHAR(256),
            status          VARCHAR(16)  NOT NULL DEFAULT 'active',
            created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_message_at TIMESTAMP,
            ext             JSONB        NOT NULL DEFAULT '{}'::jsonb
        );
        """
    )
    op.execute(
        """
        CREATE INDEX idx_ma_session_w3_biz_time
        ON ma_session(w3_account, biz_id, last_message_at DESC);
        """
    )
    op.execute(
        """
        CREATE TABLE ma_message (
            message_id      VARCHAR(64)  PRIMARY KEY,
            thread_id       VARCHAR(128) NOT NULL,
            seq             BIGINT       NOT NULL,
            role            VARCHAR(16)  NOT NULL,
            content         TEXT         NOT NULL,
            content_meta    JSONB        NOT NULL DEFAULT '{}'::jsonb,
            status          VARCHAR(16)  NOT NULL DEFAULT 'complete',
            route           VARCHAR(32),
            request_id      VARCHAR(64),
            created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ext             JSONB        NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE (thread_id, seq)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX idx_ma_message_thread_seq ON ma_message(thread_id, seq);
        """
    )
    op.execute(
        """
        CREATE INDEX idx_ma_message_request_id ON ma_message(request_id);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_ma_message_request_id;")
    op.execute("DROP INDEX IF EXISTS idx_ma_message_thread_seq;")
    op.execute("DROP TABLE IF EXISTS ma_message;")
    op.execute("DROP INDEX IF EXISTS idx_ma_session_w3_biz_time;")
    op.execute("DROP TABLE IF EXISTS ma_session;")
