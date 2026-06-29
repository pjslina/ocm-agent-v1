-- ============================================================
-- MasterAgent 建表脚本
-- 目标数据库: postgres  目标 Schema: ocm
-- 用法: psql -h <host> -p 5432 -U root -d postgres -f init.sql
--   或在 psql 中先执行: SET search_path TO ocm;
-- ============================================================

-- 确保 ocm schema 存在
CREATE SCHEMA IF NOT EXISTS ocm;
SET search_path TO ocm;

-- ── 会话表 ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ma_session (
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

CREATE INDEX IF NOT EXISTS idx_ma_session_w3_biz_time
    ON ma_session (w3_account, biz_id, last_message_at DESC);

-- ── 消息表 ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ma_message (
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

CREATE INDEX IF NOT EXISTS idx_ma_message_thread_seq
    ON ma_message (thread_id, seq);

CREATE INDEX IF NOT EXISTS idx_ma_message_request_id
    ON ma_message (request_id);
