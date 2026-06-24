"""全局 Settings：所有环境配置的唯一入口。任何缺失项启动即崩溃。"""

from __future__ import annotations

from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """所有字段经 MA_ 前缀的环境变量加载（如 MA_ENV=dev）。

    M1 字段一次性加齐，避免后续 task 反复改 Settings。具体业务字段（M2+
    的 Uniioc / KC、M5 的 AuthN JWT 等）后续再扩展。
    """

    model_config = SettingsConfigDict(
        env_prefix="MA_",
        env_file=None,
        case_sensitive=False,
        extra="ignore",
    )

    # ── 基础 ─────────────────────────
    env: Literal["dev", "test", "staging", "prod"]
    log_level: Literal["debug", "info", "warning", "error"] = "info"

    # ── 可观测性 ──────────────────────
    otel_exporter_otlp_endpoint: str | None = None
    otel_service_name: str = "master-agent"

    # ── DB (OpenGauss / PG-compatible) ───
    pg_dsn_rw: SecretStr | None = None
    pg_dsn_ro: SecretStr | None = None  # 留空 → 读用同一个 RW DSN
    pg_pool_min: int = 4
    pg_pool_max: int = 32

    # ── 配置文件目录 ──────────────────
    config_topics_dir: str = "config/topics"
    sql_templates_dir: str = "sql"

    # ── LLM (intent) ─────────────────
    # M1 用 FakeListChatModel；真 LLM 推到 M3 → 这些字段保留 None 即可
    intent_llm_provider: Literal["fake", "openai-compatible", "internal"] = "fake"
    intent_llm_base_url: str | None = None
    intent_llm_api_key: SecretStr | None = None
    intent_llm_model: str = "fake-model"

    # ── MetaGC 下游 ───────────────────
    metagc_base_url: str | None = None
    metagc_timeout_ms: int = 30000

    # ── AuthN（M0 占位）───────────────
    authn_mode: Literal["trust_header", "jwt", "gateway"] = "trust_header"
    authn_trust_header_name: str = "X-User-Account"
