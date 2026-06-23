"""全局 Settings：所有环境配置的唯一入口。任何缺失项启动即崩溃。"""
from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """M0 仅声明 env / log_level；后续里程碑在此追加 DB / 下游 / LLM / OTel 等字段。

    所有字段经 MA_ 前缀的环境变量加载（如 MA_ENV=dev）。
    """

    model_config = SettingsConfigDict(
        env_prefix="MA_",
        env_file=None,
        case_sensitive=False,
        extra="ignore",
    )

    env: Literal["dev", "test", "staging", "prod"]
    log_level: Literal["debug", "info", "warning", "error"] = "info"

    # ── 可观测性 ──────────────────────
    otel_exporter_otlp_endpoint: str | None = None
    otel_service_name: str = "master-agent"
