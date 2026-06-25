"""Settings 必须按 MA_ENV / MA_LOG_LEVEL / MA_... 加载，缺失关键字段立即报错。

M0 只覆盖最少 env / log_level，其它字段在后续里程碑接入对应基础设施时再加。
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError


def test_settings_load_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.setenv("MA_LOG_LEVEL", "info")

    from ma.infra.settings import Settings

    s = Settings()
    assert s.env == "dev"
    assert s.log_level == "info"


def test_settings_missing_env_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MA_ENV", raising=False)

    from ma.infra.settings import Settings

    with pytest.raises(ValidationError):
        Settings()


def test_settings_invalid_env_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "production")  # not in Literal

    from ma.infra.settings import Settings

    with pytest.raises(ValidationError):
        Settings()


def test_settings_default_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.delenv("MA_LOG_LEVEL", raising=False)

    from ma.infra.settings import Settings

    s = Settings()
    assert s.log_level == "info"


def test_otel_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.delenv("MA_OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("MA_OTEL_SERVICE_NAME", raising=False)

    from ma.infra.settings import Settings

    s = Settings()
    assert s.otel_exporter_otlp_endpoint is None
    assert s.otel_service_name == "master-agent"


def test_settings_default_db_dsns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.delenv("MA_PG_DSN_RW", raising=False)
    monkeypatch.delenv("MA_PG_DSN_RO", raising=False)

    from ma.infra.settings import Settings

    s = Settings()
    assert s.pg_dsn_rw is None
    assert s.pg_dsn_ro is None
    assert s.pg_pool_min == 4
    assert s.pg_pool_max == 32


def test_settings_db_dsn_secret_loaded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.setenv("MA_PG_DSN_RW", "postgresql://x:y@host:5432/db")

    from ma.infra.settings import Settings

    s = Settings()
    assert s.pg_dsn_rw is not None
    assert s.pg_dsn_rw.get_secret_value() == "postgresql://x:y@host:5432/db"


def test_settings_intent_llm_defaults_fake(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    from ma.infra.settings import Settings

    s = Settings()
    assert s.intent_llm_provider == "fake"
    assert s.intent_llm_model == "fake-model"


def test_settings_metagc_url_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    from ma.infra.settings import Settings

    s = Settings()
    assert s.metagc_base_url is None
    assert s.metagc_timeout_ms == 30000
