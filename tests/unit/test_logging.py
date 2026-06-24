"""结构化日志: 每条日志必带 request_id/thread_id/w3_account (contextvars 注入),
且敏感字段 (authorization/cookie/token/x-api-key) 必被脱敏为 ***."""

from __future__ import annotations

import json
import sys
from io import StringIO

import pytest


@pytest.fixture(autouse=True)
def reset_structlog() -> None:
    """每个测试都重新 configure, 避免 processor 链跨用例污染."""
    import structlog

    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def _capture(monkeypatch: pytest.MonkeyPatch, stream: StringIO) -> None:
    """把 structlog 的 PrintLogger 输出导向给定 stream (替换 sys.stdout)."""
    monkeypatch.setattr(sys, "stdout", stream)


def test_logger_emits_json_with_event_and_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ma.infra.logging import configure_logging, get_logger

    buf = StringIO()
    _capture(monkeypatch, buf)
    configure_logging("info")
    log = get_logger("test")
    log.info("chat_started", biz_id="x")

    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "chat_started"
    assert payload["biz_id"] == "x"
    assert payload["level"] == "info"
    assert "timestamp" in payload


def test_contextvars_inject_three_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from ma.infra.logging import (
        bind_request_ctx,
        configure_logging,
        get_logger,
    )

    buf = StringIO()
    _capture(monkeypatch, buf)
    configure_logging("info")
    bind_request_ctx(request_id="req_1", thread_id="th_1", w3_account="zhangsan")
    log = get_logger("test")
    log.info("auth_passed")

    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert payload["request_id"] == "req_1"
    assert payload["thread_id"] == "th_1"
    assert payload["w3_account"] == "zhangsan"


def test_sensitive_keys_are_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    from ma.infra.logging import configure_logging, get_logger

    buf = StringIO()
    _capture(monkeypatch, buf)
    configure_logging("info")
    log = get_logger("test")
    log.info(
        "downstream_called",
        authorization="Bearer xxx",
        cookie="abc=1",
        token="t-secret",
        x_api_key="k-secret",
        other_field="ok",
    )

    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert payload["authorization"] == "***"
    assert payload["cookie"] == "***"
    assert payload["token"] == "***"
    assert payload["x_api_key"] == "***"
    assert payload["other_field"] == "ok"


def test_log_level_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    from ma.infra.logging import configure_logging, get_logger

    buf = StringIO()
    _capture(monkeypatch, buf)
    configure_logging("warning")
    log = get_logger("test")
    log.info("ignored_event")
    log.warning("kept_event")

    lines = [line for line in buf.getvalue().strip().splitlines() if line]
    payloads = [json.loads(line) for line in lines]
    events = [p["event"] for p in payloads]
    assert "ignored_event" not in events
    assert "kept_event" in events
