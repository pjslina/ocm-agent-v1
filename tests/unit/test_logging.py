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
    """绑定 3 个 ID 之后，后续日志自动带上；未绑定时不出现 null 字段。"""
    from ma.infra.logging import bind_request_ctx, configure_logging, get_logger

    buf = StringIO()
    configure_logging("info")
    monkeypatch.setattr("sys.stdout", buf)
    log = get_logger("test")

    # 未 bind 时，三 ID 字段不应出现
    log.info("startup_event")
    first = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert "request_id" not in first
    assert "thread_id" not in first
    assert "w3_account" not in first

    # bind 之后三 ID 出现
    bind_request_ctx(request_id="req_1", thread_id="th_1", w3_account="zhangsan")
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


def test_trace_id_injected_when_span_active(monkeypatch: pytest.MonkeyPatch) -> None:
    """在 active span 内打日志：自动带 trace_id（32-char hex）。"""
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    from ma.infra.logging import configure_logging, get_logger

    if not isinstance(trace.get_tracer_provider(), TracerProvider):
        trace.set_tracer_provider(TracerProvider())

    buf = StringIO()
    configure_logging("info")
    monkeypatch.setattr("sys.stdout", buf)
    tracer = trace.get_tracer("test")
    log = get_logger("test")
    with tracer.start_as_current_span("test_span"):
        log.info("event_in_span")
    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert "trace_id" in payload
    assert isinstance(payload["trace_id"], str)
    assert len(payload["trace_id"]) == 32
    assert all(c in "0123456789abcdef" for c in payload["trace_id"])


def test_trace_id_absent_when_no_span(monkeypatch: pytest.MonkeyPatch) -> None:
    """无 active span 时，trace_id 字段不出现（不写 null / 0000…）。"""
    from ma.infra.logging import configure_logging, get_logger

    buf = StringIO()
    configure_logging("info")
    monkeypatch.setattr("sys.stdout", buf)
    log = get_logger("test")
    log.info("event_outside_span")
    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert "trace_id" not in payload
