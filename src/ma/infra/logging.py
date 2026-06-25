"""structlog 配置：JSON 输出 + structlog.contextvars 自动注入 + OTel trace_id + 敏感字段脱敏。

调用顺序：configure_logging(level) 一次 → bind_request_ctx(...) 在请求入口
→ get_logger(__name__).info("event_name", **kv) 在业务代码中。
"""

from __future__ import annotations

import logging
import sys
from collections.abc import MutableMapping
from typing import Any

import structlog
from opentelemetry import trace

_SENSITIVE_KEYS = frozenset(
    {"authorization", "cookie", "token", "x-api-key", "x_api_key", "set-cookie"}
)


def bind_request_ctx(
    *,
    request_id: str | None = None,
    thread_id: str | None = None,
    w3_account: str | None = None,
) -> None:
    """在请求入口调用一次，让后续日志自动带上三 ID。

    None 值跳过 —— 不写入 contextvars，因此 merge_contextvars 不会把 null 渲染到 JSON。
    """
    kv: dict[str, Any] = {}
    if request_id is not None:
        kv["request_id"] = request_id
    if thread_id is not None:
        kv["thread_id"] = thread_id
    if w3_account is not None:
        kv["w3_account"] = w3_account
    if kv:
        structlog.contextvars.bind_contextvars(**kv)


def _inject_trace_id(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """读当前 active span，把 trace_id 注入 event_dict。

    无 active span 或 span context invalid 时不写。32-char hex 小写。
    """
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
    return event_dict


def _sanitize(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    for k in list(event_dict):
        if k.lower() in _SENSITIVE_KEYS:
            event_dict[k] = "***"
    return event_dict


class _StdoutPrintLoggerFactory:
    """structlog factory：每次 logger 调用都读当前 sys.stdout。

    用于支持 tests 用 monkeypatch.setattr(sys, "stdout", buf) 捕获日志输出。
    """

    def __call__(self, *args: Any, **kwargs: Any) -> structlog.PrintLogger:
        return structlog.PrintLogger(file=sys.stdout)


def configure_logging(level: str = "info") -> None:
    """配置 structlog 全局处理链。多次调用幂等。"""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(message)s", force=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _inject_trace_id,
            _sanitize,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=_StdoutPrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """获取一个 structlog logger。"""
    return structlog.get_logger(name)  # type: ignore[no-any-return]
