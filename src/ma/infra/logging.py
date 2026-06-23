"""structlog 配置: JSON 输出 + contextvars 三 ID 注入 + 敏感字段脱敏.

调用顺序: configure_logging(level) 一次 -> bind_request_ctx(...) 在请求入口
-> get_logger(__name__).info("event_name", **kv) 在业务代码中.
"""
from __future__ import annotations

import contextvars
import logging
import sys
from collections.abc import MutableMapping
from typing import Any

import structlog

_SENSITIVE_KEYS = frozenset(
    {"authorization", "cookie", "token", "x-api-key", "x_api_key", "set-cookie"}
)

_ctx_request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_id", default=None
)
_ctx_thread_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "thread_id", default=None
)
_ctx_w3: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "w3_account", default=None
)


def bind_request_ctx(
    *,
    request_id: str | None = None,
    thread_id: str | None = None,
    w3_account: str | None = None,
) -> None:
    """在请求入口调用一次, 让后续日志自动带上三 ID."""
    if request_id is not None:
        _ctx_request_id.set(request_id)
    if thread_id is not None:
        _ctx_thread_id.set(thread_id)
    if w3_account is not None:
        _ctx_w3.set(w3_account)


def _inject_ids(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    event_dict.setdefault("request_id", _ctx_request_id.get())
    event_dict.setdefault("thread_id", _ctx_thread_id.get())
    event_dict.setdefault("w3_account", _ctx_w3.get())
    return event_dict


def _sanitize(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    for k in list(event_dict):
        if k.lower() in _SENSITIVE_KEYS:
            event_dict[k] = "***"
    return event_dict


class _StdoutPrintLoggerFactory:
    """每次构造 PrintLogger 都读取当前的 sys.stdout.

    structlog 自带的 PrintLoggerFactory 会在工厂初始化时捕获 file 参数,
    导致测试里 monkeypatch sys.stdout 之后仍写到原 stdout.
    本工厂在每次 __call__ 时读取 sys.stdout, 配合
    cache_logger_on_first_use=False 即可让测试用 monkeypatch 捕获输出.
    """

    def __call__(self, *_args: Any) -> structlog.PrintLogger:
        return structlog.PrintLogger(file=sys.stdout)


def configure_logging(level: str = "info") -> None:
    """配置 structlog 全局处理链与 stdlib logging level.

    多次调用幂等 (每次都会 reset).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(message)s", force=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _inject_ids,
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
    """获取一个 structlog logger."""
    return structlog.get_logger(name)  # type: ignore[no-any-return]
