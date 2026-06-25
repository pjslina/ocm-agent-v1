"""会话与消息的 dataclass 模型。

故意不引入 ORM —— 这些是从 asyncpg.Record 装载来的纯数据载体。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class Session:
    thread_id: str
    biz_id: str
    w3_account: str
    title: str | None
    status: str
    created_at: datetime
    updated_at: datetime
    last_message_at: datetime | None
    ext: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Message:
    message_id: str
    thread_id: str
    seq: int
    role: str  # 'user' | 'assistant' | 'system'
    content: str
    content_meta: dict[str, Any] = field(default_factory=dict)
    status: str = "complete"  # 'complete' | 'partial' | 'failed'
    route: str | None = None
    request_id: str | None = None
    created_at: datetime | None = None  # DB 默认填，append 时可不传
    ext: dict[str, Any] = field(default_factory=dict)


class SessionBizMismatchError(ValueError):
    """同一 thread_id 已存在但 biz_id 不匹配（设计书 §3.4 应当 400）。"""
