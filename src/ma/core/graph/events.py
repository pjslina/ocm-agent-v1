"""ChatEvent：MA 内部统一的事件流元素。+ SSE 协议编码器。

参见设计书 §4.2.2：7 种事件类型 meta/thinking/progress/delta/tool/error/done。
SSE 规范：`event:` `data:` `id:` 三类字段，事件之间空行分隔。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

EventType = Literal[
    "meta",
    "thinking",
    "progress",
    "delta",
    "tool",
    "error",
    "done",
]


@dataclass(slots=True, frozen=True)
class ChatEvent:
    """MA 内部事件流的最小单元。

    type   - 设计书 §4.2.2 七选一
    data   - 必须 JSON 可序列化
    event_id - 可选，对应 SSE 的 id 字段（前端可用作 last-event-id）
    """

    type: EventType
    data: Any
    event_id: str | None = None


def encode_sse(event: ChatEvent) -> str:
    """把 ChatEvent 编码为一个完整的 SSE 帧（含末尾空行）。

    出错条件：data 不可 JSON 序列化 → 抛 TypeError。
    """
    payload = json.dumps(event.data, ensure_ascii=False, separators=(",", ":"))
    parts: list[str] = [f"event: {event.type}"]
    if event.event_id is not None:
        parts.append(f"id: {event.event_id}")
    parts.append(f"data: {payload}")
    return "\n".join(parts) + "\n\n"


def encode_keepalive() -> str:
    """SSE 注释行作为心跳，浏览器 EventSource 自动忽略。"""
    return ": keep-alive\n\n"
