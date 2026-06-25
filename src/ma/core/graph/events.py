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


# ---------------------------------------------------------------------------
# WebSocket 双向 JSON 协议（设计书 §4.3.2）
# ---------------------------------------------------------------------------


def encode_ws(event: ChatEvent, request_id: str) -> str:
    """ChatEvent → WS JSON 帧（服务端→客户端 event）。设计书 §4.3.2。"""
    frame = {
        "op": "event",
        "request_id": request_id,
        "event": event.type,
        "data": event.data,
    }
    return json.dumps(frame, ensure_ascii=False, separators=(",", ":"))


def encode_ws_ready(version: str = "0.2.0", heartbeat_interval_s: int = 20) -> str:
    """握手成功后的 ready 帧。"""
    return json.dumps(
        {"op": "ready", "server_version": version, "heartbeat_interval_s": heartbeat_interval_s},
        ensure_ascii=False,
    )


def encode_ws_pong() -> str:
    """心跳应答。"""
    return json.dumps({"op": "pong"}, ensure_ascii=False)


def encode_ws_closed(reason: str) -> str:
    """服务端主动关闭通知。"""
    return json.dumps({"op": "closed", "reason": reason}, ensure_ascii=False)


# WS 客户端帧的合法 op 值
_WS_CLIENT_OPS = frozenset({"ask", "cancel", "ping"})


class WSFrameError(ValueError):
    """WS 帧格式不合法。"""


def decode_ws_frame(raw: str) -> dict[str, Any]:
    """解析客户端 WS JSON 帧 → dict。

    返回 dict 保证含 `op` key；其它字段 caller 自行 get。
    """
    try:
        frame = json.loads(raw)
    except json.JSONDecodeError as e:
        raise WSFrameError(f"invalid JSON: {e}") from e
    if not isinstance(frame, dict):
        raise WSFrameError("frame must be a JSON object")
    op = frame.get("op")
    if op not in _WS_CLIENT_OPS:
        raise WSFrameError(f"unknown or missing op: {op!r}")
    return frame
