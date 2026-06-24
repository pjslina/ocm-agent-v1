"""ChatEvent 与 SSE 编码：7 种 type 全覆盖；编码格式严格遵守 SSE 规范。"""

from __future__ import annotations

import json

import pytest


def test_event_type_literal_covers_seven_types() -> None:
    from ma.core.graph.events import ChatEvent, EventType

    # type 必须是 Literal 七选一
    allowed = {"meta", "thinking", "progress", "delta", "tool", "error", "done"}
    # mypy 没法在运行时验证 Literal，所以这里靠值合法性校验
    for t in allowed:
        ev = ChatEvent(type=t, data={"k": "v"})  # type: ignore[arg-type]
        assert ev.type == t

    # 类型别名暴露给外部，便于注解
    assert EventType is not None


def test_event_to_sse_frame_basic() -> None:
    from ma.core.graph.events import ChatEvent, encode_sse

    ev = ChatEvent(type="delta", data={"content": "hello"})
    frame = encode_sse(ev)
    # SSE 规范：event: <name>\ndata: <json>\n\n
    assert frame.endswith("\n\n")
    lines = frame.rstrip("\n").splitlines()
    assert lines[0] == "event: delta"
    assert lines[1].startswith("data: ")
    assert json.loads(lines[1][len("data: ") :]) == {"content": "hello"}


def test_event_to_sse_frame_with_id() -> None:
    from ma.core.graph.events import ChatEvent, encode_sse

    ev = ChatEvent(type="done", data={"message_id": "msg_1"}, event_id="msg_1")
    frame = encode_sse(ev)
    lines = frame.rstrip("\n").splitlines()
    assert "id: msg_1" in lines
    assert "event: done" in lines


def test_event_sse_frame_data_is_single_line_json() -> None:
    """SSE 规范：data 字段如有换行需多行 data:。我们采用 compact JSON 强制单行。"""
    from ma.core.graph.events import ChatEvent, encode_sse

    ev = ChatEvent(
        type="meta",
        data={"thread_id": "th_1", "message_id": "msg_1", "route": "metagc"},
    )
    frame = encode_sse(ev)
    data_lines = [ln for ln in frame.rstrip("\n").splitlines() if ln.startswith("data: ")]
    assert len(data_lines) == 1


def test_sse_keepalive_comment_frame() -> None:
    """SSE 心跳：注释帧 `: keep-alive\\n\\n` 浏览器自动忽略。"""
    from ma.core.graph.events import encode_keepalive

    frame = encode_keepalive()
    assert frame == ": keep-alive\n\n"


def test_event_data_must_be_json_serializable() -> None:
    from ma.core.graph.events import ChatEvent, encode_sse

    class NotSerializable:
        pass

    ev = ChatEvent(type="delta", data={"obj": NotSerializable()})  # type: ignore[dict-item]
    with pytest.raises(TypeError):
        encode_sse(ev)
