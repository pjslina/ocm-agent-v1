"""ChatService：M0 骨架。

收到 ChatRequest → 产出固定 5 件事件流（meta/thinking/delta×2/done）。
后续里程碑会接 LangGraph、Repository、Plugin —— 但 *接口签名保持稳定*：
    async def handle(req: ChatRequest) -> AsyncIterator[ChatEvent]
这样 Transport 层（SSE/WS）从 M0 起就不用再改。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from ma.core.graph.events import ChatEvent


@dataclass(slots=True)
class ChatRequest:
    biz_id: str
    thread_id: str
    w3_account: str
    question: str
    request_id: str
    biz_params: dict[str, Any] = field(default_factory=dict)


class ChatService:
    """M0 实现：不接任何下游，只产固定 stub 事件流以验证 Transport 层。"""

    async def handle(self, req: ChatRequest) -> AsyncIterator[ChatEvent]:
        yield ChatEvent(
            type="meta",
            data={
                "thread_id": req.thread_id,
                "request_id": req.request_id,
                "route": "stub",
            },
        )
        # 轻微 yield 让事件不被同一事件循环周期吞并（更接近真实流式体感）
        await asyncio.sleep(0)
        yield ChatEvent(
            type="thinking",
            data={"phase": "stub", "label": "MA M0 stub running…"},
        )
        await asyncio.sleep(0)
        yield ChatEvent(type="delta", data={"content": "hello "})
        await asyncio.sleep(0)
        yield ChatEvent(type="delta", data={"content": "world"})
        await asyncio.sleep(0)
        yield ChatEvent(
            type="done",
            data={"message_id": f"msg_stub_{req.request_id}", "status": "complete"},
        )
