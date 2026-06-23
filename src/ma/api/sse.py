"""POST /api/v1/chat/sse 端点。

把 ChatService.handle() 产出的 ChatEvent 异步流编码成 SSE 响应。
设计书 §4.2 / §4.7.1。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import ulid
from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ma.core.chat_service import ChatRequest, ChatService
from ma.core.graph.events import encode_sse
from ma.infra.logging import bind_request_ctx, get_logger

router = APIRouter()
log = get_logger("ma.api.sse")


class ChatBody(BaseModel):
    biz_id: str = Field(min_length=1)
    thread_id: str = Field(min_length=1)
    w3_account: str = Field(min_length=1)
    question: str = Field(min_length=1)
    biz_params: dict[str, Any] = Field(default_factory=dict)


def _new_request_id() -> str:
    return f"req_{ulid.new()!s}"


@router.post("/chat/sse")
async def chat_sse(
    request: Request,
    body: ChatBody,
    x_request_id: str | None = Header(default=None),
) -> StreamingResponse:
    rid = x_request_id or _new_request_id()
    bind_request_ctx(request_id=rid, thread_id=body.thread_id, w3_account=body.w3_account)
    log.info("chat_started", biz_id=body.biz_id)

    svc: ChatService = request.app.state.chat_service
    chat_req = ChatRequest(
        biz_id=body.biz_id,
        thread_id=body.thread_id,
        w3_account=body.w3_account,
        question=body.question,
        request_id=rid,
        biz_params=body.biz_params,
    )

    async def stream() -> AsyncIterator[bytes]:
        async for ev in svc.handle(chat_req):
            yield encode_sse(ev).encode("utf-8")

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "X-Request-Id": rid,
        },
    )
