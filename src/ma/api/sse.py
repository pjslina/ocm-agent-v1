"""POST /api/v1/chat/sse 端点。

M1 改动：
- 加 AuthnMiddleware 占位（信任 X-User-Account header + X-User-Role）→ AuthnIdentity
- 在调 ChatService 之前快速校验 biz_id 是否在 TopicRegistry 中
- 若 ChatService 未装配（无 DB） → 503
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import ulid
from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ma.core.chat_service import ChatRequest, ChatService
from ma.core.graph.events import encode_sse
from ma.core.graph.state import AuthnIdentity
from ma.core.topic.registry import TopicNotFound, TopicRegistry
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
    x_user_account: str | None = Header(default=None),
    x_user_role: str | None = Header(default=None),
) -> StreamingResponse:
    rid = x_request_id or _new_request_id()
    bind_request_ctx(request_id=rid, thread_id=body.thread_id, w3_account=body.w3_account)
    log.info("chat_started", biz_id=body.biz_id)

    chat_service: ChatService | None = request.app.state.chat_service
    topic_registry: TopicRegistry = request.app.state.topic_registry

    if chat_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DB not configured (MA_PG_DSN_RW required)",
        )

    # Topic 存在性快速校验 → HTTP 404（设计书 §4.5）
    try:
        topic_registry.get(body.biz_id)
    except TopicNotFound as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown biz_id: {body.biz_id}",
        ) from e

    # AuthnMiddleware 占位 —— 信任 X-User-Account header
    if x_user_account is not None and x_user_account != body.w3_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="X-User-Account header mismatch with body.w3_account",
        )
    # M1 简化：role 从可选 header X-User-Role 拿；缺省 = "REP"
    role = x_user_role or "REP"
    identity = AuthnIdentity(
        w3_account=body.w3_account,
        raw_claims={"role": role},
    )

    chat_req = ChatRequest(
        biz_id=body.biz_id,
        thread_id=body.thread_id,
        w3_account=body.w3_account,
        question=body.question,
        request_id=rid,
        biz_params=body.biz_params,
        identity=identity,
    )

    async def stream() -> AsyncIterator[bytes]:
        async for ev in chat_service.handle(chat_req):
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
