"""POST /api/v1/chat/sse 端点。

M1 改动：
- 加 AuthnMiddleware 占位（信任 X-User-Account header + X-User-Role）→ AuthnIdentity
- 在调 ChatService 之前快速校验 biz_id 是否在 TopicRegistry 中
- 若 ChatService 未装配（无 DB） → 503
"""

from __future__ import annotations

import json
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
    biz_id: str = Field(
        min_length=1, description="专题 ID，须已在 TopicRegistry 注册（如 daibiao_xiaoguanjia）"
    )
    thread_id: str = Field(min_length=1, description="会话线程 ID，同一 thread_id 复用历史")
    w3_account: str = Field(
        min_length=1, description="用户账号（须与 X-User-Account header 一致，否则 403）"
    )
    question: str = Field(min_length=1, description="用户提问内容")
    biz_params: dict[str, Any] = Field(
        default_factory=dict, description="业务上下文参数，传给 enrich / adapter 节点"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "biz_id": "daibiao_xiaoguanjia",
                "thread_id": "th_demo_1",
                "w3_account": "zhangsan",
                "question": "你好，请介绍一下",
                "biz_params": {"region": "huadong"},
            }
        }
    }


def _new_request_id() -> str:
    return f"req_{ulid.new()!s}"


@router.post(
    "/chat/sse",
    tags=["Chat"],
    summary="对话流（SSE）",
    response_class=StreamingResponse,
    responses={
        200: {"description": "text/event-stream：meta → thinking → delta×N → done（或 error）"},
        403: {"description": "X-User-Account 与 body.w3_account 不一致"},
        404: {"description": "未知 biz_id"},
        503: {"description": "DB 未配置（MA_PG_DSN_RW 缺失）"},
    },
)
async def chat_sse(
    request: Request,
    body: ChatBody,
    x_request_id: str | None = Header(default=None, description="可选；缺省自动生成 req_<ulid>"),
    x_user_account: str | None = Header(
        default=None, description="用户账号；与 body.w3_account 不一致则 403"
    ),
    x_user_role: str | None = Header(default=None, description="用户角色，缺省 REP"),
    x_user_claims: str | None = Header(default=None, description="额外 claims，JSON 对象字符串"),
) -> StreamingResponse:
    """POST 发起一次对话，返回 SSE 流。响应帧类型：meta / thinking / delta / done / error。"""
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
    # M2: 支持 X-User-Claims header（JSON 对象）作为额外 claims
    extra_claims: dict[str, Any] = {}
    if x_user_claims is not None:
        try:
            extra_claims = json.loads(x_user_claims)
            if not isinstance(extra_claims, dict):
                raise ValueError("X-User-Claims must be a JSON object")
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"invalid X-User-Claims: {e}",
            ) from e
    identity = AuthnIdentity(
        w3_account=body.w3_account,
        raw_claims={"role": role, **extra_claims},
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
