"""REST endpoints：历史查询。

设计书 §4.4：
- GET /sessions —— 列出用户会话，w3_account 从 X-User-Account header 取
- GET /sessions/{thread_id}/messages —— 列出某会话消息，含越权防御
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

from ma.core.repo.models import Message, Session
from ma.infra.logging import bind_request_ctx, get_logger

router = APIRouter()
log = get_logger("ma.api.rest")


def _require_w3_account(x_user_account: str | None) -> str:
    if not x_user_account:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-User-Account header required",
        )
    return x_user_account


def _serialize_session(s: Session) -> dict[str, Any]:
    return {
        "thread_id": s.thread_id,
        "biz_id": s.biz_id,
        "w3_account": s.w3_account,
        "title": s.title,
        "status": s.status,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
        "last_message_at": s.last_message_at.isoformat() if s.last_message_at else None,
    }


def _serialize_message(m: Message) -> dict[str, Any]:
    return {
        "message_id": m.message_id,
        "thread_id": m.thread_id,
        "seq": m.seq,
        "role": m.role,
        "content": m.content,
        "status": m.status,
        "route": m.route,
        "created_at": m.created_at.isoformat() if m.created_at else None,
    }


@router.get(
    "/sessions",
    tags=["History"],
    summary="列出用户会话",
    responses={
        401: {"description": "缺少 X-User-Account header"},
        400: {"description": "before 参数非 ISO 8601"},
        503: {"description": "DB 未配置"},
    },
)
async def list_sessions(
    request: Request,
    biz_id: str | None = Query(default=None, description="按专题过滤"),
    limit: int = Query(default=50, ge=1, le=200, description="返回条数上限"),
    before: str | None = Query(
        default=None, description="游标：仅返回此时间点之前的会话（ISO 8601）"
    ),
    x_user_account: str | None = Header(default=None, description="用户账号，必填"),
) -> dict[str, Any]:
    """列出用户的会话。

    w3_account 强制从 header 取（设计书 §4.4：避免越权）。
    """
    w3 = _require_w3_account(x_user_account)
    bind_request_ctx(w3_account=w3)
    log.info("list_sessions", biz_id=biz_id, limit=limit)

    factory = request.app.state.session_repo_factory
    if factory is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DB not configured",
        )

    before_dt: datetime | None = None
    if before:
        try:
            before_dt = datetime.fromisoformat(before)
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"invalid 'before' (must be ISO 8601): {e}",
            ) from e

    async with factory.acquire() as conn:
        repo = factory.build(conn)
        items = await repo.list_by_user(
            w3_account=w3,
            biz_id=biz_id,
            limit=limit,
            before=before_dt,
        )
    return {"items": [_serialize_session(s) for s in items]}


@router.get(
    "/sessions/{thread_id}/messages",
    tags=["History"],
    summary="列出会话消息",
    responses={
        401: {"description": "缺少 X-User-Account header"},
        404: {"description": "thread_id 不存在或不属于该用户"},
        503: {"description": "DB 未配置"},
    },
)
async def list_messages(
    request: Request,
    thread_id: str,
    limit: int = Query(default=50, ge=1, le=200, description="返回条数上限（最新在前）"),
    x_user_account: str | None = Header(default=None, description="用户账号，必填"),
) -> dict[str, Any]:
    """列出某会话的消息。

    越权防御：thread_id 必须属于当前 w3_account，否则 404（不 403 —— 不泄漏存在性）。
    """
    w3 = _require_w3_account(x_user_account)
    bind_request_ctx(w3_account=w3, thread_id=thread_id)
    log.info("list_messages", thread_id=thread_id, limit=limit)

    sess_factory = request.app.state.session_repo_factory
    msg_factory = request.app.state.message_repo_factory
    if sess_factory is None or msg_factory is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DB not configured",
        )

    async with sess_factory.acquire() as conn:
        sess_repo = sess_factory.build(conn)
        # 拿 session 验证归属（O(1) 直查，不再 O(N) list+filter）
        session = await sess_repo.get_by_thread(thread_id=thread_id)
        if session is None or session.w3_account != w3:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown thread_id: {thread_id}",
            )

        msg_repo = msg_factory.build(conn)
        items = await msg_repo.list_recent(thread_id=thread_id, limit=limit)

    # list_recent 返回 DESC；REST 也按 DESC 给（最新在前），前端可自行 reverse
    return {
        "thread_id": thread_id,
        "items": [_serialize_message(m) for m in items],
    }
