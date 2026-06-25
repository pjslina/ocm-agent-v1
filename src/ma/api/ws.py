"""WebSocket /api/v1/chat/ws 端点。设计书 §4.3。

连接 = 一个用户在一个专题下的在线通道，可承载多次 ask。
协议：双向 JSON。单连接内最大 5 个并发任务；45s 无帧则关闭。
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import ulid
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState

from ma.core.chat_service import ChatRequest, ChatService
from ma.core.graph.events import (
    WSFrameError,
    decode_ws_frame,
    encode_ws,
    encode_ws_closed,
    encode_ws_pong,
    encode_ws_ready,
)
from ma.core.graph.state import AuthnIdentity
from ma.core.topic.registry import TopicNotFound, TopicRegistry
from ma.infra.logging import bind_request_ctx, get_logger

router = APIRouter()
log = get_logger("ma.api.ws")

_MAX_CONCURRENT = 5
_HEARTBEAT_TIMEOUT_S = 45
_PING_INTERVAL_S = 20


class ConnectionManager:
    """管理 WS 连接级状态：并发任务数、心跳计时。"""

    def __init__(self) -> None:
        self._active_tasks: dict[str, asyncio.Task[Any]] = {}

    def _request_count(self) -> int:
        return len(self._active_tasks)

    def _can_accept(self) -> bool:
        return self._request_count() < _MAX_CONCURRENT

    def _add(self, request_id: str, task: asyncio.Task[Any]) -> None:
        self._active_tasks[request_id] = task

    def _remove(self, request_id: str) -> None:
        self._active_tasks.pop(request_id, None)

    async def _cancel_all(self) -> None:
        for rid, task in list(self._active_tasks.items()):
            task.cancel()
            self._active_tasks.pop(rid, None)


def _new_request_id() -> str:
    return f"req_{ulid.new()!s}"


@router.websocket("/chat/ws")
async def chat_ws(websocket: WebSocket, request: Request) -> None:
    await websocket.accept()

    # --- 从 query params 拿上下文（M4 占位，M5 接 AuthnMiddleware） ---
    w3_account = websocket.query_params.get("w3_account", "anonymous")
    biz_id = websocket.query_params.get("biz_id", "")
    x_user_role = websocket.headers.get("X-User-Role", "REP")
    x_user_claims_raw = websocket.headers.get("X-User-Claims")

    extra_claims: dict[str, Any] = {}
    if x_user_claims_raw:
        try:
            claims_raw: Any = json.loads(x_user_claims_raw)
            if isinstance(claims_raw, dict):
                extra_claims = claims_raw
        except (ValueError, TypeError):
            pass

    identity = AuthnIdentity(
        w3_account=w3_account,
        raw_claims={"role": x_user_role, **extra_claims},
    )

    bind_request_ctx(w3_account=w3_account)
    log.info("ws_connected", w3_account=w3_account, biz_id=biz_id)

    chat_service: ChatService | None = request.app.state.chat_service
    topic_registry: TopicRegistry = request.app.state.topic_registry

    if chat_service is None:
        await websocket.send_text(
            encode_ws_closed("DB not configured"),
        )
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        return

    # Topic 校验
    try:
        topic_registry.get(biz_id)
    except TopicNotFound:
        await websocket.send_text(encode_ws_closed(f"unknown biz_id: {biz_id}"))
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # 握手完成 → ready
    await websocket.send_text(encode_ws_ready())
    mgr = ConnectionManager()

    async def _handle_ask(frame: dict[str, Any]) -> None:
        """处理单次 ask：调 ChatService → 推 event 帧。"""
        request_id = frame.get("request_id") or _new_request_id()
        thread_id = frame.get("thread_id") or "unknown"
        question = frame.get("question", "")
        biz_params = frame.get("biz_params", {})
        bind_request_ctx(request_id=request_id, thread_id=thread_id)

        chat_req = ChatRequest(
            biz_id=biz_id,
            thread_id=thread_id,
            w3_account=w3_account,
            question=question,
            request_id=request_id,
            biz_params=biz_params,
            identity=identity,
        )
        try:
            async for ev in chat_service.handle(chat_req):
                if websocket.client_state != WebSocketState.CONNECTED:
                    break
                await websocket.send_text(encode_ws(ev, request_id))
        except Exception:
            log.exception("ws_ask_error", request_id=request_id)
        finally:
            mgr._remove(request_id)

    try:
        while True:
            # 等待客户端帧（45s 超时）
            try:
                raw = await asyncio.wait_for(
                    websocket.receive_text(), timeout=_HEARTBEAT_TIMEOUT_S
                )
            except TimeoutError:
                await websocket.send_text(
                    encode_ws_closed("heartbeat timeout"),
                )
                await websocket.close(code=4408)
                break

            try:
                frame = decode_ws_frame(raw)
            except WSFrameError as e:
                await websocket.send_text(
                    json.dumps({"op": "error", "reason": str(e)}),
                )
                continue

            op = frame["op"]

            if op == "ping":
                await websocket.send_text(encode_ws_pong())

            elif op == "ask":
                if not mgr._can_accept():
                    await websocket.send_text(
                        json.dumps(
                            {
                                "op": "error",
                                "reason": f"too many concurrent requests (max {_MAX_CONCURRENT})",
                                "request_id": frame.get("request_id", ""),
                            }
                        )
                    )
                    continue
                task = asyncio.create_task(_handle_ask(frame))
                mgr._add(frame.get("request_id", ""), task)

            elif op == "cancel":
                rid = frame.get("request_id", "")
                target: asyncio.Task[Any] | None = mgr._active_tasks.get(rid)
                if target is not None:
                    target.cancel()
                    mgr._remove(rid)

    except WebSocketDisconnect:
        log.info("ws_disconnected", w3_account=w3_account)
    finally:
        await mgr._cancel_all()
