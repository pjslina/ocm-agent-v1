"""KnowledgeCenterAdapter：WebSocket 客户端 → 接收文本帧 → ChatEvent。

输入：state.enriched_question + state.biz_params
输出：AsyncIterator[ChatEvent]
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import websockets
from pydantic import BaseModel

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import GraphState
from ma.core.plugin.registry import registry
from ma.infra.logging import get_logger

_log = get_logger("ma.adapter.knowledge_center")


class KnowledgeCenterParams(BaseModel):
    ws_url: str
    timeout_ms: int = 30000


@registry.register
class KnowledgeCenterAdapter:
    name = "knowledge_center"
    plugin_kind = "adapter"

    def configure(self, params: dict[str, Any]) -> None:
        p = KnowledgeCenterParams(**params)
        self._ws_url = p.ws_url
        self._timeout_ms = p.timeout_ms

    async def run(self, state: GraphState, *, output: bool = True) -> AsyncIterator[ChatEvent]:
        body = json.dumps(
            {
                "question": state.get("enriched_question") or state.get("question", ""),
                "biz_params": state.get("biz_params", {}),
                "request_id": state.get("request_id"),
            }
        )
        try:
            async with websockets.connect(
                self._ws_url,
                open_timeout=self._timeout_ms / 1000,
                close_timeout=5,
            ) as ws:
                await ws.send(body)
                async for raw in ws:
                    try:
                        frame = json.loads(raw)
                    except json.JSONDecodeError:
                        _log.warning("kc_bad_json_frame", raw=raw)
                        continue
                    ft = frame.get("type")
                    fd = frame.get("data", {})
                    if ft == "delta":
                        yield ChatEvent(type="delta", data=fd)
                    elif ft == "error":
                        yield ChatEvent(
                            type="error",
                            data={
                                "code": fd.get("code", "DOWNSTREAM_ERROR"),
                                "message": fd.get("message", "知识中心查询出错"),
                                "retryable": False,
                            },
                        )
                    elif ft == "done":
                        return
        except TimeoutError:
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_TIMEOUT",
                    "message": "知识中心接口响应超时，请稍后重试",
                    "retryable": False,
                },
            )
        except Exception:
            _log.exception("kc_error")
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_ERROR",
                    "message": "调用知识中心失败，请稍后重试",
                    "retryable": False,
                },
            )
