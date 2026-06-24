"""MetaGCAdapter：POST 到 MetaGC SSE 端点，把 SSE 帧翻译成 ChatEvent。

设计书 §2.2 / §5.2.4。

输入：state.enriched_question + state.biz_params
输出：AsyncIterator[ChatEvent]，type=delta / type=error / type=tool
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
from httpx_sse import aconnect_sse
from pydantic import BaseModel

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import GraphState
from ma.core.plugin.registry import registry
from ma.infra.logging import get_logger

_log = get_logger("ma.adapter.metagc")


class MetaGCAdapterParams(BaseModel):
    base_url: str
    timeout_ms: int = 30000
    path: str = "/api/v1/chat/sse"


@registry.register
class MetaGCAdapter:
    name = "metagc"
    plugin_kind = "adapter"

    def configure(self, params: dict[str, Any]) -> None:
        p = MetaGCAdapterParams(**params)
        self._base_url = p.base_url
        self._timeout = httpx.Timeout(p.timeout_ms / 1000.0)
        self._path = p.path
        # 默认 client；测试可以替换 self._client
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def run(self, state: GraphState, *, output: bool = True) -> AsyncIterator[ChatEvent]:
        body = {
            "question": state.get("enriched_question") or state.get("question", ""),
            "biz_params": state.get("biz_params", {}),
            "request_id": state.get("request_id"),
        }
        url = self._base_url + self._path
        try:
            async with aconnect_sse(self._client, "POST", url, json=body) as event_source:
                async for sse in event_source.aiter_sse():
                    if sse.event == "meta":
                        # 上游 meta 不向用户透出；记录 trace 即可
                        _log.info("metagc_meta", upstream_data=sse.data)
                    elif sse.event == "delta":
                        data = json.loads(sse.data) if sse.data else {}
                        yield ChatEvent(type="delta", data=data)
                    elif sse.event == "error":
                        data = json.loads(sse.data) if sse.data else {}
                        yield ChatEvent(
                            type="error",
                            data={
                                "code": data.get("code", "DOWNSTREAM_ERROR"),
                                "message": data.get("message", "MetaGC 上游出错"),
                                "retryable": False,
                            },
                        )
                    elif sse.event == "done":
                        return
        except httpx.TimeoutException:
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_TIMEOUT",
                    "message": "MetaGC 接口响应超时，请稍后重试",
                    "retryable": False,
                },
            )
        except httpx.RequestError:
            _log.exception("metagc_request_error")
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_ERROR",
                    "message": "调用下游失败，请稍后重试",
                    "retryable": False,
                },
            )
