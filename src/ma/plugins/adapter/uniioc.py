"""UniiocAdapter：HTTP POST 到 Uniioc，解析 JSON lines 返回 → ChatEvent。

输入：state.enriched_question + state.biz_params
输出：AsyncIterator[ChatEvent]
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
from pydantic import BaseModel

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import GraphState
from ma.core.plugin.registry import registry
from ma.infra.logging import get_logger

_log = get_logger("ma.adapter.uniioc")


class UniiocAdapterParams(BaseModel):
    base_url: str
    timeout_ms: int = 30000
    path: str = "/api/v1/chat"


@registry.register
class UniiocAdapter:
    name = "uniioc"
    plugin_kind = "adapter"

    def configure(self, params: dict[str, Any]) -> None:
        p = UniiocAdapterParams(**params)
        self._url = p.base_url + p.path
        self._timeout = httpx.Timeout(p.timeout_ms / 1000.0)
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def run(self, state: GraphState, *, output: bool = True) -> AsyncIterator[ChatEvent]:
        body = {
            "question": state.get("enriched_question") or state.get("question", ""),
            "biz_params": state.get("biz_params", {}),
            "request_id": state.get("request_id"),
        }
        try:
            async with self._client.stream("POST", self._url, json=body) as response:
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    try:
                        frame = json.loads(line)
                    except json.JSONDecodeError:
                        _log.warning("uniioc_bad_json_line", line=line)
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
                                "message": fd.get("message", "Uniioc 上游出错"),
                                "retryable": False,
                            },
                        )
                    elif ft == "done":
                        return
        except httpx.TimeoutException:
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_TIMEOUT",
                    "message": "Uniioc 接口响应超时，请稍后重试",
                    "retryable": False,
                },
            )
        except httpx.RequestError:
            _log.exception("uniioc_request_error")
            yield ChatEvent(
                type="error",
                data={
                    "code": "DOWNSTREAM_ERROR",
                    "message": "调用 Uniioc 失败，请稍后重试",
                    "retryable": False,
                },
            )
