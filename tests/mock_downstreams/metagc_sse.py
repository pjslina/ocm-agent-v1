"""MetaGC 下游 mock server (FastAPI)。

提供 POST /api/v1/chat/sse 端点：
- 默认行为：流式返回预设 5 段 token 组成的一句话回复
- scenario=timeout 模拟卡住
- scenario=5xx 模拟服务错误
- scenario=slow 模拟首 chunk 延迟（默认 5s）

集成测试启动 ASGI app 用 httpx.AsyncClient + ASGITransport 直接对接；
独立运行模式 (python -m tests.mock_downstreams.metagc_sse) 起到 8001 端口便于
人工 curl 验证。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import StreamingResponse

app = FastAPI(title="MetaGC mock")


def _sse_frame(event: str, data: dict[str, Any]) -> bytes:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n".encode()


@app.post("/api/v1/chat/sse")
async def metagc_sse_endpoint(
    request: Request,
    scenario: str = Query(default="default"),
) -> StreamingResponse:
    body = await request.json()
    question = body.get("question", "?")

    async def stream() -> AsyncIterator[bytes]:
        if scenario == "5xx":
            yield _sse_frame("error", {"code": "METAGC_INTERNAL", "message": "boom"})
            return

        if scenario == "timeout":
            await asyncio.sleep(3600)
            return

        if scenario == "slow":
            await asyncio.sleep(5)

        # default: 5 chunks
        yield _sse_frame("meta", {"upstream_id": "metagc_demo"})
        chunks = ["关于 ", question, " 的回复：", "示例数据。"]
        for c in chunks:
            yield _sse_frame("delta", {"content": c})
            await asyncio.sleep(0.05)
        yield _sse_frame("done", {})

    return StreamingResponse(stream(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="warning")
