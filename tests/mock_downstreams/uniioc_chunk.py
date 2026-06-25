"""Mock Uniioc：HTTP POST 进来，返回 JSON lines chunk 流。"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()


@app.post("/api/v1/chat")
async def chat(request: Request) -> StreamingResponse:
    body = await request.json()
    question = body.get("question", "")

    async def stream():
        import json

        yield json.dumps({"type": "meta", "data": {}}) + "\n"
        yield json.dumps({"type": "delta", "data": {"content": f"Uniioc收到: {question}"}}) + "\n"
        yield json.dumps({"type": "done", "data": {}}) + "\n"

    return StreamingResponse(stream(), media_type="application/x-ndjson")
