"""Mock KnowledgeCenter WebSocket server：接收 JSON question → 发两条 delta + close。"""

from __future__ import annotations

import asyncio
import json

import websockets


async def handler(websocket):
    raw = await websocket.recv()
    msg = json.loads(raw)
    question = msg.get("question", "")

    await websocket.send(json.dumps({"type": "delta", "data": {"content": f"KC回答: {question}"}}))
    await websocket.send(json.dumps({"type": "delta", "data": {"content": "。"}}))
    await websocket.send(json.dumps({"type": "done", "data": {}}))
    await websocket.close()


async def serve(port: int):
    async with websockets.serve(handler, "127.0.0.1", port):
        await asyncio.Future()  # run forever
