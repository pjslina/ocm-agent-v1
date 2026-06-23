"""/healthz /readyz 端点。

readyz 的 ready 标志由 lifespan 在所有启动检查通过后置 True；
shutdown 时置回 False，让 K8s 30s 内摘流量（设计书 §8.4.3）。
"""
from __future__ import annotations

from dataclasses import dataclass

from fastapi import APIRouter, Response, status

router = APIRouter()


@dataclass
class _ReadyState:
    ready: bool = False


_state = _ReadyState()


def mark_ready() -> None:
    _state.ready = True


def mark_not_ready() -> None:
    _state.ready = False


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/readyz")
async def readyz(response: Response) -> dict[str, str]:
    if not _state.ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready"}
    return {"status": "ready"}
