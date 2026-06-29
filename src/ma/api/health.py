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


@router.get("/healthz", tags=["Health"], summary="存活探针")
async def healthz() -> dict[str, str]:
    """进程存活检查，恒 200。无需 DB。"""
    return {"status": "alive"}


@router.get(
    "/readyz",
    tags=["Health"],
    summary="就绪探针",
    responses={503: {"description": "启动未完成 / 关闭中"}},
)
async def readyz(response: Response) -> dict[str, str]:
    """lifespan 完成所有启动检查后置 ready；关闭时置回 False（K8s 摘流量窗口）。"""
    if not _state.ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready"}
    return {"status": "ready"}
