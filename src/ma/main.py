"""FastAPI app 入口。

启动顺序（设计书 §8.2.3）M0 简化版：
1. 加载 Settings → 失败即退出
2. configure logging
3. configure tracing
4. mark_ready
5. 接流量
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ma.api import health
from ma.infra.logging import configure_logging, get_logger
from ma.infra.settings import Settings
from ma.infra.tracing import configure_tracing


def create_app() -> FastAPI:
    settings = Settings()  # type: ignore[call-arg]  # 字段由 MA_* 环境变量加载
    configure_logging(settings.log_level)
    configure_tracing(settings)
    log = get_logger("ma.main")

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        log.info("startup_begin", env=settings.env)
        # 后续里程碑：在此处初始化 DB pool / 加载 plugins / 编译 topics
        health.mark_ready()
        log.info("startup_complete")
        yield
        health.mark_not_ready()
        log.info("shutdown_complete")

    app = FastAPI(
        title="MasterAgent",
        version="0.0.1",
        lifespan=lifespan,
    )
    app.include_router(health.router, prefix="/api/v1")
    return app


# uvicorn 入口
app = create_app()
