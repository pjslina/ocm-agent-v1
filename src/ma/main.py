"""FastAPI app 入口。

启动顺序（设计书 §8.2.3）M0 简化版：
1. 加载 Settings → 失败即退出
2. configure logging
3. configure tracing
4. 装配 ChatService 到 app.state
5. mark_ready
6. 接流量
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from ma.api import health, sse
from ma.core.chat_service import ChatService
from ma.infra.logging import configure_logging, get_logger
from ma.infra.settings import Settings
from ma.infra.tracing import configure_tracing


def create_app() -> FastAPI:
    settings = Settings()  # type: ignore[call-arg]  # 字段由 MA_* 环境变量加载
    configure_logging(settings.log_level)
    configure_tracing(settings)
    log = get_logger("ma.main")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info("startup_begin", env=settings.env)
        # TODO(Task 21): inject (topic_registry, session_repo_factory, message_repo_factory).
        # M1.H.20 后 ChatService 构造签名变了，这里要等 Task 21 完整装配；目前应用未就绪，
        # readyz / sse 端点测试已 skip。
        app.state.chat_service = ChatService()  # type: ignore[call-arg]
        health.mark_ready()
        log.info("startup_complete")
        yield
        health.mark_not_ready()
        log.info("shutdown_complete")

    app = FastAPI(
        title="MasterAgent",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(sse.router, prefix="/api/v1")
    FastAPIInstrumentor().instrument_app(app)
    return app


# uvicorn 入口
app = create_app()
