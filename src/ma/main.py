"""FastAPI app 入口。

设计书 §8.2.3 启动顺序（M1 版）：
1. Settings → fail-fast
2. configure logging
3. configure tracing
4. create DB pools（若 MA_PG_DSN_RW 未设置 → 跳过 + log warning，ChatService 为 None）
5. load_all_plugins
6. 编译所有 Topic YAML
7. 装配 ChatService 注入 app.state
8. FastAPIInstrumentor
9. mark_ready
"""

from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

# 允许直接 `python src/ma/main.py` 启动：把项目 src/ 加入 sys.path，使
# `from ma...` 可解析（`python -m ma.main` 与已安装包场景无需此步）。
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import asyncpg
import yaml  # type: ignore[import-untyped]
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from ma.api import health, rest, sse, ws
from ma.core.chat_service import ChatService
from ma.core.plugin.bootstrap import load_all_plugins
from ma.core.repo.message_repository import MessageRepository
from ma.core.repo.session_repository import SessionRepository
from ma.core.topic.compiler import TopicCompiler
from ma.core.topic.config import TopicConfig
from ma.core.topic.registry import TopicRegistry
from ma.infra.db import close_pools, create_pools
from ma.infra.logging import configure_logging, get_logger
from ma.infra.settings import Settings
from ma.infra.tracing import configure_tracing

# OpenAPI 分组标签（在各 router 里以字符串引用，保持一致）。
# 注：WebSocket 端点不进 OpenAPI schema，故无独立 tag，其用法见 app description。
_OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": "Health", "description": "存活 / 就绪探针。无需 DB，smoke 模式也可用。"},
    {
        "name": "Chat",
        "description": "对话流（SSE，HTTP）。需要 DB（MA_PG_DSN_RW）才可用，否则 503。",
    },
    {
        "name": "History",
        "description": "会话与消息历史查询。需要 DB；w3_account 强制从 X-User-Account header 取。",
    },
]


class _RepoFactory:
    """从 asyncpg pool 拿 conn 建 repository。

    `acquire()` 返回 pool.acquire() 的 async context manager（不要 await）。
    `build(conn)` 在已拿到 conn 时构造 repository 实例。
    """

    def __init__(self, pool: asyncpg.Pool, repo_cls: type[Any]) -> None:
        self._pool = pool
        self._repo_cls = repo_cls

    def acquire(self) -> Any:
        return self._pool.acquire()

    def build(self, conn: asyncpg.Connection) -> Any:
        return self._repo_cls(conn=conn)


def create_app() -> FastAPI:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)
    configure_tracing(settings)
    log = get_logger("ma.main")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info("startup_begin", env=settings.env)

        # 4. DB pools (optional if no DSN — for smoke startup only)
        if settings.pg_dsn_rw is not None:
            pool_rw, pool_ro = await create_pools(settings)
            app.state.pg_pool_rw = pool_rw
            app.state.pg_pool_ro = pool_ro
        else:
            log.warning(
                "no_pg_dsn",
                hint="MA_PG_DSN_RW not set; service running without DB (smoke only)",
            )
            app.state.pg_pool_rw = None
            app.state.pg_pool_ro = None

        # 5. plugins
        load_all_plugins()

        # 6. compile topics
        topic_registry = TopicRegistry()
        topics_dir = Path(settings.config_topics_dir)
        if topics_dir.is_dir():
            for yaml_path in sorted(topics_dir.glob("*.yaml")):
                raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
                cfg = TopicConfig.model_validate(raw)
                compiled = TopicCompiler().compile(cfg, env=dict(os.environ))
                topic_registry.register(compiled)
                log.info("topic_loaded", topic_id=compiled.topic_id)
        app.state.topic_registry = topic_registry

        # 7. ChatService —— 若 DB pool 缺失，ChatService 为 None；transport 层会回 503
        if app.state.pg_pool_rw is not None:
            sess_factory = _RepoFactory(app.state.pg_pool_rw, SessionRepository)
            msg_factory = _RepoFactory(app.state.pg_pool_rw, MessageRepository)
            app.state.chat_service = ChatService(
                topic_registry=topic_registry,
                session_repo_factory=sess_factory,
                message_repo_factory=msg_factory,
            )
        else:
            app.state.chat_service = None

        # M2: 把 factory 也挂到 app.state，供 REST history 端点使用
        if app.state.pg_pool_rw is not None:
            app.state.session_repo_factory = sess_factory
            app.state.message_repo_factory = msg_factory
        else:
            app.state.session_repo_factory = None
            app.state.message_repo_factory = None

        health.mark_ready()
        log.info("startup_complete", topics=topic_registry.ids())
        yield
        health.mark_not_ready()
        if app.state.pg_pool_rw is not None:
            await close_pools(app.state.pg_pool_rw, app.state.pg_pool_ro)
        log.info("shutdown_complete")

    app = FastAPI(
        title="MasterAgent",
        version="0.2.0",
        description=(
            "企业级智能助手服务 —— 前端（PC SSE / 移动 WS）与多个第三方 AI Agent 之间的中间桥梁。\n\n"
            "**本地测试提示：**\n"
            "- Swagger UI: `/docs`，ReDoc: `/redoc`，OpenAPI JSON: `/openapi.json`\n"
            "- 对话（Chat）与历史（History）端点需要 DB（`MA_PG_DSN_RW`）；未配置时返回 503\n"
            "- History 端点要求 `X-User-Account` header\n"
            "- 鉴权为占位（trust_header 模式）：信任 `X-User-Account` / `X-User-Role` header\n\n"
            "**WebSocket**（不在此 Swagger 列出，FastAPI 不把 WS 纳入 OpenAPI）：\n"
            "`ws://<host>/api/v1/chat/ws?w3_account=zhangsan&biz_id=daibiao_xiaoguanjia`\n"
            '协议为双向 JSON：客户端发 `{"op":"ask","request_id":"...","thread_id":"...","question":"...","biz_params":{}}`，'
            "服务端推 ready / event / pong / closed 帧。单连接最多 5 并发，45s 无帧关闭。"
        ),
        openapi_tags=_OPENAPI_TAGS,
        lifespan=lifespan,
    )
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(sse.router, prefix="/api/v1")
    app.include_router(rest.router, prefix="/api/v1")
    app.include_router(ws.router, prefix="/api/v1")
    FastAPIInstrumentor().instrument_app(app)
    return app


# 直接运行 main.py 时读取本地 .env（被 import 时不读，保持测试 / uvicorn 原行为）。
# .env 是本地启动的单一环境变量来源；topic YAML 的 ${env:...} 与 OPENAI_API_KEY
# 也由此注入 os.environ。
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")


# uvicorn 入口
app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
