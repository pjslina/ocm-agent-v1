"""Stream-contract 测试基础设施：直接调 ChatService.handle 收集 ChatEvent。"""

from __future__ import annotations

import importlib
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pytest

from ma.core.chat_service import ChatRequest, ChatService
from ma.core.graph.events import ChatEvent
from ma.core.graph.state import AuthnIdentity
from ma.core.repo.models import Message, Session


@dataclass
class FakeSessionRepo:
    sessions: dict[str, Session] = field(default_factory=dict)

    async def get_or_create(
        self, *, thread_id: str, biz_id: str, w3_account: str, ext=None
    ) -> Session:
        if thread_id in self.sessions:
            existing = self.sessions[thread_id]
            if existing.biz_id != biz_id:
                from ma.core.repo.models import SessionBizMismatchError

                raise SessionBizMismatchError(
                    f"thread_id={thread_id} already bound to biz_id={existing.biz_id}, "
                    f"cannot reuse with biz_id={biz_id}"
                )
            return existing
        self.sessions[thread_id] = Session(
            thread_id=thread_id,
            biz_id=biz_id,
            w3_account=w3_account,
            title=None,
            status="active",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_message_at=None,
            ext=ext or {},
        )
        return self.sessions[thread_id]

    async def get_by_thread(self, *, thread_id: str) -> Session | None:
        return self.sessions.get(thread_id)

    async def touch(self, thread_id: str) -> None: ...


@dataclass
class FakeMessageRepo:
    messages: list[Message] = field(default_factory=list)

    async def list_recent(self, *, thread_id: str, limit: int) -> list[Message]:
        msgs = [m for m in self.messages if m.thread_id == thread_id]
        return list(reversed(msgs))[:limit]

    async def append(self, *, msg: Message) -> None:
        if msg.seq == 0:
            msg.seq = sum(1 for m in self.messages if m.thread_id == msg.thread_id) + 1
        self.messages.append(msg)


class FakeFactory:
    def __init__(self, repo: Any) -> None:
        self._repo = repo

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Any]:
        yield None

    def build(self, conn: Any) -> Any:
        return self._repo


def _topic_config(intent_responses: list[str] | None = None):
    from ma.core.topic.config import TopicConfig

    return TopicConfig.model_validate(
        {
            "topic_id": "test_topic",
            "display_name": "测试",
            "default_route": "metagc",
            "history": {"max_turns": 5, "scope": "per_topic"},
            "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
            "enrich": {"plugin": "generic_enrich", "params": {"fields_to_inject": ["region"]}},
            "intent": {
                "plugin": "llm_classifier",
                "params": {
                    "provider": "fake",
                    "model": "fake-model",
                    "labels": ["metagc", "uniioc", "knowledge_center"],
                    "prompt_template": "p.txt",
                    "fake_responses": intent_responses
                    or [
                        "route: metagc\nreason: ok",
                        "route: uniioc\nreason: ok",
                        "route: knowledge_center\nreason: ok",
                    ],
                },
                "retry": {"max_attempts": 1, "backoff_ms": 0},
            },
            "graph": {
                "nodes": [
                    {
                        "id": "metagc_adapter",
                        "adapter": "metagc",
                        "output": True,
                        "params": {"base_url": "http://unused", "timeout_ms": 5000},
                        "retry": {"max_attempts": 1, "backoff_ms": 0},
                    },
                    {
                        "id": "uniioc_adapter",
                        "adapter": "uniioc",
                        "output": True,
                        "params": {"base_url": "http://unused"},
                    },
                    {
                        "id": "kc_adapter",
                        "adapter": "knowledge_center",
                        "output": True,
                        "params": {"ws_url": "ws://unused"},
                    },
                ],
                "edges": [
                    {
                        "from": "intent",
                        "type": "conditional",
                        "route_by": "route",
                        "mapping": {
                            "metagc": "metagc_adapter",
                            "uniioc": "uniioc_adapter",
                            "knowledge_center": "kc_adapter",
                        },
                    }
                ],
            },
            "biz_params_schema": {
                "type": "object",
                "required": ["region"],
                "properties": {"region": {"type": "string"}},
            },
            "error_messages": {
                "FORBIDDEN_TOPIC": "无权限",
                "FORBIDDEN_SCOPE": "范围",
                "DOWNSTREAM_TIMEOUT": "慢",
                "DOWNSTREAM_ERROR": "错",
                "INTERNAL": "炸",
            },
        }
    )


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    # 让 compiler 模块下次 import 时重新捕获 `_global_registry`，
    # 否则它会缓存第一次加载时的 registry 引用，污染后续 unit 测试。
    sys.modules.pop("ma.core.topic.compiler", None)
    for modname in [
        "ma.plugins.auth.representative_auth",
        "ma.plugins.enrich.generic_enrich",
        "ma.plugins.intent.llm_classifier",
        "ma.plugins.adapter.metagc",
        "ma.plugins.adapter.uniioc",
        "ma.plugins.adapter.knowledge_center",
    ]:
        sys.modules.pop(modname, None)
        importlib.import_module(modname)
    yield new_reg
    # 清掉本次导入留下的状态，避免下一个 test 文件的 fixture 拿到陈旧引用。
    sys.modules.pop("ma.core.topic.compiler", None)
    for modname in [
        "ma.plugins.auth.representative_auth",
        "ma.plugins.enrich.generic_enrich",
        "ma.plugins.intent.llm_classifier",
        "ma.plugins.adapter.metagc",
        "ma.plugins.adapter.uniioc",
        "ma.plugins.adapter.knowledge_center",
    ]:
        sys.modules.pop(modname, None)


@pytest.fixture
def fake_repos():
    return FakeSessionRepo(), FakeMessageRepo()


@pytest.fixture
def make_service(fake_repos, fresh_registry):
    def _make(intent_responses=None) -> tuple[ChatService, tuple]:
        # 延迟 import：compiler 模块在 import 时把 `registry` 绑定为
        # `_global_registry`。我们依赖 fresh_registry fixture 先 monkeypatch
        # 了 registry_module.registry，所以这里才能让 compiler 抓到正确的实例。
        from ma.core.topic.compiler import TopicCompiler
        from ma.core.topic.registry import TopicRegistry

        cfg = _topic_config(intent_responses)
        compiled = TopicCompiler(registry=fresh_registry).compile(cfg, env={})
        reg = TopicRegistry()
        reg.register(compiled)
        sess_repo, msg_repo = fake_repos
        svc = ChatService(
            topic_registry=reg,
            session_repo_factory=FakeFactory(sess_repo),
            message_repo_factory=FakeFactory(msg_repo),
        )
        return svc, (sess_repo, msg_repo)

    return _make


def make_request(
    *,
    thread_id: str = "th_test",
    role: str = "REP",
    region: str = "huadong",
    question: str = "上个月销售？",
    request_id: str = "req_test",
) -> ChatRequest:
    return ChatRequest(
        biz_id="test_topic",
        thread_id=thread_id,
        w3_account="alice",
        question=question,
        request_id=request_id,
        biz_params={"region": region},
        identity=AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": role, "regions": [region]},
        ),
    )


async def collect(svc: ChatService, req: ChatRequest) -> list[ChatEvent]:
    return [ev async for ev in svc.handle(req)]
