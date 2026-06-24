"""ChatService 单测：用 fake repo factory + fake plugins，跑真 LangGraph。"""

from __future__ import annotations

import importlib
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from unittest.mock import patch

import pytest

from ma.core.graph.events import ChatEvent
from ma.core.graph.state import AuthnIdentity
from ma.core.repo.models import Message, Session


@dataclass
class _FakeSessionRepo:
    sessions: dict[str, Session] = field(default_factory=dict)

    async def get_or_create(
        self, *, thread_id: str, biz_id: str, w3_account: str, ext: Any = None
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

    async def touch(self, thread_id: str) -> None: ...


@dataclass
class _FakeMessageRepo:
    messages: list[Message] = field(default_factory=list)

    async def list_recent(self, *, thread_id: str, limit: int) -> list[Message]:
        msgs = [m for m in self.messages if m.thread_id == thread_id]
        return list(reversed(msgs))[:limit]

    async def append(self, *, msg: Message) -> None:
        if msg.seq == 0:
            msg.seq = sum(1 for m in self.messages if m.thread_id == msg.thread_id) + 1
        self.messages.append(msg)


class _FakeFactory:
    def __init__(self, repo: Any) -> None:
        self._repo = repo

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Any]:
        yield None

    def build(self, conn: Any) -> Any:
        return self._repo


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    for modname in [
        "ma.plugins.auth.representative_auth",
        "ma.plugins.enrich.generic_enrich",
        "ma.plugins.intent.llm_classifier",
        "ma.plugins.adapter.metagc",
    ]:
        sys.modules.pop(modname, None)
        importlib.import_module(modname)
    yield new_reg


def _build_topic(metagc_url: str):
    """编译代表小管家 topic，base_url 注入测试用 url。"""
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(
        {
            "topic_id": "daibiao_xiaoguanjia",
            "display_name": "代表小管家",
            "default_route": "metagc",
            "history": {"max_turns": 5, "scope": "per_topic"},
            "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
            "enrich": {
                "plugin": "generic_enrich",
                "params": {"fields_to_inject": ["region"]},
            },
            "intent": {
                "plugin": "llm_classifier",
                "params": {
                    "provider": "fake",
                    "model": "fake-model",
                    "labels": ["metagc"],
                    "prompt_template": "p.txt",
                    "fake_responses": ["route: metagc\nreason: 业务"],
                },
            },
            "graph": {
                "nodes": [
                    {
                        "id": "metagc_adapter",
                        "adapter": "metagc",
                        "output": True,
                        "params": {"base_url": metagc_url, "timeout_ms": 5000},
                    }
                ],
                "edges": [
                    {
                        "from": "intent",
                        "type": "conditional",
                        "route_by": "route",
                        "mapping": {"metagc": "metagc_adapter"},
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
    return TopicCompiler().compile(cfg, env={})


@pytest.mark.asyncio
async def test_chat_service_happy_path():
    """走完整流程：auth 通过 → enrich → intent → metagc → persist → done。"""
    from ma.core.chat_service import ChatRequest, ChatService
    from ma.core.topic.registry import TopicRegistry

    topic = _build_topic("http://unused.test")
    reg = TopicRegistry()
    reg.register(topic)

    session_repo = _FakeSessionRepo()
    message_repo = _FakeMessageRepo()
    session_factory = _FakeFactory(session_repo)
    message_factory = _FakeFactory(message_repo)

    svc = ChatService(
        topic_registry=reg,
        session_repo_factory=session_factory,
        message_repo_factory=message_factory,
    )

    async def fake_run(self, state, *, output: bool):
        yield ChatEvent(type="delta", data={"content": "hello "})
        yield ChatEvent(type="delta", data={"content": "world"})

    with patch(
        "ma.plugins.adapter.metagc.MetaGCAdapter.run",
        new=fake_run,
    ):
        req = ChatRequest(
            biz_id="daibiao_xiaoguanjia",
            thread_id="th_happy",
            w3_account="alice",
            question="销售额？",
            request_id="req_happy",
            biz_params={"region": "huadong"},
            identity=AuthnIdentity(w3_account="alice", raw_claims={"role": "REP"}),
        )
        events = [ev async for ev in svc.handle(req)]

    types = [e.type for e in events]
    assert types[0] == "meta"
    assert types[-1] == "done"
    assert "delta" in types
    deltas = [e for e in events if e.type == "delta"]
    full = "".join(e.data["content"] for e in deltas)
    assert full == "hello world"

    # 鉴权通过 → 不应有 error 事件
    err_evs = [e for e in events if e.type == "error"]
    assert err_evs == []


@pytest.mark.asyncio
async def test_chat_service_auth_failure_yields_done():
    """鉴权失败 → reject 节点 → persist → done。M1 简化：reject 不 dispatch delta。"""
    from ma.core.chat_service import ChatRequest, ChatService
    from ma.core.topic.registry import TopicRegistry

    topic = _build_topic("http://unused.test")
    reg = TopicRegistry()
    reg.register(topic)
    svc = ChatService(
        topic_registry=reg,
        session_repo_factory=_FakeFactory(_FakeSessionRepo()),
        message_repo_factory=_FakeFactory(_FakeMessageRepo()),
    )
    req = ChatRequest(
        biz_id="daibiao_xiaoguanjia",
        thread_id="th_rej",
        w3_account="bob",
        question="?",
        request_id="req_rej",
        biz_params={"region": "huadong"},
        identity=AuthnIdentity(w3_account="bob", raw_claims={"role": "VIEWER"}),
    )
    events = [ev async for ev in svc.handle(req)]
    # M1 简化：reject 节点写 answer_chunks 但不 dispatch_custom_event delta，前端看不到话术
    assert events[0].type == "meta"
    assert events[-1].type == "done"


@pytest.mark.asyncio
async def test_chat_service_unknown_topic_returns_error_done():
    """未知 biz_id → error event + done。"""
    from ma.core.chat_service import ChatRequest, ChatService
    from ma.core.topic.registry import TopicRegistry

    svc = ChatService(
        topic_registry=TopicRegistry(),  # empty
        session_repo_factory=_FakeFactory(_FakeSessionRepo()),
        message_repo_factory=_FakeFactory(_FakeMessageRepo()),
    )
    req = ChatRequest(
        biz_id="ghost_topic",
        thread_id="th",
        w3_account="alice",
        question="?",
        request_id="req",
        identity=AuthnIdentity(w3_account="alice"),
    )
    events = [ev async for ev in svc.handle(req)]
    types = [e.type for e in events]
    assert types == ["error", "done"]
    assert events[0].data["code"] == "TOPIC_NOT_FOUND"
