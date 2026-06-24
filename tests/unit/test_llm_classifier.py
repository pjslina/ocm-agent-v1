"""llm_classifier：FakeListChatModel 输出固定 text，解析出 route。"""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    sys.modules.pop("ma.plugins.intent.llm_classifier", None)
    importlib.import_module("ma.plugins.intent.llm_classifier")
    yield new_reg


@pytest.mark.asyncio
async def test_llm_classifier_fake_route_extraction() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "intent",
        "llm_classifier",
        {
            "provider": "fake",
            "model": "fake-model",
            "labels": ["metagc", "kc"],
            "prompt_template": "prompts/intent/test.txt",
            "fake_responses": ["route: metagc\nreason: 业务数据查询"],
        },
    )
    state = {"enriched_question": "上个月销售额？"}
    result = await plugin.classify(state)
    assert result.route == "metagc"
    assert "业务数据" in result.reason


@pytest.mark.asyncio
async def test_llm_classifier_invalid_response_raises() -> None:
    """LLM 返回的 text 找不到 route → IntentFailed。"""
    from ma.core.plugin.base import IntentFailed
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "intent",
        "llm_classifier",
        {
            "provider": "fake",
            "model": "fake-model",
            "labels": ["metagc"],
            "prompt_template": "prompts/intent/test.txt",
            "fake_responses": ["totally unparseable garbage"],
        },
    )
    with pytest.raises(IntentFailed):
        await plugin.classify({"enriched_question": "?"})


@pytest.mark.asyncio
async def test_llm_classifier_unknown_provider_at_configure() -> None:
    """非 fake / openai-compatible / internal → 启动期 fail fast。"""
    from pydantic import ValidationError

    from ma.core.plugin.registry import registry

    with pytest.raises(ValidationError):
        registry.create(
            "intent",
            "llm_classifier",
            {
                "provider": "unknown",
                "model": "x",
                "labels": ["metagc"],
                "prompt_template": "p.txt",
            },
        )
