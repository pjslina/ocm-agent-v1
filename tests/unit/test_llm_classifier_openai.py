"""LLMClassifier openai-compatible provider 单元测试。"""

from __future__ import annotations

import pytest


def test_openai_compatible_params_parse() -> None:
    """验证 openai-compatible provider 的参数解析（不调真实 API）。"""
    from ma.plugins.intent.llm_classifier import LLMClassifierParams

    p = LLMClassifierParams(
        provider="openai-compatible",
        model="gpt-4o-mini",
        labels=["metagc", "uniioc"],
        prompt_template="p.txt",
        base_url="https://llm.internal.example/v1",
        api_key="test-key",
    )
    assert p.provider == "openai-compatible"
    assert p.model == "gpt-4o-mini"
    assert p.labels == ["metagc", "uniioc"]


def test_internal_provider_raises_not_implemented() -> None:
    """provider='internal' 尚未实现（M4）。"""
    from ma.plugins.intent.llm_classifier import LLMClassifierParams, _build_chat_model

    p = LLMClassifierParams(
        provider="internal",
        model="internal-model",
        labels=["metagc"],
        prompt_template="p.txt",
    )
    with pytest.raises(NotImplementedError, match="internal"):
        _build_chat_model(p)


def test_fake_provider_still_works() -> None:
    """确保 provider='fake' 仍然可用（M1/M2 行为不变）。"""
    from ma.plugins.intent.llm_classifier import LLMClassifierParams, _build_chat_model

    p = LLMClassifierParams(
        provider="fake",
        model="fake-model",
        labels=["metagc"],
        prompt_template="p.txt",
        fake_responses=["route: metagc\nreason: ok"],
    )
    llm = _build_chat_model(p)
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    assert isinstance(llm, FakeListChatModel)
