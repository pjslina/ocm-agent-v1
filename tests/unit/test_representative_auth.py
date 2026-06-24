"""representative_auth：基于 GraphState.identity.raw_claims["role"] 决定通过/拒绝。"""

from __future__ import annotations

import pytest

from ma.core.graph.state import AuthnIdentity


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    """每个测试独立 registry，避免跨用例污染。"""
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    # 重新 import 触发装饰器 —— 先丢掉 sys.modules 缓存，确保模块体只跑一次
    import importlib
    import sys

    sys.modules.pop("ma.plugins.auth.representative_auth", None)
    importlib.import_module("ma.plugins.auth.representative_auth")
    yield new_reg


@pytest.mark.asyncio
async def test_auth_passes_with_matching_role() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(w3_account="alice", raw_claims={"role": "REP"}),
    }
    result = await plugin.authorize(state)
    assert result.passed is True
    assert result.user_ctx is not None
    assert result.user_ctx["role"] == "REP"


@pytest.mark.asyncio
async def test_auth_rejects_with_wrong_role() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(w3_account="bob", raw_claims={"role": "VIEWER"}),
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_TOPIC"
    assert result.reject_message is not None


@pytest.mark.asyncio
async def test_auth_rejects_when_no_role_claim() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("auth", "representative_auth", {"required_role": "REP"})
    state = {
        "identity": AuthnIdentity(w3_account="ghost", raw_claims={}),
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_TOPIC"
