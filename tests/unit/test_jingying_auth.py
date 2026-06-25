"""jingying_auth：经营小助手鉴权（role + business_unit 范围）。"""

from __future__ import annotations

import importlib
import sys

import pytest

from ma.core.graph.state import AuthnIdentity


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    sys.modules.pop("ma.plugins.auth.jingying_auth", None)
    importlib.import_module("ma.plugins.auth.jingying_auth")
    yield new_reg


@pytest.mark.asyncio
async def test_jingying_passes_with_allowed_role_and_unit() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "jingying_auth",
        {"allowed_roles": ["MANAGER", "REP"]},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "MANAGER", "business_units": ["bu_a", "bu_b"]},
        ),
        "biz_params": {"business_unit": "bu_a"},
    }
    result = await plugin.authorize(state)
    assert result.passed is True
    assert result.user_ctx is not None
    assert result.user_ctx["role"] == "MANAGER"
    assert result.user_ctx["business_units"] == ["bu_a", "bu_b"]
    assert result.user_ctx["business_unit"] == "bu_a"


@pytest.mark.asyncio
async def test_jingying_rejects_role_not_in_allowed() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "jingying_auth",
        {"allowed_roles": ["MANAGER", "REP"]},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="bob",
            raw_claims={"role": "VIEWER", "business_units": ["bu_a"]},
        ),
        "biz_params": {"business_unit": "bu_a"},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_TOPIC"


@pytest.mark.asyncio
async def test_jingying_rejects_business_unit_not_authorized() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "jingying_auth",
        {"allowed_roles": ["MANAGER"]},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "MANAGER", "business_units": ["bu_a"]},
        ),
        "biz_params": {"business_unit": "bu_z"},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_SCOPE"


@pytest.mark.asyncio
async def test_jingying_rejects_missing_business_unit() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "jingying_auth",
        {"allowed_roles": ["MANAGER"]},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "MANAGER", "business_units": ["bu_a"]},
        ),
        "biz_params": {},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_SCOPE"
