"""sales_contract_auth：销售合同责任人鉴权（role + contract_id 范围）。"""

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
    sys.modules.pop("ma.plugins.auth.sales_contract_auth", None)
    importlib.import_module("ma.plugins.auth.sales_contract_auth")
    yield new_reg


@pytest.mark.asyncio
async def test_sales_contract_passes_with_owner_role_and_contract() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "sales_contract_auth",
        {"required_role": "SALES_OWNER"},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "SALES_OWNER", "contract_ids": ["C001", "C002"]},
        ),
        "biz_params": {"contract_id": "C001"},
    }
    result = await plugin.authorize(state)
    assert result.passed is True
    assert result.user_ctx is not None
    assert result.user_ctx["role"] == "SALES_OWNER"
    assert result.user_ctx["contract_ids"] == ["C001", "C002"]
    assert result.user_ctx["contract_id"] == "C001"


@pytest.mark.asyncio
async def test_sales_contract_rejects_wrong_role() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "sales_contract_auth",
        {"required_role": "SALES_OWNER"},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "REP", "contract_ids": ["C001"]},
        ),
        "biz_params": {"contract_id": "C001"},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_TOPIC"


@pytest.mark.asyncio
async def test_sales_contract_rejects_contract_not_owned() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "sales_contract_auth",
        {"required_role": "SALES_OWNER"},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "SALES_OWNER", "contract_ids": ["C001"]},
        ),
        "biz_params": {"contract_id": "C999"},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_SCOPE"


@pytest.mark.asyncio
async def test_sales_contract_rejects_missing_contract_id() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "auth",
        "sales_contract_auth",
        {"required_role": "SALES_OWNER"},
    )
    state = {
        "identity": AuthnIdentity(
            w3_account="alice",
            raw_claims={"role": "SALES_OWNER", "contract_ids": ["C001"]},
        ),
        "biz_params": {},
    }
    result = await plugin.authorize(state)
    assert result.passed is False
    assert result.reject_code == "FORBIDDEN_SCOPE"
