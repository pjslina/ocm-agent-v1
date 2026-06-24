"""generic_enrich：按白名单从 biz_params/user_ctx 取字段拼前缀。"""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    """每个测试独立 registry。"""
    from ma.core.plugin import registry as registry_module

    new_reg = registry_module.PluginRegistry()
    monkeypatch.setattr(registry_module, "registry", new_reg)
    # 强制模块重新执行，把装饰器副作用打到新 registry 上
    sys.modules.pop("ma.plugins.enrich.generic_enrich", None)
    importlib.import_module("ma.plugins.enrich.generic_enrich")
    yield new_reg


@pytest.mark.asyncio
async def test_enrich_injects_whitelist_fields() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("enrich", "generic_enrich", {"fields_to_inject": ["region"]})
    state = {
        "question": "上个月销售额？",
        "biz_params": {"region": "huadong"},
        "user_ctx": {"role": "REP"},
    }
    out = await plugin.enrich(state)
    assert "huadong" in out.enriched_question
    assert "上个月销售额" in out.enriched_question
    assert out.enrich_meta["injected"] == ["region"]


@pytest.mark.asyncio
async def test_enrich_skips_missing_fields() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create(
        "enrich", "generic_enrich", {"fields_to_inject": ["region", "customer"]}
    )
    state = {"question": "Q", "biz_params": {"region": "huadong"}, "user_ctx": {}}
    out = await plugin.enrich(state)
    assert "region" in out.enrich_meta["injected"]
    assert "customer" not in out.enrich_meta["injected"]


@pytest.mark.asyncio
async def test_enrich_no_fields_keeps_question_unchanged() -> None:
    from ma.core.plugin.registry import registry

    plugin = registry.create("enrich", "generic_enrich", {"fields_to_inject": []})
    state = {"question": "Q", "biz_params": {}, "user_ctx": {}}
    out = await plugin.enrich(state)
    assert out.enriched_question == "Q"
    assert out.enrich_meta["injected"] == []
