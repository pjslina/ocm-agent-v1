"""Plugin 基础契约：Protocol 形态、4 个子契约、AuthResult/EnrichResult/IntentResult dataclasses。"""

from __future__ import annotations

import pytest


def test_plugin_protocol_has_required_fields() -> None:
    from ma.core.plugin.base import Plugin

    # Plugin 是 runtime_checkable Protocol
    class _OK:
        name = "x"
        plugin_kind = "auth"

        def configure(self, params: dict) -> None: ...

    assert isinstance(_OK(), Plugin)


def test_plugin_protocol_rejects_missing_fields() -> None:
    from ma.core.plugin.base import Plugin

    class _NoName:
        plugin_kind = "auth"

        def configure(self, params: dict) -> None: ...

    assert not isinstance(_NoName(), Plugin)


def test_auth_result_dataclass() -> None:
    from ma.core.plugin.base import AuthResult

    r = AuthResult(passed=True, user_ctx={"role": "rep"})
    assert r.passed is True
    assert r.user_ctx == {"role": "rep"}
    assert r.reject_code is None
    assert r.reject_message is None


def test_auth_result_rejection() -> None:
    from ma.core.plugin.base import AuthResult

    r = AuthResult(
        passed=False,
        reject_code="FORBIDDEN_TOPIC",
        reject_message="您暂无权限",
    )
    assert r.passed is False
    assert r.reject_code == "FORBIDDEN_TOPIC"
    assert r.user_ctx is None


def test_enrich_result_dataclass() -> None:
    from ma.core.plugin.base import EnrichResult

    r = EnrichResult(enriched_question="补全后", enrich_meta={"injected": ["region"]})
    assert r.enriched_question == "补全后"
    assert r.enrich_meta == {"injected": ["region"]}


def test_intent_result_dataclass() -> None:
    from ma.core.plugin.base import IntentResult

    r = IntentResult(route="metagc", confidence=0.92, reason="业务数据查询")
    assert r.route == "metagc"
    assert r.confidence == 0.92


def test_intent_failed_exception() -> None:
    from ma.core.plugin.base import IntentFailed

    with pytest.raises(IntentFailed):
        raise IntentFailed("LLM timeout")
