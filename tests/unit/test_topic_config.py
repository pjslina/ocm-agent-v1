"""TopicConfig Pydantic schema：合法 YAML 解析 + 非法 YAML fail-fast。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

_MIN_VALID = {
    "topic_id": "daibiao_xiaoguanjia",
    "display_name": "代表小管家",
    "default_route": "metagc",
    "history": {"max_turns": 10, "scope": "per_topic"},
    "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
    "enrich": {"plugin": "generic_enrich", "params": {}},
    "intent": {
        "plugin": "llm_classifier",
        "params": {
            "model": "fake-model",
            "labels": ["metagc"],
            "prompt_template": "prompts/intent/representative.txt",
        },
    },
    "graph": {
        "nodes": [
            {"id": "metagc_adapter", "adapter": "metagc", "output": True},
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
    "biz_params_schema": {"type": "object", "required": [], "properties": {}},
    "error_messages": {
        "FORBIDDEN_TOPIC": "您暂无权限",
        "FORBIDDEN_SCOPE": "范围不允许",
        "DOWNSTREAM_TIMEOUT": "稍后再试",
        "DOWNSTREAM_ERROR": "查询失败",
        "INTERNAL": "服务异常",
    },
}


def test_topic_config_minimal_valid_loads() -> None:
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(_MIN_VALID)
    assert cfg.topic_id == "daibiao_xiaoguanjia"
    assert cfg.default_route == "metagc"
    assert cfg.history.max_turns == 10
    assert cfg.auth.plugin == "representative_auth"
    assert len(cfg.graph.nodes) == 1


def test_topic_config_missing_required_field_fails() -> None:
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID}
    del bad["topic_id"]
    with pytest.raises(ValidationError):
        TopicConfig.model_validate(bad)


def test_topic_config_default_route_must_match_a_route_or_default() -> None:
    """default_route 必须是 graph.edges.mapping 的 keys 之一或 'default'。"""
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID, "default_route": "uniioc"}  # mapping 只有 metagc
    with pytest.raises(ValidationError, match="default_route"):
        TopicConfig.model_validate(bad)


def test_topic_config_history_scope_literal() -> None:
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID, "history": {"max_turns": 10, "scope": "invalid"}}
    with pytest.raises(ValidationError):
        TopicConfig.model_validate(bad)


def test_topic_config_error_messages_required_keys_present() -> None:
    """5 个必填 error_messages 缺一个就报错（设计书 §6.6.3）。"""
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID}
    bad_err = dict(_MIN_VALID["error_messages"])
    del bad_err["INTERNAL"]
    bad["error_messages"] = bad_err
    with pytest.raises(ValidationError, match="INTERNAL"):
        TopicConfig.model_validate(bad)


def test_topic_config_intent_labels_match_mapping_keys() -> None:
    """intent.params.labels 必须等于 graph.edges.mapping 的 keys。"""
    from ma.core.topic.config import TopicConfig

    bad = {**_MIN_VALID}
    bad["intent"] = {**_MIN_VALID["intent"]}
    bad["intent"]["params"] = {
        **_MIN_VALID["intent"]["params"],
        "labels": ["metagc", "uniioc"],  # uniioc 没在 mapping 里
    }
    with pytest.raises(ValidationError, match="labels"):
        TopicConfig.model_validate(bad)


def test_topic_config_with_retry_policy() -> None:
    """验证 YAML 中 node.retry 字段可被正确解析。"""
    from ma.core.topic.config import TopicConfig

    data = {
        "topic_id": "test",
        "display_name": "Test",
        "default_route": "metagc",
        "history": {"max_turns": 5, "scope": "per_topic"},
        "auth": {"plugin": "representative_auth", "params": {"required_role": "REP"}},
        "enrich": {"plugin": "generic_enrich", "params": {"fields_to_inject": ["region"]}},
        "intent": {
            "plugin": "llm_classifier",
            "params": {
                "provider": "fake",
                "model": "m",
                "labels": ["metagc"],
                "prompt_template": "p.txt",
                "fake_responses": ["route: metagc\nreason: ok"],
            },
            "retry": {"max_attempts": 3, "backoff_ms": 500},
        },
        "graph": {
            "nodes": [
                {
                    "id": "metagc_adapter",
                    "adapter": "metagc",
                    "output": True,
                    "params": {"base_url": "http://x", "timeout_ms": 5000},
                    "retry": {"max_attempts": 2, "backoff_ms": 1000},
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
            "FORBIDDEN_TOPIC": "x",
            "FORBIDDEN_SCOPE": "x",
            "DOWNSTREAM_TIMEOUT": "x",
            "DOWNSTREAM_ERROR": "x",
            "INTERNAL": "x",
        },
    }
    cfg = TopicConfig.model_validate(data)
    assert cfg.intent.retry is not None
    assert cfg.intent.retry.max_attempts == 3
    assert cfg.intent.retry.backoff_ms == 500
    assert cfg.graph.nodes[0].retry is not None
    assert cfg.graph.nodes[0].retry.max_attempts == 2
    assert cfg.graph.nodes[0].retry.backoff_ms == 1000


def test_retry_policy_defaults() -> None:
    """RetryPolicy 默认值：不 retry（max_attempts=1, backoff_ms=1000）。"""
    from ma.core.topic.config import RetryPolicy

    r = RetryPolicy()
    assert r.max_attempts == 1
    assert r.backoff_ms == 1000
