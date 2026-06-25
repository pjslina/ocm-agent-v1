"""TopicCompiler：从 TopicConfig 装配插件 + 编译 biz_params_schema + 解析 env 占位符。"""

from __future__ import annotations

import pytest


def _register_fake_plugins(reg) -> None:
    """在给定 registry 上注册一组 fake 插件供测试用。"""

    @reg.register
    class FakeAuth:
        name = "fake_auth"
        plugin_kind = "auth"

        def configure(self, params: dict) -> None:
            self.role = params["required_role"]

    @reg.register
    class FakeEnrich:
        name = "fake_enrich"
        plugin_kind = "enrich"

        def configure(self, params: dict) -> None:
            self.fields = params.get("fields_to_inject", [])

    @reg.register
    class FakeIntent:
        name = "fake_intent"
        plugin_kind = "intent"

        def configure(self, params: dict) -> None:
            self.labels = params["labels"]

    @reg.register
    class FakeAdapter:
        name = "fake_adapter"
        plugin_kind = "adapter"

        def configure(self, params: dict) -> None:
            self.params = params


_TOPIC = {
    "topic_id": "test_topic",
    "display_name": "测试专题",
    "default_route": "metagc",
    "history": {"max_turns": 10, "scope": "per_topic"},
    "auth": {"plugin": "fake_auth", "params": {"required_role": "TESTER"}},
    "enrich": {"plugin": "fake_enrich", "params": {"fields_to_inject": ["region"]}},
    "intent": {
        "plugin": "fake_intent",
        "params": {
            "model": "fake-model",
            "labels": ["metagc"],
            "prompt_template": "prompts/intent/test.txt",
        },
    },
    "graph": {
        "nodes": [{"id": "metagc_node", "adapter": "fake_adapter", "output": True}],
        "edges": [
            {
                "from": "intent",
                "type": "conditional",
                "route_by": "route",
                "mapping": {"metagc": "metagc_node"},
            }
        ],
    },
    "biz_params_schema": {
        "type": "object",
        "required": ["region"],
        "properties": {"region": {"type": "string"}},
    },
    "error_messages": {
        "FORBIDDEN_TOPIC": "no",
        "FORBIDDEN_SCOPE": "no",
        "DOWNSTREAM_TIMEOUT": "slow",
        "DOWNSTREAM_ERROR": "err",
        "INTERNAL": "boom",
    },
}


@pytest.fixture
def fresh_registry():
    """每个测试用全新 registry —— 直接传给 TopicCompiler 注入，避免全局污染。"""
    from ma.core.plugin.registry import PluginRegistry

    return PluginRegistry()


def test_compile_minimal_topic_instantiates_plugins(fresh_registry) -> None:
    _register_fake_plugins(fresh_registry)
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(_TOPIC)
    compiled = TopicCompiler(fresh_registry).compile(cfg)
    assert compiled.topic_id == "test_topic"
    assert compiled.auth.role == "TESTER"
    assert compiled.enrich.fields == ["region"]
    assert compiled.intent.labels == ["metagc"]
    assert "metagc_node" in compiled.adapters
    assert compiled.error_messages["INTERNAL"] == "boom"


def test_compile_biz_params_schema_to_pydantic(fresh_registry) -> None:
    _register_fake_plugins(fresh_registry)
    from pydantic import ValidationError

    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(_TOPIC)
    compiled = TopicCompiler(fresh_registry).compile(cfg)

    # 合法 params
    parsed = compiled.biz_params_model.model_validate({"region": "huadong"})
    assert parsed.region == "huadong"  # type: ignore[attr-defined]

    # 缺少 required field
    with pytest.raises(ValidationError):
        compiled.biz_params_model.model_validate({})


def test_compile_unknown_plugin_raises(fresh_registry) -> None:
    """auth.plugin 在 registry 里找不到 → 启动期 fail fast。"""
    from ma.core.plugin.registry import PluginNotFound
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    cfg = TopicConfig.model_validate(_TOPIC)  # registry 是空的
    with pytest.raises(PluginNotFound):
        TopicCompiler(fresh_registry).compile(cfg)


def test_topic_registry_stores_compiled_topics(fresh_registry) -> None:
    _register_fake_plugins(fresh_registry)
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig
    from ma.core.topic.registry import TopicNotFound, TopicRegistry

    cfg = TopicConfig.model_validate(_TOPIC)
    compiled = TopicCompiler(fresh_registry).compile(cfg)
    reg = TopicRegistry()
    reg.register(compiled)
    assert reg.get("test_topic") is compiled
    with pytest.raises(TopicNotFound):
        reg.get("ghost")


def test_compile_resolves_env_placeholders(fresh_registry) -> None:
    """${env:VAR} 占位符在 compile 时由 env 参数替换。"""
    _register_fake_plugins(fresh_registry)
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    topic = {
        **_TOPIC,
        "graph": {
            "nodes": [
                {
                    "id": "metagc_node",
                    "adapter": "fake_adapter",
                    "output": True,
                    "params": {"base_url": "${env:META_TEST_URL}"},
                }
            ],
            "edges": _TOPIC["graph"]["edges"],
        },
    }
    cfg = TopicConfig.model_validate(topic)
    compiled = TopicCompiler(fresh_registry).compile(cfg, env={"META_TEST_URL": "http://x.test"})
    assert compiled.adapters["metagc_node"].params == {"base_url": "http://x.test"}


def test_compile_missing_env_raises(fresh_registry) -> None:
    """${env:VAR} 引用了不存在的环境变量 → 启动期 fail fast。"""
    _register_fake_plugins(fresh_registry)
    from ma.core.topic.compiler import TopicCompiler
    from ma.core.topic.config import TopicConfig

    topic = {
        **_TOPIC,
        "graph": {
            "nodes": [
                {
                    "id": "metagc_node",
                    "adapter": "fake_adapter",
                    "output": True,
                    "params": {"base_url": "${env:NOPE}"},
                }
            ],
            "edges": _TOPIC["graph"]["edges"],
        },
    }
    cfg = TopicConfig.model_validate(topic)
    with pytest.raises(KeyError, match="NOPE"):
        TopicCompiler(fresh_registry).compile(cfg, env={})
