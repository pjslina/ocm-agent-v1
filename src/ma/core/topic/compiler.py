"""TopicCompiler：把 TopicConfig 装配成可运行的 CompiledTopic。

CompiledTopic 包含：
- 已 configure 好的插件实例 (auth/enrich/intent/adapters)
- 从 biz_params_schema (JSON Schema dict) 编译出的 Pydantic Model
- error_messages / history policy / default_route 等运行期数据

它 *不* 直接编译 LangGraph StateGraph —— 那是 ChatService 在拿到
CompiledTopic 时即时做的事（Task 20）。这样 compiler 单测可以脱离
LangGraph 跑。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ConfigDict, create_model

from ma.core.plugin.registry import PluginRegistry
from ma.core.plugin.registry import registry as _global_registry
from ma.core.topic.config import HistoryPolicy, RetryPolicy, TopicConfig


@dataclass(slots=False)
class CompiledTopic:
    topic_id: str
    display_name: str
    default_route: str
    history: HistoryPolicy
    auth: Any
    enrich: Any
    intent: Any
    adapters: dict[str, Any]
    biz_params_model: type[BaseModel]
    error_messages: dict[str, str]
    config: TopicConfig
    # M3 新增
    intent_retry: RetryPolicy | None = None
    adapter_retries: dict[str, RetryPolicy] = field(default_factory=dict)


_ENV_PLACEHOLDER = re.compile(r"\$\{env:([A-Z_][A-Z0-9_]*)\}")


def _resolve_env(value: Any, env: dict[str, str]) -> Any:
    """递归把 dict / list / str 里的 ${env:VAR} 替换为环境值。"""
    if isinstance(value, str):

        def _sub(m: re.Match[str]) -> str:
            var = m.group(1)
            v = env.get(var)
            if v is None:
                raise KeyError(f"env var {var!r} required by topic config but unset")
            return v

        return _ENV_PLACEHOLDER.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _resolve_env(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env(v, env) for v in value]
    return value


# JSON Schema → Pydantic Model 的最小翻译（M1 只支持 type=object + string properties）
def _compile_biz_params_schema(schema: dict[str, Any]) -> type[BaseModel]:
    if schema.get("type") != "object":
        raise ValueError(f"biz_params_schema must be type=object (got {schema.get('type')!r})")
    required = set(schema.get("required", []))
    fields: dict[str, Any] = {}
    for name, prop in schema.get("properties", {}).items():
        prop_type = prop.get("type", "string")
        if prop_type != "string":
            raise ValueError(
                f"biz_params_schema property {name!r}: only type='string' supported in M1 "
                f"(got {prop_type!r})"
            )
        if name in required:
            fields[name] = (str, ...)
        else:
            fields[name] = (str | None, None)

    model: type[BaseModel] = create_model(
        "BizParams",
        __config__=ConfigDict(extra="forbid"),
        **fields,
    )
    return model


class TopicCompiler:
    """一次性使用：compile(cfg) → CompiledTopic。"""

    def __init__(self, registry: PluginRegistry | None = None) -> None:
        # registry 可注入便于测试；默认用模块级单例
        self._registry = registry or _global_registry

    def compile(
        self,
        cfg: TopicConfig,
        *,
        env: dict[str, str] | None = None,
    ) -> CompiledTopic:
        envmap = env if env is not None else dict(os.environ)
        auth_params = _resolve_env(cfg.auth.params, envmap)
        enrich_params = _resolve_env(cfg.enrich.params, envmap)
        intent_params = _resolve_env(cfg.intent.params, envmap)

        auth = self._registry.create("auth", cfg.auth.plugin, auth_params)
        enrich = self._registry.create("enrich", cfg.enrich.plugin, enrich_params)
        intent = self._registry.create("intent", cfg.intent.plugin, intent_params)

        adapters: dict[str, Any] = {}
        for node in cfg.graph.nodes:
            adapters[node.id] = self._registry.create(
                "adapter", node.adapter, _resolve_env(node.params, envmap)
            )

        biz_params_model = _compile_biz_params_schema(cfg.biz_params_schema)

        # M3: 提取 retry 配置
        intent_retry = cfg.intent.retry if hasattr(cfg.intent, "retry") else None
        adapter_retries: dict[str, RetryPolicy] = {}
        for node in cfg.graph.nodes:
            if node.retry is not None:
                adapter_retries[node.id] = node.retry

        return CompiledTopic(
            topic_id=cfg.topic_id,
            display_name=cfg.display_name,
            default_route=cfg.default_route,
            history=cfg.history,
            auth=auth,
            enrich=enrich,
            intent=intent,
            adapters=adapters,
            biz_params_model=biz_params_model,
            error_messages=cfg.error_messages,
            config=cfg,
            intent_retry=intent_retry,
            adapter_retries=adapter_retries,
        )
