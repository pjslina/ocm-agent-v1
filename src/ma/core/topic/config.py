"""Topic YAML 的 Pydantic schema。

设计书 §2.4。任何不合法配置 → 启动时 ValidationError → fail fast。
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class HistoryPolicy(BaseModel):
    max_turns: int = Field(ge=1, le=200)
    scope: Literal["per_topic"] = "per_topic"


class PluginRef(BaseModel):
    plugin: str
    params: dict[str, Any] = Field(default_factory=dict)


class RetryPolicy(BaseModel):
    """M3: 节点级可配置重试策略。"""

    max_attempts: int = Field(ge=1, le=5, default=1)
    backoff_ms: int = Field(ge=0, le=30000, default=1000)


class IntentRef(BaseModel):
    plugin: str
    params: dict[str, Any]
    retry: RetryPolicy | None = None  # M3: intent 节点也可 retry


class GraphNode(BaseModel):
    id: str
    adapter: str
    params: dict[str, Any] = Field(default_factory=dict)
    output: bool = True
    retry: RetryPolicy | None = None  # M3: 可选 retry 配置


class ConditionalEdge(BaseModel):
    src: Literal["intent"] = Field(alias="from")
    type: Literal["conditional"]
    route_by: Literal["route"]
    mapping: dict[str, str]


class GraphSpec(BaseModel):
    nodes: list[GraphNode]
    edges: list[ConditionalEdge]


_REQUIRED_ERROR_CODES = (
    "FORBIDDEN_TOPIC",
    "FORBIDDEN_SCOPE",
    "DOWNSTREAM_TIMEOUT",
    "DOWNSTREAM_ERROR",
    "INTERNAL",
)


class TopicConfig(BaseModel):
    topic_id: str = Field(min_length=1)
    display_name: str
    default_route: str
    history: HistoryPolicy
    auth: PluginRef
    enrich: PluginRef
    intent: IntentRef
    graph: GraphSpec
    biz_params_schema: dict[str, Any]
    error_messages: dict[str, str]

    @model_validator(mode="after")
    def _validate_consistency(self) -> TopicConfig:
        # 1) error_messages 必须含所有必填 code
        missing = [c for c in _REQUIRED_ERROR_CODES if c not in self.error_messages]
        if missing:
            raise ValueError(f"error_messages missing required codes: {missing}")

        # 2) 找 conditional edge 的 mapping keys
        cond_edges = [e for e in self.graph.edges if e.type == "conditional"]
        if not cond_edges:
            raise ValueError("graph.edges must contain at least one conditional edge")
        mapping_keys = set()
        node_ids = {n.id for n in self.graph.nodes}
        for e in cond_edges:
            for label, node_id in e.mapping.items():
                mapping_keys.add(label)
                if node_id not in node_ids:
                    raise ValueError(f"edge mapping points to non-existent node: {node_id!r}")

        # 3) default_route 必须在 mapping_keys 或者是 'default' / 'reject'
        if self.default_route not in mapping_keys and self.default_route not in {
            "default",
            "reject",
        }:
            raise ValueError(
                f"default_route={self.default_route!r} must be one of {mapping_keys} "
                f"or 'default' / 'reject'"
            )

        # 4) intent.params.labels 必须与 mapping_keys 完全一致
        labels = self.intent.params.get("labels")
        if labels is None:
            raise ValueError("intent.params.labels required (list of route labels)")
        if set(labels) != mapping_keys:
            raise ValueError(
                f"intent.params.labels={set(labels)!r} must equal "
                f"graph.edges.mapping keys={mapping_keys!r}"
            )

        return self
