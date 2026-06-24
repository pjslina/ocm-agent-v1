"""通用补全：按白名单从 biz_params / user_ctx 取字段拼前缀。

设计书 §6.9 规则 6：不允许把 user_ctx 整体扔进 prompt；必须白名单。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from ma.core.graph.state import GraphState
from ma.core.plugin.base import EnrichResult
from ma.core.plugin.registry import registry


class GenericEnrichParams(BaseModel):
    fields_to_inject: list[str] = Field(default_factory=list)


@registry.register
class GenericEnrich:
    name = "generic_enrich"
    plugin_kind = "enrich"

    def configure(self, params: dict[str, Any]) -> None:
        p = GenericEnrichParams(**params)
        self._fields = p.fields_to_inject

    async def enrich(self, state: GraphState) -> EnrichResult:
        biz_params = state.get("biz_params", {})
        user_ctx = state.get("user_ctx", {})
        question = state.get("question", "")
        injected: list[str] = []
        prefix_parts: list[str] = []
        for field in self._fields:
            if field in biz_params:
                prefix_parts.append(f"{field}={biz_params[field]}")
                injected.append(field)
            elif field in user_ctx:
                prefix_parts.append(f"{field}={user_ctx[field]}")
                injected.append(field)
        if prefix_parts:
            prefix = "[" + " ".join(prefix_parts) + "] "
        else:
            prefix = ""
        return EnrichResult(
            enriched_question=prefix + question,
            enrich_meta={"injected": injected},
        )
