"""Plugin 体系的基础契约。

设计书 §5.2。

四种插件子契约：
- AuthPlugin       业务鉴权（专题 + 问题范围）
- EnrichPlugin     问题补全
- IntentPlugin     意图识别（LLM 分类）
- DownstreamAdapter 下游 API 适配器

GraphState / ChatEvent 类型导入是 Forward Reference，因为 plugin/base.py 在
graph/state.py 之前可能被 import；用 TYPE_CHECKING 保护即可。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ma.core.graph.events import ChatEvent
    from ma.core.graph.state import GraphState


@runtime_checkable
class Plugin(Protocol):
    """所有插件的最低契约。"""

    name: str
    plugin_kind: str

    def configure(self, params: dict[str, Any]) -> None: ...


@dataclass(slots=True)
class AuthResult:
    passed: bool
    reject_code: str | None = None
    reject_message: str | None = None
    user_ctx: dict[str, Any] | None = None


class AuthPlugin(Plugin, Protocol):
    plugin_kind: str = "auth"

    async def authorize(self, state: GraphState) -> AuthResult: ...


@dataclass(slots=True)
class EnrichResult:
    enriched_question: str
    enrich_meta: dict[str, Any] = field(default_factory=dict)


class EnrichPlugin(Plugin, Protocol):
    plugin_kind: str = "enrich"

    async def enrich(self, state: GraphState) -> EnrichResult: ...


@dataclass(slots=True)
class IntentResult:
    route: str
    confidence: float
    reason: str = ""


class IntentFailed(RuntimeError):
    """意图识别失败 —— ChatService 会兜底走 default_route。"""


class IntentPlugin(Plugin, Protocol):
    plugin_kind: str = "intent"

    async def classify(self, state: GraphState) -> IntentResult: ...


class DownstreamAdapter(Plugin, Protocol):
    plugin_kind: str = "adapter"

    def run(self, state: GraphState, *, output: bool = True) -> AsyncIterator[ChatEvent]: ...
