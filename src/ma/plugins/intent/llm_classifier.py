"""LLM 分类器：用 LangChain 风格的 ChatModel 抽象。

M1 实现：
- provider="fake" → langchain_core.language_models.fake_chat_models.FakeListChatModel
- 其它 provider → M3 实现；M1 raise NotImplementedError

输出协议：让 LLM 用纯文本回答，第一行 "route: <label>"，第二行可选 "reason: ..."
正则解析 route；解析失败 → IntentFailed。
"""

from __future__ import annotations

import re
from typing import Any, Literal

from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import IntentFailed, IntentResult
from ma.core.plugin.registry import registry


class LLMClassifierParams(BaseModel):
    provider: Literal["fake", "openai-compatible", "internal"] = "fake"
    model: str
    labels: list[str]
    prompt_template: str  # M1 不读模板文件，留着字段作为 schema 校验占位
    fake_responses: list[str] | None = None  # provider=fake 时必填


_ROUTE_RE = re.compile(r"^\s*route\s*:\s*([a-zA-Z0-9_-]+)\s*$", re.MULTILINE)
_REASON_RE = re.compile(r"^\s*reason\s*:\s*(.+)$", re.MULTILINE)


@registry.register
class LLMClassifier:
    name = "llm_classifier"
    plugin_kind = "intent"

    def configure(self, params: dict[str, Any]) -> None:
        p = LLMClassifierParams(**params)
        self._labels = set(p.labels)
        if p.provider == "fake":
            if not p.fake_responses:
                raise ValueError("provider='fake' requires non-empty fake_responses")
            self._llm = FakeListChatModel(responses=p.fake_responses)
        else:
            raise NotImplementedError(
                f"provider={p.provider!r} not implemented in M1 (use 'fake'; "
                f"real LLM接入 is M3 work)"
            )

    async def classify(self, state: GraphState) -> IntentResult:
        question = state.get("enriched_question") or state.get("question", "")
        prompt = (
            "你是路由分类器。返回纯文本，第一行 'route: <label>'，"
            "第二行可选 'reason: ...'\n"
            f"可选 label: {', '.join(sorted(self._labels))}\n\n"
            f"用户问题：{question}"
        )
        response = await self._llm.ainvoke([HumanMessage(content=prompt)])
        text = response.content if isinstance(response.content, str) else str(response.content)
        m = _ROUTE_RE.search(text)
        if not m:
            raise IntentFailed(f"cannot parse route from LLM response: {text!r}")
        route = m.group(1)
        reason_m = _REASON_RE.search(text)
        reason = reason_m.group(1).strip() if reason_m else ""
        return IntentResult(route=route, confidence=1.0, reason=reason)
