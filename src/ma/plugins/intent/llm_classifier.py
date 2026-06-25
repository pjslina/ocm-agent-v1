"""LLM 分类器：用 LangChain 风格的 ChatModel 抽象。

M1 实现：
- provider="fake" → langchain_core.language_models.fake_chat_models.FakeListChatModel
- M3 新增：
  - provider="openai-compatible" → langchain_openai.ChatOpenAI
  - provider="internal" → M4 实现（企业网关）

输出协议：让 LLM 用纯文本回答，第一行 "route: <label>"，第二行可选 "reason: ..."
正则解析 route；解析失败 → IntentFailed。
"""

from __future__ import annotations

import os
import re
from typing import Any, Literal

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import HumanMessage
from pydantic import BaseModel, SecretStr

from ma.core.graph.state import GraphState
from ma.core.plugin.base import IntentFailed, IntentResult
from ma.core.plugin.registry import registry
from ma.infra.logging import get_logger

_log = get_logger("ma.intent.llm_classifier")


class LLMClassifierParams(BaseModel):
    provider: Literal["fake", "openai-compatible", "internal"] = "fake"
    model: str
    labels: list[str]
    prompt_template: str
    fake_responses: list[str] | None = None

    # openai-compatible 参数（M3），仅 provider="openai-compatible" 时生效
    base_url: str | None = None
    api_key: str | None = None


_ROUTE_RE = re.compile(r"^\s*route\s*:\s*([a-zA-Z0-9_-]+)\s*$", re.MULTILINE)
_REASON_RE = re.compile(r"^\s*reason\s*:\s*(.+)$", re.MULTILINE)


def _build_chat_model(params: LLMClassifierParams) -> BaseChatModel:
    if params.provider == "fake":
        if not params.fake_responses:
            raise ValueError("provider='fake' requires non-empty fake_responses")
        return FakeListChatModel(responses=params.fake_responses)

    if params.provider == "openai-compatible":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=params.model,
            base_url=params.base_url,
            api_key=SecretStr(params.api_key or os.environ.get("OPENAI_API_KEY", "not-set")),
            temperature=0.0,
        )

    # provider == "internal" — M4 实现（企业 LLM 网关）
    raise NotImplementedError(
        f"provider={params.provider!r} not implemented (use 'fake' or 'openai-compatible')"
    )


@registry.register
class LLMClassifier:
    name = "llm_classifier"
    plugin_kind = "intent"

    def configure(self, params: dict[str, Any]) -> None:
        p = LLMClassifierParams(**params)
        self._labels = set(p.labels)
        self._llm = _build_chat_model(p)

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
        _log.info("llm_response", text=text)
        m = _ROUTE_RE.search(text)
        if not m:
            raise IntentFailed(f"cannot parse route from LLM response: {text!r}")
        route = m.group(1)
        reason_m = _REASON_RE.search(text)
        reason = reason_m.group(1).strip() if reason_m else ""
        return IntentResult(route=route, confidence=1.0, reason=reason)
