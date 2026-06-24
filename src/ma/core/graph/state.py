"""GraphState：LangGraph 图执行的共享状态对象。

设计书 §2.1。M0 阶段先把字段全声明出来，让后续里程碑的节点能直接 import；
LangGraph 真正用它（编译图、跑节点）是 M1 的任务。
"""

from __future__ import annotations

from operator import add
from typing import Annotated, Any, Literal, TypedDict

Route = Literal["metagc", "uniioc", "kc", "reject", "default"]


class GraphState(TypedDict, total=False):
    # ── 输入区 ─────────────────────────
    request_id: str
    biz_id: str
    thread_id: str
    identity: Any  # AuthnIdentity，M5+ 落地具体类型
    question: str
    biz_params: dict[str, Any]
    history: list[Any]  # list[Message]，M1 落地 Message 模型

    # ── 鉴权区 ─────────────────────────
    user_ctx: dict[str, Any]
    auth: Any  # AuthResult，M1 落地

    # ── 补全区 ─────────────────────────
    enriched_question: str
    enrich_meta: dict[str, Any]

    # ── 路由区 ─────────────────────────
    route: Route
    route_confidence: float
    route_reason: str

    # ── 下游输出区 ────────────────────────
    answer_chunks: Annotated[list[str], add]
    answer_meta: dict[str, Any]

    # ── 错误区 ─────────────────────────
    errors: Annotated[list[dict[str, Any]], add]
