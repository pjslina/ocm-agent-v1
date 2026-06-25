"""GraphState：LangGraph 图执行的共享状态对象（设计书 §2.1）。

M1 版：字段类型从 Any 落实为具体类型。所有节点共用此 state。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from operator import add
from typing import Annotated, Any, Literal, TypedDict

from ma.core.plugin.base import AuthResult
from ma.core.repo.models import Message

Route = Literal["metagc", "uniioc", "kc", "reject", "default"]


@dataclass(slots=True)
class AuthnIdentity:
    """从 Transport 层 AuthnMiddleware 注入的身份对象（M0 占位实现：信任 header）。"""

    w3_account: str
    raw_claims: dict[str, Any] = field(default_factory=dict)


class GraphState(TypedDict, total=False):
    # ── 输入区（请求进入时填好）─────────
    request_id: str
    biz_id: str
    thread_id: str
    identity: AuthnIdentity
    question: str
    biz_params: dict[str, Any]
    history: list[Message]

    # ── 鉴权区（auth 节点写）─────────────
    user_ctx: dict[str, Any]
    auth: AuthResult

    # ── 补全区（enrich 节点写）───────────
    enriched_question: str
    enrich_meta: dict[str, Any]

    # ── 路由区（intent 节点写）───────────
    route: Route
    route_confidence: float
    route_reason: str

    # ── 下游输出区（adapter 节点写）──────
    answer_chunks: Annotated[list[str], add]
    answer_meta: dict[str, Any]

    # ── 错误区 ────────────────────────
    errors: Annotated[list[dict[str, Any]], add]

    # ── 持久化辅助 ──────────────────────
    user_message_id: str  # ChatService 写入 user 消息后填回，供 persist 节点关联
    assistant_message_id: str  # 同上
