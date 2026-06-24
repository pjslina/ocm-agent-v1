"""代表小管家鉴权插件。

M1 简化实现：从 GraphState.identity.raw_claims 中读取 role，与 required_role 比对。
M2 起接外部用户中心 API 拉真实角色 + 客户归属 + 客户级权限。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import AuthResult
from ma.core.plugin.registry import registry


class RepresentativeAuthParams(BaseModel):
    required_role: str


@registry.register
class RepresentativeAuth:
    name = "representative_auth"
    plugin_kind = "auth"

    def configure(self, params: dict[str, Any]) -> None:
        p = RepresentativeAuthParams(**params)
        self._required_role = p.required_role

    async def authorize(self, state: GraphState) -> AuthResult:
        identity = state.get("identity")
        if identity is None:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_TOPIC",
                reject_message="无法识别用户身份。",
            )
        claims = identity.raw_claims or {}
        role = claims.get("role")
        if role != self._required_role:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_TOPIC",
                reject_message=f"该专题仅限 {self._required_role} 角色访问。",
            )
        return AuthResult(passed=True, user_ctx={"role": role})
