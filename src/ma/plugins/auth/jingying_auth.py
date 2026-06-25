"""经营小助手鉴权插件。

M2 实现：role 必须在 allowed_roles 内，且 biz_params.business_unit 必须在
identity.raw_claims.business_units 内。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import AuthResult
from ma.core.plugin.registry import registry


class JingyingAuthParams(BaseModel):
    allowed_roles: list[str]


@registry.register
class JingyingAuth:
    name = "jingying_auth"
    plugin_kind = "auth"

    def configure(self, params: dict[str, Any]) -> None:
        p = JingyingAuthParams(**params)
        self._allowed_roles = set(p.allowed_roles)

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
        if role not in self._allowed_roles:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_TOPIC",
                reject_message=f"该专题仅限 {'/'.join(sorted(self._allowed_roles))} 角色访问。",
            )

        biz_params = state.get("biz_params", {})
        business_unit = biz_params.get("business_unit")
        allowed_units = claims.get("business_units", [])
        if business_unit is None:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message="请求缺少 business_unit 参数。",
            )
        if business_unit not in allowed_units:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message=f"您无权访问 {business_unit} 业务单元数据。",
            )

        return AuthResult(
            passed=True,
            user_ctx={
                "role": role,
                "business_units": allowed_units,
                "business_unit": business_unit,
            },
        )
