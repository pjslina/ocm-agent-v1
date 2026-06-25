"""销售合同责任人鉴权插件。

M2 实现：role 必须等于 required_role，且 biz_params.contract_id 必须在
identity.raw_claims.contract_ids 内。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from ma.core.graph.state import GraphState
from ma.core.plugin.base import AuthResult
from ma.core.plugin.registry import registry


class SalesContractAuthParams(BaseModel):
    required_role: str


@registry.register
class SalesContractAuth:
    name = "sales_contract_auth"
    plugin_kind = "auth"

    def configure(self, params: dict[str, Any]) -> None:
        p = SalesContractAuthParams(**params)
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

        biz_params = state.get("biz_params", {})
        contract_id = biz_params.get("contract_id")
        owned_contracts = claims.get("contract_ids", [])
        if contract_id is None:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message="请求缺少 contract_id 参数。",
            )
        if contract_id not in owned_contracts:
            return AuthResult(
                passed=False,
                reject_code="FORBIDDEN_SCOPE",
                reject_message=f"您不是 {contract_id} 合同的责任人。",
            )

        return AuthResult(
            passed=True,
            user_ctx={
                "role": role,
                "contract_ids": owned_contracts,
                "contract_id": contract_id,
            },
        )
