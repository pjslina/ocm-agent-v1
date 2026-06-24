"""SqlExecutor：把模板化 SQL 跑到 asyncpg pool 上。

设计书 §3.3.2 / §3.3.3。

模板用 :name 占位符（与人类阅读直觉一致），asyncpg 只懂 $1/$2 ——
本模块负责翻译；翻译做到：
- 同一个 :name 多次出现复用同一个 $n
- 区分 PG 类型转换语法 ::jsonb（双冒号）
- 缺参数立即抛错（启动期/请求期都立即可见）

DB 真正执行通过 SqlExecutor.fetch_all/fetch_one/execute 方法（M1 后续 task 实现，
这里先把 translator 这个纯函数交付了，便于单测）。
"""

from __future__ import annotations

import re
from typing import Any, cast

import asyncpg

from ma.core.repo.sql_loader import SqlLoader

# 匹配 :name 但排除 :: 双冒号 (PG 类型转换)。
# 用 (?<!:) 排除前面已经有一个冒号 (例如 ::jsonb 第二个冒号)。
_NAMED_PARAM_RE = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")


class MissingSqlParam(KeyError):
    """模板里引用了 :name，但 params 字典没有这个 key。"""


def translate_named_params(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """把 :name 占位符替换成 $1/$2/...，并返回对应顺序的参数列表。

    同名占位符复用 index：:x 第一次见 → $1，:x 再出现还是 $1，不重复绑定。
    """
    name_to_idx: dict[str, int] = {}
    args: list[Any] = []

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in name_to_idx:
            if name not in params:
                raise MissingSqlParam(name)
            name_to_idx[name] = len(args) + 1
            args.append(params[name])
        return f"${name_to_idx[name]}"

    new_sql = _NAMED_PARAM_RE.sub(_replace, sql)
    return new_sql, args


class SqlExecutor:
    """跑模板化 SQL 到 asyncpg pool。

    M1 阶段：单 DSN 实现；pool_rw == pool_ro。M2+ 拆只读副本时改这里。
    """

    def __init__(
        self,
        *,
        loader: SqlLoader,
        pool_rw: asyncpg.Pool,
        pool_ro: asyncpg.Pool | None = None,
    ) -> None:
        self._loader = loader
        self._rw = pool_rw
        self._ro = pool_ro or pool_rw

    async def fetch_all(self, name: str, **params: Any) -> list[asyncpg.Record]:
        sql = self._loader.get(name)
        translated, args = translate_named_params(sql, params)
        async with self._ro.acquire() as conn:
            return cast(list[asyncpg.Record], await conn.fetch(translated, *args))

    async def fetch_one(self, name: str, **params: Any) -> asyncpg.Record | None:
        sql = self._loader.get(name)
        translated, args = translate_named_params(sql, params)
        async with self._ro.acquire() as conn:
            return cast(asyncpg.Record | None, await conn.fetchrow(translated, *args))

    async def execute(self, name: str, **params: Any) -> str:
        sql = self._loader.get(name)
        translated, args = translate_named_params(sql, params)
        async with self._rw.acquire() as conn:
            return cast(str, await conn.execute(translated, *args))
