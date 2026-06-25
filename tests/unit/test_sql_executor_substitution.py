"""SqlExecutor 占位符翻译：:name → $1/$2 + 参数顺序对齐；同 :name 复用同一个 $n。"""

from __future__ import annotations

import pytest


def test_translate_single_placeholder() -> None:
    from ma.core.repo.sql_executor import translate_named_params

    sql = "SELECT * FROM t WHERE id = :id"
    params = {"id": 42}
    new_sql, args = translate_named_params(sql, params)
    assert new_sql == "SELECT * FROM t WHERE id = $1"
    assert args == [42]


def test_translate_multiple_placeholders_ordered_by_appearance() -> None:
    from ma.core.repo.sql_executor import translate_named_params

    sql = "INSERT INTO t (a, b, c) VALUES (:a, :b, :c)"
    params = {"a": 1, "b": 2, "c": 3}
    new_sql, args = translate_named_params(sql, params)
    assert new_sql == "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"
    assert args == [1, 2, 3]


def test_translate_repeated_placeholder_reuses_index() -> None:
    """同一个 :name 在 SQL 里出现两次 → asyncpg 用同一个 $1 引用一次绑定。"""
    from ma.core.repo.sql_executor import translate_named_params

    sql = "SELECT * FROM t WHERE a = :x OR b = :x"
    params = {"x": 7}
    new_sql, args = translate_named_params(sql, params)
    assert new_sql == "SELECT * FROM t WHERE a = $1 OR b = $1"
    assert args == [7]


def test_translate_missing_param_raises() -> None:
    from ma.core.repo.sql_executor import MissingSqlParam, translate_named_params

    sql = "SELECT * FROM t WHERE id = :id"
    with pytest.raises(MissingSqlParam):
        translate_named_params(sql, {})


def test_translate_ignores_pgsql_cast_double_colon() -> None:
    """asyncpg / PG 语法里 ::jsonb 是类型转换，不是占位符 → 不能误识别。"""
    from ma.core.repo.sql_executor import translate_named_params

    sql = "INSERT INTO t (ext) VALUES (:ext::jsonb)"
    params = {"ext": "{}"}
    new_sql, args = translate_named_params(sql, params)
    assert new_sql == "INSERT INTO t (ext) VALUES ($1::jsonb)"
    assert args == ["{}"]
