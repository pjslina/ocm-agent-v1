"""SqlLoader：启动时一次性加载所有 sql/*.sql；按 namespace/name 取。"""

from __future__ import annotations

from pathlib import Path

import pytest


def test_sql_loader_loads_all_templates(tmp_path: Path) -> None:
    """模拟一个 sql/ 目录，确保 loader 能按 namespace/name 索引到内容。"""
    sql_dir = tmp_path / "sql"
    (sql_dir / "session").mkdir(parents=True)
    (sql_dir / "session" / "get.sql").write_text("SELECT 1;", encoding="utf-8")
    (sql_dir / "message").mkdir()
    (sql_dir / "message" / "append.sql").write_text("INSERT INTO t VALUES (:x);", encoding="utf-8")

    from ma.core.repo.sql_loader import SqlLoader

    loader = SqlLoader(sql_dir)
    assert loader.get("session/get") == "SELECT 1;"
    assert loader.get("message/append") == "INSERT INTO t VALUES (:x);"


def test_sql_loader_unknown_key_raises() -> None:
    from ma.core.repo.sql_loader import SqlLoader, SqlTemplateNotFound

    loader = SqlLoader(Path("sql"))  # use real sql/ dir
    with pytest.raises(SqlTemplateNotFound):
        loader.get("nonexistent/key")


def test_sql_loader_real_sql_dir_has_required_keys() -> None:
    """跑真 sql/ 目录，确保 Task 3 创建的 8 个模板都加载到。"""
    from ma.core.repo.sql_loader import SqlLoader

    loader = SqlLoader(Path("sql"))
    required = [
        "session/get_by_thread",
        "session/insert",
        "session/update_touch",
        "session/list_by_w3",
        "message/append",
        "message/list_recent",
        "message/mark_partial",
        "message/mark_failed",
    ]
    for key in required:
        sql = loader.get(key)
        assert sql.strip(), f"template {key} is empty"


def test_sql_loader_rejects_format_strings(tmp_path: Path) -> None:
    """硬约束（设计书 §3.3.2）：模板里不能包含 f-string / .format / %s 风格占位符。"""
    sql_dir = tmp_path / "sql"
    (sql_dir / "bad").mkdir(parents=True)
    (sql_dir / "bad" / "fstring.sql").write_text(
        "SELECT * FROM t WHERE id = {x};", encoding="utf-8"
    )

    from ma.core.repo.sql_loader import SqlLoader, UnsafeSqlTemplate

    with pytest.raises(UnsafeSqlTemplate):
        SqlLoader(sql_dir)
