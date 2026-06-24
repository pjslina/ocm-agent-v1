"""SqlLoader：启动时把 sql/*/*.sql 一次性加载到内存，运行期按 'namespace/name' 取。

设计约束（§3.3.2）：
- 模板里不允许任何 f-string / .format() / %s / printf 占位符
- 唯一允许的参数化形式是具名占位符 :name（asyncpg 不直接支持具名 → SqlExecutor 负责翻译）
- 表名/列名不允许在模板里动态拼接（这一点 SqlLoader 不强校验，由 code review 把关）
"""

from __future__ import annotations

import re
from pathlib import Path


class SqlTemplateNotFound(KeyError):
    """请求的 'namespace/name' 模板不存在。"""


class UnsafeSqlTemplate(ValueError):
    """模板里出现了禁止的占位符风格。"""


# 禁用的占位符 patterns
_FORBIDDEN_PATTERNS = [
    (re.compile(r"\{[a-zA-Z_]\w*\}"), "Python f-string / str.format() placeholder"),
    (re.compile(r"%\([a-zA-Z_]\w*\)s"), "percent-style named placeholder"),
    (re.compile(r"%s"), "percent-style positional placeholder"),
]


class SqlLoader:
    """加载 sql/ 目录下所有 .sql 文件。

    路径 sql/foo/bar.sql → key "foo/bar"。
    """

    def __init__(self, root: Path) -> None:
        self._templates: dict[str, str] = {}
        if not root.is_dir():
            raise FileNotFoundError(f"SQL root not found: {root}")
        for path in root.rglob("*.sql"):
            rel = path.relative_to(root).with_suffix("")
            key = "/".join(rel.parts)
            content = path.read_text(encoding="utf-8")
            for pattern, label in _FORBIDDEN_PATTERNS:
                if pattern.search(content):
                    raise UnsafeSqlTemplate(f"{path}: forbidden {label} (must use :name only)")
            self._templates[key] = content

    def get(self, name: str) -> str:
        try:
            return self._templates[name]
        except KeyError:
            raise SqlTemplateNotFound(name) from None

    def keys(self) -> list[str]:
        """主要供调试 / 启动校验用。"""
        return sorted(self._templates.keys())
