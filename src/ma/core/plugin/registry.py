"""PluginRegistry：装饰器风格注册插件类，by (kind, name) 唯一索引。

每个 Plugin 类用 `@registry.register` 注册；TopicCompiler 在编译期间
通过 `registry.create(kind, name, params)` 实例化并 configure。
"""

from __future__ import annotations

from typing import Any, TypeVar


class PluginConflict(RuntimeError):
    """同 (kind, name) 重复注册。"""


class PluginNotFound(KeyError):
    """请求的 (kind, name) 不存在。"""


T = TypeVar("T")


class PluginRegistry:
    def __init__(self) -> None:
        self._by_kind_name: dict[tuple[str, str], type[Any]] = {}

    def register(self, cls: type[T]) -> type[T]:
        kind = getattr(cls, "plugin_kind", None)
        name = getattr(cls, "name", None)
        if not isinstance(kind, str) or not isinstance(name, str):
            raise TypeError(
                f"{cls.__name__} must declare class attrs `name: str` and `plugin_kind: str`"
            )
        key = (kind, name)
        if key in self._by_kind_name:
            raise PluginConflict(
                f"plugin already registered: kind={kind!r}, name={name!r} "
                f"({self._by_kind_name[key].__name__} vs {cls.__name__})"
            )
        self._by_kind_name[key] = cls
        return cls

    def create(self, kind: str, name: str, params: dict[str, Any]) -> Any:
        cls = self._by_kind_name.get((kind, name))
        if cls is None:
            raise PluginNotFound(f"no plugin registered for kind={kind!r}, name={name!r}")
        instance = cls()
        instance.configure(params)
        return instance


# 模块级单例 —— 业务代码统一用这个
registry = PluginRegistry()
