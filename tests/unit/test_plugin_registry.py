"""PluginRegistry：注册 / 查找 / 重复注册冲突 / 未知 kind+name / configure 失败。"""

from __future__ import annotations

import pytest


def test_registry_register_and_create() -> None:
    from ma.core.plugin.registry import PluginRegistry

    reg = PluginRegistry()

    @reg.register
    class FooAuth:
        name = "foo_auth"
        plugin_kind = "auth"

        def configure(self, params: dict) -> None:
            self.params = params

    inst = reg.create("auth", "foo_auth", {"x": 1})
    assert isinstance(inst, FooAuth)
    assert inst.params == {"x": 1}


def test_registry_duplicate_raises() -> None:
    from ma.core.plugin.registry import PluginConflict, PluginRegistry

    reg = PluginRegistry()

    @reg.register
    class A:
        name = "n"
        plugin_kind = "auth"

        def configure(self, params: dict) -> None: ...

    with pytest.raises(PluginConflict):

        @reg.register
        class A2:
            name = "n"
            plugin_kind = "auth"

            def configure(self, params: dict) -> None: ...


def test_registry_create_unknown_raises() -> None:
    from ma.core.plugin.registry import PluginNotFound, PluginRegistry

    reg = PluginRegistry()
    with pytest.raises(PluginNotFound):
        reg.create("auth", "ghost", {})


def test_registry_create_propagates_configure_errors() -> None:
    """configure 抛错 → create 抛同样的错（不要吞掉）。"""
    from ma.core.plugin.registry import PluginRegistry

    reg = PluginRegistry()

    @reg.register
    class Bad:
        name = "bad"
        plugin_kind = "auth"

        def configure(self, params: dict) -> None:
            raise ValueError("missing required param")

    with pytest.raises(ValueError, match="missing required param"):
        reg.create("auth", "bad", {})


def test_registry_separates_by_kind() -> None:
    """同名不同 kind 不冲突 (auth.foo vs adapter.foo)。"""
    from ma.core.plugin.registry import PluginRegistry

    reg = PluginRegistry()

    @reg.register
    class FooAuth:
        name = "foo"
        plugin_kind = "auth"

        def configure(self, params: dict) -> None: ...

    @reg.register
    class FooAdapter:
        name = "foo"
        plugin_kind = "adapter"

        def configure(self, params: dict) -> None: ...

    a = reg.create("auth", "foo", {})
    b = reg.create("adapter", "foo", {})
    assert isinstance(a, FooAuth)
    assert isinstance(b, FooAdapter)


def test_global_registry_singleton() -> None:
    """模块级 registry 单例 —— bootstrap 用它。"""
    from ma.core.plugin.registry import registry

    assert registry is not None
