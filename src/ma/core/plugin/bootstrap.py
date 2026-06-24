"""插件加载入口。

设计书 §5.4.2：故意 *显式 import* 而非自动扫描，让加载链路可追溯。
M1 阶段插件还没全写出来；这个文件占位，后续 task 实现各插件时往里加 import。
"""

from __future__ import annotations


def load_all_plugins() -> None:
    """启动期调一次。

    每个插件模块 import 一次就够 —— 装饰器副作用把类注册进 registry。
    """
    import ma.plugins.auth.representative_auth
    import ma.plugins.enrich.generic_enrich  # noqa: F401
    # 后续 task 加：
    # import ma.plugins.intent.llm_classifier
    # import ma.plugins.adapter.metagc
