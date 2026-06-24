"""插件加载入口。

设计书 §5.4.2：故意 *显式 import* 而非自动扫描，让加载链路可追溯。
M1 阶段插件还没全写出来；这个文件占位，后续 task 实现各插件时往里加 import。
"""

from __future__ import annotations


def load_all_plugins() -> None:
    """在 lifespan 启动期调用一次。

    每个插件模块 import 一次就够 —— 装饰器副作用把类注册进 registry。
    M1 末期这个函数体会引用：
        import ma.plugins.auth.representative_auth  # noqa: F401
        import ma.plugins.enrich.generic_enrich     # noqa: F401
        import ma.plugins.intent.llm_classifier     # noqa: F401
        import ma.plugins.adapter.metagc            # noqa: F401
    M1 早期还没有这些模块，所以现在留空（带 docstring 占位）。
    """
    # 各插件 import 在 Phase F/G 实现各插件后逐步加进来；
    # 见 Task 14/15/16/17 末尾的 "回到 bootstrap 加 import" 步骤
    return None
