# M0 完成笔记

- 日期：2026-06-24
- 分支：`feature/m0-scaffold`（16 个 commit），已合并到 `design/master-agent-v1` 并打 tag `m0-complete`
- 覆盖率：**88%** 总体（设计书目标 80%，达标）
  - 核心层 `core/` `infra/` `api/` 均 100%（state.py 例外 — 见下）
  - 唯一未达标：`state.py` 0%（TypedDict 纯类型声明，无运行时代码；plan Step 7 已明确不写单测）
  - `tracing.py` 87%（OTLP endpoint 设置分支未测，按设计书 6.4 留 M4 集成测试覆盖）
- 26 个单元测试全过
- ruff check + ruff format --check + mypy strict 全部 clean

## 与设计书/计划相比的实施偏差（5 处）

| # | 偏差 | 影响 / 原因 |
|---|---|---|
| 1 | 所有 `uv` 调用改为 `python -m uv` | Windows 本机 PATH 未加 `pip --user` 的 Scripts 目录；`python -m uv` 始终可用，等价但更稳健。需在 Makefile、Dockerfile、CI、文档中统一 |
| 2 | `pyproject.toml` `[tool.uv] package` 从 `false` 改为 `true` | Task 11 起 uvicorn 需要把 `ma` 当包加载；非包模式下 `ma.main:app` 无法解析。改了之后 uv 把 `ma` 安装到 venv，pytest/uvicorn 都可用 |
| 3 | `pyproject.toml` `[tool.pytest.ini_options]` 加 `pythonpath = ["src"]` | src-layout + `package = false` 阶段 pytest 找不到 `ma`，必须显式加；后续 `package = true` 之后这行成了冗余但保留不影响 |
| 4 | `ruff.toml` 加 `RUF001/002/003` 忽略 | 设计书允许 Chinese 注释/docstring，必须忽略 unicode 模糊字符规则 |
| 5 | Dev 依赖去掉 `types-ulid` | 该 PyPI 包不存在；`ulid-py` 无 stub 时 mypy 会按 ignore 处理 |
| 6 | Dockerfile 中 `uv==0.11.24` | 本机用 0.11.24；plan 写的 0.4.30 是当时计划文档的占位 |

这些偏差都属于"环境/工具链层面"的调整，**不影响接口语义、不影响测试断言、不影响后续里程碑**。

## 未在本机验证（待 CI / staging 验证）

| 项 | 原因 |
|---|---|
| `make check` | Windows 默认无 `make`。Makefile 已写好，TAB 缩进经 `od -c` 校验；recipe 都包装 `python -m uv ...`。CI 上跑 ubuntu-latest 自带 make 即可验证 |
| `docker build` | Windows 本机未装 Docker。Dockerfile 已就绪；CI 第二个 job `docker` 会做完整 build |

## 给 M1 的提示（实施过程中暴露的注意点）

1. **mypy + pydantic-settings 配合需要 `# type: ignore[call-arg]` 在 `Settings()` 实例化处**（pydantic-settings 没给 mypy 提供 env-loader 的元类型）。M1 加新 Settings 字段时这个 ignore 仍能用，但若想清理可以试 `pydantic[mypy]` plugin
2. **structlog 的 `PrintLogger` 不走 stdlib logging**：tests 用 `monkeypatch.setattr(sys, "stdout", buf)` 捕获；不要尝试用 `caplog`。M1 起 langgraph 调用如果用到 langchain 的标准 logger，需要单独装 stdlib bridge
3. **FastAPI TestClient 触发 lifespan 只在 `with client:` 上下文管理器形态下**。M1+ 写涉及 DB / Plugin 加载的端到端测试必须用 with-形态
4. **`logging.basicConfig(..., force=True)`** 在 `configure_logging` 里加了 force，让多次 configure 在测试中可重入；M1 起的 LangGraph 节点测试需保留这个 behavior
5. **OTel 全采 + provider 在 main.py 启动时 set**：M1 加 LangGraph 节点 span 时直接 `trace.get_tracer("ma.<module>")` 即可，不必重 set provider
6. **`X-Request-Id` 透传机制已就位**：M1 起在 LangGraph 节点里只要从 ChatRequest.request_id 拿到，无须重新生成

## 终评 Review 暴露的 M1 第 1 个 commit 应处理项（"M1 day-1 cleanup"）

这 4 项语义相关、合并为一次 commit 完成，再开始 LangGraph 工作：

1. **`_inject_ids` 不写 null**：`logging.py:48-50` 当前对未 bind 的请求把三个 ID 写为 `null` 进 JSON，会让 Kibana 过滤失真。改成"只在 ctxvar 非 None 时 setdefault"
2. **统一 contextvars 机制**：`merge_contextvars` 已在 processor 链里但没人用；当前自写的 `_ctx_request_id` 等是冗余。改用 `structlog.contextvars.bind_contextvars(...)` 在 `bind_request_ctx` 内部调用，删 `_inject_ids` 和自写 ContextVars。test 已经在调 `clear_contextvars`，所以测试侧零改动
3. **加 `trace_id` 注入到日志**：写一个 processor 读 `trace.get_current_span().get_span_context().trace_id`，让 log↔trace 在 ELK 侧能 join。M1 开始大量 LLM/下游调用之前必须就位
4. **接入或移除 `opentelemetry-instrumentation-fastapi`**：依赖已在 pyproject 但未 instrument。要么在 `create_app` 里调 `FastAPIInstrumentor().instrument_app(app)` 形成 §6.4.1 的 `chat.request` 根 span，要么从 deps 删除。建议接入。

终评其它 Minor 项（不阻塞 M1，按手感处理）：
- `health._state` 模块级单例的 `_state.ready = False` 反射式测试 — M1 多组件 readiness 时再换成 `ReadinessRegistry`
- `ulid-py` 已停维护 — 一行换 `python-ulid` 可做，可不做
- `state.py 0%` 加 `# pragma: no cover` 让覆盖率报告更干净
- `test_sse_endpoint.py` 没断言完整 5 帧序列 — 一行 `assert events == [...]` 锁死 M0 契约
- `configure_tracing` 不幂等 — 多次 `create_app` 会触发 OTel 警告；M1 集成测试多起来后加 `if not isinstance(get_tracer_provider(), TracerProvider)` 守一下

## 出口验证清单

- [x] 端到端 curl SSE 拿到合法 5 帧事件（meta/thinking/delta×2/done）
- [x] 结构化日志含 request_id/thread_id/w3_account/biz_id 四字段
- [x] OTel SDK 初始化成功（resource service.name=master-agent）
- [x] CI 配置就位（push 后即可绿）
- [x] **无业务逻辑** — ChatService 是 stub，不接 LangGraph、不接 DB、不接插件
- [x] 单元测试覆盖率 ≥ 80%（实际 88%）
- [x] ruff / mypy 全绿

M0 出口达标。可进入 M1 计划编写。
