# M1 完成笔记

- **日期**：2026-06-24
- **分支**：`feature/m1-metagc-e2e`（合并到 `design/master-agent-v1`，tag `m1-complete`）
- **commit 数**：22 个 feature commits + 1 merge commit + 1 docs commit = ~24
- **覆盖率**：**85%** 总体（设计书 §7.6 目标 80%，达标）
- **测试套**：95 passed + 12 skipped（10 docker-gated integration/e2e + 2 stream contract placeholders）
- **lint/type**：ruff check / ruff format / mypy strict 全部 clean

## Phase 完成情况

| Phase | Task # | 内容 | commits |
|---|---|---|---|
| A | 1 | M0 day-1 cleanup（structlog contextvars + trace_id + FastAPIInstrumentor）| 1 |
| B | 2-6 | DB infra（Settings + SQL 模板 + asyncpg + Alembic + 2 Repositories）| 5 |
| C | 7-8 | Plugin Protocol + Registry | 2 |
| D | 9-10 | TopicConfig schema + TopicCompiler（含 env 占位符）+ TopicRegistry | 2 |
| E | 11-13 | GraphState 类型 + 3 个内置节点 + 4 个 wrapper | 3 |
| F | 14-16 | representative_auth + generic_enrich + llm_classifier（FakeListChatModel）| 3 |
| G | 17-18 | MetaGCAdapter + mock server + ASGI 单测 + real-port integration | 2 |
| H | 19-21 | Topic YAML + ChatService 重写 + main.py lifespan 装配 | 3 |
| I | 22 | 9 条 stream contract 测试（7 实测 + 2 skip）| 1 |
| J | 24-25 | e2e + Dockerfile + CI integration job + Makefile + README | 2 |

## 与计划相比的偏差（实施过程中暴露）

### Phase B 阶段

1. **asyncpg / langgraph / langchain_core / yaml 都缺 mypy stubs** —— 各加了 `[mypy-X.*] ignore_missing_imports = True` 到 mypy.ini，以及在使用点加 `cast()` / `# type: ignore`。无功能影响
2. **testcontainers 用 `postgres:16-alpine` 而非真 OpenGauss 镜像** —— 协议兼容但生产是 OpenGauss。**M2 之前在 staging 验证 OpenGauss 兼容**。这是设计书 §7.2.3 的偏差，已记录

### Phase E 阶段

3. **LangGraph 节点名不能与 GraphState 字段名重名** —— LangGraph 跑 `add_node("auth", ...)` 时若 GraphState 已经有 `auth: AuthResult` 字段会抛 `ValueError: 'auth' is already being used as a state key`。修正：节点名加 `_node` 后缀（`auth_node` / `enrich_node` / `intent_node` / `reject_node` / `persist_node`），adapter 节点名走 YAML 配置不受影响。这一限制设计书 §2.2 没提到，M2 起新增节点要注意
4. **`adispatch_custom_event` 的 config 参数需要 `RunnableConfig` TypedDict** —— wrapper 实现里引入了一个 `_emit(name, data, config)` 内部帮助函数，把 `cast(RunnableConfig, config)` 收敛到一个位置而不是散落 10 处

### Phase F 阶段

5. **`fresh_registry` fixture 必须用 `sys.modules.pop` + `importlib.import_module` 而不是 `importlib.reload`** —— 后者在测试 session 首次执行时会重复注册触发 `PluginConflict`。所有 plugin 测试都用统一的修正版 fixture
6. **`TopicCompiler` 默认捕获 `_global_registry` at import time** —— 跨测试文件时如果先 import 了 compiler，后续测试想换 registry 必须显式传 `TopicCompiler(registry=...)` + 从 sys.modules 弹出 compiler。`tests/stream/conftest.py` 已采用此策略

### Phase H 阶段

7. **ChatService 构造签名破坏性变更**（`(topic_registry, session_repo_factory, message_repo_factory)`） —— M0 的 3 个 stub 测试在 `tests/unit/test_events.py` 被删除（契约已被新的 `test_chat_service_with_fake_topic` 覆盖）
8. **`tests/unit/test_sse_endpoint.py` 完全重写** —— M0 的 "5 帧 stub 流断言" 不再适用 M1 的真 ChatService。M1 只测 422 / 503 fast-path；完整流式契约由 stream/ + e2e/ 覆盖

### Phase I 阶段

9. **Contract 9 简化**：M1 的 reject 节点仅写 `answer_chunks` 不 dispatch `delta`，前端实际看不到鉴权失败的话术 —— **M2 第一个 commit 必须修复**。设计书 §4.5 应当下发话术；M1 用 done + 空 delta 占位
10. **Contract 8（retry 透明）skip** —— M3 实现
11. **Contract 10（WS 多 request 并发）skip** —— M4 实现

### CI 阶段

12. **GitHub Actions CI `integration` job 用 service container postgres** 而非 testcontainers —— `MA_TEST_PG_DSN` 已设置，`pg_dsn` fixture 走非容器分支跑真 postgres。CI 上 testcontainers 也可用但被 env 短路了

## 给 M2 的提示

### M2 day-1 cleanup（M1 review 未做，留给 M2）

1. **修 Contract 9**：让 reject 节点也通过 `dispatch_custom_event` 发 delta，让鉴权失败话术真的到达前端
2. **历史拼接"整轮一起丢"语义**：M1 的 `load_session_node` 简化为按消息数取，没实现"整轮一起丢"。如果 M2 要支持多专题且历史较长时这可能造成 LLM context 溢出 —— M2 上线前补
3. **`SessionBizMismatchError` 应当从 ChatService 转成 HTTP 400**：目前会冒成 `INTERNAL` error。M2 加 transport 层映射

### M2 主体（设计书 §9.4）

1. **新增经营小助手 + 销售合同责任人 Topic YAML**（纯配置，不写代码 —— 验证插件化承诺）
2. **三套 AuthPlugin 实现** —— 每个专题鉴权逻辑独立
3. **REST endpoints**：`/api/v1/sessions` + `/api/v1/sessions/{id}/messages`
4. **越权防御**：thread_id 的 w3_account 校验

### M1 已就位、M2 直接用的能力

- LangGraph + Plugin + Topic 体系完整
- DB 层（asyncpg + Repository）完整
- MetaGC adapter 跑通（M3 接 Uniioc / KC 时复用同一抽象）
- Stream contract 测试基础设施完整
- testcontainers 集成测试 fixture 完整

## 出口验证清单

- [x] curl SSE 代表小管家 + mock MetaGC，拿到 meta/thinking/progress/delta×N/done
- [x] 历史落库（user + assistant 都有 seq）
- [x] 鉴权失败 → 流终止（M1 简化的话术不下发是已知偏差，M2 day-1 修复）
- [x] mock MetaGC 5xx → error event + done
- [x] mypy / ruff / pytest 全绿
- [x] 覆盖率达标（85% ≥ 80%）
- [x] Stream contracts 7/9 实测（contract 8 推 M3 retry，contract 10 推 M4 WS）
