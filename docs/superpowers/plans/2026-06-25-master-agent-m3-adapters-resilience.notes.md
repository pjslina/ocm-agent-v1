# M3 完成笔记

- 日期：2026-06-25
- 分支：feature/m3-adapters-resilience（合入 main，tag m3-complete）
- commit 数：10（含 format pass）
- 覆盖率：80%

## 与计划相比的偏差

1. **基本无偏差** — 所有 9 个 task 按计划执行完成：
   - Task 1: UniiocAdapter + mock + 集成测试
   - Task 2: KnowledgeCenterAdapter (WS) + mock + 集成测试
   - Task 3: LLMClassifier openai-compatible provider
   - Task 4: 节点级 retry + timeout (YAML + node_wrappers)
   - Task 5: error_messages 全链路 + Contract 8 实测
   - Task 6: 三专题 YAML 三路路由
   - Task 7: get_by_thread + REST 优化
   - Task 8: OpenInference LangChain instrumentation
   - Task 9: 出口验证 + 合并 + tag

2. **CompiledTopic slots** — Task 4 将 `CompiledTopic` 从 `slots=True` 改为 `slots=False` 以支持 `field(default_factory=dict)`。实际 Python 3.12 的 `dataclass(slots=True)` 已支持此用法，但无实际影响（CompiledTopic 实例很少）。

3. **SecretStr 包装** — Task 3 的 `api_key` 参数用 `SecretStr` 包装以满足 `ChatOpenAI` 类型签名，比计划中的 `str` 更安全。

4. **Contract 8 测试** — 需要手动 patch `adapter_retries` 到 `max_attempts=2`（默认 1 = 不 retry），测试通过内部属性访问实现，脆但有效。

## 给 M4 的提示

1. **WebSocket endpoint**：`/api/v1/chat/ws` + 握手鉴权 + 心跳。参考 KnowledgeCenterAdapter 的 WS 客户端实现
2. **provider="internal"**：企业网关接入 LLM。M3 只做了 openai-compatible，internal 留 M4
3. **OpenInference 验证**：代码已加 `LangChainInstrumentor().instrument()`，M4 需在 staging 验证 spans 可见
4. **KnowledgeCenter 真接**：M3 的 KC adapter 是 mock + 集成测试；M4 接真实 KC 服务
5. **e2e 三路回归**：YAML 已有三路 route（metagc + uniioc + kc），但 e2e test 仍只测 metagc adapter（因为 fake intent 总是路由到 metagc）；M4/M5 加多路 e2e
6. **uv.lock 膨胀**：`langchain-openai` 和 `openinference-instrumentation-langchain` 引入了大量传递依赖，M4 可考虑瘦身

## 验收清单

- [x] UniiocAdapter + KnowledgeCenterAdapter 实现 + 集成测试
- [x] LLMClassifier openai-compatible provider
- [x] 节点级 retry + timeout（YAML 可配）
- [x] error_messages 全链路生效（ChatService 映射 YAML 话术）
- [x] Contract 8（retry）从 skip 变实测
- [x] 三专题 YAML 三路路由（metagc + uniioc + kc）
- [x] REST list_messages O(1) 优化（get_by_thread 直查）
- [x] OpenInference LangChain instrumentation
- [x] mypy / ruff / pytest 全绿（118 passed, 19 skipped, 80% 覆盖率）
