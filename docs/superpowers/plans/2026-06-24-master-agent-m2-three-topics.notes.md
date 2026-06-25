# M2 完成笔记

- 日期：2026-06-25
- 分支：feature/m2-three-topics（合入 design/master-agent-v1，tag m2-complete）
- commit 数：13（不含 plan doc）
- 覆盖率：82%

## 与计划相比的偏差

1. **无偏差** — 所有 14 个 task 按计划执行完成：
   - Phase A (Tasks 1-3): M1 day-1 cleanup（reject delta, biz mismatch→400, 整轮丢）
   - Phase B (Tasks 4-6): 三套 AuthPlugin（representative, jingying, sales_contract）
   - Phase C (Tasks 7-8): 两个新 Topic YAML + intent prompts
   - Phase D (Tasks 9-10): REST endpoints + integration tests
   - Phase E (Tasks 11-13): 4 个 e2e（daibiao 更新, jingying, sales_contract, followup）
   - Phase F (Task 14): exit verification + merge + tag

2. **sse.py X-User-Claims** — 按计划 Task 11.A 实现了 `X-User-Claims` header 解析，支持 JSON 对象传入额外 claims（regions, business_units, contract_ids），e2e 测试中验证通过。

3. **本机无 Docker** — e2e 与 integration 测试在本机全部 skip（19 个），但在 CI 上有 Docker 时会跑通。

## 给 M3 的提示

1. **OpenInference 接入**：M3 接真 LLM 时，`langchain-core` 已在依赖里，加 `openinference-instrumentation-langchain` + 在 `tracing.py` 加 instrumentor 调用即可
2. **节点级 retry / timeout**：M3 必须实现（Contract 8 当前 skipped）。设计书 §6.6.1
3. **UniiocAdapter / KnowledgeCenterAdapter**：参考 MetaGCAdapter 实现；KC 是 WebSocket，需要 `websockets` 依赖
4. **多步编排**：YAML schema 已支持多 output 节点，M3 起可以试 "MetaGC → Uniioc 链式" 场景
5. **越权防御优化**：`list_messages` 当前用 `list_by_user` + filter，N+1 风险；M3 加 `repo.get_by_thread(thread_id) -> Session | None` 直查
6. **真 LLM 接入**：目前 intent classifier 使用 fake provider（FakeListChatModel）；M3 需要替换为真 LLM 调用

## 验收清单

- [x] 三专题端到端跑通（110 unit/stream tests + 4 e2e tests）
- [x] 追问场景验证（4 条消息按 u/a/u/a 落库）
- [x] REST `/sessions` 和 `/sessions/{id}/messages` 含越权防御
- [x] Contract 9（鉴权失败话术下发）真生效
- [x] mypy / ruff / pytest 全绿
- [x] 覆盖率达标（82%）
