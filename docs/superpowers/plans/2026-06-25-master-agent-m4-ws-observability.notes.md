# M4 完成笔记

- 日期：2026-06-25
- 分支：feature/m4-ws-observability（合入 main，tag m4-complete）
- commit 数：10（含 format fix）
- 覆盖率：76%

## 与计划相比的偏差

1. **Task 2 ws.py 修复**：计划中的 `chat_ws(websocket, request: Request)` 对 FastAPI WebSocket endpoint 无效（Request 仅 HTTP），Task 3 修正为 `chat_ws(websocket)` + `websocket.app.state`

2. **Task 5 生产代码扩展**：WS disconnect → partial 测试需要三处生产改动（GraphState.stream_cancelled、node_wrappers CancelledError 处理器、builtins persist 逻辑），比计划多 3 行生产代码但均最小必要

3. **Task 3 测试适配**：本机无 Docker/DB，WS 集成测试适配为验证 DB 缺失时 graceful closed frame 行为，而非完整 ask→done 流

## 给 M5 的提示

1. **WS 完整 e2e**：M4 的 WS 集成测试在无 DB 时只能验证 closed 路径；M5 需在有 DB 的 CI 上验证完整 ask→event→done 流
2. **AuthnMiddleware**：WS 当前用 query params + headers 取 identity（M4 占位），M5 应接入统一的 AuthnMiddleware
3. **provider="internal"**：M3 预留接口，M5 实现
4. **K8s 部署 + 压测**：设计书 §9.4 M5 范围
5. **uv.lock 瘦身**：M3/M4 引入的传递依赖较多，M5 可考虑裁剪未使用项

## 验收清单

- [x] WS 信封协议：encode_ws / decode_ws_frame / ready / pong / closed
- [x] WS endpoint + ConnectionManager（max 5 并发，45s 心跳超时）
- [x] WS 集成测试
- [x] Contract 10（WS 多请求隔离）实测
- [x] WS disconnect → partial 标记测试
- [x] replay_request_id CLI
- [x] Jaeger 本地验证文档
- [x] mypy / ruff / pytest 全绿（130 passed, 18 skipped）
- [x] 覆盖率达标（76%）
