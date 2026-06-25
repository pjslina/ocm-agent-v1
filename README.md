# MasterAgent (MA)

企业级智能助手服务，作为前端（PC SSE / 移动 WS）与多个第三方 AI Agent 之间的中间桥梁。

## 文档

- 设计：`docs/superpowers/specs/2026-06-23-master-agent-design.md`
- 计划：`docs/superpowers/plans/`
- 编码准则：`CLAUDE.md`

## 当前状态

**M1 — 代表小管家专题端到端跑通。**
- POST /api/v1/chat/sse → 鉴权 → 补全 → 意图识别（fake LLM） → MetaGC adapter → SSE 7-type 流 → 落库 → done
- 仅支持代表小管家一个专题（M2 加经营小助手 / 销售合同责任人）
- MetaGC 用本地 mock server；真接入 M3
- WebSocket 通道 M4
- 完整测试套（unit + stream + integration + e2e），9 条流式契约（2 个推后到 M3/M4）

## 快速启动

需要：Python 3.12、[uv](https://github.com/astral-sh/uv)。

```bash
# 安装依赖
make dev

# 跑所有检查（lint + type + test）
make check

# 本地起服务（自动 reload）
make run
```

如果本机没有 `make`（Windows 默认就没装），把每个目标里的 `python -m uv ...` 直接拷出来跑即可。

环境变量：

| 变量 | 必填 | 默认 | 含义 |
|---|---|---|---|
| `MA_ENV` | ✅ | — | `dev` / `test` / `staging` / `prod` |
| `MA_LOG_LEVEL` |  | `info` | `debug` / `info` / `warning` / `error` |
| `MA_OTEL_EXPORTER_OTLP_ENDPOINT` |  | `None` | OTLP gRPC endpoint，留空则 trace 仅在内存中 |
| `MA_OTEL_SERVICE_NAME` |  | `master-agent` | OTel resource attribute |
| `MA_PG_DSN_RW` | 必填（生产） | — | OpenGauss / PG DSN |
| `MA_METAGC_BASE_URL` | 必填（生产） | — | MetaGC 下游 base URL |
| `MA_CONFIG_TOPICS_DIR` |  | `config/topics` | YAML 目录 |

## 本地 OTel / Jaeger 验证

```bash
# 1. 启 Jaeger all-in-one（内存存储）
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest

# 2. 设置 MA 的 OTLP endpoint 指向 Jaeger
export MA_OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

# 3. 启 MA
uvicorn ma.main:app --port 8000

# 4. 发一个请求
curl -X POST http://localhost:8000/api/v1/chat/sse \
  -H 'Content-Type: application/json' \
  -H 'X-User-Role: REP' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_jaeger","w3_account":"alice","question":"测试","biz_params":{"region":"huadong"}}'

# 5. 打开 Jaeger UI → http://localhost:16686
#    搜索 service=ma，应能看到 graph.node.* spans + llm.classify span
```

## 手动验证 SSE

```bash
curl -N -X POST http://127.0.0.1:8000/api/v1/chat/sse \
  -H "Content-Type: application/json" \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_1","w3_account":"zhangsan","question":"hi"}'
```

应当看到 `meta → thinking → delta×2 → done` 共 5 个 SSE 帧。

## 目录结构

见设计书 §5.9。当前已落地：

```
src/ma/
├── api/        # FastAPI 路由（health / sse）
├── core/       # ChatService + GraphState + ChatEvent
└── infra/      # Settings / logging / tracing
```

## Docker

```bash
make build
docker run --rm -p 8000:8000 -e MA_ENV=dev master-agent:dev
```
