# MasterAgent (MA)

企业级智能助手服务，作为前端（PC SSE / 移动 WS）与多个第三方 AI Agent 之间的中间桥梁。

## 文档

- 设计：`docs/superpowers/specs/2026-06-23-master-agent-design.md`
- 计划：`docs/superpowers/plans/`
- 编码准则：`CLAUDE.md`

## 当前状态

**M0 — 脚手架与骨架已落地。** 服务能起来、SSE 端点能返回合法 stub 事件流、结构化日志 + OTel + CI 全部联动。无业务逻辑（M1 开始接 LangGraph + DB）。

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
