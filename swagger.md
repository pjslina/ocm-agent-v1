# MasterAgent 接口与业务场景验证手册

本文档用于**人工验证** MasterAgent（MA）各业务场景是否正确。每个场景都给出：触发条件、可直接粘贴的请求（Swagger / curl / WebSocket）、以及**期望结果**（HTTP 状态、SSE 事件序列、DB 落库、错误码）。

> 启动：`python -m ma.main`（从 `.env` 读环境变量），浏览器打开 **http://localhost:8000/docs** 进入 Swagger UI。
>
> 前置说明：
> - **对话 / 历史端点需要数据库**（`MA_PG_DSN_RW`）。未配置 DB 时为 smoke 模式，对话返回 503，历史返回 503。
> - 鉴权当前为占位（`trust_header` 模式）：服务端信任请求头 `X-User-Account` / `X-User-Role` / `X-User-Claims`，不校验真伪。所以下文"权限场景"靠**构造不同的请求头**来触发。
> - 默认配置下 `MA_INTENT_LLM_PROVIDER=openai-compatible`（走真实大模型做意图路由）。若无可用 LLM key，可临时改 `.env` 里 `MA_INTENT_LLM_PROVIDER=fake` —— 此时路由用预设回复，确定性可测，但需在 topic YAML 的 `intent.params` 里配 `fake_responses`（默认未配，fake 模式会报错）。

---

## 0. 三个业务专题（biz_id）总览

| biz_id | 名称 | 鉴权插件 | 必需角色 | 必需 biz_param | enrich 注入字段 |
|---|---|---|---|---|---|
| `daibiao_xiaoguanjia` | 代表小管家 | representative_auth | `REP` | `region` | `region` |
| `jingying_xiaozhushou` | 经营小助手 | jingying_auth | `MANAGER` 或 `REP` | `business_unit` | `business_unit` |
| `sales_contract_agent` | 销售合同责任人 Agent | sales_contract_auth | `SALES_OWNER` | `contract_id` | `contract_id` |

三个专题都内置 **3 条路由**（由意图分类器决定走哪个下游）：`metagc` / `uniioc` / `knowledge_center`，默认路由 `metagc`。下游地址来自环境变量 `MA_METAGC_BASE_URL` / `MA_UNIIOC_BASE_URL` / `MA_KC_WS_URL`。

---

## 1. 接口清单

| 方法 | 路径 | 用途 | 是否需 DB |
|---|---|---|---|
| GET | `/api/v1/healthz` | 存活探针，恒 200 | 否 |
| GET | `/api/v1/readyz` | 就绪探针，未就绪 503 | 否 |
| POST | `/api/v1/chat/sse` | 对话流（SSE） | 是 |
| GET | `/api/v1/sessions` | 列出当前用户会话 | 是 |
| GET | `/api/v1/sessions/{thread_id}/messages` | 列出某会话消息 | 是 |
| WS | `/api/v1/chat/ws` | 对话流（WebSocket） | 是 |

---

## 2. SSE 事件协议（对话流的输出）

`POST /api/v1/chat/sse` 返回 `text/event-stream`，每帧格式：

```
event: <类型>
data: <JSON>

```

共 7 种事件类型，`data` 形状如下：

| event | data 形状 | 说明 |
|---|---|---|
| `meta` | `{thread_id, request_id, message_id, topic}` | 流的第一个事件，每条流恰好一次 |
| `thinking` | `{phase: "auth"\|"enrich"\|"intent", label?}` | 阶段开始提示。auth 有 label，enrich 无 label |
| `progress` | `{step, status, detail?, extra}` | 阶段完成。`step` ∈ `auth`/`enrich`/`intent`；`status` ∈ `done`/`fallback` |
| `delta` | `{content, ...}` | 增量回答片段，拼接所有 delta 的 `content` 得到完整回答 |
| `tool` | `{...}`（适配器透传） | **当前无任何适配器产生 tool 事件**，预留 |
| `error` | `{code, message, retryable, detail?, request_id?}` | 错误，`retryable` 恒为 false |
| `done` | 成功：`{message_id, status:"complete"}`；快失败：`{status:"failed"}` | 流的最后一个事件，每条流恰好一次 |

**正常成功流的事件顺序：**
```
meta → thinking(auth) → progress(auth) → thinking(enrich) → progress(enrich)
     → thinking(intent) → progress(intent) → delta×N → done
```

**关键行为（验证时留意）：**
- 鉴权失败（FORBIDDEN_TOPIC / FORBIDDEN_SCOPE）**不会**产生 `error` 事件，而是走 reject 节点，把拒绝话术作为 **`delta`** 输出，最后 `done`（成功语义）。话术如 `该专题仅限 REP 角色访问。`
- 图内 `error` 的 `message` 会被专题的 `error_messages[code]` 覆盖为友好文案；图前快失败（TOPIC_NOT_FOUND / BAD_REQUEST）保留原始 message。
- 意图分类失败/解析不出时走 **fallback**：`progress(intent, status:"fallback")`，路由退回 `default_route`（metagc），流继续。
- 下游超时/出错后流以 `error` 结束，但 `done` 仍是 `status:"complete"`（已知行为：仅图前快失败才是 `status:"failed"`）。落库的 assistant 消息 `status` 为 `partial`（内容为空）。

---

## 3. WebSocket 协议

连接：`ws://localhost:8000/api/v1/chat/ws?w3_account=<账号>&biz_id=<专题>`

**服务端 → 客户端帧：**

| op | 内容 |
|---|---|
| `ready` | `{"op":"ready","server_version":"0.2.0","heartbeat_interval_s":20}` 握手成功 |
| `event` | `{"op":"event","request_id","event","data"}` 转发一个 ChatEvent |
| `pong` | `{"op":"pong"}` 心跳应答 |
| `closed` | `{"op":"closed","reason"}` 服务端主动关闭通知 |
| `error` | `{"op":"error","reason","request_id?"}` 帧错误（如未知 op、超并发） |

**客户端 → 服务端帧（合法 op 只有 3 个）：**

| op | 字段 |
|---|---|
| `ask` | `{"op":"ask","request_id?","thread_id?","question","biz_params?"}` |
| `cancel` | `{"op":"cancel","request_id"}` |
| `ping` | `{"op":"ping"}` |

约束：单连接最多 **5 个并发 ask**；**45s 无帧**则服务端发 `closed`（"heartbeat timeout"）并关闭。非合法 op 回 `{"op":"error","reason":"..."}` 并忽略该帧。

---

# 业务场景验证清单

下文每个场景用 ✅ 标记核心断言。curl 命令可直接复制运行；`<HOST>` 默认 `http://localhost:8000`。

## A. 健康检查

### A1. 存活探针
```
GET /api/v1/healthz
```
**期望：** ✅ 200，`{"status":"alive"}`。无需 DB，smoke 模式也返回 200。

### A2. 就绪探针
```
GET /api/v1/readyz
```
**期望：** 启动完成后 ✅ 200 `{"status":"ready"}`；启动中或关闭过程中 ✅ 503 `{"status":"not_ready"}`。

---

## B. 对话流（SSE）正常场景

### B1. 代表小管家 —— 正常全链路（REP + region 鉴权通过，路由 metagc）

```bash
curl -N -X POST http://localhost:8000/api/v1/chat/sse \
  -H 'Content-Type: application/json' \
  -H 'X-User-Account: alice' \
  -H 'X-User-Role: REP' \
  -H 'X-User-Claims: {"regions":["huadong"]}' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_demo_1","w3_account":"alice","question":"上个月华东大区销售额？","biz_params":{"region":"huadong"}}'
```
**期望：**
- ✅ HTTP 200
- ✅ 事件序列：首帧 `meta`，末帧 `done`，中间含 `thinking`、`progress`（step 出现 auth/enrich/intent）、`delta` ≥ 2 个
- ✅ 拼接所有 `delta.data.content` 含原问题相关内容
- ✅ DB 落库：`ma_message` 该 thread 下 2 行（user → assistant），assistant `status=complete`
- ✅ `done.data` = `{message_id, status:"complete"}`

### B2. 经营小助手 —— 正常全链路（MANAGER + business_unit）

```bash
curl -N -X POST http://localhost:8000/api/v1/chat/sse \
  -H 'Content-Type: application/json' \
  -H 'X-User-Account: manager_alice' \
  -H 'X-User-Role: MANAGER' \
  -H 'X-User-Claims: {"business_units":["bu_a"]}' \
  -d '{"biz_id":"jingying_xiaozhushou","thread_id":"th_jy_1","w3_account":"manager_alice","question":"bu_a 上月业绩怎样？","biz_params":{"business_unit":"bu_a"}}'
```
**期望：** ✅ 200，meta 首 / done 末，delta ≥ 2；DB 落库 user→assistant，assistant `status=complete`。

### B3. 销售合同责任人 —— 正常全链路（SALES_OWNER + contract_id）

```bash
curl -N -X POST http://localhost:8000/api/v1/chat/sse \
  -H 'Content-Type: application/json' \
  -H 'X-User-Account: owner_alice' \
  -H 'X-User-Role: SALES_OWNER' \
  -H 'X-User-Claims: {"contract_ids":["C001","C002"]}' \
  -d '{"biz_id":"sales_contract_agent","thread_id":"th_sc_1","w3_account":"owner_alice","question":"合同 C001 付款进度？","biz_params":{"contract_id":"C001"}}'
```
**期望：** ✅ 200，meta 首 / done 末，delta ≥ 2；DB 落库 user→assistant。

### B4. 追问（同 thread_id 复用历史）
连续发两个请求，`thread_id` 相同（如 `th_followup`），第二个问题是对第一个的追问：
```bash
# 第一问
curl -N -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Account: alice' -H 'X-User-Role: REP' -H 'X-User-Claims: {"regions":["huadong"]}' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_followup","w3_account":"alice","question":"第一个问题","biz_params":{"region":"huadong"}}'

# 追问
curl -N -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Account: alice' -H 'X-User-Role: REP' -H 'X-User-Claims: {"regions":["huadong"]}' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_followup","w3_account":"alice","question":"第二个问题（追问）","biz_params":{"region":"huadong"}}'
```
**期望：**
- ✅ 两个请求都 200
- ✅ DB `ma_message` 该 thread 下 **4 行**，角色顺序 user→assistant→user→assistant
- ✅ 第 1、3 行 content 分别为 `第一个问题`、`第二个问题（追问）`

> 注：历史拼接进 LLM context 的验证需接真实 LLM 才能看 prompt，当前仅验证落库。

---

## C. 鉴权失败场景（构造错误请求头触发）

> 这类场景**不返回 HTTP 4xx**，而是返回 200 的 SSE 流，内部走 reject 节点：输出一条 `delta`（拒绝话术）+ `done`。**不会**有 `error` 事件，也**不会**调用下游适配器。

### C1. 角色不符 —— FORBIDDEN_TOPIC
代表小管家要求 `REP`，给 `VIEWER`：
```bash
curl -N -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Account: alice' -H 'X-User-Role: VIEWER' -H 'X-User-Claims: {"regions":["huadong"]}' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_c1","w3_account":"alice","question":"hi","biz_params":{"region":"huadong"}}'
```
**期望：** ✅ 200；✅ 末帧 `done`；✅ 恰好 1 个 `delta`，其 `content` 含 `REP`（话术：`该专题仅限 REP 角色访问。`）；✅ 不含下游回答内容；✅ DB 落 assistant 消息（reject 文案，status=complete）。

### C2. 数据范围越权 —— FORBIDDEN_SCOPE
代表小管家：region 不在用户的 `regions` 列表里：
```bash
curl -N -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Account: alice' -H 'X-User-Role: REP' -H 'X-User-Claims: {"regions":["huadong"]}' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_c2","w3_account":"alice","question":"hi","biz_params":{"region":"huabei"}}'
```
**期望：** ✅ 200；✅ 1 个 `delta` 含 `huabei`（话术：`您无权访问 huabei 大区数据。`）；✅ `done` 末帧。

### C3. 缺少必需 biz_param —— FORBIDDEN_SCOPE
代表小管家不带 `region`：
```bash
curl -N -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Account: alice' -H 'X-User-Role: REP' -H 'X-User-Claims: {"regions":["huadong"]}' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_c3","w3_account":"alice","question":"hi","biz_params":{}}'
```
**期望：** ✅ 200；✅ 1 个 `delta` 含 `region`（话术：`请求缺少 region 参数。`）；✅ `done`。

> 同理可对经营小助手（business_unit）、销售合同（contract_id）验证缺参 / 越权话术。

---

## D. 请求校验失败场景

### D1. 未知 biz_id —— 404（HTTP 层快速校验）
```bash
curl -i -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -d '{"biz_id":"not_exist","thread_id":"th","w3_account":"alice","question":"hi"}'
```
**期望：** ✅ HTTP 404，detail 含 `unknown biz_id`（topic 存在性在进入 ChatService 前校验）。

### D2. 缺少必填字段 question —— 422（pydantic 校验）
```bash
curl -i -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th","w3_account":"alice"}'
```
**期望：** ✅ HTTP 422（FastAPI 自动校验 min_length=1）。

### D3. X-User-Account 与 body.w3_account 不一致 —— 403
```bash
curl -i -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Account: bob' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th","w3_account":"alice","question":"hi","biz_params":{"region":"huadong"}}'
```
**期望：** ✅ HTTP 403，detail 含 `X-User-Account header mismatch`。

### D4. 非法 X-User-Claims —— 400
```bash
curl -i -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Claims: not-a-json' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th","w3_account":"alice","question":"hi","biz_params":{"region":"huadong"}}'
```
**期望：** ✅ HTTP 400，detail 含 `invalid X-User-Claims`。

### D5. thread_id 跨专题复用 —— BAD_REQUEST
先用 `th_x` 发代表小管家，再用同一 `th_x` 发经营小助手：
```bash
curl -N -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Account: alice' -H 'X-User-Role: REP' -H 'X-User-Claims: {"regions":["huadong"]}' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_x","w3_account":"alice","question":"q1","biz_params":{"region":"huadong"}}'

curl -N -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -H 'X-User-Account: alice' -H 'X-User-Role: MANAGER' -H 'X-User-Claims: {"business_units":["bu_a"]}' \
  -d '{"biz_id":"jingying_xiaozhushou","thread_id":"th_x","w3_account":"alice","question":"q2","biz_params":{"business_unit":"bu_a"}}'
```
**期望：** ✅ 第二个流返回 200，含 1 个 `error` 事件 `code=BAD_REQUEST`（message 含 `跨专题`），末帧 `done` 且 `status="failed"`。

---

## E. 无 DB / 服务降级场景

> 把 `.env` 里 `MA_PG_DSN_RW` 注释掉重启，进入 smoke 模式。

### E1. 对话无 DB —— 503
```bash
curl -i -X POST http://localhost:8000/api/v1/chat/sse -H 'Content-Type: application/json' \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th","w3_account":"alice","question":"hi","biz_params":{"region":"huadong"}}'
```
**期望：** ✅ HTTP 503，detail 含 `DB not configured`。

### E2. 历史无 DB —— 503
```bash
curl -i http://localhost:8000/api/v1/sessions -H 'X-User-Account: alice'
```
**期望：** ✅ HTTP 503，detail `DB not configured`。

---

## F. 下游异常场景（需把下游指向会出错的环境）

> 通过 `.env` 把 `MA_METAGC_BASE_URL` 指向一个不存在的地址 / 慢响应来触发。

### F1. 下游超时 —— DOWNSTREAM_TIMEOUT
将 `MA_METAGC_BASE_URL` 指向 `http://127.0.0.1:9`（拒绝连接）或一个慢服务后发起 B1 的请求。
**期望：** ✅ 200 流；✅ 出现 `error` 事件，`code=DOWNSTREAM_TIMEOUT`，message 为专题话术 `服务繁忙，请稍后再试。`，`retryable=false`；✅ 末帧 `done`；✅ DB 落 assistant 消息 `status=partial`、内容为空。

### F2. 下游错误 —— DOWNSTREAM_ERROR
下游返回非 2xx 或异常帧。**期望：** ✅ `error` 事件 `code=DOWNSTREAM_ERROR`，message `查询遇到问题，请稍后再试或联系管理员。`；✅ `done`；✅ assistant `status=partial`。

---

## G. 意图路由 / fallback 场景

### G1. 意图分类 fallback（解析失败回退默认路由）
> 难以稳定触发真实 LLM 失败。用 fake 模式可确定性验证：在 topic YAML 的 `intent.params` 加 `fake_responses: ["unparseable garbage"]`，`MA_INTENT_LLM_PROVIDER=fake` 后发起请求。
**期望：** ✅ `progress(intent, status:"fallback")`，`detail` 含 `retried`；✅ `extra.route` = `metagc`（default_route）；✅ 流继续到 `done`，仍有 delta。

### G2. 不同问题路由到不同下游
用真实 LLM 时，提不同意图的问题（如"查合同"→可能 knowledge_center；"查经营数据"→metagc）。**期望：** ✅ `progress(intent).extra.route` 反映分类结果；✅ 对应下游被调用（看日志 / 下游服务是否收到请求）。

---

## H. 历史查询场景

> 需 DB，且 `X-User-Account` 头必填。

### H1. 列出会话 —— 缺 header —— 401
```bash
curl -i http://localhost:8000/api/v1/sessions
```
**期望：** ✅ HTTP 401，detail `X-User-Account header required`。

### H2. 列出会话 —— 仅返回本人会话（越权隔离）
先用 B1/B2 产生 alice 和别的账号的会话后：
```bash
curl -s http://localhost:8000/api/v1/sessions -H 'X-User-Account: alice'
```
**期望：** ✅ 200；✅ `items[].w3_account` 全为 `alice`，不含他人会话；✅ 可用 `biz_id` query 过滤、`limit` 限数（1–200）。

### H3. 列出会话消息 —— 本人 thread
```bash
curl -s http://localhost:8000/api/v1/sessions/th_demo_1/messages -H 'X-User-Account: alice'
```
**期望：** ✅ 200；✅ `items` 含 user + assistant 消息（最新在前 DESC）。

### H4. 列出会话消息 —— 他人 thread —— 404（不泄漏存在性）
```bash
curl -i http://localhost:8000/api/v1/sessions/th_demo_1/messages -H 'X-User-Account: bob'
```
**期望：** ✅ HTTP 404（**不是 403**，避免泄漏 thread 是否存在），detail `unknown thread_id`。

---

## I. WebSocket 场景

> 用 `websocat` 或 `wscat` 测试：`wscat -c "ws://localhost:8000/api/v1/chat/ws?w3_account=alice&biz_id=daibiao_xiaoguanjia"`

### I1. 握手 —— ready 帧
连接后服务端先发：
**期望：** ✅ `{"op":"ready","server_version":"0.2.0","heartbeat_interval_s":20}`。

### I2. ask —— 正常对话
连接收到 ready 后发送：
```json
{"op":"ask","request_id":"req_1","thread_id":"th_ws_1","question":"你好","biz_params":{"region":"huadong"}}
```
（需带 header 才能鉴权通过：连接前在 `wscat` 里用 `wscat -c ... -H "X-User-Role:REP" -H "X-User-Claims:{\"regions\":[\"huadong\"]}"`）
**期望：** ✅ 收到多条 `{"op":"event","request_id":"req_1","event":"meta|thinking|progress|delta|done",...}`；✅ 末条 `event":"done"`。

### I3. ping / pong 心跳
发送 `{"op":"ping"}`。**期望：** ✅ 收到 `{"op":"pong"}`。

### I4. 心跳超时
连接后 **45s 不发任何帧**。**期望：** ✅ 收到 `{"op":"closed","reason":"heartbeat timeout"}`，连接关闭（code 4408）。

### I5. 并发上限
快速连发 6 个不同 `request_id` 的 `ask`。**期望：** ✅ 前 5 个正常处理；✅ 第 6 个收到 `{"op":"error","reason":"too many concurrent requests (max 5)"}`。

### I6. cancel 取消任务
先发一个会跑较久的 ask，再发 `{"op":"cancel","request_id":"req_1"}`。**期望：** ✅ 该任务停止推送 event；✅ 该 request 的 assistant 消息落库 `status=partial`。

### I7. 非法 op
发送 `{"op":"foo"}`。**期望：** ✅ 收到 `{"op":"error","reason":"unknown or missing op: 'foo'"}`，连接不中断。

### I8. 无 DB —— closed 帧
smoke 模式下连接。**期望：** ✅ 收到 `{"op":"closed","reason":"DB not configured"}`，连接关闭（code 1011）。

### I9. 未知 biz_id —— closed 帧
连接 `?biz_id=not_exist`。**期望：** ✅ `{"op":"closed","reason":"unknown biz_id: not_exist"}`，关闭（code 1008）。

---

## J. 验证结果记录表

| 编号 | 场景 | 期望关键点 | 实际结果 | 通过 |
|---|---|---|---|---|
| A1 | 存活探针 | 200 alive | | ☐ |
| A2 | 就绪探针 | 200 ready / 503 not_ready | | ☐ |
| B1 | 代表小管家全链路 | meta→…→done, delta≥2, 落库 | | ☐ |
| B2 | 经营小助手全链路 | meta→…→done, 落库 | | ☐ |
| B3 | 销售合同全链路 | meta→…→done, 落库 | | ☐ |
| B4 | 追问复用历史 | 4 行 u/a/u/a | | ☐ |
| C1 | 角色不符 | reject delta 含 REP | | ☐ |
| C2 | 数据范围越权 | reject delta 含 huabei | | ☐ |
| C3 | 缺 biz_param | reject delta 含 region | | ☐ |
| D1 | 未知 biz_id | 404 | | ☐ |
| D2 | 缺 question | 422 | | ☐ |
| D3 | header 不一致 | 403 | | ☐ |
| D4 | 非法 claims | 400 | | ☐ |
| D5 | 跨专题复用 thread | error BAD_REQUEST, done failed | | ☐ |
| E1 | 对话无 DB | 503 | | ☐ |
| E2 | 历史无 DB | 503 | | ☐ |
| F1 | 下游超时 | error DOWNSTREAM_TIMEOUT, partial | | ☐ |
| F2 | 下游错误 | error DOWNSTREAM_ERROR, partial | | ☐ |
| G1 | 意图 fallback | progress fallback, route=metagc | | ☐ |
| H1 | 历史 缺 header | 401 | | ☐ |
| H2 | 历史 越权隔离 | 仅本人会话 | | ☐ |
| H3 | 历史 本人 thread | 200 含消息 | | ☐ |
| H4 | 历史 他人 thread | 404 | | ☐ |
| I1 | WS 握手 | ready 帧 | | ☐ |
| I2 | WS ask | event 流, done | | ☐ |
| I3 | WS ping | pong | | ☐ |
| I4 | WS 心跳超时 | closed heartbeat timeout | | ☐ |
| I5 | WS 并发上限 | 第 6 个 error | | ☐ |
| I6 | WS cancel | partial | | ☐ |
| I7 | WS 非法 op | error, 不中断 | | ☐ |
| I8 | WS 无 DB | closed DB not configured | | ☐ |
| I9 | WS 未知 biz_id | closed unknown biz_id | | ☐ |

---

## 附：已知行为 / 注意事项（验证时不必当 bug）

1. **鉴权失败是 `delta` 不是 `error`**：FORBIDDEN_TOPIC/FORBIDDEN_SCOPE 走 reject 节点输出拒绝话术，最后 `done` 仍为成功语义。这是设计行为，不是 bug。
2. **下游出错后 `done` 仍是 `complete`**：图内 `error` 后的 `done` 为 `{message_id, status:"complete"}`；只有图前快失败（TOPIC_NOT_FOUND / BAD_REQUEST / 跨专题）才是 `{status:"failed"}`。判断失败看 `error` 事件，不看 `done.status`。
3. **`tool` 事件当前不会出现**：协议预留，无适配器产生。
4. **adapter 的 `retryable` 恒为 false**：下游出错不会自动重试；只有适配器 `run` 抛异常才触发 `max_attempts=2` 重试。
5. **意图路由置信度恒为 1.0**：LLM 文本协议不带分数。
6. **WS 端点不在 Swagger 里**：FastAPI 不把 WebSocket 纳入 OpenAPI，需用 `wscat` 等工具测。
7. **e2e/integration 测试需 Docker**：要跑 OpenGauss 容器；无 Docker 时那些测试 skip。本文档的手工场景同样需要你本机起好数据库。
