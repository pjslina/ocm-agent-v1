# MasterAgent M0 — 脚手架与骨架 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 MasterAgent (MA) 的工程骨架立起来：FastAPI 异步服务能跑、SSE 端点返回合法 stub 事件流、结构化日志 + OTel + CI 全部联动；**不**做任何业务逻辑。

**Architecture:**
- 单 Docker 镜像、FastAPI + uvicorn + Python 3.12 全异步栈。
- 分层目录结构按设计书 §5.9 落定（`api/` `core/` `plugins/` `infra/`）。
- 关键约束：Settings 启动 fail-fast、structlog contextvars 注入三 ID、OTel SDK 织入根 span、`ChatEvent` 抽象与 SSE 编码器先就位（后续里程碑直接用）。

**Tech Stack:** Python 3.12、FastAPI 0.115+、uvicorn 0.32+、Pydantic v2、pydantic-settings、structlog、OpenTelemetry SDK + OTLP exporter、ulid-py、pytest + pytest-asyncio + pytest-cov、ruff、mypy、uv 包管理。

**Spec reference:** `docs/superpowers/specs/2026-06-23-master-agent-design.md` 第 1 / 5.9 / 6 / 8.2 / 8.3 / 9.4 (M0) 节。

**Branch:** 当前 `design/master-agent-v1`（设计书所在分支）。M0 在新分支 `feature/m0-scaffold` 开发。

---

## File Structure (M0 创建/修改清单)

### 创建
```
pyproject.toml                              ← uv + 依赖声明
uv.lock                                     ← lockfile（uv 生成，进仓库）
.python-version                             ← "3.12"
Dockerfile                                  ← 多阶段构建
.dockerignore
README.md                                   ← 项目入口（一段话 + 快速启动）
Makefile                                    ← dev/test/lint/build 一键入口
ruff.toml                                   ← lint 配置
mypy.ini                                    ← 类型检查配置

src/ma/__init__.py
src/ma/main.py                              ← FastAPI app + lifespan
src/ma/api/__init__.py
src/ma/api/health.py                        ← /healthz /readyz
src/ma/api/sse.py                           ← /api/v1/chat/sse (stub 流)
src/ma/core/__init__.py
src/ma/core/chat_service.py                 ← 骨架（吐固定 stub 事件流）
src/ma/core/graph/__init__.py
src/ma/core/graph/state.py                  ← GraphState TypedDict
src/ma/core/graph/events.py                 ← ChatEvent + 7 种类型 + SSE 编码器
src/ma/infra/__init__.py
src/ma/infra/settings.py                    ← Pydantic Settings
src/ma/infra/logging.py                     ← structlog + contextvars + 脱敏
src/ma/infra/tracing.py                     ← OTel SDK 初始化

tests/__init__.py
tests/conftest.py                           ← 通用 fixture
tests/unit/__init__.py
tests/unit/test_events.py                   ← ChatEvent + SSE 编码器单测
tests/unit/test_settings.py                 ← Settings 加载/fail-fast 单测
tests/unit/test_logging.py                  ← contextvars 注入 + 脱敏单测
tests/unit/test_sse_endpoint.py             ← /api/v1/chat/sse stub 端到端单测
tests/unit/test_health.py                   ← healthz/readyz 单测

.github/workflows/ci.yml                    ← GitHub Actions（如团队用 GitLab/Jenkins 自行换）
.gitignore                                  ← 追加 .venv/ __pycache__/ .pytest_cache/ 等
```

### 修改
（无 —— M0 全新文件）

### M0 不创建（留给后续里程碑）
- `src/ma/api/ws.py`（M4）
- `src/ma/api/rest.py`（M2）
- `src/ma/core/graph/builtins.py` `node_wrappers.py`（M1）
- `src/ma/core/plugin/`（M1）
- `src/ma/core/topic/`（M1）
- `src/ma/core/repo/`（M1）
- `src/ma/plugins/`（M1）
- `src/ma/infra/db.py` `http_client.py` `ws_client.py`（M1）
- `sql/` `prompts/` `config/topics/` `db/`（M1）

---

## Task 1: 仓库脚手架与分支

**Files:**
- Modify: 当前 git 分支

- [ ] **Step 1: 切到 M0 工作分支**

设计书在 `design/master-agent-v1` 分支已合入。M0 在它之上拉新分支：

```bash
cd E:/work/source/python/ocm-agent-v1
git checkout design/master-agent-v1
git pull
git checkout -b feature/m0-scaffold
git status
```

Expected: `On branch feature/m0-scaffold, nothing to commit, working tree clean`

- [ ] **Step 2: 确认 Python 3.12 在 PATH 上**

```bash
python --version
```

Expected: `Python 3.12.x`（任一 3.12.x 都可）

若未安装：先装 Python 3.12（项目工作目录是 Windows，可用 [python.org installer] 或 `winget install Python.Python.3.12`），再回到本步骤。

- [ ] **Step 3: 安装 uv（包管理器）**

```bash
pip install --user uv
uv --version
```

Expected: `uv 0.4.x` 或更高。

---

## Task 2: pyproject.toml + 依赖锁定

**Files:**
- Create: `pyproject.toml`
- Create: `.python-version`
- Create: `uv.lock`（uv 生成）

- [ ] **Step 1: 创建 `.python-version`**

```text
3.12
```

- [ ] **Step 2: 创建 `pyproject.toml`**

```toml
[project]
name = "ma"
version = "0.0.1"
description = "MasterAgent - enterprise AI agent gateway"
requires-python = ">=3.12,<3.13"
readme = "README.md"

dependencies = [
    "fastapi>=0.115,<0.116",
    "uvicorn[standard]>=0.32,<0.33",
    "pydantic>=2.9,<3",
    "pydantic-settings>=2.6,<3",
    "structlog>=24.4,<25",
    "ulid-py>=1.1,<2",
    "opentelemetry-api>=1.27,<2",
    "opentelemetry-sdk>=1.27,<2",
    "opentelemetry-exporter-otlp-proto-grpc>=1.27,<2",
    "opentelemetry-instrumentation-fastapi>=0.48b0,<1",
    "httpx>=0.27,<0.28",
]

[dependency-groups]
dev = [
    "pytest>=8.3,<9",
    "pytest-asyncio>=0.24,<0.25",
    "pytest-cov>=5.0,<6",
    "ruff>=0.7,<0.8",
    "mypy>=1.13,<2",
    "types-ulid>=1.0,<2",
]

[tool.uv]
package = true

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
addopts = "-ra --strict-markers"

[tool.coverage.run]
source = ["src/ma"]
branch = true

[tool.coverage.report]
show_missing = true
skip_covered = false
```

- [ ] **Step 3: 用 uv 解析并生成 lockfile**

```bash
uv sync --all-groups
```

Expected: 创建 `.venv/`，生成 `uv.lock`，无错误。

- [ ] **Step 4: 验证 import**

```bash
uv run python -c "import fastapi, uvicorn, pydantic, structlog, opentelemetry; print('ok')"
```

Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock .python-version
git commit -m "build: bootstrap project with uv and pinned core deps"
```

---

## Task 3: ruff / mypy / .gitignore

**Files:**
- Create: `ruff.toml`
- Create: `mypy.ini`
- Modify: `.gitignore`

- [ ] **Step 1: 创建 `ruff.toml`**

```toml
target-version = "py312"
line-length = 100
src = ["src", "tests"]

[lint]
select = [
    "E", "F", "W",       # pycodestyle + pyflakes
    "I",                  # isort
    "B",                  # bugbear
    "UP",                 # pyupgrade
    "ASYNC",              # async pitfalls
    "RUF",                # ruff-specific
]
ignore = [
    "E501",   # line length (formatter handles)
]

[format]
quote-style = "double"
indent-style = "space"
```

- [ ] **Step 2: 创建 `mypy.ini`**

```ini
[mypy]
python_version = 3.12
strict = True
warn_unreachable = True
mypy_path = src
explicit_package_bases = True
namespace_packages = True

[mypy-tests.*]
disallow_untyped_decorators = False

[mypy-ulid.*]
ignore_missing_imports = True
```

- [ ] **Step 3: 更新 `.gitignore`（追加；保留既有内容）**

读已有 `.gitignore` 并在末尾追加（如果对应行已存在则跳过该行）：

```
# Python
__pycache__/
*.pyc
*.pyo
*.egg-info/
.venv/
.eggs/

# Test / coverage
.pytest_cache/
.coverage
coverage.xml
htmlcov/

# Type checker
.mypy_cache/
.ruff_cache/

# OS
.DS_Store
Thumbs.db
```

- [ ] **Step 4: 验证 lint 通过（暂无源码，应当 No issues）**

```bash
uv run ruff check .
uv run ruff format --check .
```

Expected: `All checks passed!` 与 `0 files would be reformatted`

- [ ] **Step 5: Commit**

```bash
git add ruff.toml mypy.ini .gitignore
git commit -m "build: configure ruff + mypy + ignore patterns"
```

---

## Task 4: Settings（fail-fast 加载） — TDD

**Files:**
- Create: `src/ma/__init__.py`
- Create: `src/ma/infra/__init__.py`
- Create: `src/ma/infra/settings.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/unit/test_settings.py`

参考：设计书 §8.2.2、§8.2.3。

- [ ] **Step 1: 创建空 `__init__.py`**

```bash
mkdir -p src/ma/infra tests/unit
touch src/ma/__init__.py src/ma/infra/__init__.py tests/__init__.py tests/unit/__init__.py
```

- [ ] **Step 2: 写失败测试 `tests/unit/test_settings.py`**

```python
"""Settings 必须按 MA_ENV / MA_LOG_LEVEL / MA_... 加载，缺失关键字段立即报错。

M0 只覆盖最少 env / log_level，其它字段在后续里程碑接入对应基础设施时再加。
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError


def test_settings_load_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.setenv("MA_LOG_LEVEL", "info")

    from ma.infra.settings import Settings

    s = Settings()
    assert s.env == "dev"
    assert s.log_level == "info"


def test_settings_missing_env_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MA_ENV", raising=False)

    from ma.infra.settings import Settings

    with pytest.raises(ValidationError):
        Settings()


def test_settings_invalid_env_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "production")  # not in Literal

    from ma.infra.settings import Settings

    with pytest.raises(ValidationError):
        Settings()


def test_settings_default_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.delenv("MA_LOG_LEVEL", raising=False)

    from ma.infra.settings import Settings

    s = Settings()
    assert s.log_level == "info"
```

- [ ] **Step 3: 跑测试，确认失败**

```bash
uv run pytest tests/unit/test_settings.py -v
```

Expected: 4 个测试全部失败，错误信息含 `ModuleNotFoundError: No module named 'ma.infra.settings'`。

- [ ] **Step 4: 实现 `src/ma/infra/settings.py`**

```python
"""全局 Settings：所有环境配置的唯一入口。任何缺失项启动即崩溃。"""
from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """M0 仅声明 env / log_level；后续里程碑在此追加 DB / 下游 / LLM / OTel 等字段。

    所有字段经 MA_ 前缀的环境变量加载（如 MA_ENV=dev）。
    """

    model_config = SettingsConfigDict(
        env_prefix="MA_",
        env_file=None,
        case_sensitive=False,
        extra="ignore",
    )

    env: Literal["dev", "test", "staging", "prod"]
    log_level: Literal["debug", "info", "warning", "error"] = "info"
```

- [ ] **Step 5: 跑测试，确认通过**

```bash
uv run pytest tests/unit/test_settings.py -v
```

Expected: 4 passed。

- [ ] **Step 6: Commit**

```bash
git add src/ma/__init__.py src/ma/infra/__init__.py src/ma/infra/settings.py \
        tests/__init__.py tests/unit/__init__.py tests/unit/test_settings.py
git commit -m "feat(infra): add Settings with fail-fast env loading"
```

---

## Task 5: 结构化日志 + contextvars 注入 — TDD

**Files:**
- Create: `src/ma/infra/logging.py`
- Create: `tests/unit/test_logging.py`

参考：设计书 §6.2 / §6.3.1 / §6.3.3。

- [ ] **Step 1: 写失败测试 `tests/unit/test_logging.py`**

```python
"""结构化日志：每条日志必带 request_id/thread_id/w3_account（contextvars 注入），
   且敏感字段（authorization/cookie/token/x-api-key）必被脱敏为 ***。"""
from __future__ import annotations

import json
import logging
from io import StringIO

import pytest


@pytest.fixture(autouse=True)
def reset_structlog() -> None:
    """每个测试都重新 configure，避免 processor 链跨用例污染。"""
    import structlog
    structlog.reset_defaults()


def _capture(stream: StringIO) -> None:
    """把 structlog 输出导向给定 stream。"""
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.DEBUG)


def test_logger_emits_json_with_event_and_timestamp() -> None:
    from ma.infra.logging import configure_logging, get_logger

    buf = StringIO()
    configure_logging("info")
    _capture(buf)
    log = get_logger("test")
    log.info("chat_started", biz_id="x")

    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "chat_started"
    assert payload["biz_id"] == "x"
    assert payload["level"] == "info"
    assert "timestamp" in payload


def test_contextvars_inject_three_ids() -> None:
    from ma.infra.logging import (
        bind_request_ctx,
        configure_logging,
        get_logger,
    )

    buf = StringIO()
    configure_logging("info")
    _capture(buf)
    bind_request_ctx(request_id="req_1", thread_id="th_1", w3_account="zhangsan")
    log = get_logger("test")
    log.info("auth_passed")

    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert payload["request_id"] == "req_1"
    assert payload["thread_id"] == "th_1"
    assert payload["w3_account"] == "zhangsan"


def test_sensitive_keys_are_redacted() -> None:
    from ma.infra.logging import configure_logging, get_logger

    buf = StringIO()
    configure_logging("info")
    _capture(buf)
    log = get_logger("test")
    log.info(
        "downstream_called",
        authorization="Bearer xxx",
        cookie="abc=1",
        token="t-secret",
        x_api_key="k-secret",
        other_field="ok",
    )

    payload = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert payload["authorization"] == "***"
    assert payload["cookie"] == "***"
    assert payload["token"] == "***"
    assert payload["x_api_key"] == "***"
    assert payload["other_field"] == "ok"


def test_log_level_filters() -> None:
    from ma.infra.logging import configure_logging, get_logger

    buf = StringIO()
    configure_logging("warning")
    _capture(buf)
    log = get_logger("test")
    log.info("ignored_event")
    log.warning("kept_event")

    lines = [line for line in buf.getvalue().strip().splitlines() if line]
    payloads = [json.loads(line) for line in lines]
    events = [p["event"] for p in payloads]
    assert "ignored_event" not in events
    assert "kept_event" in events
```

- [ ] **Step 2: 跑测试，确认失败**

```bash
uv run pytest tests/unit/test_logging.py -v
```

Expected: 4 个测试全部失败，错误信息含 `ModuleNotFoundError: No module named 'ma.infra.logging'`。

- [ ] **Step 3: 实现 `src/ma/infra/logging.py`**

```python
"""structlog 配置：JSON 输出 + contextvars 三 ID 注入 + 敏感字段脱敏。

调用顺序：configure_logging(level) 一次 → bind_request_ctx(...) 在请求入口
→ get_logger(__name__).info("event_name", **kv) 在业务代码中。
"""
from __future__ import annotations

import contextvars
import logging
from typing import Any

import structlog

_SENSITIVE_KEYS = frozenset(
    {"authorization", "cookie", "token", "x-api-key", "x_api_key", "set-cookie"}
)

_ctx_request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_id", default=None
)
_ctx_thread_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "thread_id", default=None
)
_ctx_w3: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "w3_account", default=None
)


def bind_request_ctx(
    *,
    request_id: str | None = None,
    thread_id: str | None = None,
    w3_account: str | None = None,
) -> None:
    """在请求入口调用一次，让后续日志自动带上三 ID。"""
    if request_id is not None:
        _ctx_request_id.set(request_id)
    if thread_id is not None:
        _ctx_thread_id.set(thread_id)
    if w3_account is not None:
        _ctx_w3.set(w3_account)


def _inject_ids(
    _logger: Any, _method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    event_dict.setdefault("request_id", _ctx_request_id.get())
    event_dict.setdefault("thread_id", _ctx_thread_id.get())
    event_dict.setdefault("w3_account", _ctx_w3.get())
    return event_dict


def _sanitize(
    _logger: Any, _method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    for k in list(event_dict):
        if k.lower() in _SENSITIVE_KEYS:
            event_dict[k] = "***"
    return event_dict


def configure_logging(level: str = "info") -> None:
    """配置 structlog 全局处理链与 stdlib logging level。

    多次调用幂等（每次都会 reset）。
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _inject_ids,
            _sanitize,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """获取一个 structlog logger。"""
    return structlog.get_logger(name)  # type: ignore[return-value]
```

- [ ] **Step 4: 跑测试，确认通过**

```bash
uv run pytest tests/unit/test_logging.py -v
```

Expected: 4 passed。

- [ ] **Step 5: Commit**

```bash
git add src/ma/infra/logging.py tests/unit/test_logging.py
git commit -m "feat(infra): structlog with contextvars-injected request/thread/w3 IDs and sensitive-key redaction"
```

---

## Task 6: ChatEvent + SSE 编码器 — TDD

**Files:**
- Create: `src/ma/core/__init__.py`
- Create: `src/ma/core/graph/__init__.py`
- Create: `src/ma/core/graph/events.py`
- Create: `tests/unit/test_events.py`

参考：设计书 §1.2、§4.2.2（7 种事件类型与 schema）、§4.7.1（SSE 实现要点）。

- [ ] **Step 1: 创建空 `__init__.py`**

```bash
mkdir -p src/ma/core/graph
touch src/ma/core/__init__.py src/ma/core/graph/__init__.py
```

- [ ] **Step 2: 写失败测试 `tests/unit/test_events.py`**

```python
"""ChatEvent 与 SSE 编码：7 种 type 全覆盖；编码格式严格遵守 SSE 规范。"""
from __future__ import annotations

import json

import pytest


def test_event_type_literal_covers_seven_types() -> None:
    from ma.core.graph.events import ChatEvent, EventType

    # type 必须是 Literal 七选一
    allowed = {"meta", "thinking", "progress", "delta", "tool", "error", "done"}
    # mypy 没法在运行时验证 Literal，所以这里靠值合法性校验
    for t in allowed:
        ev = ChatEvent(type=t, data={"k": "v"})  # type: ignore[arg-type]
        assert ev.type == t

    # 类型别名暴露给外部，便于注解
    assert EventType is not None


def test_event_to_sse_frame_basic() -> None:
    from ma.core.graph.events import ChatEvent, encode_sse

    ev = ChatEvent(type="delta", data={"content": "hello"})
    frame = encode_sse(ev)
    # SSE 规范：event: <name>\ndata: <json>\n\n
    assert frame.endswith("\n\n")
    lines = frame.rstrip("\n").splitlines()
    assert lines[0] == "event: delta"
    assert lines[1].startswith("data: ")
    assert json.loads(lines[1][len("data: ") :]) == {"content": "hello"}


def test_event_to_sse_frame_with_id() -> None:
    from ma.core.graph.events import ChatEvent, encode_sse

    ev = ChatEvent(type="done", data={"message_id": "msg_1"}, event_id="msg_1")
    frame = encode_sse(ev)
    lines = frame.rstrip("\n").splitlines()
    assert "id: msg_1" in lines
    assert "event: done" in lines


def test_event_sse_frame_data_is_single_line_json() -> None:
    """SSE 规范：data 字段如有换行需多行 data:。我们采用 compact JSON 强制单行。"""
    from ma.core.graph.events import ChatEvent, encode_sse

    ev = ChatEvent(
        type="meta",
        data={"thread_id": "th_1", "message_id": "msg_1", "route": "metagc"},
    )
    frame = encode_sse(ev)
    data_lines = [
        ln for ln in frame.rstrip("\n").splitlines() if ln.startswith("data: ")
    ]
    assert len(data_lines) == 1


def test_sse_keepalive_comment_frame() -> None:
    """SSE 心跳：注释帧 `: keep-alive\\n\\n` 浏览器自动忽略。"""
    from ma.core.graph.events import encode_keepalive

    frame = encode_keepalive()
    assert frame == ": keep-alive\n\n"


def test_event_data_must_be_json_serializable() -> None:
    from ma.core.graph.events import ChatEvent, encode_sse

    class NotSerializable:
        pass

    ev = ChatEvent(type="delta", data={"obj": NotSerializable()})  # type: ignore[dict-item]
    with pytest.raises(TypeError):
        encode_sse(ev)
```

- [ ] **Step 3: 跑测试，确认失败**

```bash
uv run pytest tests/unit/test_events.py -v
```

Expected: 全部失败（`ModuleNotFoundError`）。

- [ ] **Step 4: 实现 `src/ma/core/graph/events.py`**

```python
"""ChatEvent：MA 内部统一的事件流元素。+ SSE 协议编码器。

参见设计书 §4.2.2：7 种事件类型 meta/thinking/progress/delta/tool/error/done。
SSE 规范：`event:` `data:` `id:` 三类字段，事件之间空行分隔。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

EventType = Literal[
    "meta",
    "thinking",
    "progress",
    "delta",
    "tool",
    "error",
    "done",
]


@dataclass(slots=True, frozen=True)
class ChatEvent:
    """MA 内部事件流的最小单元。

    type   - 设计书 §4.2.2 七选一
    data   - 必须 JSON 可序列化
    event_id - 可选，对应 SSE 的 id 字段（前端可用作 last-event-id）
    """

    type: EventType
    data: Any
    event_id: str | None = None


def encode_sse(event: ChatEvent) -> str:
    """把 ChatEvent 编码为一个完整的 SSE 帧（含末尾空行）。

    出错条件：data 不可 JSON 序列化 → 抛 TypeError。
    """
    payload = json.dumps(event.data, ensure_ascii=False, separators=(",", ":"))
    parts: list[str] = [f"event: {event.type}"]
    if event.event_id is not None:
        parts.append(f"id: {event.event_id}")
    parts.append(f"data: {payload}")
    return "\n".join(parts) + "\n\n"


def encode_keepalive() -> str:
    """SSE 注释行作为心跳，浏览器 EventSource 自动忽略。"""
    return ": keep-alive\n\n"
```

- [ ] **Step 5: 跑测试，确认通过**

```bash
uv run pytest tests/unit/test_events.py -v
```

Expected: 6 passed。

- [ ] **Step 6: Commit**

```bash
git add src/ma/core/__init__.py src/ma/core/graph/__init__.py \
        src/ma/core/graph/events.py tests/unit/test_events.py
git commit -m "feat(core): ChatEvent (7 types) and SSE encoder + keepalive"
```

---

## Task 7: GraphState 骨架

**Files:**
- Create: `src/ma/core/graph/state.py`

参考：设计书 §2.1。

M0 不写单测：`GraphState` 是 `TypedDict`，纯类型声明，没有运行时行为；它的字段合并语义（`Annotated[..., add]`）要在 M1 才有 LangGraph 真跑可测。

- [ ] **Step 1: 实现 `src/ma/core/graph/state.py`**

```python
"""GraphState：LangGraph 图执行的共享状态对象。

设计书 §2.1。M0 阶段先把字段全声明出来，让后续里程碑的节点能直接 import；
LangGraph 真正用它（编译图、跑节点）是 M1 的任务。
"""
from __future__ import annotations

from operator import add
from typing import Annotated, Any, Literal, TypedDict

Route = Literal["metagc", "uniioc", "kc", "reject", "default"]


class GraphState(TypedDict, total=False):
    # ── 输入区 ─────────────────────────
    request_id: str
    biz_id: str
    thread_id: str
    identity: Any            # AuthnIdentity，M5+ 落地具体类型
    question: str
    biz_params: dict[str, Any]
    history: list[Any]       # list[Message]，M1 落地 Message 模型

    # ── 鉴权区 ─────────────────────────
    user_ctx: dict[str, Any]
    auth: Any                # AuthResult，M1 落地

    # ── 补全区 ─────────────────────────
    enriched_question: str
    enrich_meta: dict[str, Any]

    # ── 路由区 ─────────────────────────
    route: Route
    route_confidence: float
    route_reason: str

    # ── 下游输出区 ────────────────────────
    answer_chunks: Annotated[list[str], add]
    answer_meta: dict[str, Any]

    # ── 错误区 ─────────────────────────
    errors: Annotated[list[dict[str, Any]], add]
```

- [ ] **Step 2: 验证类型 + lint 通过**

```bash
uv run mypy src/ma/core/graph/state.py
uv run ruff check src/ma/core/graph/state.py
```

Expected: `Success: no issues found`、`All checks passed!`

- [ ] **Step 3: Commit**

```bash
git add src/ma/core/graph/state.py
git commit -m "feat(core): declare GraphState TypedDict skeleton"
```

---

## Task 8: ChatService 骨架（吐固定 stub 事件流）— TDD

**Files:**
- Create: `src/ma/core/chat_service.py`

M0 的 ChatService 极简：传入一个最小 ChatRequest（dataclass）→ 异步产出固定 5 件事件 → 用于让 `/api/v1/chat/sse` 端到端连通。**不接 LangGraph、不接 DB、不接任何插件。**

参考：设计书 §1.3 大致流程、§4.2.2 事件序。

- [ ] **Step 1: 写失败测试（追加到 `tests/unit/test_events.py` 末尾**

```python
# ────────────────────────── ChatService 骨架 ──────────────────────────
import asyncio


@pytest.mark.asyncio
async def test_chat_service_yields_meta_first_done_last() -> None:
    from ma.core.chat_service import ChatRequest, ChatService

    svc = ChatService()
    req = ChatRequest(
        biz_id="daibiao_xiaoguanjia",
        thread_id="th_test",
        w3_account="zhangsan",
        question="hello",
        request_id="req_test",
    )
    events = [ev async for ev in svc.handle(req)]
    assert events[0].type == "meta"
    assert events[-1].type == "done"


@pytest.mark.asyncio
async def test_chat_service_yields_at_least_one_delta() -> None:
    from ma.core.chat_service import ChatRequest, ChatService

    svc = ChatService()
    req = ChatRequest(
        biz_id="x", thread_id="y", w3_account="z", question="hi", request_id="r1"
    )
    deltas = [ev async for ev in svc.handle(req) if ev.type == "delta"]
    assert len(deltas) >= 1
    for d in deltas:
        assert "content" in d.data


@pytest.mark.asyncio
async def test_chat_service_meta_contains_thread_id_and_request_id() -> None:
    from ma.core.chat_service import ChatRequest, ChatService

    svc = ChatService()
    req = ChatRequest(
        biz_id="b",
        thread_id="th_xyz",
        w3_account="z",
        question="q",
        request_id="req_xyz",
    )
    first = await anext(svc.handle(req).__aiter__())  # type: ignore[func-returns-value]
    assert first.type == "meta"
    assert first.data["thread_id"] == "th_xyz"
    assert first.data["request_id"] == "req_xyz"
```

注意：上面那行 `anext(...)` 在某些 Python 版本下需要直接迭代器。改用更稳的写法：

```python
@pytest.mark.asyncio
async def test_chat_service_meta_contains_thread_id_and_request_id() -> None:
    from ma.core.chat_service import ChatRequest, ChatService

    svc = ChatService()
    req = ChatRequest(
        biz_id="b",
        thread_id="th_xyz",
        w3_account="z",
        question="q",
        request_id="req_xyz",
    )
    first = None
    async for ev in svc.handle(req):
        first = ev
        break
    assert first is not None
    assert first.type == "meta"
    assert first.data["thread_id"] == "th_xyz"
    assert first.data["request_id"] == "req_xyz"
```

把上一段三个测试用例追加进 `tests/unit/test_events.py`。**确保第三个用例用稳的写法（不用 `anext`）。**

- [ ] **Step 2: 跑测试，确认失败**

```bash
uv run pytest tests/unit/test_events.py -v -k chat_service
```

Expected: 3 个用例失败（`ModuleNotFoundError`）。

- [ ] **Step 3: 实现 `src/ma/core/chat_service.py`**

```python
"""ChatService：M0 骨架。

收到 ChatRequest → 产出固定 5 件事件流（meta/thinking/delta×2/done）。
后续里程碑会接 LangGraph、Repository、Plugin —— 但 *接口签名保持稳定*：
    async def handle(req: ChatRequest) -> AsyncIterator[ChatEvent]
这样 Transport 层（SSE/WS）从 M0 起就不用再改。
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from ma.core.graph.events import ChatEvent


@dataclass(slots=True)
class ChatRequest:
    biz_id: str
    thread_id: str
    w3_account: str
    question: str
    request_id: str
    biz_params: dict[str, Any] = field(default_factory=dict)


class ChatService:
    """M0 实现：不接任何下游，只产固定 stub 事件流以验证 Transport 层。"""

    async def handle(self, req: ChatRequest) -> AsyncIterator[ChatEvent]:
        yield ChatEvent(
            type="meta",
            data={
                "thread_id": req.thread_id,
                "request_id": req.request_id,
                "route": "stub",
            },
        )
        # 轻微 yield 让事件不被同一事件循环周期吞并（更接近真实流式体感）
        await asyncio.sleep(0)
        yield ChatEvent(
            type="thinking",
            data={"phase": "stub", "label": "MA M0 stub running…"},
        )
        await asyncio.sleep(0)
        yield ChatEvent(type="delta", data={"content": "hello "})
        await asyncio.sleep(0)
        yield ChatEvent(type="delta", data={"content": "world"})
        await asyncio.sleep(0)
        yield ChatEvent(
            type="done",
            data={"message_id": f"msg_stub_{req.request_id}", "status": "complete"},
        )
```

- [ ] **Step 4: 跑测试，确认通过**

```bash
uv run pytest tests/unit/test_events.py -v
```

Expected: 9 passed（前 6 个 + 新 3 个）。

- [ ] **Step 5: Commit**

```bash
git add src/ma/core/chat_service.py tests/unit/test_events.py
git commit -m "feat(core): ChatService skeleton yielding stub event stream"
```

---

## Task 9: OTel SDK 初始化

**Files:**
- Create: `src/ma/infra/tracing.py`

M0 的 OTel：能初始化 + 设置 service.name + 可选导出到 OTLP endpoint。**不**写单测（OTel 全局 state、provider 一旦设置就难重置，单元测试性价比低；后续里程碑用集成测试验证导出真到 collector）。

参考：设计书 §6.4。

- [ ] **Step 1: 把 OTel 字段加入 Settings**

修改 `src/ma/infra/settings.py`，在 `log_level` 之后追加：

```python
    # ── 可观测性 ──────────────────────
    otel_exporter_otlp_endpoint: str | None = None
    otel_service_name: str = "master-agent"
```

- [ ] **Step 2: 在 `tests/unit/test_settings.py` 末尾追加测试**

```python
def test_otel_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_ENV", "dev")
    monkeypatch.delenv("MA_OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("MA_OTEL_SERVICE_NAME", raising=False)

    from ma.infra.settings import Settings

    s = Settings()
    assert s.otel_exporter_otlp_endpoint is None
    assert s.otel_service_name == "master-agent"
```

跑测试：

```bash
uv run pytest tests/unit/test_settings.py -v
```

Expected: 5 passed。

- [ ] **Step 3: 实现 `src/ma/infra/tracing.py`**

```python
"""OpenTelemetry SDK 初始化。

调用一次 `configure_tracing(settings)`，之后业务代码用：
    from opentelemetry import trace
    tracer = trace.get_tracer("ma.<module>")
"""
from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from ma.infra.settings import Settings


def configure_tracing(settings: Settings) -> None:
    """初始化全局 TracerProvider。

    无 OTLP endpoint 时仍设置 provider（spans 进内存即被丢弃），
    便于本地开发不依赖 collector 也能跑。
    """
    resource = Resource.create(
        {
            "service.name": settings.otel_service_name,
            "service.environment": settings.env,
        }
    )
    provider = TracerProvider(resource=resource)
    if settings.otel_exporter_otlp_endpoint:
        provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
            )
        )
    trace.set_tracer_provider(provider)
```

- [ ] **Step 4: 验证 import 可用**

```bash
uv run python -c "from ma.infra.tracing import configure_tracing; print('ok')"
```

Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add src/ma/infra/settings.py src/ma/infra/tracing.py tests/unit/test_settings.py
git commit -m "feat(infra): OpenTelemetry SDK init with OTLP exporter"
```

---

## Task 10: Health / Ready endpoints — TDD

**Files:**
- Create: `src/ma/api/__init__.py`
- Create: `src/ma/api/health.py`
- Create: `tests/unit/test_health.py`

参考：设计书 §4.1、§6.7。M0 阶段 readyz 只检查 "settings 已加载" 这一项；DB / Topic 编译等后续里程碑追加。

- [ ] **Step 1: 创建 `src/ma/api/__init__.py`**

```bash
mkdir -p src/ma/api
touch src/ma/api/__init__.py
```

- [ ] **Step 2: 写失败测试 `tests/unit/test_health.py`**

```python
"""/healthz 任何时候 200；/readyz 启动期可能 503，settings 就绪后 200。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MA_ENV", "dev")
    from ma.main import create_app

    return create_app()


def test_healthz_returns_200(app) -> None:
    client = TestClient(app)
    r = client.get("/api/v1/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "alive"}


def test_readyz_returns_200_after_startup(app) -> None:
    """create_app 完成后系统已 mark_ready，readyz=200。"""
    client = TestClient(app)
    r = client.get("/api/v1/readyz")
    assert r.status_code == 200
    assert r.json()["status"] == "ready"


def test_readyz_returns_503_before_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """直接 import 模块 + 不走 lifespan 的情况下 readyz 应当 503。"""
    monkeypatch.setenv("MA_ENV", "dev")
    from ma.api.health import _state, router
    from fastapi import FastAPI

    _state.ready = False  # 强制重置
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    client = TestClient(app)
    r = client.get("/api/v1/readyz")
    assert r.status_code == 503
```

- [ ] **Step 3: 跑测试，确认失败**

```bash
uv run pytest tests/unit/test_health.py -v
```

Expected: 3 个用例失败（`ma.main` 与 `ma.api.health` 都不存在）。

- [ ] **Step 4: 实现 `src/ma/api/health.py`**

```python
"""/healthz /readyz 端点。

readyz 的 ready 标志由 lifespan 在所有启动检查通过后置 True；
shutdown 时置回 False，让 K8s 30s 内摘流量（设计书 §8.4.3）。
"""
from __future__ import annotations

from dataclasses import dataclass

from fastapi import APIRouter, Response, status

router = APIRouter()


@dataclass
class _ReadyState:
    ready: bool = False


_state = _ReadyState()


def mark_ready() -> None:
    _state.ready = True


def mark_not_ready() -> None:
    _state.ready = False


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/readyz")
async def readyz(response: Response) -> dict[str, str]:
    if not _state.ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready"}
    return {"status": "ready"}
```

- [ ] **Step 5: 测试还会失败 —— 因为 `ma.main` 还没创建**

继续 Task 11 创建 main.py。

---

## Task 11: FastAPI app + lifespan

**Files:**
- Create: `src/ma/main.py`

参考：设计书 §8.2.3 启动顺序。M0 阶段简化版：load Settings → configure logging → configure tracing → mark_ready → 接流量。后续 DB/Topic 加在 lifespan 里。

- [ ] **Step 1: 实现 `src/ma/main.py`**

```python
"""FastAPI app 入口。

启动顺序（设计书 §8.2.3）M0 简化版：
1. 加载 Settings → 失败即退出
2. configure logging
3. configure tracing
4. mark_ready
5. 接流量
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI

from ma.api import health
from ma.infra.logging import configure_logging, get_logger
from ma.infra.settings import Settings
from ma.infra.tracing import configure_tracing


def create_app() -> FastAPI:
    settings = Settings()
    configure_logging(settings.log_level)
    configure_tracing(settings)
    log = get_logger("ma.main")

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        log.info("startup_begin", env=settings.env)
        # 后续里程碑：在此处初始化 DB pool / 加载 plugins / 编译 topics
        health.mark_ready()
        log.info("startup_complete")
        yield
        health.mark_not_ready()
        log.info("shutdown_complete")

    app = FastAPI(
        title="MasterAgent",
        version="0.0.1",
        lifespan=lifespan,
    )
    app.include_router(health.router, prefix="/api/v1")
    return app


# uvicorn 入口
app = create_app()
```

- [ ] **Step 2: 跑测试，确认 health 测试全过**

```bash
uv run pytest tests/unit/test_health.py -v
```

Expected: 3 passed。

- [ ] **Step 3: 手动跑一次服务确认能起来**

```bash
MA_ENV=dev uv run uvicorn ma.main:app --host 127.0.0.1 --port 8000 --log-level info
```

新开一个 terminal：

```bash
curl -s http://127.0.0.1:8000/api/v1/healthz
curl -s http://127.0.0.1:8000/api/v1/readyz
```

Expected:
- `/healthz` → `{"status":"alive"}`
- `/readyz` → `{"status":"ready"}`

Ctrl+C 关掉 uvicorn。

- [ ] **Step 4: Commit**

```bash
git add src/ma/api/__init__.py src/ma/api/health.py src/ma/main.py \
        tests/unit/test_health.py
git commit -m "feat(api): FastAPI app with lifespan and /healthz /readyz endpoints"
```

---

## Task 12: SSE 端点 `/api/v1/chat/sse` — TDD

**Files:**
- Create: `src/ma/api/sse.py`
- Create: `tests/unit/test_sse_endpoint.py`
- Modify: `src/ma/main.py`（注册路由）

参考：设计书 §4.2 SSE 通道、§4.7.1 实现要点。

- [ ] **Step 1: 写失败测试 `tests/unit/test_sse_endpoint.py`**

```python
"""POST /api/v1/chat/sse：体合法 → 200 + text/event-stream + 合法 SSE 帧序列。"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("MA_ENV", "dev")
    from ma.main import create_app

    return TestClient(create_app())


def _parse_sse(body: str) -> list[dict]:
    """把 SSE 响应体解析为 [{event, data, id?}, ...]。"""
    events: list[dict] = []
    for block in body.split("\n\n"):
        block = block.strip("\n")
        if not block or block.startswith(":"):
            continue
        ev: dict = {}
        for line in block.splitlines():
            if line.startswith("event: "):
                ev["event"] = line[len("event: ") :]
            elif line.startswith("data: "):
                ev["data"] = json.loads(line[len("data: ") :])
            elif line.startswith("id: "):
                ev["id"] = line[len("id: ") :]
        if ev:
            events.append(ev)
    return events


def test_sse_returns_text_event_stream_header(client: TestClient) -> None:
    body = {
        "biz_id": "daibiao_xiaoguanjia",
        "thread_id": "th_test",
        "w3_account": "zhangsan",
        "question": "hi",
    }
    with client.stream("POST", "/api/v1/chat/sse", json=body) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        assert r.headers["cache-control"] == "no-cache"
        assert r.headers["x-accel-buffering"] == "no"
        assert "x-request-id" in r.headers


def test_sse_yields_meta_first_done_last(client: TestClient) -> None:
    body = {
        "biz_id": "daibiao_xiaoguanjia",
        "thread_id": "th_a",
        "w3_account": "zhangsan",
        "question": "hi",
    }
    with client.stream("POST", "/api/v1/chat/sse", json=body) as r:
        text = "".join(r.iter_text())
    events = _parse_sse(text)
    assert events[0]["event"] == "meta"
    assert events[-1]["event"] == "done"


def test_sse_request_id_propagates_to_meta(client: TestClient) -> None:
    body = {
        "biz_id": "x",
        "thread_id": "th_a",
        "w3_account": "z",
        "question": "hi",
    }
    headers = {"X-Request-Id": "req_caller"}
    with client.stream("POST", "/api/v1/chat/sse", json=body, headers=headers) as r:
        assert r.headers["x-request-id"] == "req_caller"
        text = "".join(r.iter_text())
    events = _parse_sse(text)
    meta = events[0]
    assert meta["event"] == "meta"
    assert meta["data"]["request_id"] == "req_caller"


def test_sse_request_id_auto_generated_when_missing(client: TestClient) -> None:
    body = {
        "biz_id": "x",
        "thread_id": "th_a",
        "w3_account": "z",
        "question": "hi",
    }
    with client.stream("POST", "/api/v1/chat/sse", json=body) as r:
        rid = r.headers["x-request-id"]
        text = "".join(r.iter_text())
    assert rid.startswith("req_")
    events = _parse_sse(text)
    assert events[0]["data"]["request_id"] == rid


def test_sse_rejects_missing_question(client: TestClient) -> None:
    body = {
        "biz_id": "x",
        "thread_id": "th_a",
        "w3_account": "z",
        # missing question
    }
    r = client.post("/api/v1/chat/sse", json=body)
    assert r.status_code == 422  # FastAPI validation
```

- [ ] **Step 2: 跑测试，确认失败**

```bash
uv run pytest tests/unit/test_sse_endpoint.py -v
```

Expected: 全部失败（路由不存在）。

- [ ] **Step 3: 实现 `src/ma/api/sse.py`**

```python
"""POST /api/v1/chat/sse 端点。

把 ChatService.handle() 产出的 ChatEvent 异步流 编码成 SSE 响应。
设计书 §4.2 / §4.7.1。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import ulid
from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ma.core.chat_service import ChatRequest, ChatService
from ma.core.graph.events import encode_sse
from ma.infra.logging import bind_request_ctx, get_logger

router = APIRouter()
log = get_logger("ma.api.sse")


class ChatBody(BaseModel):
    biz_id: str = Field(min_length=1)
    thread_id: str = Field(min_length=1)
    w3_account: str = Field(min_length=1)
    question: str = Field(min_length=1)
    biz_params: dict[str, Any] = Field(default_factory=dict)


def _new_request_id() -> str:
    return f"req_{ulid.new()!s}"


@router.post("/chat/sse")
async def chat_sse(
    request: Request,
    body: ChatBody,
    x_request_id: str | None = Header(default=None),
) -> StreamingResponse:
    rid = x_request_id or _new_request_id()
    bind_request_ctx(
        request_id=rid, thread_id=body.thread_id, w3_account=body.w3_account
    )
    log.info("chat_started", biz_id=body.biz_id)

    svc: ChatService = request.app.state.chat_service  # type: ignore[attr-defined]
    chat_req = ChatRequest(
        biz_id=body.biz_id,
        thread_id=body.thread_id,
        w3_account=body.w3_account,
        question=body.question,
        request_id=rid,
        biz_params=body.biz_params,
    )

    async def stream() -> AsyncIterator[bytes]:
        async for ev in svc.handle(chat_req):
            yield encode_sse(ev).encode("utf-8")

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "X-Request-Id": rid,
        },
    )
```

- [ ] **Step 4: 把 SSE router 注册到 app；把 ChatService 挂到 app.state**

修改 `src/ma/main.py`：

```python
"""FastAPI app 入口。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI

from ma.api import health, sse
from ma.core.chat_service import ChatService
from ma.infra.logging import configure_logging, get_logger
from ma.infra.settings import Settings
from ma.infra.tracing import configure_tracing


def create_app() -> FastAPI:
    settings = Settings()
    configure_logging(settings.log_level)
    configure_tracing(settings)
    log = get_logger("ma.main")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info("startup_begin", env=settings.env)
        app.state.chat_service = ChatService()
        health.mark_ready()
        log.info("startup_complete")
        yield
        health.mark_not_ready()
        log.info("shutdown_complete")

    app = FastAPI(title="MasterAgent", version="0.0.1", lifespan=lifespan)
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(sse.router, prefix="/api/v1")
    return app


app = create_app()
```

- [ ] **Step 5: 跑测试**

```bash
uv run pytest tests/unit/test_sse_endpoint.py -v
```

Expected: 5 passed。

- [ ] **Step 6: 手动端到端验证**

```bash
MA_ENV=dev uv run uvicorn ma.main:app --host 127.0.0.1 --port 8000
```

另一 terminal：

```bash
curl -N -X POST http://127.0.0.1:8000/api/v1/chat/sse \
  -H "Content-Type: application/json" \
  -H "X-Request-Id: req_manual" \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_1","w3_account":"zhangsan","question":"hi"}'
```

Expected: 在终端看到合法 SSE 帧序列：
```
event: meta
data: {"thread_id":"th_1","request_id":"req_manual","route":"stub"}

event: thinking
data: {"phase":"stub","label":"MA M0 stub running…"}

event: delta
data: {"content":"hello "}

event: delta
data: {"content":"world"}

event: done
data: {"message_id":"msg_stub_req_manual","status":"complete"}
```

Ctrl+C 关掉 uvicorn。

- [ ] **Step 7: Commit**

```bash
git add src/ma/api/sse.py src/ma/main.py tests/unit/test_sse_endpoint.py
git commit -m "feat(api): SSE chat endpoint streaming stub ChatEvent flow"
```

---

## Task 13: 全套测试 + 覆盖率确认

**Files:**
- Modify: `pyproject.toml`（可选追加覆盖率门槛）

- [ ] **Step 1: 跑全套测试 + 覆盖率**

```bash
uv run pytest --cov=ma --cov-report=term-missing -v
```

Expected: 全部 passed；总体覆盖率 ≥ 80%。

如果某个文件覆盖率显著低于 80% 且不在 M0 边界外：补测试。

- [ ] **Step 2: 跑 ruff + mypy 全量检查**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
```

Expected: 三条都通过。

- [ ] **Step 3: 如有 lint 或类型问题，修复并 commit**

```bash
git add <files>
git commit -m "chore: fix lint/type issues from full sweep"
```

---

## Task 14: Makefile（开发者一键入口）

**Files:**
- Create: `Makefile`

- [ ] **Step 1: 创建 `Makefile`**

```makefile
.PHONY: dev test lint type fmt check build run

dev:
	uv sync --all-groups

test:
	uv run pytest -v

cov:
	uv run pytest --cov=ma --cov-report=term-missing --cov-report=html

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

fmt-check:
	uv run ruff format --check .

type:
	uv run mypy src

check: lint fmt-check type test

run:
	MA_ENV=dev uv run uvicorn ma.main:app --host 0.0.0.0 --port 8000 --reload

build:
	docker build -t master-agent:dev .
```

- [ ] **Step 2: 测试 `make check` 跑通**

```bash
make check
```

Expected: 全部通过。

> Windows 用户没有 `make`：装一个 `choco install make` 或直接复制 Makefile 里对应 `uv run ...` 命令运行。这一点写进 README Step 3。

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "chore: Makefile with dev/test/lint/type/check/run targets"
```

---

## Task 15: Dockerfile + .dockerignore

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

参考：设计书 §8.3。

- [ ] **Step 1: 创建 `.dockerignore`**

```
.git/
.github/
.venv/
__pycache__/
*.pyc
*.pyo
.pytest_cache/
.mypy_cache/
.ruff_cache/
.coverage
coverage.xml
htmlcov/
docs/
tests/
*.md
!README.md
.idea/
.vscode/
.DS_Store
Thumbs.db
```

- [ ] **Step 2: 创建 `Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /build
RUN pip install --no-cache-dir uv==0.4.30
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ ./src/
COPY README.md ./
RUN uv sync --frozen --no-dev

# ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd -g 1000 ma && useradd -u 1000 -g ma -m ma

WORKDIR /app
COPY --from=builder /build/.venv /app/.venv
COPY --from=builder /build/src /app/src
COPY pyproject.toml /app/

USER ma
EXPOSE 8000
CMD ["uvicorn", "ma.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

> 后续里程碑追加 `COPY config/ ./config/` `COPY sql/ ./sql/` `COPY prompts/ ./prompts/` `COPY db/ ./db/`。M0 不需要这些目录，先不 COPY。

- [ ] **Step 3: 构建镜像**

```bash
docker build -t master-agent:dev .
```

Expected: 构建成功，无 error。镜像 size 在 ~250 MB 量级（slim 基础 + 依赖）。

- [ ] **Step 4: 跑镜像验证**

```bash
docker run --rm -d --name ma-m0 -p 8000:8000 -e MA_ENV=dev master-agent:dev
sleep 3
curl -s http://127.0.0.1:8000/api/v1/healthz
curl -s http://127.0.0.1:8000/api/v1/readyz
docker stop ma-m0
```

Expected:
- `/healthz` → `{"status":"alive"}`
- `/readyz` → `{"status":"ready"}`

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "build: multi-stage Dockerfile + .dockerignore"
```

---

## Task 16: CI（GitHub Actions）

**Files:**
- Create: `.github/workflows/ci.yml`

> 若团队用 GitLab CI / Jenkins / Drone，原则相同（lint → type → test → build），把 YAML 翻译成对应 DSL 即可。

- [ ] **Step 1: 创建 `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main, "feature/**", "design/**"]
  pull_request:
    branches: [main]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install uv
        run: pip install uv==0.4.30

      - name: Sync deps
        run: uv sync --frozen --all-groups

      - name: Ruff lint
        run: uv run ruff check .

      - name: Ruff format check
        run: uv run ruff format --check .

      - name: mypy
        run: uv run mypy src

      - name: pytest with coverage
        env:
          MA_ENV: test
        run: uv run pytest --cov=ma --cov-report=xml --cov-report=term -v

      - name: Upload coverage artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: coverage-xml
          path: coverage.xml

  docker:
    runs-on: ubuntu-latest
    needs: check
    steps:
      - uses: actions/checkout@v4
      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3
      - name: Build image
        run: docker build -t master-agent:ci .
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: GitHub Actions for lint/type/test + docker build"
```

> CI 真正绿要等推到远程后才能看；本机已经 `make check` 全过即可。

---

## Task 17: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: 创建 `README.md`**

```markdown
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

环境变量：

| 变量 | 必填 | 默认 | 含义 |
|---|---|---|---|
| `MA_ENV` | ✅ | — | `dev` / `test` / `staging` / `prod` |
| `MA_LOG_LEVEL` | | `info` | `debug` / `info` / `warning` / `error` |
| `MA_OTEL_EXPORTER_OTLP_ENDPOINT` | | `None` | OTLP gRPC endpoint，留空则 trace 仅在内存中 |
| `MA_OTEL_SERVICE_NAME` | | `master-agent` | OTel resource attribute |

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
````

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: README with quickstart and M0 status"
```

---

## Task 18: M0 出口验证（清单核对）

参考：设计书 §9.4 M0 出口标准。

- [ ] **Step 1: 全套检查通过**

```bash
make check
```

Expected: 全绿。

- [ ] **Step 2: Docker 镜像构建通过**

```bash
make build
```

Expected: 构建成功。

- [ ] **Step 3: 跑 Docker 镜像 + curl SSE 拿到合法事件帧**

```bash
docker run --rm -d --name ma-m0 -p 8000:8000 -e MA_ENV=dev master-agent:dev
sleep 3

# 健康检查
curl -s http://127.0.0.1:8000/api/v1/healthz
curl -s http://127.0.0.1:8000/api/v1/readyz

# SSE 端到端
curl -N -X POST http://127.0.0.1:8000/api/v1/chat/sse \
  -H "Content-Type: application/json" \
  -H "X-Request-Id: req_m0_verify" \
  -d '{"biz_id":"daibiao_xiaoguanjia","thread_id":"th_m0","w3_account":"zhangsan","question":"hello m0"}'

docker stop ma-m0
```

Expected:
- 健康检查返回 200
- SSE 至少有 1 个 `meta`、≥ 1 个 `delta`、1 个 `done` 事件
- 响应 header 含 `X-Request-Id: req_m0_verify`

- [ ] **Step 4: 检查日志为结构化 JSON 且含三 ID**

跑 SSE 时 uvicorn stdout 应当输出类似：

```json
{"event":"chat_started","biz_id":"daibiao_xiaoguanjia","request_id":"req_m0_verify","thread_id":"th_m0","w3_account":"zhangsan","level":"info","timestamp":"2026-06-23T..."}
```

含 `event` / `request_id` / `thread_id` / `w3_account` / `level` / `timestamp` 6 个字段。

- [ ] **Step 5: 覆盖率 ≥ 80%**

```bash
make cov
```

查看 `htmlcov/index.html`，确认总体覆盖率达标。如未达标，补测试到达标后提交。

- [ ] **Step 6: 合并到 design 分支并打 tag**

```bash
git checkout design/master-agent-v1
git merge --no-ff feature/m0-scaffold -m "merge: M0 scaffold"
git tag m0-complete
git status
```

- [ ] **Step 7: 写 M0 完成笔记到 plans 目录**

创建 `docs/superpowers/plans/2026-06-23-master-agent-m0-scaffold.notes.md`：

```markdown
# M0 完成笔记

- 日期：<完成日期>
- 覆盖率：<实际数值>
- 镜像 size：<docker images master-agent:dev 的 size>
- 已知偏差（与设计书不同的实现细节）：
  - <若有，列出来；若无，写"无"。>
- 给 M1 的提示：
  - <实施过程中发现的需要在 M1 注意的事项>
```

提交：

```bash
git add docs/superpowers/plans/2026-06-23-master-agent-m0-scaffold.notes.md
git commit -m "docs: M0 completion notes"
```

---

## M0 出口标准（来自设计书 §9.4）

到此 M0 完成的硬指标：

- [x] 端到端 curl SSE 能拿合法事件帧
- [x] 结构化日志、trace、CI 全部联动
- [x] **没有**任何业务逻辑
- [x] Docker 镜像可构建并运行
- [x] 单元测试覆盖率 ≥ 80%
- [x] `make check` 全绿

满足以上即可宣布 M0 完成，进入 M1 计划。

---

## Self-Review 结果

**1. Spec coverage（设计书 M0 节覆盖）**：

| 设计书 M0 任务 | 对应 Task |
|---|---|
| 仓库初始化、目录结构 | Task 1 / 2 / 5-12 文件创建 |
| pyproject.toml + uv.lock + Dockerfile | Task 2 / 15 |
| FastAPI app + /healthz/readyz + Settings | Task 4 / 10 / 11 |
| structlog + contextvars 注入 | Task 5 |
| OTel SDK + 根 span | Task 9 + FastAPI instrumentor 已在依赖里（M1 织入 graph span） |
| GraphState + events + 最小 ChatService 骨架 | Task 6 / 7 / 8 |
| /api/v1/chat/sse 返回 stub 流 | Task 12 |
| CI：ruff + mypy + pytest | Task 3 / 16 |

**注**：OTel FastAPI instrumentor 已加入依赖但 M0 不显式 `.instrument()` 调用 —— 因为目前 ChatService 还没织 graph span，外层 HTTP span 价值有限；M1 加 LangGraph 节点 span 时一并织入。该决策见 M0 完成笔记。

**2. Placeholder scan**：已扫一遍，无 TBD / TODO / 占位符。每步都有完整代码与命令。

**3. Type consistency**：
- `ChatRequest` 字段（biz_id/thread_id/w3_account/question/request_id/biz_params）在 chat_service.py / sse.py / 测试 三处一致
- `ChatEvent.type` 在 events.py 的 `EventType` Literal 与所有测试断言一致（7 种）
- `Settings.env` Literal `["dev","test","staging","prod"]` 与所有测试用例一致
- `bind_request_ctx` 三参数命名（request_id/thread_id/w3_account）与 sse.py / logging.py / 测试 三处一致

---

**Plan complete and saved to** `docs/superpowers/plans/2026-06-23-master-agent-m0-scaffold.md`.

**Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
