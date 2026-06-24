# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /build
RUN pip install --no-cache-dir uv==0.11.24
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
COPY config/ /app/config/
COPY sql/ /app/sql/
COPY prompts/ /app/prompts/
COPY db/ /app/db/

USER ma
EXPOSE 8000
CMD ["uvicorn", "ma.main:app", "--host", "0.0.0.0", "--port", "8000"]
