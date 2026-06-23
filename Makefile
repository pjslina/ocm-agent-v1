.PHONY: dev test lint type fmt check build run cov fmt-check

dev:
	python -m uv sync --all-groups

test:
	python -m uv run pytest -v

cov:
	python -m uv run pytest --cov=ma --cov-report=term-missing --cov-report=html

lint:
	python -m uv run ruff check .

fmt:
	python -m uv run ruff format .

fmt-check:
	python -m uv run ruff format --check .

type:
	python -m uv run mypy src

check: lint fmt-check type test

run:
	MA_ENV=dev python -m uv run uvicorn ma.main:app --host 0.0.0.0 --port 8000 --reload

build:
	docker build -t master-agent:dev .
