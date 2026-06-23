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
    """create_app + TestClient lifespan completes → readyz=200."""
    client = TestClient(app)
    # TestClient context manager triggers lifespan
    with client:
        r = client.get("/api/v1/readyz")
        assert r.status_code == 200
        assert r.json()["status"] == "ready"


def test_readyz_returns_503_before_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """No lifespan triggered → readyz remains not_ready → 503."""
    monkeypatch.setenv("MA_ENV", "dev")
    from fastapi import FastAPI

    from ma.api.health import _state, router

    _state.ready = False  # force reset
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    client = TestClient(app)
    r = client.get("/api/v1/readyz")
    assert r.status_code == 503
