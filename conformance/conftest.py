"""Where the suite points.

``WHOSTHERE_URL`` (and optionally ``WHOSTHERE_TOKEN``) selects the relay
under test. Without it, the reference relay is started in-process on a free
port, so ``pytest conformance`` passes on a fresh checkout and the same
tests then apply unchanged to any other implementation.
"""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid

import httpx
import pytest


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def relay_url() -> str:
    url = os.environ.get("WHOSTHERE_URL")
    if url:
        yield url.rstrip("/")
        return

    import uvicorn

    from whosthere.relay import build_app
    from whosthere.store import Store

    port = _free_port()
    config = uvicorn.Config(build_app(Store(":memory:")), host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            httpx.get(f"{base}/v1/limits", timeout=1)
            break
        except Exception:
            time.sleep(0.05)
    else:
        raise RuntimeError("reference relay did not start")
    yield base
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture(scope="session")
def token() -> str | None:
    return os.environ.get("WHOSTHERE_TOKEN")


@pytest.fixture
def space() -> str:
    """A fresh space per test, so runs against a shared relay do not collide."""
    return f"conformance/{uuid.uuid4().hex[:12]}"


@pytest.fixture
def make_client(relay_url: str, token: str | None):
    from whosthere.client import RelayClient

    made = []

    def _make(agent: str) -> RelayClient:
        c = RelayClient(relay_url, agent=agent, token=token)
        made.append(c)
        return c

    yield _make
    for c in made:
        c.close()
