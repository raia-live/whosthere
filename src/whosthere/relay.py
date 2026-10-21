"""The reference relay: SPEC.md §5 as a Starlette app over :class:`Store`.

Local-first. ``whosthere relay`` starts it on localhost with no auth and a
SQLite file; set ``WHOSTHERE_TOKEN`` to require a bearer token. Spaces are
open: the first knock or join creates them, and any authenticated caller
may join any space. A multi-tenant deployment wants a stricter
:class:`Authorizer`; the hooks are there, the policy is not.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .store import BROADCAST, NotAMember, Store

AGENT_HEADER = "X-Whosthere-Agent"

MAX_WAIT_S = 240
DEFAULT_WAIT_S = 240
MAX_TEXT_BYTES = 16_000
MAX_LIMIT = 200
POLL_INTERVAL_S = 0.5
MAX_WAITERS_PER_PRINCIPAL = 20


class Problem(Exception):
    def __init__(self, status: int, detail: str, headers: dict[str, str] | None = None):
        self.status, self.detail, self.headers = status, detail, headers


Authorizer = Callable[[Request], str | None]
"""Maps a request to a tenant key, or raises :class:`Problem`. The default
accepts everything when ``WHOSTHERE_TOKEN`` is unset and requires
``Authorization: Bearer <token>`` otherwise."""


def token_authorizer(token: str | None) -> Authorizer:
    def _auth(request: Request) -> str | None:
        if not token:
            return "local"
        header = request.headers.get("authorization", "")
        if header != f"Bearer {token}":
            raise Problem(401, "missing or invalid bearer token")
        return "token"

    return _auth


def _agent_name(request: Request, body: dict[str, Any] | None = None) -> str:
    name = (
        request.headers.get(AGENT_HEADER)
        or request.query_params.get("agent")
        or (body or {}).get("agent")
        or ""
    ).strip()
    if not name:
        raise Problem(400, f"agent is required: pass ?agent=<name> or the {AGENT_HEADER} header")
    return name


def _envelope(
    items: list[dict[str, Any]], *, space: str | None, timed_out: bool = False,
) -> dict[str, Any]:
    from .client import envelope

    return envelope(items, space=space, timed_out=timed_out)


def _limit(request: Request) -> int:
    raw = request.query_params.get("limit")
    if raw is None:
        return 50
    try:
        value = int(raw)
    except ValueError:
        raise Problem(400, "limit must be an integer") from None
    if value < 1:
        raise Problem(400, "limit must be at least 1")
    return min(value, MAX_LIMIT)


def build_app(
    store: Store | None = None,
    *,
    authorize: Authorizer | None = None,
    open_membership: bool = True,
) -> Starlette:
    store = store or Store(os.environ.get("WHOSTHERE_DB", ":memory:"))
    authorize = authorize or token_authorizer(os.environ.get("WHOSTHERE_TOKEN"))
    waiters: dict[str, int] = defaultdict(int)
    waiters_lock = asyncio.Lock()

    async def _json(request: Request) -> dict[str, Any]:
        try:
            body = await request.json()
        except Exception:
            raise Problem(400, "body must be a JSON object") from None
        if not isinstance(body, dict):
            raise Problem(400, "body must be a JSON object") from None
        return body

    async def limits(request: Request) -> JSONResponse:
        authorize(request)
        return JSONResponse({
            "max_wait_s": MAX_WAIT_S,
            "default_wait_s": DEFAULT_WAIT_S,
            "max_text_bytes": MAX_TEXT_BYTES,
            "max_limit": MAX_LIMIT,
        })

    async def join(request: Request) -> JSONResponse:
        authorize(request)
        body = await _json(request)
        name = _agent_name(request, body)
        space = str(body.get("space") or "").strip()
        if not space:
            raise Problem(400, "space is required")
        await run_in_threadpool(store.join, space, name)
        return JSONResponse(
            {"space": space, "agent": name, "members": store.members_of(space)}, status_code=201,
        )

    async def spaces(request: Request) -> JSONResponse:
        authorize(request)
        name = _agent_name(request)
        return JSONResponse({"spaces": await run_in_threadpool(store.spaces_of, name)})

    async def knock(request: Request) -> JSONResponse:
        authorize(request)
        body = await _json(request)
        missing = [f for f in ("space", "from", "to", "text") if not str(body.get(f) or "").strip()]
        if missing:
            raise Problem(400, f"missing required field(s): {', '.join(missing)}")
        text = str(body["text"])
        if len(text.encode("utf-8")) > MAX_TEXT_BYTES:
            raise Problem(413, f"text exceeds {MAX_TEXT_BYTES} bytes")
        thread = body.get("thread")
        if thread is not None and not isinstance(thread, int):
            raise Problem(400, "thread must be an integer event_id or null")
        meta = body.get("meta") or {}
        if not isinstance(meta, dict):
            raise Problem(400, "meta must be an object")
        sender = str(body["from"]).strip()
        space = str(body["space"]).strip()
        if open_membership:
            await run_in_threadpool(store.join, space, sender)
        try:
            event = await run_in_threadpool(
                lambda: store.knock(
                    space=space, sender=sender, recipient=str(body["to"]).strip(),
                    text=text, kind=str(body.get("kind") or "message"), thread=thread, meta=meta,
                )
            )
        except NotAMember as exc:
            raise Problem(403, str(exc)) from exc
        return JSONResponse(event, status_code=201)

    async def inbox(request: Request) -> JSONResponse:
        authorize(request)
        name = _agent_name(request)
        space = request.query_params.get("space") or None
        items = await run_in_threadpool(store.inbox, name=name, space=space, limit=_limit(request))
        return JSONResponse(_envelope(items, space=space))

    async def wait(request: Request) -> JSONResponse:
        tenant = authorize(request) or "local"
        name = _agent_name(request)
        space = request.query_params.get("space") or None
        limit = _limit(request)
        raw = request.query_params.get("timeout_s")
        try:
            timeout_s = float(raw) if raw is not None else DEFAULT_WAIT_S
        except ValueError:
            raise Problem(400, "timeout_s must be a number") from None
        timeout_s = max(0.0, min(timeout_s, MAX_WAIT_S))

        key = f"{tenant}:{name}"
        async with waiters_lock:
            if waiters[key] >= MAX_WAITERS_PER_PRINCIPAL:
                raise Problem(429, "too many parked waiters for this agent", {"Retry-After": "5"})
            waiters[key] += 1
        deadline = time.monotonic() + timeout_s
        try:
            while True:
                items = await run_in_threadpool(store.inbox, name=name, space=space, limit=limit)
                if items:
                    return JSONResponse(_envelope(items, space=space))
                remaining = deadline - time.monotonic()
                if remaining <= 0 or await request.is_disconnected():
                    return JSONResponse(_envelope([], space=space, timed_out=True))
                await asyncio.sleep(min(POLL_INTERVAL_S, remaining))
        finally:
            async with waiters_lock:
                waiters[key] -= 1
                if waiters[key] <= 0:
                    waiters.pop(key, None)

    async def ack(request: Request) -> JSONResponse:
        from .client import parse_cursor

        authorize(request)
        body = await _json(request)
        name = _agent_name(request, body)
        cursor = body.get("cursor")
        if not isinstance(cursor, str) or not cursor.strip():
            raise Problem(400, "cursor is required: pass the value a read returned")
        try:
            event_id, space = parse_cursor(cursor)
        except ValueError:
            raise Problem(400, "cursor must be the value returned by /v1/inbox or /v1/inbox/wait") from None
        acked = await run_in_threadpool(store.ack, name=name, event_id=event_id, space=space)
        return JSONResponse({"acknowledged": acked, "cursor": cursor})

    async def on_problem(request: Request, exc: Problem) -> JSONResponse:
        return JSONResponse({"detail": exc.detail}, status_code=exc.status, headers=exc.headers)

    app = Starlette(
        routes=[
            Route("/v1/limits", limits, methods=["GET"]),
            Route("/v1/spaces", spaces, methods=["GET"]),
            Route("/v1/spaces/join", join, methods=["POST"]),
            Route("/v1/knock", knock, methods=["POST"]),
            Route("/v1/inbox", inbox, methods=["GET"]),
            Route("/v1/inbox/wait", wait, methods=["GET"]),
            Route("/v1/inbox/ack", ack, methods=["POST"]),
        ],
        exception_handlers={Problem: on_problem},
    )
    app.state.store = store
    return app


__all__ = ["build_app", "Problem", "token_authorizer", "AGENT_HEADER", "BROADCAST"]
