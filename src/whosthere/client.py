"""Clients with one interface: :class:`RelayClient` over HTTP to any relay
(SPEC.md §5), and :class:`LocalClient` straight onto a SQLite file so agents
on one machine need no server at all."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol

import httpx

from .relay import AGENT_HEADER, DEFAULT_WAIT_S, MAX_LIMIT, MAX_TEXT_BYTES, MAX_WAIT_S
from .store import Store


def make_cursor(event_id: int, space: str | None) -> str:
    """The token a read hands out: ``"60"`` unscoped, ``"60@<space>"`` scoped.

    Event ids are one sequence across spaces, so a bare id cannot be
    acknowledged safely: acking 60 from a one-space read across every space
    would move another space's cursor past an unshown 50. The token carries
    the read's scope so the ack cannot reach wider than the read did.
    """
    return f"{event_id}@{space}" if space else str(event_id)


def parse_cursor(token: str) -> tuple[int, str | None]:
    """``(event_id, space)`` from a token; ``ValueError`` if malformed."""
    text = str(token).strip()
    event_part, sep, space_part = text.partition("@")
    event_id = int(event_part)
    if event_id < 0:
        raise ValueError("event id must be non-negative")
    if sep and not space_part:
        raise ValueError("scoped cursor without a space")
    return event_id, (space_part if sep else None)


def envelope(
    items: list[dict[str, Any]], *, space: str | None, timed_out: bool = False,
) -> dict[str, Any]:
    top = max((i["event_id"] for i in items), default=None)
    out: dict[str, Any] = {
        "items": items,
        "count": len(items),
        "cursor": make_cursor(top, space) if top is not None else None,
    }
    if timed_out:
        out["timed_out"] = True
    return out


class Client(Protocol):
    agent: str

    def limits(self) -> dict[str, Any]: ...
    def join(self, space: str) -> dict[str, Any]: ...
    def spaces(self) -> list[str]: ...
    def knock(self, *, space: str, to: str, text: str, kind: str = "message",
              thread: int | None = None, meta: dict[str, Any] | None = None) -> dict[str, Any]: ...
    def peek(self, *, space: str | None = None, limit: int = 50) -> dict[str, Any]: ...
    def wait(self, *, space: str | None = None, timeout_s: float = 240,
             limit: int = 50) -> dict[str, Any]: ...
    def ack(self, cursor: str) -> dict[str, Any]: ...
    def close(self) -> None: ...


def default_db_path() -> str:
    return os.environ.get("WHOSTHERE_DB") or str(Path.home() / ".whosthere" / "relay.db")


def from_env(agent: str | None = None) -> Client:
    """The client the environment describes: ``WHOSTHERE_URL`` for a relay,
    otherwise the local SQLite file at ``WHOSTHERE_DB``."""
    name = (agent or os.environ.get("WHOSTHERE_AGENT") or "").strip()
    url = os.environ.get("WHOSTHERE_URL", "").strip()
    if url:
        return RelayClient(url, agent=name, token=os.environ.get("WHOSTHERE_TOKEN"))
    return LocalClient(default_db_path(), agent=name)


class LocalClient:
    """Same verbs as :class:`RelayClient`, no HTTP: the store is the relay."""

    def __init__(self, db: str | Path | Store, *, agent: str) -> None:
        self.agent = agent
        if isinstance(db, Store):
            self._store = db
        else:
            Path(db).parent.mkdir(parents=True, exist_ok=True) if str(db) != ":memory:" else None
            self._store = Store(db)

    def close(self) -> None:
        self._store.close()

    def limits(self) -> dict[str, Any]:
        return {"max_wait_s": MAX_WAIT_S, "default_wait_s": DEFAULT_WAIT_S,
                "max_text_bytes": MAX_TEXT_BYTES, "max_limit": MAX_LIMIT}

    def join(self, space: str) -> dict[str, Any]:
        self._store.join(space, self.agent)
        return {"space": space, "agent": self.agent, "members": self._store.members_of(space)}

    def spaces(self) -> list[str]:
        return self._store.spaces_of(self.agent)

    def knock(self, *, space: str, to: str, text: str, kind: str = "message",
              thread: int | None = None, meta: dict[str, Any] | None = None) -> dict[str, Any]:
        self._store.join(space, self.agent)
        return self._store.knock(space=space, sender=self.agent, recipient=to, text=text,
                                 kind=kind, thread=thread, meta=meta)

    def peek(self, *, space: str | None = None, limit: int = 50) -> dict[str, Any]:
        return envelope(self._store.inbox(name=self.agent, space=space, limit=limit), space=space)

    def wait(self, *, space: str | None = None, timeout_s: float = 240, limit: int = 50) -> dict[str, Any]:
        items = self._store.wait(name=self.agent, space=space, timeout_s=timeout_s, limit=limit)
        return envelope(items, space=space, timed_out=not items)

    def ack(self, cursor: str) -> dict[str, Any]:
        event_id, space = parse_cursor(cursor)
        return {"acknowledged": self._store.ack(name=self.agent, event_id=event_id, space=space),
                "cursor": str(cursor)}


class RelayClient:
    def __init__(
        self,
        base_url: str,
        *,
        agent: str,
        token: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.agent = agent
        headers = {AGENT_HEADER: agent}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.Client(base_url=self.base_url, headers=headers, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> RelayClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- protocol ------------------------------------------------------------

    def limits(self) -> dict[str, Any]:
        r = self._client.get("/v1/limits")
        r.raise_for_status()
        return r.json()

    def join(self, space: str) -> dict[str, Any]:
        r = self._client.post("/v1/spaces/join", json={"space": space, "agent": self.agent})
        r.raise_for_status()
        return r.json()

    def spaces(self) -> list[str]:
        r = self._client.get("/v1/spaces")
        r.raise_for_status()
        return r.json()["spaces"]

    def knock(
        self,
        *,
        space: str,
        to: str,
        text: str,
        kind: str = "message",
        thread: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "space": space, "from": self.agent, "to": to, "text": text, "kind": kind,
        }
        if thread is not None:
            body["thread"] = thread
        if meta:
            body["meta"] = meta
        r = self._client.post("/v1/knock", json=body)
        r.raise_for_status()
        return r.json()

    def peek(self, *, space: str | None = None, limit: int = 50) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if space:
            params["space"] = space
        r = self._client.get("/v1/inbox", params=params)
        r.raise_for_status()
        return r.json()

    def wait(
        self, *, space: str | None = None, timeout_s: float = 240, limit: int = 50,
    ) -> dict[str, Any]:
        """One bounded round of the relay's long-poll.

        The socket timeout is widened past ``timeout_s`` so the relay, not
        this client, ends the round. Callers that want to wait longer than
        the relay's cap loop this.
        """
        params: dict[str, Any] = {"timeout_s": timeout_s, "limit": limit}
        if space:
            params["space"] = space
        r = self._client.get(
            "/v1/inbox/wait", params=params, timeout=httpx.Timeout(timeout_s + 30, connect=10),
        )
        r.raise_for_status()
        return r.json()

    def ack(self, cursor: str) -> dict[str, Any]:
        """Acknowledge with the token a read returned, untouched."""
        r = self._client.post("/v1/inbox/ack", json={"agent": self.agent, "cursor": str(cursor)})
        r.raise_for_status()
        return r.json()
