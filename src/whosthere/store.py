"""SQLite store for the reference relay.

One file, three tables, WAL mode. Everything the protocol guarantees (§3 of
SPEC.md) is a property of this module:

* durability — a knock is a committed row before the relay answers 201;
* ordering — ``event_id`` is an AUTOINCREMENT rowid, so it only ever grows,
  and the inbox query orders by it;
* at-least-once — nothing is deleted or claimed when read; only ``ack``
  moves a cursor, and only forwards;
* no locks while waiting — the wait loop in the relay sleeps between calls
  to :meth:`Store.inbox`, each of which is a short read transaction.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS members (
    space TEXT NOT NULL,
    name  TEXT NOT NULL,
    joined_at TEXT NOT NULL,
    PRIMARY KEY (space, name)
);

CREATE TABLE IF NOT EXISTS events (
    event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    space      TEXT NOT NULL,
    sender     TEXT NOT NULL,
    recipient  TEXT NOT NULL,
    kind       TEXT NOT NULL DEFAULT 'message',
    text       TEXT NOT NULL,
    thread     INTEGER,
    meta       TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_recipient ON events (space, recipient, event_id);

CREATE TABLE IF NOT EXISTS cursors (
    space    TEXT NOT NULL,
    name     TEXT NOT NULL,
    last_event_id INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (space, name)
);
"""

BROADCAST = "*"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "space": row["space"],
        "from": row["sender"],
        "to": row["recipient"],
        "kind": row["kind"],
        "text": row["text"],
        "thread": row["thread"],
        "meta": json.loads(row["meta"] or "{}"),
        "created_at": row["created_at"],
    }


class NotAMember(Exception):
    """The named agent is not in the space."""


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self._path = str(path)
        self._lock = threading.RLock()
        # check_same_thread=False: the relay serves requests from a thread
        # pool and the lock above serialises access.
        self._conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        if self._path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        # Wakes parked waiters in-process the moment a knock lands. A relay
        # spread across processes falls back to the poll interval.
        self.changed = threading.Condition(self._lock)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- membership --------------------------------------------------------

    def join(self, space: str, name: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO members (space, name, joined_at) VALUES (?, ?, ?)",
                (space, name, _now()),
            )

    def leave(self, space: str, name: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM members WHERE space = ? AND name = ?", (space, name))

    def is_member(self, space: str, name: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM members WHERE space = ? AND name = ?", (space, name)
            ).fetchone()
        return row is not None

    def spaces_of(self, name: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT space FROM members WHERE name = ? ORDER BY space", (name,)
            ).fetchall()
        return [r["space"] for r in rows]

    def members_of(self, space: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name FROM members WHERE space = ? ORDER BY name", (space,)
            ).fetchall()
        return [r["name"] for r in rows]

    # -- knocking ----------------------------------------------------------

    def knock(
        self,
        *,
        space: str,
        sender: str,
        recipient: str,
        text: str,
        kind: str = "message",
        thread: int | None = None,
        meta: dict[str, Any] | None = None,
        require_membership: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            if require_membership and not self.is_member(space, sender):
                raise NotAMember(f"{sender!r} is not in space {space!r}")
            cur = self._conn.execute(
                """INSERT INTO events (space, sender, recipient, kind, text, thread, meta, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (space, sender, recipient, kind, text, thread, json.dumps(meta or {}), _now()),
            )
            row = self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (cur.lastrowid,)
            ).fetchone()
            self.changed.notify_all()
        return _row_to_event(row)

    # -- reading -----------------------------------------------------------

    def inbox(
        self, *, name: str, space: str | None = None, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Unread events addressed to ``name`` (or to everyone), oldest first.

        Only spaces the agent is a member of count, and only events above
        the agent's cursor in each. The agent's own broadcasts are excluded.
        """
        params: list[Any] = [name, BROADCAST, name]
        space_clause = ""
        if space is not None:
            space_clause = "AND e.space = ?"
            params.append(space)
        params.append(limit)
        sql = f"""
            SELECT e.*
            FROM events e
            JOIN members m ON m.space = e.space AND m.name = ?
            LEFT JOIN cursors c ON c.space = e.space AND c.name = m.name
            WHERE (e.recipient = m.name OR e.recipient = ?)
              AND e.sender <> ?
              AND e.event_id > COALESCE(c.last_event_id, 0)
              {space_clause}
            ORDER BY e.event_id
            LIMIT ?
        """
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_event(r) for r in rows]

    def ack(self, *, name: str, event_id: int, space: str | None = None) -> list[dict[str, Any]]:
        """Advance ``name``'s cursor to at least ``event_id`` in each space."""
        now = _now()
        with self._lock:
            spaces = [space] if space is not None else self.spaces_of(name)
            out = []
            for sp in spaces:
                if not self.is_member(sp, name):
                    continue
                self._conn.execute(
                    """INSERT INTO cursors (space, name, last_event_id, updated_at)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT (space, name) DO UPDATE
                         SET last_event_id = MAX(cursors.last_event_id, excluded.last_event_id),
                             updated_at = excluded.updated_at""",
                    (sp, name, event_id, now),
                )
                row = self._conn.execute(
                    "SELECT last_event_id FROM cursors WHERE space = ? AND name = ?", (sp, name)
                ).fetchone()
                out.append({"space": sp, "cursor": row["last_event_id"]})
        return out

    def cursor(self, *, name: str, space: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT last_event_id FROM cursors WHERE space = ? AND name = ?", (space, name)
            ).fetchone()
        return int(row["last_event_id"]) if row else 0

    def wait(
        self, *, name: str, space: str | None, timeout_s: float, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Block the calling thread until the inbox is non-empty or timeout.

        In-process waiters are woken by :meth:`knock`; the condition's own
        timeout bounds the block. No transaction is open while waiting.
        """
        import time

        deadline = time.monotonic() + timeout_s
        while True:
            items = self.inbox(name=name, space=space, limit=limit)
            if items:
                return items
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []
            with self.changed:
                self.changed.wait(timeout=min(remaining, 1.0))
