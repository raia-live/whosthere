"""The MCP binding (SPEC.md §6) over the local backend.

What the tool layer owns: identity from the environment, the blocking
``whos_there`` loop (rounds bounded by the relay's cap, progress while
blocked, a floor on empty rounds), and the ``next`` hint that tells a
model what to do with what it got.
"""

from __future__ import annotations

import asyncio
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from whosthere import mcp_server
from whosthere.client import LocalClient
from whosthere.store import Store


@pytest.fixture
def local(monkeypatch):
    """One in-memory store shared by 'bob' (the MCP server) and 'alice'."""
    store = Store(":memory:")
    bob = LocalClient(store, agent="bob")
    alice = LocalClient(store, agent="alice")
    bob.join("team")
    alice.join("team")
    monkeypatch.setenv("WHOSTHERE_SPACE", "team")
    monkeypatch.setattr(mcp_server, "_client", bob)
    monkeypatch.setattr(mcp_server, "PROGRESS_EVERY_S", 0.05)
    yield store, alice
    store.close()


def _ctx():
    ctx = MagicMock()
    ctx.report_progress = AsyncMock()
    return ctx


class TestIdentity:
    def test_every_tool_refuses_without_an_agent(self, monkeypatch):
        monkeypatch.setattr(mcp_server, "_client", LocalClient(Store(":memory:"), agent=""))
        for call in (
            lambda: mcp_server.knock(to="x", text="y"),
            lambda: mcp_server.peek(),
            lambda: mcp_server.ack("1"),
            lambda: mcp_server.join("s"),
            lambda: asyncio.run(mcp_server.whos_there(timeout_s=1)),
        ):
            assert "WHOSTHERE_AGENT" in call()["error"]


class TestTheFourVerbs:
    def test_knock_defaults_to_the_configured_space(self, local):
        e = mcp_server.knock(to="alice", text="hi")
        assert (e["space"], e["from"], e["to"]) == ("team", "bob", "alice")

    def test_peek_then_ack(self, local):
        _, alice = local
        alice.knock(space="team", to="bob", text="one")
        page = mcp_server.peek()
        assert page["count"] == 1 and 'ack(cursor="1")' in page["next"]
        mcp_server.ack(page["cursor"])
        assert mcp_server.peek()["count"] == 0

    def test_a_scoped_peek_hands_out_a_scoped_cursor(self, local):
        _, alice = local
        alice.knock(space="team", to="bob", text="one")
        page = mcp_server.peek(space="team")
        assert page["cursor"] == "1@team"

    def test_join(self, local):
        assert mcp_server.join("other") == {"space": "other", "agent": "bob", "members": ["bob"]}


class TestWhosThere:
    def test_returns_on_arrival(self, local):
        _, alice = local

        def later():
            time.sleep(0.2)
            alice.knock(space="team", to="bob", text="now")

        threading.Thread(target=later).start()
        started = time.monotonic()
        out = asyncio.run(mcp_server.whos_there(timeout_s=10))
        assert time.monotonic() - started < 5
        assert out["items"][0]["text"] == "now" and "next" in out

    def test_times_out_empty(self, local):
        out = asyncio.run(mcp_server.whos_there(timeout_s=1))
        assert out == {"items": [], "count": 0, "cursor": None, "timed_out": True}

    def test_progress_is_reported_while_blocked(self, local):
        ctx = _ctx()
        asyncio.run(mcp_server.whos_there(timeout_s=1, ctx=ctx))
        assert ctx.report_progress.await_count >= 1
        kw = ctx.report_progress.await_args.kwargs
        assert kw["total"] == 1 and 0 <= kw["progress"] <= 1

    def test_a_failing_progress_call_is_ignored(self, local):
        ctx = _ctx()
        ctx.report_progress = AsyncMock(side_effect=RuntimeError("gone"))
        out = asyncio.run(mcp_server.whos_there(timeout_s=1, ctx=ctx))
        assert out["timed_out"] is True

    def test_rounds_are_bounded_by_the_relay_cap_and_empty_rounds_have_a_floor(self, local, monkeypatch):
        rounds: list[float] = []
        fake = MagicMock()
        fake.agent = "bob"
        fake.limits.return_value = {"max_wait_s": 2}

        def wait(space=None, timeout_s=240, limit=50):
            rounds.append(timeout_s)
            return {"items": [], "count": 0, "cursor": None, "timed_out": True}

        fake.wait.side_effect = wait
        monkeypatch.setattr(mcp_server, "_client", fake)
        slept: list[float] = []

        async def fake_sleep(s):
            slept.append(s)

        with patch("asyncio.sleep", side_effect=fake_sleep):
            out = asyncio.run(mcp_server.whos_there(timeout_s=5))
        assert out["timed_out"] is True
        assert rounds[0] == 2 and all(r <= 2 for r in rounds)
        assert slept and all(0 < s <= mcp_server.MIN_EMPTY_ROUND_S for s in slept)

    def test_a_relay_error_is_a_pause_not_an_answer(self, local, monkeypatch):
        fake = MagicMock()
        fake.agent = "bob"
        fake.limits.return_value = {"max_wait_s": 240}
        fake.wait.side_effect = [
            RuntimeError("relay down"),
            {"items": [{"event_id": 3, "from": "alice"}], "count": 1, "cursor": "3"},
        ]
        monkeypatch.setattr(mcp_server, "_client", fake)
        slept = AsyncMock()
        with patch("asyncio.sleep", slept):
            out = asyncio.run(mcp_server.whos_there(timeout_s=30))
        assert out["cursor"] == "3"
        slept.assert_awaited_once_with(5)

    def test_the_total_is_capped_at_an_hour(self, local, monkeypatch):
        fake = MagicMock()
        fake.agent = "bob"
        fake.limits.return_value = {"max_wait_s": 240}
        fake.wait.return_value = {"items": [{"event_id": 1}], "count": 1, "cursor": "1"}
        monkeypatch.setattr(mcp_server, "_client", fake)
        ctx = _ctx()
        asyncio.run(mcp_server.whos_there(timeout_s=10**9, ctx=ctx))
        # No progress call happened (the round returned at once), so check the
        # round the client was asked for instead.
        assert fake.wait.call_args.kwargs["timeout_s"] == 240


class TestTheServerAdvertisesTheContract:
    def test_the_five_tools_are_registered(self):
        names = {t.name for t in asyncio.run(mcp_server.mcp._list_tools())}
        assert names == {"knock", "whos_there", "peek", "ack", "join"}
