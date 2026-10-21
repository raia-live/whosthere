"""The store is where the guarantees live (SPEC.md §3)."""

from __future__ import annotations

import threading
import time

import pytest

from whosthere.store import NotAMember, Store


@pytest.fixture
def store() -> Store:
    s = Store(":memory:")
    s.join("team", "alice")
    s.join("team", "bob")
    yield s
    s.close()


class TestOrdering:
    def test_event_ids_only_grow_and_inbox_is_ordered(self, store: Store) -> None:
        ids = [store.knock(space="team", sender="alice", recipient="bob", text=str(i))["event_id"]
               for i in range(5)]
        assert ids == sorted(ids) and len(set(ids)) == 5
        inbox = store.inbox(name="bob")
        assert [e["event_id"] for e in inbox] == ids

    def test_acking_n_covers_everything_at_or_below_n(self, store: Store) -> None:
        for i in range(4):
            store.knock(space="team", sender="alice", recipient="bob", text=str(i))
        shown = store.inbox(name="bob", limit=2)
        store.ack(name="bob", event_id=shown[-1]["event_id"])
        rest = store.inbox(name="bob")
        assert [e["text"] for e in rest] == ["2", "3"]


class TestAtLeastOnce:
    def test_reading_consumes_nothing(self, store: Store) -> None:
        store.knock(space="team", sender="alice", recipient="bob", text="hi")
        assert len(store.inbox(name="bob")) == 1
        assert len(store.inbox(name="bob")) == 1

    def test_only_ack_moves_the_cursor_and_never_backwards(self, store: Store) -> None:
        e1 = store.knock(space="team", sender="alice", recipient="bob", text="1")
        e2 = store.knock(space="team", sender="alice", recipient="bob", text="2")
        store.ack(name="bob", event_id=e2["event_id"])
        assert store.inbox(name="bob") == []
        store.ack(name="bob", event_id=e1["event_id"])
        assert store.cursor(name="bob", space="team") == e2["event_id"]
        assert store.inbox(name="bob") == []


class TestAddressing:
    def test_only_the_recipient_sees_it(self, store: Store) -> None:
        store.knock(space="team", sender="alice", recipient="bob", text="private")
        assert store.inbox(name="alice") == []
        assert [e["text"] for e in store.inbox(name="bob")] == ["private"]

    def test_broadcast_reaches_everyone_but_the_sender(self, store: Store) -> None:
        store.join("team", "carol")
        store.knock(space="team", sender="alice", recipient="*", text="all")
        assert store.inbox(name="alice") == []
        assert len(store.inbox(name="bob")) == 1
        assert len(store.inbox(name="carol")) == 1

    def test_membership_is_the_authorization(self, store: Store) -> None:
        store.join("other", "alice")
        store.knock(space="other", sender="alice", recipient="bob", text="wrong room")
        # bob is not in 'other': addressed to him, but not his to read.
        assert store.inbox(name="bob") == []
        with pytest.raises(NotAMember):
            store.knock(space="other", sender="bob", recipient="alice", text="x")

    def test_a_space_filter_narrows_and_the_ack_can_be_scoped(self, store: Store) -> None:
        store.join("other", "alice")
        store.join("other", "bob")
        a = store.knock(space="team", sender="alice", recipient="bob", text="t")
        b = store.knock(space="other", sender="alice", recipient="bob", text="o")
        assert [e["event_id"] for e in store.inbox(name="bob", space="other")] == [b["event_id"]]
        store.ack(name="bob", event_id=b["event_id"], space="other")
        assert [e["event_id"] for e in store.inbox(name="bob")] == [a["event_id"]]


class TestWaiting:
    def test_wait_returns_on_arrival(self, store: Store) -> None:
        got = {}

        def waiter():
            got["items"] = store.wait(name="bob", space=None, timeout_s=5)

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.1)
        store.knock(space="team", sender="alice", recipient="bob", text="now")
        t.join(timeout=2)
        assert not t.is_alive()
        assert got["items"][0]["text"] == "now"

    def test_wait_times_out_empty(self, store: Store) -> None:
        started = time.monotonic()
        assert store.wait(name="bob", space=None, timeout_s=0.2) == []
        assert time.monotonic() - started < 1.5


class TestTheEventShape:
    def test_fields_match_the_spec(self, store: Store) -> None:
        e = store.knock(space="team", sender="alice", recipient="bob", text="hi",
                        kind="question", thread=None, meta={"k": 1})
        assert set(e) == {"event_id", "space", "from", "to", "kind", "text", "thread", "meta", "created_at"}
        assert e["created_at"].endswith("Z")
        assert e["meta"] == {"k": 1}
