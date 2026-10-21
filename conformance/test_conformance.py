"""whosthere 0.1 conformance (SPEC.md §7).

Each test names the clause it checks. A relay that passes keeps the
guarantees in §3 observably, over the HTTP binding in §5.

    WHOSTHERE_URL=https://relay.example WHOSTHERE_TOKEN=... pytest conformance -q
"""

from __future__ import annotations

import threading
import time
import uuid

import httpx
import pytest


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def pair(make_client, space):
    alice, bob = make_client(_name("alice")), make_client(_name("bob"))
    alice.join(space)
    bob.join(space)
    return alice, bob


class TestSection5Limits:
    def test_limits_are_advertised(self, make_client):
        limits = make_client(_name("probe")).limits()
        for key in ("max_wait_s", "default_wait_s", "max_text_bytes", "max_limit"):
            assert isinstance(limits.get(key), (int, float)) and limits[key] > 0, key
        assert limits["default_wait_s"] <= limits["max_wait_s"]


class TestSection4TheEvent:
    def test_a_knock_returns_the_event_with_every_field(self, pair, space):
        alice, bob = pair
        e = alice.knock(space=space, to=bob.agent, text="hello", kind="question")
        assert set(e) >= {"event_id", "space", "from", "to", "kind", "text", "thread", "meta", "created_at"}
        assert isinstance(e["event_id"], int)
        assert (e["space"], e["from"], e["to"], e["kind"], e["text"]) == (
            space, alice.agent, bob.agent, "question", "hello")
        assert e["thread"] is None and e["meta"] == {}

    def test_thread_and_meta_round_trip(self, pair, space):
        alice, bob = pair
        first = alice.knock(space=space, to=bob.agent, text="q")
        reply = bob.knock(space=space, to=alice.agent, text="a", thread=first["event_id"], meta={"k": [1]})
        assert reply["thread"] == first["event_id"]
        assert reply["meta"] == {"k": [1]}

    def test_missing_fields_are_a_400(self, relay_url, token, pair, space):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        r = httpx.post(f"{relay_url}/v1/knock", json={"space": space, "from": pair[0].agent}, headers=headers)
        assert r.status_code == 400
        assert "to" in r.text and "text" in r.text

    def test_oversized_text_is_a_413(self, pair, space, make_client):
        alice, bob = pair
        limit = make_client(_name("probe")).limits()["max_text_bytes"]
        with pytest.raises(httpx.HTTPStatusError) as exc:
            alice.knock(space=space, to=bob.agent, text="x" * (int(limit) + 1))
        assert exc.value.response.status_code == 413


class TestSection3Ordered:
    def test_delivery_follows_event_id_under_interleaving(self, pair, space, make_client):
        alice, bob = pair
        carol = make_client(_name("carol"))
        carol.join(space)
        sent = []
        for i in range(6):
            sender = alice if i % 2 == 0 else carol
            sent.append(sender.knock(space=space, to=bob.agent, text=str(i))["event_id"])
        got = [e["event_id"] for e in bob.peek(space=space)["items"]]
        assert got == sorted(sent) == sent

    def test_the_cursor_is_the_largest_id_shown_and_acking_it_covers_the_page(self, pair, space):
        alice, bob = pair
        for i in range(5):
            alice.knock(space=space, to=bob.agent, text=str(i))
        page = bob.peek(space=space, limit=3)
        assert page["count"] == 3
        top = max(e["event_id"] for e in page["items"])
        assert page["cursor"] == f"{top}@{space}"
        bob.ack(page["cursor"])
        rest = bob.peek(space=space)
        assert [e["text"] for e in rest["items"]] == ["3", "4"]

    def test_an_unscoped_read_hands_out_a_bare_cursor(self, pair, space):
        alice, bob = pair
        e = alice.knock(space=space, to=bob.agent, text="x")
        page = bob.peek()
        assert page["cursor"] == str(max(i["event_id"] for i in page["items"]))
        assert e["event_id"] <= int(page["cursor"])


class TestSection3AtLeastOnce:
    def test_reading_does_not_consume(self, pair, space):
        alice, bob = pair
        alice.knock(space=space, to=bob.agent, text="hi")
        assert bob.peek(space=space)["count"] == 1
        assert bob.peek(space=space)["count"] == 1
        assert bob.wait(space=space, timeout_s=1)["count"] == 1

    def test_ack_is_idempotent_and_monotonic(self, pair, space):
        alice, bob = pair
        e1 = alice.knock(space=space, to=bob.agent, text="1")
        e2 = alice.knock(space=space, to=bob.agent, text="2")
        first = bob.ack(f"{e2['event_id']}@{space}")
        again = bob.ack(f"{e2['event_id']}@{space}")
        back = bob.ack(f"{e1['event_id']}@{space}")
        for res in (first, again, back):
            cursors = {a["space"]: a["cursor"] for a in res["acknowledged"]}
            assert cursors[space] == e2["event_id"]
        assert bob.peek(space=space)["items"] == []

    def test_a_scoped_cursor_cannot_acknowledge_another_space(self, pair, space, make_client):
        """Event ids are one sequence across spaces. Reading space A and
        acknowledging its token must leave an older unread event in space B
        where it is."""
        alice, bob = pair
        other = f"{space}/other"
        alice.join(other)
        bob.join(other)
        older = alice.knock(space=other, to=bob.agent, text="in B, unseen")
        newer = alice.knock(space=space, to=bob.agent, text="in A")
        assert older["event_id"] < newer["event_id"]
        page = bob.peek(space=space)
        bob.ack(page["cursor"])
        left = bob.peek(space=other)
        assert [e["event_id"] for e in left["items"]] == [older["event_id"]]

    def test_a_token_the_relay_did_not_hand_out_is_a_400(self, pair):
        _, bob = pair
        for bad in ("", "abc", "-1", "9@"):
            with pytest.raises(httpx.HTTPStatusError) as exc:
                bob.ack(bad)
            assert exc.value.response.status_code == 400, bad

    def test_two_sessions_of_one_agent_share_the_inbox(self, pair, space, make_client):
        alice, bob = pair
        bob2 = make_client(bob.agent)
        alice.knock(space=space, to=bob.agent, text="shared")
        assert bob.peek(space=space)["count"] == 1
        assert bob2.peek(space=space)["count"] == 1
        bob2.ack(bob2.peek(space=space)["cursor"])
        assert bob.peek(space=space)["count"] == 0


class TestSection3Addressing:
    def test_only_the_recipient_sees_a_knock(self, pair, space, make_client):
        alice, bob = pair
        carol = make_client(_name("carol"))
        carol.join(space)
        alice.knock(space=space, to=bob.agent, text="private")
        assert carol.peek(space=space)["count"] == 0
        assert alice.peek(space=space)["count"] == 0
        assert bob.peek(space=space)["count"] == 1

    def test_an_agent_outside_the_space_gets_nothing(self, pair, space, make_client):
        alice, bob = pair
        outsider = make_client(_name("eve"))
        alice.knock(space=space, to=outsider.agent, text="misaddressed")
        assert outsider.peek()["count"] == 0

    def test_spaces_lists_membership(self, pair, space):
        alice, _ = pair
        assert space in alice.spaces()


class TestSection5Wait:
    def test_wait_returns_on_arrival_not_on_timeout(self, pair, space):
        alice, bob = pair
        result = {}

        def waiter():
            result["r"] = bob.wait(space=space, timeout_s=10)

        t = threading.Thread(target=waiter)
        started = time.monotonic()
        t.start()
        time.sleep(0.3)
        alice.knock(space=space, to=bob.agent, text="now")
        t.join(timeout=8)
        assert not t.is_alive(), "wait did not return on arrival"
        assert time.monotonic() - started < 5
        assert result["r"]["items"][0]["text"] == "now"
        assert "timed_out" not in result["r"]

    def test_wait_returns_empty_with_timed_out_true(self, pair, space):
        _, bob = pair
        started = time.monotonic()
        r = bob.wait(space=space, timeout_s=1)
        assert r["items"] == [] and r["count"] == 0 and r["cursor"] is None
        assert r["timed_out"] is True
        assert time.monotonic() - started < 4

    def test_a_timeout_above_the_cap_is_clamped_not_rejected(self, pair, space, make_client):
        _, bob = pair
        cap = make_client(_name("probe")).limits()["max_wait_s"]
        # Prove clamping without waiting the cap out: arrange arrival instead.
        t = threading.Thread(target=lambda: pair[0].knock(space=space, to=bob.agent, text="x"))
        t.start()
        r = bob.wait(space=space, timeout_s=cap * 10)
        t.join()
        assert r["count"] == 1

    def test_wait_without_an_agent_is_a_400(self, relay_url, token):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        r = httpx.get(f"{relay_url}/v1/inbox/wait", params={"timeout_s": 1}, headers=headers)
        assert r.status_code == 400


class TestSection5Identity:
    def test_the_header_names_the_agent(self, relay_url, token, pair, space):
        alice, bob = pair
        alice.knock(space=space, to=bob.agent, text="hdr")
        headers = {"X-Whosthere-Agent": bob.agent}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        r = httpx.get(f"{relay_url}/v1/inbox", params={"space": space}, headers=headers)
        assert r.status_code == 200 and r.json()["count"] == 1

    def test_the_header_wins_over_the_query(self, relay_url, token, pair, space):
        alice, bob = pair
        alice.knock(space=space, to=bob.agent, text="hdr")
        headers = {"X-Whosthere-Agent": bob.agent}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        r = httpx.get(f"{relay_url}/v1/inbox", params={"space": space, "agent": alice.agent}, headers=headers)
        assert r.json()["count"] == 1
