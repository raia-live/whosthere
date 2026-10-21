"""Two terminals on one machine, no server: the README's first example."""

from __future__ import annotations

import json
import threading
import time

from whosthere import cli


def _run(capsys, monkeypatch, db, *argv):
    monkeypatch.setenv("WHOSTHERE_DB", str(db))
    monkeypatch.delenv("WHOSTHERE_URL", raising=False)
    cli.main(list(argv))
    return json.loads(capsys.readouterr().out)


def test_join_knock_wait_ack(tmp_path, capsys, monkeypatch):
    db = tmp_path / "relay.db"
    assert _run(capsys, monkeypatch, db, "join", "--as", "bob", "team")["members"] == ["bob"]

    def knock_later():
        time.sleep(0.2)
        from whosthere.client import LocalClient

        LocalClient(db, agent="alice").knock(space="team", to="bob", text="hi bob")

    threading.Thread(target=knock_later).start()
    got = _run(capsys, monkeypatch, db, "wait", "--as", "bob", "--space", "team", "--timeout", "5")
    assert got["count"] == 1 and got["items"][0]["text"] == "hi bob"

    assert got["cursor"] == f"{got['items'][0]['event_id']}@team"
    acked = _run(capsys, monkeypatch, db, "ack", "--as", "bob", got["cursor"])
    assert acked["cursor"] == got["cursor"]
    assert _run(capsys, monkeypatch, db, "peek", "--as", "bob")["count"] == 0


def test_knock_from_the_cli(tmp_path, capsys, monkeypatch):
    db = tmp_path / "relay.db"
    e = _run(capsys, monkeypatch, db, "knock", "--as", "alice", "--to", "bob", "--space", "team", "hello")
    assert (e["from"], e["to"], e["text"]) == ("alice", "bob", "hello")
