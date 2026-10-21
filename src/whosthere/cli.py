"""``whosthere`` on the command line.

    whosthere relay [--host 127.0.0.1] [--port 7777] [--db ~/.whosthere/relay.db]
    whosthere mcp                      # stdio MCP server (see mcp_server.py)
    whosthere join  --as bob team
    whosthere knock --as alice --to bob --space team "text"
    whosthere peek  --as bob [--space team]
    whosthere wait  --as bob [--space team] [--timeout 240]
    whosthere ack   --as bob 42@team

The agent commands talk to ``--url`` / ``WHOSTHERE_URL``; without one they
use the local SQLite file directly, so two terminals on one machine can
knock at each other with no server running.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from .client import Client, LocalClient, RelayClient, default_db_path


def _client(args: argparse.Namespace) -> Client:
    agent = args.as_agent or os.environ.get("WHOSTHERE_AGENT")
    if not agent:
        sys.exit("--as <agent> (or WHOSTHERE_AGENT) is required")
    url = args.url or os.environ.get("WHOSTHERE_URL")
    if url:
        return RelayClient(url, agent=agent, token=os.environ.get("WHOSTHERE_TOKEN"))
    return LocalClient(default_db_path(), agent=agent)


def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2))


def cmd_relay(args: argparse.Namespace) -> None:
    import uvicorn

    from .relay import build_app
    from .store import Store

    db = args.db or default_db_path()
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    print(f"whosthere relay on http://{args.host}:{args.port}  (db: {db})", file=sys.stderr)
    uvicorn.run(build_app(Store(db)), host=args.host, port=args.port, log_level="info")


def cmd_mcp(_: argparse.Namespace) -> None:
    from .mcp_server import main

    main()


def cmd_join(args: argparse.Namespace) -> None:
    _print(_client(args).join(args.space_name))


def cmd_knock(args: argparse.Namespace) -> None:
    space = args.space or os.environ.get("WHOSTHERE_SPACE", "default")
    _print(_client(args).knock(space=space, to=args.to, text=args.text, kind=args.kind, thread=args.thread))


def cmd_peek(args: argparse.Namespace) -> None:
    _print(_client(args).peek(space=args.space))


def cmd_wait(args: argparse.Namespace) -> None:
    _print(_client(args).wait(space=args.space, timeout_s=args.timeout))


def cmd_ack(args: argparse.Namespace) -> None:
    _print(_client(args).ack(args.cursor))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="whosthere", description="An inbox for agents.")
    p.add_argument("--url", help="relay base URL (default: WHOSTHERE_URL, else local SQLite)")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("relay", help="run the reference relay")
    r.add_argument("--host", default="127.0.0.1")
    r.add_argument("--port", type=int, default=7777)
    r.add_argument("--db")
    r.set_defaults(fn=cmd_relay)

    m = sub.add_parser("mcp", help="run the stdio MCP server")
    m.set_defaults(fn=cmd_mcp)

    def _agent(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--as", dest="as_agent", help="your agent name (or WHOSTHERE_AGENT)")
        sp.add_argument("--space")

    j = sub.add_parser("join", help="join a space")
    _agent(j)
    j.add_argument("space_name", metavar="space")
    j.set_defaults(fn=cmd_join)

    k = sub.add_parser("knock", help="send a message")
    _agent(k)
    k.add_argument("--to", required=True)
    k.add_argument("--kind", default="message")
    k.add_argument("--thread", type=int)
    k.add_argument("text")
    k.set_defaults(fn=cmd_knock)

    pk = sub.add_parser("peek", help="list unread messages")
    _agent(pk)
    pk.set_defaults(fn=cmd_peek)

    w = sub.add_parser("wait", help="block until a message arrives")
    _agent(w)
    w.add_argument("--timeout", type=float, default=240)
    w.set_defaults(fn=cmd_wait)

    a = sub.add_parser("ack", help="acknowledge with the cursor a peek/wait returned")
    a.add_argument("--as", dest="as_agent", help="your agent name (or WHOSTHERE_AGENT)")
    a.add_argument("cursor")
    a.set_defaults(fn=cmd_ack)
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
