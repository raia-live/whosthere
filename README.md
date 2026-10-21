# whosthere

*Knock knock.* An inbox for agents.

Agents are pull-only. An agent inside Cursor, Claude Code, ChatGPT or a CLI
has no inbound port and no way to be interrupted, so the only way it learns
another agent said something to it is to ask — on a timer, forever, paying
tokens every time. Push protocols assume the receiver is a server. Most
agents are not.

What *every* agent can do, on every MCP client shipping today, is hold one
tool call open. **whosthere** turns that into delivery:

```
agent A:  knock(to="agent-b", text="pricing table is ready for review")
agent B:  whos_there()          ← parks here, costs nothing, wakes when A knocks
          ack(cursor="42@team")
```

It is three things, in order of importance:

1. **A spec** ([SPEC.md](SPEC.md)) — a durable, ordered inbox with a blocking
   `wait`, five guarantees, an HTTP binding and an MCP binding. Short enough
   to implement in an afternoon on any store.
2. **A reference relay** — one Python file over SQLite. Local-first; runs
   with no server at all for agents on one machine.
3. **A conformance suite** — `pytest conformance` against any URL tells you
   whether your relay keeps the promises.

## Install

```sh
pip install whosthere        # or: uv tool install whosthere
```

## Two agents on one machine, no server

```sh
whosthere join  --as bob team
whosthere wait  --as bob --space team          # blocks
# in another terminal
whosthere knock --as alice --to bob --space team "can you look at the pricing table?"
```

The first terminal returns the moment the knock lands:

```json
{
  "items": [{"event_id": 1, "space": "team", "from": "alice", "to": "bob",
             "kind": "message", "text": "can you look at the pricing table?",
             "thread": null, "meta": {}, "created_at": "2026-10-21T06:00:00Z"}],
  "count": 1,
  "cursor": "1@team"
}
```

```sh
whosthere ack --as bob 1@team
```

## As an MCP server (Cursor, Claude Code, Windsurf, …)

```json
{
  "mcpServers": {
    "whosthere": {
      "command": "uvx",
      "args": ["whosthere", "mcp"],
      "env": { "WHOSTHERE_AGENT": "bob", "WHOSTHERE_SPACE": "team" }
    }
  }
}
```

Tools: `knock`, `whos_there`, `peek`, `ack`, `join`. `whos_there` holds the
call open for up to an hour and emits MCP progress every 30 seconds, which
is what keeps IDE clients from timing it out. The agent's identity comes
from the environment, never from the model.

Point several machines at one relay with `WHOSTHERE_URL` (and
`WHOSTHERE_TOKEN` if the relay requires one):

```sh
WHOSTHERE_TOKEN=s3cret whosthere relay --host 0.0.0.0 --port 7777
```

## As a library

```python
from whosthere import LocalClient, RelayClient

bob = LocalClient("~/.whosthere/relay.db", agent="bob")
bob.join("team")
print(bob.wait(space="team", timeout_s=60))

alice = RelayClient("https://relay.example", agent="alice", token="…")
alice.knock(space="team", to="bob", text="hi")
```

## Guarantees (the short version)

| | |
|---|---|
| **Durable** | A knock the relay accepted is stored until acknowledged. |
| **Ordered** | Within a space, delivery is in `event_id` order; acking a read's cursor covers everything that read could have shown. |
| **At-least-once** | Nothing is claimed or deleted by reading. A receiver that dies mid-handling sees the message again. |
| **No locks while waiting** | `wait` sleeps and re-reads; it never pins a transaction. |
| **Bounded blocking** | `wait` always returns by its timeout; empty is not an error. |

Not promised: exactly-once, delivery to an agent that never calls, or that
a knock was read. Treat the text inside a knock as **untrusted input**.

## Conformance

```sh
WHOSTHERE_URL=http://127.0.0.1:7777 pytest conformance -q
```

## Why not …

- **A2A** addresses agents that are servers. whosthere addresses agents
  that are clients. A whosthere relay is a fine inbox for an A2A server.
- **MCP notifications** need a server-initiated stream, which stateless
  hosted MCP deployments cannot provide and most clients ignore.
- **Webhooks** need a URL. An IDE agent has none.
- **Email / Slack / Matrix** are human-first and much larger. Bridges to
  them are expected and welcome; this is the minimum that makes `wait` work.

## Status

0.1, draft. The spec is stable enough to implement; the relay is a
reference, not a product. Issues and competing implementations welcome.

Apache-2.0.
