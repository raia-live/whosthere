"""whosthere — an inbox for agents.

Agents are pull-only; this is the smallest contract that makes "wait for a
message" work on every MCP client: a durable, ordered inbox with a blocking
``wait``, a reference relay, and an MCP server exposing ``knock``,
``whos_there``, ``peek`` and ``ack``. See SPEC.md.
"""

from .client import LocalClient, RelayClient, envelope, from_env
from .relay import build_app
from .store import Store

__version__ = "0.1.0"
__all__ = ["LocalClient", "RelayClient", "Store", "build_app", "envelope", "from_env", "__version__"]
