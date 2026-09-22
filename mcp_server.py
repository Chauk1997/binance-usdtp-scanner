import os
import httpx
from scan_summary import compact_scan_feed

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

mcp = MCPServer(
    name="scanner-v54-mcp",
    description="MCP server for Binance USDT perpetual scanner V5.4",
)

SCANNER_BASE_URL = "https://binance-usdtp-scanner-production.up.railway.app"


@mcp.tool(annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
def ping() -> str:
    """Check whether the scanner MCP server is online."""
    return "scanner-v54-mcp is online"


@mcp.tool(annotations=ToolAnnotations(read_only_hint=True, open_world_hint=True))
def get_scan_feed() -> dict:
    """Get freshness/coverage, independently ranked 1H/4H Top 10 and intersection.

    Historical rows must not be published when their timeframe is not fresh.
    Full diagnostics remain at /scan/feed, never returned by this tool.
    """
    url = f"{SCANNER_BASE_URL}/scan/feed"

    with httpx.Client(timeout=120.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return compact_scan_feed(response.json())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
    )
