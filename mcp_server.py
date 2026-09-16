import os
import httpx

from mcp.server.mcpserver import MCPServer

mcp = MCPServer(
    name="scanner-v54-mcp",
    description="MCP server for Binance USDT perpetual scanner V5.4",
)

SCANNER_BASE_URL = "https://binance-usdtp-scanner-production.up.railway.app"


@mcp.tool()
def ping() -> str:
    """Check whether the scanner MCP server is online."""
    return "scanner-v54-mcp is online"


@mcp.tool()
def get_scan_feed() -> dict:
    """Get the latest V5.4 ChatGPT scanner feed."""
    url = f"{SCANNER_BASE_URL}/scan/feed"

    with httpx.Client(timeout=120.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
    )
