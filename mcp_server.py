import os

from mcp.server.mcpserver import MCPServer

mcp = MCPServer(
    name="scanner-v54-mcp",
    description="MCP server for Binance USDT perpetual scanner V5.4",
)


@mcp.tool()
def ping() -> str:
    """Check whether the scanner MCP server is online."""
    return "scanner-v54-mcp is online"


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
    )
