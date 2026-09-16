---
name: scanner-v54
description: Retrieve Binance USDT perpetual scanner data using scanner-v54-mcp.
---

# Scanner V5.4

Use the `scanner-v54-mcp` MCP server when the user asks for cryptocurrency scanner data.

## Tools

### ping

Use `ping` to verify that the scanner MCP server is online.

### get_scan_feed

Use `get_scan_feed` when the user asks to:
- get the current scanner feed
- scan the market
- retrieve current V5.4 scan data
- obtain Binance USDT perpetual scanner results

When current scanner data is requested, call `get_scan_feed` rather than inventing or estimating scanner results.

Return the data provided by the MCP server and clearly report the scanner status if the feed is stopped or unavailable.
