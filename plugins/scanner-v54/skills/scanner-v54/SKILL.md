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

## Current output contract

Require strategy `V5.9_CONFIRMED_20260926_TRADE_CVD` when reporting the confirmed 2026-09-26 rules. Output independent 1H and 4H Top10, special formal and approaching boards (including completed stages and missing conditions), and market warnings. Never output intersection/resonance or entry arrows. Respect freshness flags. CVD is calculated from aggregate market trades, labeled calculated_from_trades with a zero-at-window-start anchor. Incomplete CVD is unavailable, never proxy; do not infer a market retreat from Taker alone. If the deployed feed has another strategy, explicitly report that the backend has not migrated.
