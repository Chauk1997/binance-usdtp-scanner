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

Require strategy `V5.9_CONFIRMED_20260926_VOLUME_CVD` when reporting the confirmed 2026-09-26 rules. Output independent 1H and 4H Top10, special formal and approaching boards (including completed stages and missing conditions), and market warnings. Never output intersection/resonance or entry arrows. Respect freshness flags. CVD is calculated from exchange-reported executed volume and taker-buy volume, labeled calculated_from_exchange_volume with a zero-at-window-start anchor. Incomplete CVD is unavailable, never proxy; do not infer a market retreat from Taker alone. If the deployed feed has another strategy, explicitly report that the backend has not migrated.

## Confirmed output contract (2026-09-27)

Always present five sections, including empty sections: 1H Top10, 4H Top10,
Special Formal, Special Approaching, and Auxiliary / Market State. The two
Top10 lists are independent; never fill vacancies or construct an intersection.
Show completed/current/missing/next stages for approaching patterns. Use the
exact label **CVD Proxy** for `auxiliary.cvd_proxy`; it replaces the former CVD
strategy role and does not require Direct CVD. Report unavailable auxiliary
fields without discarding technically qualified rows. Include coverage,
missing, latest closed candles, freshness and feed_ready; never present hidden
stale/pending rows as current candidates.
