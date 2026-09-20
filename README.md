# Completed scanner feed

`GET /scan/feed` reads the most recently completed V5.4 snapshot from memory.
It never starts scans, reads candle files, computes indicators, or fetches
Binance/derivatives data. Before the first successful scan it returns HTTP 200
with `status: not_ready`, `feed_ready: false`; during the first scan it returns
`status: running`. These are readiness responses, not empty market results.

`GET /scan/run/all` remains the explicit full-scan entry point and retains its
V5.0 response shape. It also builds the existing V5.2 watchlists and V5.3/V5.4
projections, then atomically saves `cache/scan_feed_v54.json`. The `/v52` and
`/scan/run/compact` entry points use the same guarded worker and publish the
same feed. Concurrent calls return `status: running` instead of duplicating
work. The worker runs outside the HTTP event loop so feed/status requests
remain responsive. A disconnected caller does not cancel the worker.

Failed/stopped scans and cache-write failures preserve the previous complete
snapshot. `GET /scan/status` reports the last attempt, readiness, original
snapshot generation time, age, and `stale` (older than one hour). A stale feed
is still returned with its original timestamp; fetching it does not refresh it.
`elapsed_seconds` in the feed measures scan work, not feed request latency.

Initialize existing candle caches with the existing initialization endpoints
on a new container, then explicitly call `/scan/run/all`. Schedule that scan
endpoint externally to refresh results; feed polling no longer refreshes them.
No new scheduler is installed. Snapshots survive process restarts when the
cache directory survives. Railway deployments without a persistent volume
start without candle/feed caches and need initialization again. This targets
the current single-process, single-replica deployment; multiple workers need
a shared snapshot store and distributed scan lock.

Tests: `python -m pytest -q test_scan_snapshot.py` (FastAPI, pandas, httpx, pytest).

## V5.4 relative BTC resilience

`btc_resilience.py` computes both OHLC records, fractional `body_return`
(not percent), and `close_position`. Only the most recent completed operation
interval is accepted. Both open and close timestamps must match the expected
Binance interval; missing, stale, invalid, or zero-range data produces an
explicit `unavailable` assessment, never a daily-return fallback.

`main.py` calls `rank_items` in `build_v36_results` (1H),
`build_4h_final_results` (4H), and `build_watchlist_for_timeframe` before
truncation. The compact and V5.4 projections preserve `timeframe`, `candle`,
`btc_candle`, `relative_btc_resilience`, `auxiliary_score`, and `ranking_key`.

Descending ranking is lexicographic: existing structure score, relative BTC
score, then existing composite derivatives adjustment. Daily `vs_btc_pct`
remains informational only. Legacy scalar score is a display sum and MUST NOT
be used to re-sort the feed. Unknown resilience receives no bonus (zero), and
its status distinguishes it from an observed neutral result.

Tier 3 (highest) requires BTC body_return < 0 and close_position <= 0.20,
and coin body_return > 0 and close_position >= 0.80. Tier 2 requires a rising
coin with a higher close position while BTC falls. Tier 1 requires a better
body return and close position while BTC falls. Tier -1 is worse in both;
otherwise tier 0. Within tiers, bounded candle-strength detail breaks ties;
it cannot cross tier boundaries. Threshold constants are in btc_resilience.py.
These are explicit implementation defaults, not empirically calibrated signals.

Existing derivatives combine OI changes, taker flow, funding, and positioning;
there is no sequential priority among those inputs. Existing CVD proxy and
taker ratio originate from the same buy/sell totals, so the flow contribution
is not counted twice. Absolute OI is not treated as a bullish score by itself.
Existing technical qualification, entry/watch classifications, and legacy
resonance scoring are outside this change; this is not a rewrite of all
strategy rules discussed in the referenced conversation.

Run `python -m pytest -q test_btc_resilience.py test_scan_snapshot.py`.
After deployment, explicitly run `/scan/run/all`, then inspect `/scan/feed`:
strategy must be `V5.4_BTC_RESILIENCE`; candle timestamps must match within
each row; 1H and 4H must use their respective periods; ranking_key must be
non-increasing within each list. Old persisted snapshots retain the old
strategy and data until a successful new scan completes. Feed reads remain
read-only and never recalculate candles.
