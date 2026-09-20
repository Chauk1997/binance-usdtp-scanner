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
