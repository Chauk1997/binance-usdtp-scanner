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
snapshot. Every feed/status read recomputes freshness against Asia/Taipei's
current expected closed 1H and 4H bars. Stale snapshots return `status: stale`,
`feed_ready: false`, `stale: true`, and explicit reasons, retaining their original
completion timestamp and rows only as historical results.

The backend lifespan starts one guarded scheduler (single process/replica).
It catches up on startup, then triggers at each hour's `:00:05`. Missing caches
bootstrap automatically. Incomplete rounds retry after 30 seconds; rate-limited
rounds honor Retry-After (15 minutes if missing). Successful response weight
headers trigger proactive minute-boundary throttling at 800 used weight.
Already current symbols are skipped during recovery. Set
`SCANNER_SCHEDULER_ENABLED=0` only to disable this scheduler. Multiple workers
require a distributed scan lock and shared snapshot storage.

Each round freezes its candle cutoff. Indicators and daily-strength inputs use
only `close_time < cutoff`; no unconditional last-row deletion remains. Each
symbol must contain the expected closed candle and post-boundary fetch evidence
(the next open candle in the raw cache). Raw caches may retain a forming candle;
it never enters technical calculations. After long downtime a full 250-bar fetch
repairs history. Existing short-history technical gates remain in force.

The feed exposes millisecond Unix timestamps `latest_closed_1h_open_time`,
`latest_closed_1h_close_time`, `latest_closed_4h_open_time`, and
`latest_closed_4h_close_time`, plus `fresh_for_1h`, `fresh_for_4h`, expected bars,
coverage, and timezone. Evidence comes from actual cached candles and all-symbol
coverage, not the generation timestamp. Publishing atomically replaces
`completed_snapshot` and stamps `generated_at_utc` at completion.

Recommended reader schedule: hourly at `:02` Asia/Taipei. At 22:02 the required
1H candle is 21:00–21:59:59.999; at 20:02 the required 4H candle is
16:00–19:59:59.999. Consumers must check the relevant freshness flags and withhold
formal rankings while stale. Bootstrap, slow scans, or upstream failures can
exceed two minutes; timestamps are never advanced to conceal that delay.
At 08:00 Taiwan (UTC daily reset), daily strength uses the last completed UTC
session until the first new-session 1H candle closes, avoiding forming prices.

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
After deployment, wait for the automatic startup scan, then inspect `/scan/feed`:
strategy must be `V5.4_BTC_RESILIENCE`; candle timestamps must match within
each row; 1H and 4H must use their respective periods; ranking_key must be
non-increasing within each list. Old persisted snapshots retain the old
strategy and data until a successful new scan completes. Feed reads remain
read-only and never recalculate candles.

A per-round cache reuses dataframes and symbol technical classifications across
candidate discovery, entry, resonance, and watchlist builders. Cached values are
copied for callers and discarded after each round, so no result crosses the
closed-bar cutoff. Candidate derivatives retain the existing two-request
semaphore; per-symbol pacing is 50 ms rather than 500 ms. Response weight
throttling and Retry-After handling remain authoritative.

Partial candle coverage retries after five seconds and only requests symbols
whose cache is still behind. Current symbols are removed before batch pacing,
so a one-symbol retry does not sleep through the whole market. Status errors
include the incomplete intervals and missing symbols for diagnosis.

## V5.5 fixed higher-timeframe policy (2026-09-21)

The production scanner now evaluates the full Binance USDT perpetual universe
independently for 1H and 4H. Neither board supplies the other's candidate pool.

* 1H first rejects 4H bearish alignment with declining averages and widening
  downside spread. Missing background data fails closed. The existing operation
  structure, pullback/stop and overextension checks remain active.
* 4H accepts only daily bullish divergence, preserved consolidation after a
  confirmed bullish regime, or a completed/clearly forming bullish reversal.
  A later bearish alignment invalidates older bullish history; a low base alone
  is not sufficient. Symbol names never affect qualification.
* Background ranks are bullish divergence (4), bullish consolidation (3),
  bottom reversal (2), unconfirmed/base (1), bearish divergence (0), unavailable
  (-1). Evidence includes MA values, 5-bar slopes, normalized gaps, timestamps,
  and the most recent confirmed bull/bear regimes. History is 60 bars, requiring
  three consecutive regime bars. Divergence compares normalized EMA15–SMA45
  spread over three bars with EMA15 leading the slow MA. A forming reversal
  requires all three slopes positive, EMA15 leading and bending upward, and
  SMA30 at least 99.75% of SMA45. Consolidation requires SMA30 >= 99.5% of
  SMA45, slow slope >= -0.2%, and close >= 97% of SMA45.
* Key K uses exactly the latest 12 closed operation bars: positive body at least
  the preceding 24-bar mean absolute body, upper wick strictly below half the
  range, volume >= 2.2 times previous volume and strictly above the preceding
  24-bar mean volume. Lower wick is unrestricted. EMA pull is evaluated after
  qualification, not an extra Key K gate. The current Key K gets the label
  `★ 本輪新關鍵K`, with no label/recency ranking bonus.
* 1H ranking is lexicographic: 4H background rank, structural cohort, daily
  background rank, exact structure score, Key K/structure stage, same-operation
  same-time BTC resilience, then derivatives. Cohorts start at the highest
  remaining structure score and include gaps <= 1.0; this avoids non-transitive
  pairwise approximate comparisons. Daily quality dominates the exact score
  within each cohort, while much weaker structures remain in later cohorts.
* 4H ranking stays independent: its own structure, Key K/stage, same-time 4H BTC,
  then derivatives. Auxiliary evidence includes OI/changes, three long/short
  ratios, funding and Binance taker-flow CVD **proxy**, not aggregated CVD.
  Absolute OI is context, not a cross-symbol size bonus.

`get_scan_feed` / `/scan/feed` now reports `V5.5_HIGHER_TF_QUALITY`, explicit
per-timeframe ranking policies, all ranked entry `candidates` before Top 10,
background fields, ranking keys, Key K details, structure stage, auxiliary
inputs, and `validation_samples` for the five requested examples.
`/scan/quality/{symbol}` exposes the same qualification diagnostics for any
cached universe symbol. Snapshot timestamps/freshness behavior is preserved.
The per-symbol diagnostic uses current cached closed bars; the feed diagnostics
use the scan's frozen cutoff.

Samples are time-dependent examples, not permanent inclusion/exclusion rules.
At the 2026-09-21 daily close, IRYS has not completed reversal; ME and SKL have
positive ordered averages with expanding spread and can pass reversal background.
1000FLOKI and OPG pass daily background. A separate latest-12 4H Key K gate still
applies, so passing background does not guarantee inclusion in the entry board.


## V5.6 integrated continuation ranking

Qualification gates and independent full-universe scans are unchanged. No symbol
profiles, reserved slots or ranking cohorts are used. 1H ranking is lexicographic:
1D background quality → 4H continuation → integrated 1H structure → high
compression/reexpansion → Key K/stage → upside space → same-bar BTC resilience
→ OI/CVD confirmation → existing auxiliary (including Funding/position ratios).
4H uses 1D background and 1D continuation before its own operation structure.

`daily_background_quality.rank` retains the prior classification; `quality_rank`
is authoritative for ranking, putting bullish divergence and preserved high
bull consolidation in tier 4. All new evidence and thresholds are exported.
`structure_quality.score` is raw structure plus continuation adjustment:
restart +2, healthy high compression +1.5, healthy continuation +1, preserved
consolidation +0.5, ordinary 0, unconfirmed/low rebound -1, overextended -2.
These are ranking effects only; existing entry/qualification rules still apply.

Continuation requires a prior three-bar established bull, followed by a
three-bar consolidation in the upper 35% of the prior 24-bar range, at most
50% retracement, at most 3 ATR wide, and preserved major MAs. Restart requires
rising price above EMA15, accelerating EMA15 and expanding bullish MA spread.
Extension above 3 ATR or 12% over EMA15, or the existing entry overextension
flag, overrides restart quality. A daily established/preserved consolidation in
the upper 35% of its 30-bar range also qualifies as high-quality background.
These transparent initial thresholds are not a profitability backtest.

Upside uses the nearest prior 60-bar local swing high above price, capped at
10%; no observed overhead swing is explicitly labelled and assigned that cap,
not infinite room. OI/CVD confirmation compares signs, not absolute cross-coin
sizes; CVD remains Binance six-hour taker-flow proxy with timeframe disclosed.
`ranking_key`, not legacy display `score`, is the ordering authority. Every
board is sorted before slicing its top ten. MUBARAK is a diagnostic sample only.
