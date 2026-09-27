> 最新正式規格見 [CONFIRMED_20260927.md](CONFIRMED_20260927.md)。下文為歷史稽核；其中5秒排程與Direct CVD要求已被60秒啟動及CVD Proxy取代。

# Scanner V54 code-level audit — 2026-09-27

Baseline: `abf1c32bb7c3e21f147baa2697a6fdf68cc298d8`, confirmed against both Railway production deployments. Active code path: `main._build_complete_snapshot → scanner_latest.build → strategy_latest`, with `ScanSnapshot` and MCP `scan_summary`. Historical strategy functions in main.py are not the active strategy and their HTTP routes remain disabled.

## Rule-by-rule findings

PASS means verified in executable code and relevant tests, not merely declared by a feed. NEEDS SPEC means the user's qualitative wording does not uniquely define an algorithm; this audit does not invent confirmation.

| Requirement | Finding | Code evidence / limits |
|---|---|---|
| All Binance tradable USDT perpetuals, no Top100/sample | PASS | `scanner_latest.select_universe`, `universe`, all-symbol technical loop |
| Exclude stocks and USDC | PASS for exchange metadata | quote/margin/base checks, STOCK/EQUITY/EQUITIES classification and explicit overrides; unknown class stops publication. Cannot identify a stock mislabelled COIN by upstream without an authoritative stock list. |
| Latest closed K only | FAIL → FIXED | `main` fetch timestamps + `confirmed_cache`; retain strict close_time < frozen server cutoff |
| openTime/closeTime and UTC/Taipei | PASS, hardened | Integer epoch alignment; duration minus 1 ms. 4H Taipei 00/04/08/12/16/20; daily Taipei 08:00, not midnight. Prior timezone formula was already correct. |
| Exchange visibility / boundary / cache freshness | FAIL → FIXED | 5-second request availability delay, bounded missing-only retries, post-close evidence, 90-second pending state without displaying old boards |
| 1D freshness exposed and background dependencies checked | Dependencies PASS; output FIXED | `freshness` now exports expected/fresh/state for all three intervals; 1H depends on 4H/1D, 4H on 1D |
| EMA15 / SMA30 / SMA45 | PASS | `prepare`: pandas ewm(span=15,adjust=False), rolling(30), rolling(45) |
| volume >= previous*2.2 AND > preceding 24 mean | PASS; zero-mean edge FIXED | Decimal boundary comparisons; current candle excluded from mean; quality no longer divides by zero |
| Upper wick only quality penalty; no lower wick penalty | PASS | `key_at`; volume-only key qualification distinct from long structure qualification |
| 1H primary 4H; 4H bearish divergence veto | PASS | `qualify` checks primary background before event and auxiliary evaluation |
| 4H primary 1D; 1D bearish divergence veto | PASS | Same branch with daily background |
| Exact quantitative bearish divergence/compression | NEEDS SPEC | Existing engineering parameters: 3 consecutive expanding/downward bearish bars; 4 compression bars, 2% MA spread / 6% range / .85 contraction. These thresholds were not explicitly fixed by the supplied user rules. |
| 1H types 1/2/3; 4H types 1/2 | Presence PASS; exact historical equivalence NEEDS SPEC | Existing types: compression launch, preceding-24-high breakout, 1H bullish acceleration. STRATEGY.md explicitly states these are engineering interpretations, not recovered original definitions. |
| 4H type 1 no automatic priority | PASS | Type name absent from ranking key; all compete on same technical dimensions |
| Independent Top10, no backfill | PASS | Separate pools, technical-tie enrichment frontier, final [:10]; test insufficient pools and ranking equivalence |
| Remove intersection | PASS | Active response contains no intersection/resonance and old routes return 404 |
| Special strictly ordered stages | PASS for stage order | `special`: daily known before 4H launch; later closed 4H retrace; subsequent full 1H compression window in settling phase; then new 1H type-1 Key. Tests reject future daily data and missing/reordered stages. Exact type/shape thresholds remain NEEDS SPEC. |
| approaching completed/missing stages | PASS | Prefix evidence, completed_count, missing_conditions, next_signal; not limited to Top10 intersection |
| Auxiliary after technical only | PASS | All-symbol qualification completes before endpoint enrichment; technical key dominates; no rescue for bad technical candidates |
| OI, delta, position/account/global ratios, Taker, funding | PASS | Explicit endpoint/window provenance and missing fields. Ratios displayed without arbitrary bullish bonus. |
| Reliable CVD, no proxy | PASS for executed-volume calculation; provenance explicit | `trade_cvd.calculate_cvd_from_exchange_volume`: exact sum(2*executed taker-buy base - total executed base), complete-window validation; no candle-color or ratio estimate. Label is calculated_from_exchange_volume, NOT exchange-published direct CVD. If “direct” must mean a separately published CVD series only, that is an unresolved source-policy distinction. |
| Broad CVD<0 and Taker<1 retreat warning | Mechanism PASS; thresholds NEEDS SPEC | Each board >=5 weak and >=60%; global warning needs all nonempty boards. Missing/unreliable CVD never counted. User did not specify numeric breadth. |
| No actual entry / white-arrow workflow | PASS | Active pipeline does not call historical entry routes or require white-arrow signals |
| Additional structural gates | NEEDS SPEC | Existing current-timeframe bearish veto, close below SMA45 by 3% invalidation, special-chain floor invalidation. Preserved rather than silently changing unspecified pattern validity. These prevent a claim of exact 100% spec equivalence. |

## Root cause and changes

1. Mathematical expected K advances immediately at the boundary, whereas the scheduled scan begins at :00:05 and needs additional computation/network time. Old code called every mismatch stale, even normal publication delay. Fixed with a bounded pending state; feed_ready remains false and affected rows are hidden. Delays beyond 90 seconds remain stale, intentionally.
2. Cache currency required a following forming candle. A valid, post-close response containing only the latest closed candle was rejected. The replacement requires an actual request after that candle closed plus five seconds. The timestamp is stamped only onto returned rows, not all merged rows; an old partial row cannot be falsely refreshed by an API response that omits it.
3. Fixed scan cutoff previously came from host wall clock; server time was not checked. New scans fail closed if Binance time cannot be obtained, then use a monotonic server anchor and freeze cutoff after the short wait. Strict close-time filtering remains.
4. Missing bars now get bounded in-round retries (1/2/4 seconds, only incomplete caches), plus existing scheduled retry/cooldown. Complete coverage is still required. The expected bar is never moved backwards to make a stale feed look fresh.
5. Daily expected/actual/freshness is now exported. Last-candle timestamps come from validated frames, rather than generated labels alone. Direct run responses receive request-time freshness checks too.
6. Hardened off-grid timestamp rejection and zero preceding-volume quality computation.

The observed pre-change feed was already fresh at inspection: scan 2026-09-27 04:03:16 UTC, published 04:04:22 UTC, 526/526 each interval. Logs available at audit time did not establish why that particular scan began at :03:16. Therefore API visibility, rate limiting, or previous failed rounds are possible explanations, not proven root causes of the user's historical incident. No timezone bug was reproduced.

## Validation

208 tests passed on Python 3.12. Tests include hour-before/instant/after milliseconds, seconds and minutes; all six Taipei 4H boundaries; UTC daily and Taipei midnight non-boundary; forming exclusion; delayed and permanently absent API candle; post-close response without next forming candle; cached pre-close partial replacement; skewed local wall clock; server-time failure; pending expiry and stale-row suppression; complete coverage/no partial publication; volume edge and off-grid timestamps. Existing strategy, CVD, ranking, routing and snapshot regressions also pass.

No live boundary delay can be forced at Binance; those cases are deterministic mocked regressions. Live post-deployment evidence is recorded separately in the deliverable report. Grace/retry values are explicit operational settings, not a promise of exchange availability within that interval.

Official field reference: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data (serverTime; kline openTime, closeTime, total volume, taker-buy base volume).
