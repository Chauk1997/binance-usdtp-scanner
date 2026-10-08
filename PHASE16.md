# Phase16 implementation

Default SCANNER_STRATEGY=phase16; set legacy to run the existing strategy. All old modules, endpoints and tests remain. SCANNER_DATA_DIR controls cache, SQLite and feed placement. Run one worker only. Versioned SQLite is authoritative for state and board recovery; JSON is the API snapshot.

User approved test defaults on 2026-10-08:

- EMA15 starts with 15-close SMA; ATR14 starts with first14 true-range SMA and Wilder recursion; 200-bar warmup. Persist recursive values across cache sliding.
- SMA45 significant decline: three-bar slope/current ATR <= -0.20. Natural finish requires each of last3 bars to have strict bullish order, rising EMA/SMA30, strictly widening both adjacent gaps, and SMA45 slope > -0.20.
- Bear invalidation: mirrored bearish key followed by the next3 strictly bearish-order bars, falling EMA/SMA30, nonrising SMA45 and widening both gaps. It is a consolidation condition, distinct from signal invalidation within3 bars.
- Resistance: prior120 bars, left/right3 pivot, all right3 confirmations beforeK1; >=2 tests with high cluster <=.5 pivot ATR and intervening pullback >=.5 ATR. Close >= zone high + .5 current ATR retires a zone. Highest still-valid zone is the frozen key snapshot target. No hindsight.
- Escape ranks <=0, (0,.5), [.5,1), >=1 ATR. Key body grade <.4/.4-.7/>=.7; upper wick>.5 caps quality at middle. No extra close-strength factor, volume bonus or A/B/C/D name bonus.
- Longer consolidation ranks by 12/24/48 observation bars. These duration and escape grades are adjustable defaults, not adopted hard requirements.
- SpecialBTC mode compares latest synchronized closed1H returns for all candidates (including4H) minus BTC1H return. Historical six-bar down-BTC median remains immutable.
- Existing legacy special recognizer produces label-only MUBARAK; this does not claim a newly validated exact machine definition. Special legacy boards are retained for legacy mode, not mixed into Phase16 Top10.

K1 counts as observation unless itself a key; currentkey is excluded. State stores latest8 observationbars and confirms only on a nonkey. Currentkey cannot retroactively confirm itself. KeyRun continuation uses only four volume/range/body requirements, even after A/B ends the structure. No notification is sent by the test runner; notifications table supports confirmed delivery deduplication.

Data: 720-bar cache, common Binance cutoff; stale/malformed/gapped/candle-unconfirmed data stops publication. New listings without200 warmup bars have explicit UNKNOWN diagnostics and partial coverage, no artificial listing-age screen. Partial results are labeled; never described as full-marketTop10.

SQLite stores historical signals, structures, runs, early/rejected research, notification confirmations and scan audit. First-key OHLCV, eventHTF, resistance, historicalBTC and event ranking stay fixed; currentHTF and lifecycle update independently. The API keeps 4H scan time and ranking between boundaries and filters TTL on reads. Restart catch-up processes the latest missing closedboundary once; it does not fabricate every missed historical leaderboard.

Deployment remains unverified. Parameter tuning and replay backtesting must precede any performance claim.

Override only the documented implementation defaults with SCANNER_PHASE16_DEFAULTS containing a JSON object (Config field names). Fixed adopted technical thresholds are not overridden here. A parameter fingerprint is persisted; changing defaults requires a separate SCANNER_DATA_DIR so historical state cannot silently mix versions.

Multiple nonconsecutive formal signals of the same asset are all retained, while Top10 represents each asset once using its highest-ranked active event; complete ties use the earliest key. This is a presentation choice, not a new qualification gate. Resonance labels use all valid active events, including those outside Top10. Each timeframe publishes its original ranking policy (normal or BTC special) so an hourly mode change does not imply a re-ranked frozen4H board. Each scan audit retains the entire feed and ranked candidate pool, not only counts. Auxiliary requests have concurrency3 and share the existing Binance rate limiter.

Parameter version2 enforces the same200-bar warmup at the event timestamp of every higher timeframe, not merely the current snapshot. A later daily/4H candle cannot repair historical warmup insufficiency. Use a new data directory for this test-version migration; retain version1 measurements separately. For1H soft health, the weaker4H/daily grade is used; this is not an extra hard gate.
