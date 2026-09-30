# GitHub-centric migration gate

Strategy baseline: `5746e2b49db2de7c802bd3b1ea91bfba84697daf`.
Active version: `V5.11_CONFIRMED_20260927_PROXY_PLUS1M`.
Implementation revision: `sequence-open-space-20260928`.

The first phase adds only an observational cold benchmark. Existing strategy,
selection, ranking, cutoff, throttling and publication code is unchanged.

## Benchmark

Run `.github/workflows/benchmark.yml` manually after it is on the default branch.
The initial `migration/free-benchmark` workflow commit also triggers one bootstrap
run; there is no hourly schedule. Uses public-repository standard `ubuntu-latest`,
Python 3.12 and unchanged production dependency pins. No artifact uploads, Actions
cache, LFS, paid runners or external resources are created.

The job runs all regression tests, calls the existing `_build_complete_snapshot`
through `ScanSnapshot.run`, and records wall time, CPU time, peak RSS, disk size,
per-endpoint requests, HTTP statuses, transport failures, weight and Retry-After.
The report includes full-universe coverage, actual candle times, freshness, feed
hash, strategy version, commit SHA, GitHub run ID and attempt. Optional auxiliary
missing fields are explicit; they do not disqualify technical candidates.

Report JSON appears in the job log and summary. GitHub's run timestamps measure
job/workflow duration including installation; `scanner_wall_seconds` is only the
scanner invocation. A failed clock/universe request does not establish universe
size or full-scan runtime. No successful feed is claimed for an aborted scan.

## Observed result: blocked

Run [36683989394](https://github.com/Chauk1997/binance-usdtp-scanner/actions/runs/36683989394)
on 2026-09-30, commit `3241b46340d335a75f825a8bd216e873f8f40fb4`:

- Ubuntu 24.04.5, image 20260920.314.1, 4 CPUs, Azure East US.
- 241 regression tests passed in 7.11 seconds.
- First `/fapi/v1/time` request returned HTTP 451. The production scanner correctly
  stopped at `server_clock` without requesting or publishing a partial universe.
- Scanner invocation ended after 0.23648 seconds; whole Python process 1.00 second.
  These are failure-path measurements, **not full-universe runtime**.
- Process peak RSS from `/usr/bin/time`: 102072 KiB; cache bytes: 0.
- Universe size, three-timeframe coverage, complete-feed integrity and sustained
  rate-limit behavior remain unmeasured. No complete feed was generated.
- Evidence: `benchmark-results/2026-09-30-36683989394.json`.

The requested standard-runner deployment is not validated. Hourly production,
persistent market feed and remote MCP cutover remain blocked. An execution
location with permitted Binance access must be established before re-running.
Do not treat a paid region-selectable runner, proxy, alternate exchange, or
partial-market scan as satisfying the current requirements.

## Production prerequisites

Do not enable hourly production until a full run has completed on the target
runner with complete 1H/4H/1D coverage and current freshness. Inspect auxiliary
missing-data evidence as well as the gate result. HTTP restrictions must be
reported rather than hidden by sampling, replacing Binance data, or moving to
paid infrastructure.

After the gate passes, the intended schedule is `1 * * * *` (UTC; minute :01 is
also :01 in Asia/Taipei). GitHub schedules may be delayed or dropped under load,
and inactive public-repository schedules can be disabled after 60 days. Readers
must recalculate freshness; a scheduled timestamp cannot establish data freshness.

Planned persistence: a bounded GitHub data branch or release assets, containing
complete snapshots and run metadata, published only after validation. Keep prior
complete data when a round fails, and expose its original timestamps as historical.
Avoid Actions artifacts/cache and Git LFS because they have metered storage paths.

A static GitHub feed cannot itself provide a remote MCP POST endpoint. The API/MCP
reader must recompute request-time freshness using the same Python contract. A
local stdio reader has no hosting bill; a remote service requires a verified free
plan and deployment access. No remote endpoint or persistence is deployed by this
benchmark phase. Do not represent the previous Railway URL as working.

Cost references (verified 2026-09-30):
- https://docs.github.com/en/billing/concepts/product-billing/github-actions
- https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases
- https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows
- https://developers.cloudflare.com/workers/platform/pricing/

Free service terms and availability can change. This implementation creates no
paid resource and must stop rather than switch to a billable fallback.
