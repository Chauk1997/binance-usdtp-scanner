# Render Free validation deployment

This phase is manual only. It creates one free web service (512 MB / 0.1 CPU),
no database, disk, paid cron, background worker, artifact cache or paid fallback.
Before creation verify that the Render workspace has no payment method and the
creation summary shows Free/$0. A paid account can incur bandwidth/build overages
although the service compute plan is Free.

1. Create a Web Service using the public repository URL
   `https://github.com/Chauk1997/binance-usdtp-scanner`, branch
   `migration/free-benchmark`. `render.yaml` specifies Singapore, Python 3.12.14,
   `pip install --no-cache-dir -r requirements-render.txt`, and
   `uvicorn render_service:app --host 0.0.0.0 --port $PORT --workers 1`.
   Select Free explicitly; leave automatic deploys disabled.
2. Confirm `/health` reports the deployed commit. Set repository variable
   `SCANNER_RENDER_URL` to the HTTPS `*.onrender.com` origin.
3. Register the `render-scan.yml` manual workflow on the default branch, then
   dispatch it against `migration/free-benchmark`. The service only accepts
   GitHub-signed OIDC tokens for that exact repository ID, branch, workflow and
   manual/scheduled event. It does not accept anonymous scan starts or PR events.
4. Actions wakes the service, starts one scan, and polls every 30 seconds while
   the scan is active. No idle keepalive. A subprocess runs the unchanged full
   production scanner with a cold cache; a 40-minute limit terminates it.
5. Review the report in the job summary: 1H/4H/1D full coverage and freshness,
   API errors/weight/Retry-After, auxiliary missing fields, runtime, process RSS
   and Render cgroup resource evidence. Do not enable production unless it passes.

The service exposes read-only `/mcp`, `/scan/feed`, `/scan/feed/summary` and
`/health`. Control endpoints require short-lived GitHub identity. It has no
GitHub write token. Actions will own persistent feed publication after validation.
On restart it can restore the current-strategy snapshot from the GitHub
`scanner-feed` branch, but that branch/publication is not created by this phase.
Request-time freshness suppresses old rankings. If no current-version snapshot
exists the service says not_ready rather than inventing a feed.

The local reference run covered 526/526 symbols in all three intervals in
242.392 seconds; 1,820 HTTP 200 responses; peak RSS 376624 KiB. This does not prove
Render performance: the free CPU quota is much smaller, and server+child memory
must fit 512 MB. GitHub's own East US runner previously received Binance HTTP 451.
Render may also be restricted or may suspend high outbound API traffic. If the
free service fails, stop and report; do not upgrade or reduce the strategy's pool.

## Verified 2026-10-01

Deployment `dep-dav73uk1nsns738vf670` served commit
`9223b5927c818b4f498a6c27909f2fd3fb64d52b`; public `/health` confirmed this SHA.
The dashboard confirms Free/Singapore, a Hobby workspace, one service, no card on
file, $0.00 month-to-date and projected October charges, and a $0 September invoice.
These checks cover this Render workspace, not any old Railway subscription.

Manual Actions run [36879151585](https://github.com/Chauk1997/binance-usdtp-scanner/actions/runs/36879151585)
failed at the first `/fapi/v1/time` request with HTTP 418 and Retry-After 73494
seconds. The scanner invocation was 0.772592 seconds, NOT a full-scan runtime.
Universe, coverage, freshness and complete-feed integrity remain unmeasured on
Render. The report is `benchmark-results/2026-10-01-36879151585.json`.

Do not retry before **2026-10-02 11:13:02 UTC / 19:13:02 Asia/Taipei**. Expiry is
not proof that the upstream restriction has cleared. The service enforces the
reported cooldown across subprocesses while running; it is not persistent across
service restarts, so operators must preserve this deadline and not redeploy or
restart to evade it. Do not repeatedly retry or enable an hourly failing scan.

The corrected manual benchmark installs the Render test dependencies and has no
push bootstrap. The updated revision passed 247 local regression tests.
Hourly `:01`, durable feed publication and production MCP/plugin cutover are not
yet enabled. No paid resource was added.


## Cold retry 2026-10-04: still blocked

Run 36879151585 attempt 2, job 111440259569, retried the same deployed 9223b59
only after the previous cooldown had expired. The first /fapi/v1/time request
again returned HTTP 418, now with Retry-After 78675 seconds. Scanner invocation
was 0.875676 seconds; no universe or complete feed was obtained. Evidence:
`benchmark-results/2026-10-04-36879151585-attempt2.json`.

The new conservative not-before deadline is **2026-10-05 10:44:59 UTC /
18:44:59 Asia/Taipei**. This is an upstream cooldown, not a promised recovery time.
Do not automatically repeat retries, restart to evade the cooldown, enable
production, or upgrade to a paid plan. The current GitHub+Render execution path
has not met the full-universe gate. No resource or billing plan changed today.

A local scanning host with GitHub persistence is a possible architecture change,
based on the earlier successful local benchmark, but requires user acceptance
of an always-on local computer, network availability and electricity costs. No
local daemon, self-hosted runner or schedule has been installed.
