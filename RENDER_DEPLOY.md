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

Status: deployment and Render benchmark pending. Hourly `:01`, durable feed
publication and production MCP/plugin cutover are not yet enabled.
