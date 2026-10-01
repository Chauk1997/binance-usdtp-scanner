"""Cold full-universe measurement; calls the unchanged production entry point."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import subprocess
import time
from urllib.parse import urlsplit


def gate(feed):
    reasons = []
    if feed.get('status') != 'complete' or not feed.get('feed_ready'):
        reasons.append('no fresh completed feed')
    total = feed.get('universe_total', 0)
    if not total or feed.get('universe', {}).get('sampled') is not False:
        reasons.append('full universe not proven')
    for tf in ('1h', '4h', '1d'):
        c = feed.get('coverage', {}).get(tf, {})
        if not (c.get('complete') and c.get('symbols') == total and c.get('scanned') == total and c.get('missing') == []):
            reasons.append(tf + ' coverage incomplete')
        if not feed.get('fresh_for_' + tf):
            reasons.append(tf + ' stale or unavailable')
    if not all(k in feed for k in ('1h', '4h', 'special', 'market_state')):
        reasons.append('feed structure incomplete')
    return reasons


async def run(output):
    import httpx
    import main as api
    from scanner_contract import VERSION
    started = time.monotonic()
    endpoints = {}; statuses = Counter(); errors = Counter()
    original_send = httpx.AsyncClient.send
    async def observed_send(client, request, *args, **kwargs):
        path = urlsplit(str(request.url)).path
        stat = endpoints.setdefault(path, {'requests': 0, 'errors': 0, 'seconds': 0, 'max_weight_1m': 0, 'retry_after': []})
        stat['requests'] += 1
        begin = time.monotonic()
        try:
            response = await original_send(client, request, *args, **kwargs)
            statuses[str(response.status_code)] += 1
            stat['errors'] += int(response.status_code >= 400)
            weight = response.headers.get('x-mbx-used-weight-1m', '')
            if weight.isdigit(): stat['max_weight_1m'] = max(stat['max_weight_1m'], int(weight))
            if response.headers.get('retry-after'): stat['retry_after'].append(response.headers['retry-after'])
            return response
        except Exception as exc:
            stat['errors'] += 1; errors[type(exc).__name__] += 1
            raise
        finally:
            stat['seconds'] += time.monotonic() - begin
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    metadata = {'git_sha': sha, 'strategy_version': VERSION, 'run_id': os.getenv('GITHUB_RUN_ID'),
                'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT'), 'repository': os.getenv('GITHUB_REPOSITORY'),
                'event': os.getenv('GITHUB_EVENT_NAME'), 'started_at_utc': datetime.now(timezone.utc).isoformat(),
                'python': platform.python_version(), 'platform': platform.platform(), 'cpu_count': os.cpu_count(),
                'runner_os': os.getenv('RUNNER_OS'), 'runner_image': os.getenv('ImageVersion'), 'cache_mode': 'cold'}
    if list(api.CACHE_DIR.glob('*.json')):
        raise RuntimeError('Cold benchmark requires empty cache; use a fresh checkout')
    httpx.AsyncClient.send = observed_send
    try:
        feed = await api.scan_snapshot.run(api._build_complete_snapshot, 'formal')
    finally:
        httpx.AsyncClient.send = original_send
    reasons = gate(feed)
    if statuses['418'] or statuses['429']: reasons.append('upstream rate limited')
    usage = resource.getrusage(resource.RUSAGE_SELF)
    report = {'schema': 'scanner-benchmark-v1', 'metadata': metadata,
              'finished_at_utc': datetime.now(timezone.utc).isoformat(),
              'scanner_wall_seconds': time.monotonic() - started,
              'cpu_user_seconds': usage.ru_utime, 'cpu_system_seconds': usage.ru_stime,
              'peak_rss_kib': usage.ru_maxrss / (1024 if platform.system() == 'Darwin' else 1),
              'cache_bytes': sum(p.stat().st_size for p in api.CACHE_DIR.rglob('*') if p.is_file()),
              'http_status_counts': dict(statuses), 'transport_errors': dict(errors), 'endpoints': endpoints,
              'scan_status': api.scan_snapshot.status(), 'scan_result_status': feed.get('status'),
              'universe_total': feed.get('universe_total'), 'coverage': feed.get('coverage'),
              'freshness': {k: v for k, v in feed.items() if k.startswith(('fresh_', 'expected_closed_', 'latest_closed_'))},
              'gate_passed': not reasons, 'gate_reasons': reasons}
    if api.scan_snapshot.path.exists():
        raw = json.loads(api.scan_snapshot.path.read_text())
        raw['run_metadata'] = metadata
        payload = json.dumps(raw, ensure_ascii=False, allow_nan=False, separators=(',', ':'))
        (output / 'scan_feed.json').write_text(payload)
        report['feed_sha256'] = hashlib.sha256(payload.encode()).hexdigest()
        report['auxiliary_missing'] = {tf: {r['symbol']: r.get('auxiliary', {}).get('missing_fields', []) for r in raw[tf]['candidates']} for tf in ('1h', '4h')}
    (output / 'benchmark.json').write_text(json.dumps(report, indent=2, allow_nan=False))
    summary = '## Scanner full-universe benchmark\n\n```json\n' + json.dumps(report, indent=2) + '\n```\n'
    (output / 'summary.md').write_text(summary)
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f: f.write(summary)
    print(json.dumps(report, indent=2), flush=True)
    return 0 if not reasons else 1


if __name__ == '__main__':
    out = Path('benchmark-output'); out.mkdir(exist_ok=True)
    raise SystemExit(asyncio.run(run(out)))
