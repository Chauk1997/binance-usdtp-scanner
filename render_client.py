"""Actions controller: authenticate via OIDC, start once, poll real scan progress."""
import json
import os
from pathlib import Path
import time
from urllib.parse import urlparse
import httpx


def run():
    base = os.environ['SCANNER_RENDER_URL'].rstrip('/')
    url = urlparse(base)
    if url.scheme != 'https' or not url.hostname or not url.hostname.endswith('.onrender.com') or url.path:
        raise ValueError('Expected an HTTPS onrender.com service origin')
    with httpx.Client(timeout=90) as client:
        def headers():
            result = client.get(os.environ['ACTIONS_ID_TOKEN_REQUEST_URL'] + '&audience=scanner-v54-render',
                headers={'Authorization': 'Bearer ' + os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']})
            result.raise_for_status()
            return {'Authorization': 'Bearer ' + result.json()['value']}
        # A normal health request wakes the free service. No permanent keepalive.
        ready = False
        for _ in range(12):
            try:
                r = client.get(base + '/health')
                if r.status_code == 200 and r.json().get('status') == 'ok':
                    ready = True; break
            except (httpx.HTTPError, ValueError): pass
            time.sleep(10)
        if not ready: raise RuntimeError('Render failed to wake within the bounded retry window')
        response = client.post(base + '/control/scan', headers=headers())
        response.raise_for_status()
        expected = os.environ['GITHUB_RUN_ID'] + ':' + os.environ['GITHUB_RUN_ATTEMPT']
        deadline = time.monotonic() + 2500
        while time.monotonic() < deadline:
            response = client.get(base + '/control/status', headers=headers())
            response.raise_for_status()
            state = response.json()
            if state.get('run_id') != expected: raise RuntimeError('Render restarted or scan identity changed')
            if state['status'] != 'running': break
            print('Full-universe scan is running', flush=True)
            time.sleep(30)
        else: raise RuntimeError('Render benchmark polling deadline exceeded')
        Path('render-benchmark.json').write_text(json.dumps(state, indent=2))
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
            f.write('## Render benchmark\n\n```json\n' + json.dumps(state, indent=2) + '\n```\n')
        print(json.dumps(state, indent=2))
        if state['status'] != 'complete' or not state['report']['gate_passed']:
            raise RuntimeError('Render benchmark failed; production remains disabled')
        response = client.get(base + '/control/snapshot', headers=headers())
        response.raise_for_status()
        feed = response.json()
        from benchmark import gate
        from scan_freshness import freshness
        reasons = gate({**feed, **freshness(feed)})
        if reasons: raise RuntimeError('Snapshot no longer fresh: ' + ', '.join(reasons))
        Path('render-scan-feed.json').write_text(json.dumps(feed, allow_nan=False, separators=(',', ':')))


if __name__ == '__main__': run()
