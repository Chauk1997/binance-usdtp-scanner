"""Free Render orchestration and read-only MCP; strategy remains unchanged."""
import asyncio
from contextlib import asynccontextmanager
import json
import math
import os
from pathlib import Path
import signal
import sys
import tempfile
import time

import httpx
import jwt
from fastapi import FastAPI, Header, HTTPException
from mcp.server.mcpserver import MCPServer
from scan_snapshot import ScanSnapshot
from scan_summary import compact_scan_feed
from scanner_contract import VERSION

ROOT = Path(__file__).resolve().parent
REPOSITORY = 'Chauk1997/binance-usdtp-scanner'
AUDIENCE = 'scanner-v54-render'
ALLOWED_REF = os.getenv('SCANNER_ALLOWED_REF', 'refs/heads/migration/free-benchmark')
WORKFLOW = REPOSITORY + '/.github/workflows/render-scan.yml@' + ALLOWED_REF
JWKS = jwt.PyJWKClient('https://token.actions.githubusercontent.com/.well-known/jwks')
state = {'status': 'idle', 'run_id': None}
task = None
retry_not_before_epoch = 0.0
snapshot = ScanSnapshot(ROOT / '.render-cache' / 'scan_feed.json')
mcp = MCPServer(name='scanner-v54-mcp', version=VERSION)
mcp_app = mcp.streamable_http_app(stateless_http=True, json_response=True, host='0.0.0.0')


def check_claims(claims):
    if not (claims.get('repository') == REPOSITORY and
            str(claims.get('repository_id')) == '1371593905' and
            claims.get('ref') == ALLOWED_REF and
            claims.get('workflow_ref') == WORKFLOW and
            claims.get('event_name') in ('workflow_dispatch', 'schedule')):
        raise ValueError('Workflow identity is not permitted')
    return claims


def decode_identity(token):
    key = JWKS.get_signing_key_from_jwt(token).key
    return check_claims(jwt.decode(token, key, algorithms=['RS256'], audience=AUDIENCE,
                                 issuer='https://token.actions.githubusercontent.com',
                                 options={'require': ['exp', 'iat', 'sub', 'aud', 'iss']}))


async def authorize(authorization):
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(401, 'GitHub Actions identity required')
    try:
        return await asyncio.to_thread(decode_identity, authorization[7:])
    except Exception:
        raise HTTPException(403, 'Untrusted or expired workflow identity') from None


@mcp.tool()
def ping() -> dict:
    return {'service': 'scanner-v54-mcp', 'strategy': VERSION, 'scan_status': state['status']}


@mcp.tool()
def get_scan_feed() -> dict:
    """Read completed feed; stale boards are suppressed, never trigger a scan."""
    return compact_scan_feed(snapshot.feed())


@asynccontextmanager
async def lifespan(app):
    global snapshot
    # Restore the last complete GitHub snapshot after Render discards its disk.
    url = f'https://raw.githubusercontent.com/{REPOSITORY}/scanner-feed/scan_feed.json'
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(url)
        if response.status_code == 200:
            data = response.json()
            if data.get('strategy') == VERSION and data.get('status') == 'complete':
                snapshot.path.parent.mkdir(exist_ok=True)
                snapshot.path.write_text(json.dumps(data, allow_nan=False))
                snapshot = ScanSnapshot(snapshot.path)
    except (httpx.HTTPError, ValueError, OSError):
        pass
    async with mcp.session_manager.run():
        yield
    if task and not task.done():
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass


app = FastAPI(lifespan=lifespan)


@app.get('/health')
def health():
    return {'status': 'ok', 'strategy': VERSION, 'git_sha': os.getenv('RENDER_GIT_COMMIT'),
            'scan_status': state['status']}


@app.get('/scan/feed')
def feed():
    return snapshot.feed()


@app.get('/scan/feed/summary')
def summary():
    return get_scan_feed()


async def execute(claims):
    global snapshot, state, retry_not_before_epoch
    process = None
    try:
        with tempfile.TemporaryDirectory(prefix='.render-run-', dir=ROOT) as directory:
            path = Path(directory)
            env = {**os.environ, 'SCANNER_SCHEDULER_ENABLED': '0',
                   'GITHUB_RUN_ID': claims['run_id'], 'GITHUB_RUN_ATTEMPT': claims.get('run_attempt', '1'),
                   'GITHUB_REPOSITORY': REPOSITORY, 'GITHUB_EVENT_NAME': claims['event_name']}
            with (path / 'scan.log').open('w') as log:
                process = await asyncio.create_subprocess_exec(sys.executable, str(ROOT / 'benchmark.py'),
                            cwd=path, env=env, stdout=log, stderr=log, start_new_session=True)
                await asyncio.wait_for(process.wait(), timeout=2400)
            report_path = path / 'benchmark-output' / 'benchmark.json'
            if not report_path.exists():
                state.update(status='failed', reason='Scanner stopped without a report', exit_code=process.returncode)
                return
            report = json.loads(report_path.read_text())
            # Keep upstream cooldown across scan subprocesses in this service.
            for endpoint in report.get('endpoints', {}).values():
                for value in endpoint.get('retry_after', []):
                    try:
                        seconds = float(value)
                        if math.isfinite(seconds) and seconds > 0:
                            retry_not_before_epoch = max(retry_not_before_epoch, time.time() + seconds)
                    except (TypeError, ValueError):
                        pass
            if retry_not_before_epoch > time.time():
                report['retry_not_before_epoch'] = retry_not_before_epoch
            report['render_resources'] = {}
            for name in ('memory.peak', 'memory.max', 'cpu.max'):
                metric = Path('/sys/fs/cgroup') / name
                if metric.exists():
                    report['render_resources'][name] = metric.read_text().strip()
            state.update(report=report, exit_code=process.returncode,
                         status='complete' if process.returncode == 0 and report['gate_passed'] else 'failed')
            if state['status'] == 'complete':
                raw = path / 'benchmark-output' / 'scan_feed.json'
                snapshot.path.parent.mkdir(exist_ok=True)
                temp = snapshot.path.with_suffix('.tmp')
                temp.write_bytes(raw.read_bytes()); temp.replace(snapshot.path)
                snapshot = ScanSnapshot(snapshot.path)
    except asyncio.TimeoutError:
        state.update(status='failed', reason='40-minute scan deadline exceeded')
    except asyncio.CancelledError:
        state.update(status='failed', reason='Service shutdown interrupted the scan')
        raise
    except Exception as exc:
        state.update(status='failed', reason=type(exc).__name__)
    finally:
        if process and process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        state['finished_at_epoch'] = time.time()


@app.post('/control/scan', status_code=202)
async def start(authorization: str | None = Header(default=None)):
    global task, state
    claims = await authorize(authorization)
    run_id = claims['run_id'] + ':' + claims.get('run_attempt', '1')
    if state['run_id'] == run_id:
        return {'status': state['status'], 'run_id': run_id}
    if task and not task.done():
        raise HTTPException(409, 'Another scan is running')
    if retry_not_before_epoch > time.time():
        raise HTTPException(429, 'Binance cooldown is still active',
                            headers={'Retry-After': str(math.ceil(retry_not_before_epoch - time.time()))})
    state = {'status': 'running', 'run_id': run_id, 'started_at_epoch': time.time()}
    task = asyncio.create_task(execute(claims))
    return state


@app.get('/control/status')
async def status(authorization: str | None = Header(default=None)):
    await authorize(authorization)
    return state


@app.get('/control/snapshot')
async def raw_snapshot(authorization: str | None = Header(default=None)):
    claims = await authorize(authorization)
    if state['status'] != 'complete' or state['run_id'] != claims['run_id'] + ':' + claims.get('run_attempt', '1'):
        raise HTTPException(409, 'This workflow has no completed snapshot')
    return json.loads(snapshot.path.read_text())


app.mount('/', mcp_app)
