import pytest
from fastapi.testclient import TestClient
import render_service as service


def identity():
    return dict(repository=service.REPOSITORY, repository_id='1371593905',
                ref=service.ALLOWED_REF, workflow_ref=service.WORKFLOW, event_name='workflow_dispatch')


def test_rejects_other_repositories_branches_and_workflows():
    assert service.check_claims(identity())
    for key in ('repository','repository_id','ref','workflow_ref','event_name'):
        c = identity(); c[key] = 'untrusted'
        with pytest.raises(ValueError): service.check_claims(c)


def test_scan_cannot_be_triggered_anonymously():
    client = TestClient(service.app)
    assert client.post('/control/scan').status_code == 401
    assert client.get('/control/snapshot').status_code == 401
    assert client.get('/health').status_code == 200


def test_mcp_read_does_not_start_scanner():
    before = service.task
    data = service.get_scan_feed()
    assert service.task is before
    assert data['feed_ready'] is False


def test_bad_bearer_fails_closed():
    client = TestClient(service.app)
    assert client.post('/control/scan', headers={'Authorization':'Bearer invalid'}).status_code == 403


def test_same_run_is_idempotent_and_other_run_cannot_overlap(monkeypatch):
    import asyncio
    async def scenario():
        async def auth(_): return dict(run_id='1',run_attempt='1')
        monkeypatch.setattr(service, 'authorize', auth)
        monkeypatch.setattr(service, 'state', dict(status='running',run_id='1:1'))
        assert (await service.start('unused'))['run_id'] == '1:1'
        pending = asyncio.create_task(asyncio.sleep(100))
        monkeypatch.setattr(service, 'task', pending)
        monkeypatch.setattr(service, 'state', dict(status='running',run_id='2:1'))
        try:
            with pytest.raises(service.HTTPException) as e:
                await service.start('unused')
            assert e.value.status_code == 409
        finally:
            pending.cancel()
    asyncio.run(scenario())
