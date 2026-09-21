import asyncio
import json
import threading
import time
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient
import main
from scan_snapshot import ScanSnapshot, utc_now


def result():
    feed = {"status": "complete", "scanner": "V5.4_CHATGPT_FEED",
            "generated_at_utc": utc_now(), "1h": {}, "4h": {}, "resonance": []}
    return {"status": "complete", "feed": feed, "formal": {"status": "complete"}}


@pytest.fixture
def snapshot(tmp_path, monkeypatch):
    store = ScanSnapshot(tmp_path / "feed.json")
    monkeypatch.setattr(main, "scan_snapshot", store)
    yield store
    store._executor.shutdown(wait=True)


def test_empty_read_only(snapshot):
    with patch.object(main, "_build_complete_snapshot", side_effect=AssertionError), \
         patch.object(main, "read_json", side_effect=AssertionError), \
         patch.object(main, "build_scan_feed", side_effect=AssertionError):
        with TestClient(main.app) as client:
            start = time.monotonic()
            assert client.get("/scan/feed").json()["status"] == "not_ready"
            assert client.get("/scan/status").json()["feed_ready"] is False
            assert time.monotonic() - start < 1


def test_publish_restart_and_failures(snapshot):
    async def good():
        return result()
    asyncio.run(snapshot.run(good, "formal"))
    original = snapshot.feed()
    loaded = ScanSnapshot(snapshot.path)
    assert loaded.feed() == original
    loaded._executor.shutdown()
    async def stopped():
        return {"status": "stopped", "stage": "derivatives"}
    asyncio.run(snapshot.run(stopped, "formal"))
    assert snapshot.feed() == original
    assert snapshot.status()["status"] == "stopped"
    async def bad():
        raise RuntimeError("test")
    asyncio.run(snapshot.run(bad, "formal"))
    assert snapshot.feed() == original
    assert snapshot.status()["status"] == "failed"
    with patch("scan_snapshot.Path.replace", side_effect=OSError):
        asyncio.run(snapshot.run(good, "formal"))
    assert snapshot.feed() == original
    assert json.loads(snapshot.path.read_text())["generated_at_utc"] == original["generated_at_utc"]
    snapshot._feed = {**original, "generated_at_utc": "2000-01-01T00:00:00+00:00"}
    assert snapshot.status()["stale"] is True


def test_blocking_scan_and_duplicate(snapshot, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    async def blocking():
        entered.set()
        assert release.wait(5)
        return result()
    monkeypatch.setattr(main, "_build_complete_snapshot", blocking)
    with TestClient(main.app) as client:
        for has_previous in (False, True):
            entered.clear()
            release.clear()
            previous = snapshot.feed()
            worker = threading.Thread(target=lambda: client.get("/scan/run/all"))
            worker.start()
            try:
                assert entered.wait(3)
                start = time.monotonic()
                feed = client.get("/scan/feed").json()
                assert feed == previous if has_previous else feed["status"] == "running"
                assert client.get("/scan/status").json()["status"] == "running"
                assert client.get("/scan/run/all/v52").json()["status"] == "running"
                assert time.monotonic() - start < 1
            finally:
                release.set()
                worker.join(5)
            assert snapshot.status()["status"] == "complete"


def test_cancelled_caller_keeps_guard(snapshot):
    entered, release = threading.Event(), threading.Event()
    async def blocking():
        entered.set()
        release.wait(5)
        return result()
    async def scenario():
        task = asyncio.create_task(snapshot.run(blocking, "formal"))
        await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await snapshot.run(blocking, "formal"))["status"] == "running"
        release.set()
    asyncio.run(scenario())
    snapshot._executor.shutdown(wait=True)
    assert snapshot.status()["status"] == "complete"


def test_corrupt_cache(tmp_path):
    path = tmp_path / "feed.json"
    for value in ('broken', 'null', '{"status":"running"}'):
        path.write_text(value)
        store = ScanSnapshot(path)
        assert store.feed()["status"] == "not_ready"
        store._executor.shutdown()


def test_pipeline_once(snapshot):
    formal = {"status": "complete", "elapsed_seconds": 23,
              "1h": {"top10": []}, "4h": {"top10": []}, "resonance": {"results": []}}
    with patch.object(main, "get_symbols", return_value=[]), \
         patch.object(main, "_scan_run_formal", return_value=formal) as scan, \
         patch.object(main, "build_watchlist_for_timeframe", return_value={}) as watch:
        with TestClient(main.app) as client:
            assert client.get("/scan/run/all").json() == formal
            feed = client.get("/scan/feed").json()
            assert feed["status"] == "stale"
            assert feed["strategy"] == "V5.5_HIGHER_TF_QUALITY"
            assert scan.call_count == 1
            assert watch.call_count == 2
            client.get("/scan/feed")
            assert scan.call_count == 1
