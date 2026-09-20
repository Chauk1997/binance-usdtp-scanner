"""Single-worker, atomic completed-feed cache. No market I/O on read paths."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from threading import Lock
from scan_freshness import freshness


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class ScanSnapshot:
    def __init__(self, path):
        self.path = Path(path)
        self._lock = Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="full-scan")
        self._feed = None
        self._state = {"status": "idle", "started_at_utc": None,
                       "finished_at_utc": None, "last_error": None}
        # Load once, outside request handling. Ignore incomplete/corrupt snapshots.
        try:
            cached = json.loads(self.path.read_text())
            if (cached.get("status") == "complete"
                    and cached.get("scanner") == "V5.4_CHATGPT_FEED"
                    and cached.get("generated_at_utc")
                    and all(key in cached for key in ("1h", "4h", "resonance"))):
                self._feed = cached
                self._state["status"] = "complete"
        except (OSError, ValueError, AttributeError):
            pass

    def status(self):
        with self._lock:
            feed = self._feed
            result = dict(self._state)
        generated = feed.get("generated_at_utc") if feed else None
        age = None
        if generated:
            try:
                age = max(0, (datetime.now(timezone.utc) -
                              datetime.fromisoformat(generated)).total_seconds())
            except (ValueError, TypeError):
                pass
        checks = freshness(feed)
        return {**result, "feed_ready": feed is not None and not checks["stale"],
                "scan_complete": feed is not None,
                "generated_at_utc": generated, "age_seconds": age,
                **checks,
                "cache_mode": "completed_snapshot"}

    def feed(self):
        # Immutable reference swap: readers never wait on scan or disk I/O.
        with self._lock:
            feed = self._feed
        if feed is not None:
            checks = freshness(feed)
            return {**feed, **checks,
                    "status": "stale" if checks['stale'] else "complete",
                    "feed_ready": not checks['stale']}
        status = self.status()
        return {"status": "running" if status["status"] == "running" else "not_ready",
                "scanner": "V5.4_CHATGPT_FEED", "feed_ready": False,
                "scan_complete": False, "generated_at_utc": None,
                "message": "No completed snapshot. Scheduled scan is pending or running.",
                **freshness(None), "scan_status": status}

    async def run(self, build, response_key):
        with self._lock:
            if self._state["status"] == "running":
                return {"status": "running", "feed_ready": self._feed is not None}
            self._state = {"status": "running", "started_at_utc": utc_now(),
                           "finished_at_utc": None, "last_error": None}
        # Client disconnect/cancellation must not release the running guard early.
        future = self._executor.submit(self._build_and_publish, build, response_key)
        return await asyncio.shield(asyncio.wrap_future(future))

    def _build_and_publish(self, build, response_key):
        try:
            result = asyncio.run(build())
            if result.get("status") != "complete":
                with self._lock:
                    self._state.update(status="stopped", finished_at_utc=utc_now(),
                                       last_error={"status": result.get("status"),
                                                   "stage": result.get("stage"),
                                                   "incomplete_intervals": result.get("incomplete_intervals"),
                                                   "missing_symbols": result.get("missing_symbols")})
                return result
            feed = result["feed"]
            feed = {**feed, "generated_at_utc": utc_now(), "feed_ready": True, "scan_complete": True,
                    "cache_mode": "completed_snapshot"}
            # Persist completely before publishing. Failed writes retain old data.
            payload = json.dumps(feed, separators=(",", ":"), allow_nan=False)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temp = self.path.with_suffix(".tmp")
            temp.write_text(payload)
            temp.replace(self.path)
            with self._lock:
                self._feed = feed
                self._state.update(status="complete", finished_at_utc=utc_now(), last_error=None)
            return result[response_key]
        except Exception as exc:
            logging.exception("Full scan failed; keeping previous completed feed")
            with self._lock:
                self._state.update(status="failed", finished_at_utc=utc_now(),
                                   last_error=type(exc).__name__)
            return {"status": "failed", "stage": "scan_or_cache_publish",
                    "error": type(exc).__name__}
