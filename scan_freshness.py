"""Closed-candle clock, evidence, and request-time freshness checks."""
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import time
from functools import wraps
from copy import deepcopy

TAIPEI = ZoneInfo('Asia/Taipei')
SCAN_TIME = ContextVar('scan_time', default=None)
SCAN_CACHE = ContextVar('scan_cache', default=None)
DURATIONS = {'1h': 3600000, '4h': 14400000, '1d': 86400000}


def scan_now_ms():
    fixed = SCAN_TIME.get()
    return fixed if fixed is not None else live_now_ms()


# Availability is separate from mathematical closure. Never advance a candle's
# close time or call the previous snapshot fresh during this bounded wait.
AVAILABILITY_DELAY_MS = 5000
PUBLICATION_GRACE_MS = 90000
_CLOCK = None


def set_server_clock(server_ms):
    global _CLOCK
    _CLOCK = (int(server_ms), time.monotonic())


def live_now_ms():
    if _CLOCK is None:
        return int(time.time() * 1000)
    server, anchor = _CLOCK
    return server + int((time.monotonic() - anchor) * 1000)


def expected_bar(interval, now_ms=None):
    now_ms = scan_now_ms() if now_ms is None else now_ms
    span = DURATIONS[interval]
    boundary = int(now_ms) // span * span
    return {'open_time': boundary - span, 'close_time': boundary - 1}


def confirmed_cache(bars, interval, now_ms=None):
    expected = expected_bar(interval, now_ms)
    if latest_closed(bars, interval, now_ms) != expected:
        return False
    # A forming candle stored before its close cannot become final just because
    # time passed. Require a post-close API request of this very candle.
    return any(int(b.get('open_time', -1)) == expected['open_time'] and
               int(b.get('fetched_after_ms', 0)) >= expected['close_time'] + 1 + AVAILABILITY_DELAY_MS
               for b in bars)


def closed_bars(bars, now_ms=None):
    cutoff = scan_now_ms() if now_ms is None else now_ms
    return [b for b in bars if int(b['close_time']) < cutoff]


def latest_closed(bars, interval, now_ms=None):
    bars = closed_bars(bars or [], now_ms)
    if not bars:
        return None
    bar = max(bars, key=lambda b: int(b['open_time']))
    start, end = int(bar['open_time']), int(bar['close_time'])
    if end != start + DURATIONS[interval] - 1:
        return None
    return {'open_time': start, 'close_time': end}


def freshness(feed, now_ms=None):
    feed = feed or {}
    now = live_now_ms() if now_ms is None else now_ms
    result = {'freshness_timezone': 'Asia/Taipei', 'stale_reasons': [],
              'pending_reasons': [], 'publication_grace_ms': PUBLICATION_GRACE_MS}
    modern = feed.get('freshness_version') in ('closed-bars-v2', 'closed-bars-v3')
    states = {}
    for tf in DURATIONS:
        expected = expected_bar(tf, now)
        actual = {k: feed.get(f'latest_closed_{tf}_{k}') for k in expected}
        covered = bool(feed.get('coverage', {}).get(tf, {}).get('complete'))
        valid_rows = True
        for rows in feed.get(tf, {}).values():
            if not isinstance(rows, list):
                continue
            for row in rows:
                for field in ('candle', 'btc_candle'):
                    if field == 'btc_candle' and modern and row.get(field) is None:
                        continue
                    valid_rows &= row.get(field) == actual
        previous = {k: v - DURATIONS[tf] for k, v in expected.items()}
        pending = (covered and valid_rows and actual == previous and
                   now - (expected['close_time'] + 1) < PUBLICATION_GRACE_MS)
        states[tf] = ('fresh' if covered and valid_rows and actual == expected else
                      'pending' if pending else 'stale')
        result['expected_closed_' + tf] = expected
        result.update({f'latest_closed_{tf}_{k}': v for k, v in actual.items()})
    for tf in DURATIONS:
        dependencies = ([tf] + (['4h', '1d'] if tf == '1h' else ['1d'] if tf == '4h' else [])) if modern else [tf]
        relevant = [states[d] for d in dependencies]
        state = 'stale' if 'stale' in relevant else 'pending' if 'pending' in relevant else 'fresh'
        result['fresh_for_' + tf] = state == 'fresh'
        result['freshness_state_' + tf] = state
        if state != 'fresh' and (modern or tf != '1d'):
            result['pending_reasons' if state == 'pending' else 'stale_reasons'].append(
                f'{tf}: awaiting completed snapshot' if state == 'pending' else
                f'{tf}: snapshot does not cover expected closed bar')
    result['stale'] = bool(result['stale_reasons'])
    result['pending'] = bool(result['pending_reasons']) and not result['stale']
    result['feed_ready'] = result['fresh_for_1h'] and result['fresh_for_4h']
    return result


def seconds_until_scan(now=None):
    now = now or datetime.now(TAIPEI)
    target = now.replace(minute=0, second=5, microsecond=0)
    if target <= now:
        target += timedelta(hours=1)
    return (target - now).total_seconds()


def per_scan_cached(function):
    """Reuse deterministic technical calculations only within one frozen scan."""
    @wraps(function)
    def wrapped(*args, **kwargs):
        cache = SCAN_CACHE.get()
        if cache is None:
            return function(*args, **kwargs)
        key = (function.__name__, args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = function(*args, **kwargs)
        return deepcopy(cache[key])
    return wrapped
