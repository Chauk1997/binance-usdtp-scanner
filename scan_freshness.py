"""Closed-candle clock, evidence, and request-time freshness checks."""
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import time

TAIPEI = ZoneInfo('Asia/Taipei')
SCAN_TIME = ContextVar('scan_time', default=None)
DURATIONS = {'1h': 3600000, '4h': 14400000, '1d': 86400000}


def scan_now_ms():
    fixed = SCAN_TIME.get()
    return fixed if fixed is not None else int(time.time() * 1000)


def expected_bar(interval, now_ms=None):
    now_ms = scan_now_ms() if now_ms is None else now_ms
    local = datetime.fromtimestamp(now_ms / 1000, TAIPEI)
    if interval == '1d':
        boundary = local.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        hours = DURATIONS[interval] // 3600000
        boundary = local.replace(hour=local.hour // hours * hours, minute=0, second=0, microsecond=0)
    end = int(boundary.timestamp() * 1000) - 1
    return {'open_time': end + 1 - DURATIONS[interval], 'close_time': end}


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
    result = {'freshness_timezone': 'Asia/Taipei', 'stale_reasons': []}
    for tf in ('1h', '4h'):
        expected = expected_bar(tf, now_ms)
        result['expected_closed_' + tf] = expected
        actual = {key: (feed or {}).get(f'latest_closed_{tf}_{key}') for key in expected}
        good = actual == expected and bool((feed or {}).get('coverage', {}).get(tf, {}).get('complete'))
        for rows in (feed or {}).get(tf, {}).values():
            if isinstance(rows, list):
                for row in rows:
                    for field in ('candle', 'btc_candle'):
                        candle = row.get(field) or {}
                        good = good and all(candle.get(k) == v for k, v in expected.items())
        result[f'fresh_for_{tf}'] = bool(good)
        result.update({f'latest_closed_{tf}_{k}': v for k, v in actual.items()})
        if not good:
            result['stale_reasons'].append(f'{tf}: snapshot does not cover expected closed bar')
    result['stale'] = not (result['fresh_for_1h'] and result['fresh_for_4h'])
    return result


def seconds_until_scan(now=None):
    now = now or datetime.now(TAIPEI)
    target = now.replace(minute=0, second=5, microsecond=0)
    if target <= now:
        target += timedelta(hours=1)
    return (target - now).total_seconds()
