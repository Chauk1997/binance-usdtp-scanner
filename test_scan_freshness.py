from datetime import datetime, timezone
import asyncio
import json
import pytest
import main
from scan_freshness import (expected_bar, freshness, closed_bars, SCAN_TIME,
                            seconds_until_scan, TAIPEI)
from scan_snapshot import ScanSnapshot


def ms(text):
    return int(datetime.fromisoformat(text).timestamp() * 1000)


@pytest.mark.parametrize('clock,tf,opening', [
    ('2026-09-20T22:01:00+08:00', '1h', '2026-09-20T21:00:00+08:00'),
    ('2026-09-20T22:02:00+08:00', '1h', '2026-09-20T21:00:00+08:00'),
] + [(f'2026-09-21T{hour:02}:0{minute}:00+08:00', '4h',
      ('2026-09-20T20:00:00+08:00' if hour == 0 else f'2026-09-21T{hour-4:02}:00:00+08:00'))
     for hour in (0, 4, 8, 12, 16, 20) for minute in (1, 2)])
def test_taipei_boundaries(clock, tf, opening):
    result = expected_bar(tf, ms(clock))
    assert result['open_time'] == ms(opening)
    assert result['close_time'] == ms(opening) + (3600000 if tf == '1h' else 14400000) - 1


def evidence(now):
    feed = {'coverage': {}, '1h': {}, '4h': {}}
    for tf in ('1h', '4h'):
        bar = expected_bar(tf, now)
        feed.update({f'latest_closed_{tf}_{k}': v for k,v in bar.items()})
        feed['coverage'][tf] = {'complete': True}
        feed[tf] = {'entry': [{'candle': bar, 'btc_candle': bar}]}
    return feed


def test_freshness_rollover_and_partial():
    now = ms('2026-09-20T22:02:00+08:00')
    feed = evidence(now)
    assert not freshness(feed, now)['stale']
    later = freshness(feed, now + 3600000)
    assert not later['fresh_for_1h'] and later['fresh_for_4h']
    feed['1h']['entry'][0]['candle'] = None
    assert freshness(feed, now)['stale']
    feed = evidence(now)
    feed['coverage']['4h']['complete'] = False
    assert not freshness(feed, now)['fresh_for_4h']
    assert freshness({}, now)['stale']


def test_closed_boundary_strict_and_no_blind_drop(tmp_path, monkeypatch):
    now = ms('2026-09-20T22:00:00+08:00')
    bar = expected_bar('1h', now)
    bar.update({k: 1 for k in ('open','high','low','close','volume','quote_volume','taker_buy_base','taker_buy_quote')})
    live = {**bar, 'open_time': now, 'close_time': now+3599999, 'close': 99999}
    assert closed_bars([bar, live], now) == [bar]
    assert closed_bars([bar], bar['close_time']) == []
    monkeypatch.setattr(main, 'read_json', lambda p: [bar,live])
    token = SCAN_TIME.set(now)
    try:
        frame = main.load_dataframe('X','1h')
        assert len(frame) == 1 and frame.iloc[-1]['close'] == 1
        monkeypatch.setattr(main, 'read_json', lambda p: [bar])
        assert len(main.load_dataframe('X','1h')) == 1
    finally:
        SCAN_TIME.reset(token)


def test_schedule():
    for text, seconds in [('2026-09-20T21:59:59+08:00',6),
                          ('2026-09-20T22:00:00+08:00',5),
                          ('2026-09-20T22:00:05+08:00',3600),
                          ('2026-09-20T23:59:59+08:00',6)]:
        assert seconds_until_scan(datetime.fromisoformat(text)) == seconds


def test_read_recomputes_and_keeps_generation(tmp_path, monkeypatch):
    now = ms('2026-09-20T22:02:00+08:00')
    monkeypatch.setattr('scan_freshness.time.time', lambda: now/1000)
    store = ScanSnapshot(tmp_path/'feed.json')
    async def build():
        return {'status':'complete', 'formal':{}, 'feed': {
            **evidence(now), 'status':'complete', 'scanner':'V5.4_CHATGPT_FEED',
            'generated_at_utc':'old', 'resonance':[]}}
    asyncio.run(store.run(build,'formal'))
    fresh = store.feed()
    assert fresh['status'] == 'complete' and fresh['generated_at_utc'] != 'old'
    now += 3600000
    stale = store.feed()
    assert stale['status'] == 'stale' and not stale['feed_ready']
    assert stale['generated_at_utc'] == fresh['generated_at_utc']
    assert json.loads(store.path.read_text())['status'] == 'complete'
    store._executor.shutdown()


def test_cache_requires_every_symbol_and_post_close_fetch(monkeypatch):
    now = ms('2026-09-20T22:02:00+08:00')
    bar = expected_bar('1h', now)
    live = {k:v+3600000 for k,v in bar.items()}
    data = {'BTCUSDT': [bar, live], 'ALT': [bar]}
    def read(path):
        if path == main.SYMBOL_CACHE:
            return list(data)
        return data[path.name.split('_')[0]]
    monkeypatch.setattr(main, 'read_json', read)
    token = SCAN_TIME.set(now)
    try:
        assert not main.interval_cache_is_current('1h')
        data['ALT'].append(live)
        assert main.interval_cache_is_current('1h')
    finally:
        SCAN_TIME.reset(token)


def test_scheduler_bootstrap_and_next_hour(monkeypatch):
    events = []
    class Store:
        async def run(self, build, response):
            events.append('scan')
        def feed(self):
            return {'stale': False}
    async def sleep(seconds):
        events.append(seconds)
        raise asyncio.CancelledError()
    monkeypatch.setattr(main, 'scan_snapshot', Store())
    monkeypatch.setattr(main, 'seconds_until_scan', lambda *args: 123)
    monkeypatch.setattr(main.asyncio, 'sleep', sleep)
    class Clock:
        @staticmethod
        def now(tz):
            return datetime(2026,9,20,14,2,tzinfo=timezone.utc)
    monkeypatch.setattr(main, 'datetime', Clock)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(main._scheduled_scans())
    assert events == ['scan',123]


def test_rate_limit_honors_retry_after(monkeypatch):
    import httpx
    monkeypatch.setattr(main.time, 'time', lambda: 1000)
    monkeypatch.setattr(main, 'RATE_LIMITED', False)
    monkeypatch.setattr(main, 'BINANCE_RETRY_AT', 0)
    monkeypatch.setattr(main, 'BINANCE_THROTTLE_UNTIL', 0)
    calls = []
    class Client:
        async def get(self, *args, **kwargs):
            calls.append(1)
            return httpx.Response(429, headers={'retry-after':'120'})
    first = asyncio.run(main.safe_get(Client(), 'https://example.test'))
    assert first['status_code'] == 429
    assert main.BINANCE_RETRY_AT == 1121
    main.RATE_LIMITED = False  # Pipeline reset cannot bypass upstream cooldown.
    assert asyncio.run(main.safe_get(Client(), 'https://example.test'))['_error'] == 'scanner_rate_limited'
    assert len(calls) == 1


def test_per_scan_cache_isolated_and_copies_mutable_results():
    from scan_freshness import SCAN_CACHE, per_scan_cached
    calls=[]
    @per_scan_cached
    def calculate(symbol):
        calls.append(symbol)
        return {'scores':[1,2]}
    for _ in range(2):
        token=SCAN_CACHE.set({})
        try:
            first=calculate('ALT')
            first['scores'].append(99)
            assert calculate('ALT') == {'scores':[1,2]}
        finally:
            SCAN_CACHE.reset(token)
    assert calls == ['ALT','ALT']


def test_partial_retry_only_requests_missing_symbols(monkeypatch):
    calls=[];waits=[]
    monkeypatch.setattr(main, 'RATE_LIMITED', False)
    monkeypatch.setattr(main, 'symbol_cache_is_current', lambda symbol,tf: symbol != 'late')
    async def update(client,symbol,tf,semaphore):
        calls.append(symbol)
        return {'status':'ok','symbol':symbol}
    async def sleep(seconds):
        waits.append(seconds)
    monkeypatch.setattr(main,'update_one_kline_cache',update)
    monkeypatch.setattr(main.asyncio,'sleep',sleep)
    result=asyncio.run(main.update_interval_incremental(None,[str(i) for i in range(520)]+['late'],'1h'))
    assert calls == ['late'] and len(result)==1
    assert len(waits)==1  # No 26 idle batch sleeps on already-current symbols.
