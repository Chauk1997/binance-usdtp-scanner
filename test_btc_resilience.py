import json
import pytest
import main
from btc_resilience import candle_metrics, compare_candles, rank_items, DURATIONS


def bar(tf='1h', o=100, h=111, l=99, c=110, start=0):
    return dict(open_time=start, close_time=start+DURATIONS[tf]-1,
                open=o, high=h, low=l, close=c)


@pytest.mark.parametrize('tf', ['1h', '4h'])
def test_alignment_and_highest(tf):
    duration = DURATIONS[tf]
    btc = bar(tf, c=90, h=101, l=89)
    coin = bar(tf)
    rows = [dict(symbol='ALT', structure_score=10, final_score=0)]
    data = {'ALT': [coin, bar(tf, start=duration)], 'BTCUSDT': [btc]}
    rank_items(rows, tf, lambda s,t: data[s], duration+100)
    x = rows[0]
    assert x['candle']['open_time'] == x['btc_candle']['open_time'] == 0
    assert x['candle']['body_return'] == pytest.approx(.1)
    assert x['btc_candle']['body_return'] == pytest.approx(-.1)
    assert x['relative_btc_resilience']['tier'] == 3
    assert x['candle']['timeframe'] == tf
    # Missing latest BTC cannot silently compare to an older candle.
    rank_items(rows, tf, lambda s,t: data[s], duration*2+100)
    assert rows[0]['relative_btc_resilience']['status'] == 'unavailable'


def test_red_high_close_and_bad_data():
    btc = candle_metrics(bar(c=90,h=101,l=89), '1h', 0)
    red = candle_metrics(bar(c=99,h=101,l=90), '1h', 0)
    assert red['close_position'] > .8
    assert compare_candles(red, btc)['tier'] == 1
    flat = candle_metrics(bar(o=100,h=100,l=100,c=100), '1h', 0)
    assert compare_candles(flat, btc)['reason'] == 'zero_range'
    for bad in [bar(o=0), bar(h=95), bar(c=float('nan')), bar(start=1), {}]:
        assert candle_metrics(bad, '1h', 0) is None
    mismatch = {**btc, 'timeframe': '4h'}
    assert compare_candles(red, mismatch)['reason'] == 'time_mismatch'


def test_priority_and_projection():
    rows = [dict(symbol=str(i), structure_score=10, final_score=999,
                 relative_vs_btc_pct=1000, derivatives_context={'adjustment': 1}) for i in range(12)]
    rows[11]['structure_score'] = 11
    rows[10]['derivatives_context']['adjustment'] = -1
    def read(s, tf):
        if s == 'BTCUSDT': return [bar(c=90,h=101,l=89)]
        return [bar()] if s == '10' else [bar(c=99,h=101,l=90)]
    rank_items(rows, '1h', read, 3600100)
    assert [x['symbol'] for x in rows[:2]] == ['11', '10']
    compact = main.compact_formal_item(rows[1])
    feed = main.build_scan_feed({'status':'complete', '1h':{'top10':[compact]}})
    output = feed['1h']['entry'][0]
    assert output['candle'] == rows[1]['candle']
    assert output['btc_candle'] == rows[1]['btc_candle']
    assert output['ranking_key'] == rows[1]['ranking_key']
    assert main.v54_watch_item(main.compact_watch_item(rows[1], 'test'))['candle'] == rows[1]['candle']
    json.dumps(feed, allow_nan=False)


@pytest.mark.parametrize('tf,builder,scan,stage', [
    ('1h', 'build_v36_results','scan_one_symbol_v33','qualified'),
    ('4h', 'build_4h_final_results','scan_one_symbol_4h_v44','entry_structure_checked')])
def test_real_builders_rank_before_top10(monkeypatch, tf, builder, scan, stage):
    symbols = [str(i) for i in range(12)]
    def read(path):
        if path == main.SYMBOL_CACHE: return symbols
        if path in (main.DAILY_CACHE, main.DERIVATIVES_CACHE, main.DERIVATIVES_4H_CACHE):
            return {'symbols':{s:{'daily_return_pct':1} for s in symbols}}
        return [bar(tf, c=90,h=101,l=89)] if 'BTCUSDT' in str(path) else [bar(tf)]
    monkeypatch.setattr(main, 'read_json', read)
    monkeypatch.setattr(main.time, 'time', lambda: DURATIONS[tf]/1000+.1)
    monkeypatch.setattr(main, scan, lambda s: {'stage':stage,'status':'可進場','v33_structure_score':int(s), 'test_score':int(s)})
    monkeypatch.setattr(main, 'calculate_4h_structure_score', lambda b: b['test_score'])
    result = getattr(main,builder)()
    assert len(result) == 10
    assert result[0]['symbol'] == '11'
    assert result[-1]['symbol'] == '2'
    assert all(x['relative_btc_resilience']['tier'] == 3 for x in result)
