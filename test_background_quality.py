import json
import pandas as pd
import pytest
import main
from background_quality import background_quality, rank_background


def frame(kind='bull'):
    n=100
    slow=pd.Series([100+i*.1 for i in range(n)])
    fast=pd.Series([100+i*.3 for i in range(n)])
    mid=pd.Series([100+i*.2 for i in range(n)])
    if kind == 'bear':
        slow=200-slow; fast=200-fast; mid=200-mid
    return pd.DataFrame(dict(ema15=fast,sma30=mid,sma45=slow,close=fast+1))


def test_background_states_and_hard_filter():
    bull=background_quality(frame())
    bear=background_quality(frame('bear'))
    assert bull['state']=='bullish_divergence' and bull['eligible_4h']
    assert bear['hard_exclude'] and not bear['eligible_4h']
    base=frame('bear')
    base.iloc[-10:]=[75,76,77,76]
    q=background_quality(base)
    assert q['state']=='bearish_base_or_unconfirmed' and not q['eligible_4h']
    # Prior bull followed by compression can retain a healthy slow MA.
    consolidation=frame()
    consolidation.iloc[-5:]=[128,119.5,109.9,128]
    assert background_quality(consolidation)['state']=='bullish_consolidation'
    # Fully bearish alignment with contracting spread is not the hard filter.
    contracting=frame('bear')
    contracting.iloc[-1]=[71,79.8,90,72]
    assert not background_quality(contracting)['hard_exclude']


def key_frame(ago=0, volume=220, high=103, low=90):
    df=pd.DataFrame([dict(open_time=i,open=100.,close=101.,high=102.,low=99.,
                          volume=100.,ema15=100.) for i in range(50)])
    df.loc[49-ago,['close','high','low','volume']]=[102,high,low,volume]
    return df


def test_key_boundaries_lower_wick_and_no_ema_gate():
    q=main.find_key_candles(key_frame())
    assert q['passed'] and q['label']=='★ 本輪新關鍵K'
    assert q['latest']['body_ratio'] < .5  # long lower wick is allowed
    assert q['latest']['ema15_pull_pct']==0 # evaluated separately from gate
    assert not main.find_key_candles(key_frame(volume=219.99))['passed']
    assert main.find_key_candles(key_frame(ago=11))['passed']
    assert not main.find_key_candles(key_frame(ago=12),lookback=48)['passed']
    assert not main.find_key_candles(key_frame(high=104,low=100))['passed'] # exactly half
    tiny=key_frame(); tiny.loc[49,'close']=100.1
    assert not main.find_key_candles(tiny)['passed']
    no_volume=key_frame(); no_volume.loc[:47,'volume']=300
    assert not main.find_key_candles(no_volume)['passed']


def row(symbol, structure, daily, four=3, btc=0, aux=0):
    return dict(symbol=symbol,structure_score=structure,
                daily_background_quality={'rank':daily,'quality_rank':daily},
                four_hour_continuation_quality={'score':four}, daily_continuation_quality={'score':daily},
                structure_quality={'score':structure},four_hour_background_quality={'rank':four},
                higher_tf_quality={}, key_structure_quality=1, relative_btc_resilience={'score':btc},
                auxiliary_score=aux)


def test_1h_structure_first_and_4h_unchanged():
    rows=[row('weak_day',11,1,btc=3,aux=100),row('good_day',10.25,4),
          row('much_weaker_structure',8,4),row('best_4h',5,0,four=4)]
    rank_background(rows,'1h')
    assert [x['symbol'] for x in rows]==['weak_day','good_day','much_weaker_structure','best_4h']
    assert all(rows[i]['ranking_key']>=rows[i+1]['ranking_key'] for i in range(len(rows)-1))
    # 4H independently uses daily quality and daily continuation.
    rank_background(rows,'4h')
    assert rows[0]['symbol']=='good_day'


def test_btc_before_aux_and_new_marker_no_bonus():
    rows=[row('aux',10,4,aux=999),row('btc',10,4,btc=1)]
    rank_background(rows,'1h')
    assert rows[0]['symbol']=='btc'
    keys=[x['ranking_key'][:] for x in rows]
    rows[1]['key_candle']={'new_key_candle':True,'label':'★ 本輪新關鍵K'}
    rank_background(rows,'1h')
    assert keys==[x['ranking_key'] for x in rows]


def test_background_projection_survives_all_layers():
    x=row('X',10,4)
    x.update(key_candle={'passed':True},structure_stage='compression',timeframe='1h')
    rank_background([x],'1h')
    compact=main.compact_formal_item(x)
    feed=main.build_scan_feed({'status':'complete','1h':{'top10':[compact]}})
    out=feed['1h']['entry'][0]
    for key in ['daily_background_quality','four_hour_background_quality','higher_tf_quality',
                'ranking_key','key_candle','structure_stage']:
        assert out[key]==x[key]
    json.dumps(feed, allow_nan=False)


def test_1h_pool_does_not_use_4h_top10(monkeypatch):
    monkeypatch.setattr(main,'read_json',lambda path:['ONLY_1H'])
    monkeypatch.setattr(main,'scan_one_symbol_v33',lambda s:{'stage':'qualified','status':'可進場'})
    monkeypatch.setattr(main,'get_4h_entry_candidates',lambda:[])
    assert main.get_current_entry_candidates()==['ONLY_1H']


def test_higher_tf_filter_runs_first(monkeypatch):
    monkeypatch.setattr(main,'load_dataframe',lambda *args:frame('bear'))
    def must_not_run(df): raise AssertionError('1h structure called before veto')
    monkeypatch.setattr(main,'analyze_1h_ma_structure',must_not_run)
    assert main.scan_one_symbol_v321('X')['stage']=='4h_hard_veto'


def test_missing_background_fails_closed():
    assert main.analyze_4h_hard_veto(None)['hard_veto']
    assert main.analyze_1d_hard_veto(None)['veto']


def test_dated_market_background_examples():
    from pathlib import Path
    fixture=json.loads((Path(__file__).parent/'tests/fixtures/background-2026-09-21.json').read_text())
    q={s:background_quality(pd.DataFrame(rows)) for s,rows in fixture['symbols'].items()}
    assert not q['IRYSUSDT']['eligible_4h']
    assert q['1000FLOKIUSDT']['state']=='bullish_consolidation'
    assert q['OPGUSDT']['eligible_4h']
    # Actual closed candles have changed relative to the supplied negative
    # examples: never hardcode their symbols to manufacture a passing test.
    assert q['MEUSDT']['state']==q['SKLUSDT']['state']=='bottom_reversal'
    # Replaying their prior low-base candles must reject those backgrounds.
    for symbol in ['MEUSDT','SKLUSDT']:
        rows=fixture['symbols'][symbol]
        assert not background_quality(pd.DataFrame(rows[:-5]))['eligible_4h']
