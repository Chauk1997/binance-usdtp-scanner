from copy import deepcopy
from background_quality import POLICY_1H, POLICY_4H, rank_background, VERSION, VERSION_4H
from test_background_quality import row
import main


def test_every_1h_dimension_dominates_all_later_dimensions():
    expected = ['daily_background_quality.quality_rank', 'four_hour_continuation_quality.score',
                'structure_quality.score', 'high_compression_reexpansion_quality.score',
                'relative_btc_resilience.score', 'upside_space.score',
                'capital_confirmation.score', 'auxiliary_score']
    assert POLICY_1H == expected
    def put(item, path, value):
        parts = path.split('.')
        for part in parts[:-1]:
            item = item.setdefault(part, {})
        item[parts[-1]] = value
    for index, path in enumerate(expected):
        winner, loser = {'symbol': 'Z'}, {'symbol': 'A'}
        for j, field in enumerate(expected):
            put(winner, field, 1 if j == index else 0)
            put(loser, field, 999 if j > index else 0)
        rows = rank_background([loser, winner], '1h')
        assert rows[0]['symbol'] == 'Z'


def test_4h_policy_and_keys_remain_v56():
    expected = ['daily_background_quality.quality_rank', 'daily_continuation_quality.score',
                'structure_quality.score', 'high_compression_reexpansion_quality.score',
                'key_structure_quality', 'upside_space.score', 'relative_btc_resilience.score',
                'capital_confirmation.score', 'auxiliary_score']
    assert POLICY_4H == expected
    a = row('A', 12, 1); b = row('B', 7, 4)
    rows = rank_background([a, b], '4h')
    assert [x['symbol'] for x in rows] == ['B', 'A']
    feed = main.build_scan_feed({'status':'complete','1h':{},'4h':{}})
    assert feed['strategy'] == VERSION == 'V5.8_1H_BACKGROUND_FIRST'
    assert feed['strategy_by_timeframe']['4h'] == VERSION_4H == 'V5.6_INTEGRATED_CONTINUATION'
    assert feed['ranking_policy_by_timeframe'] == {'1h': POLICY_1H, '4h': POLICY_4H}


def test_1h_key_threshold_mandatory_and_no_entry_gate(monkeypatch):
    from test_background_quality import key_frame, frame
    df=key_frame(volume=249.99)
    assert not main.find_key_candles(df,volume_multiplier=2.5)['passed']
    df.loc[49,'volume']=250
    assert main.find_key_candles(df,volume_multiplier=2.5)['passed']
    assert main.find_key_candles(key_frame(volume=220))['passed'] # unchanged 4H
    from test_continuation_quality import sample
    df=sample()
    monkeypatch.setattr(main,'load_dataframe',lambda s,tf: frame() if tf=='4h' else df)
    monkeypatch.setattr(main,'analyze_current_entry',lambda *a: (_ for _ in ()).throw(AssertionError('entry gate')))
    monkeypatch.setattr(main,'find_key_candles',lambda *a,**k: {'passed':False})
    assert main.qualify_background_first('X')['stage']=='key_candle_reject'
    monkeypatch.setattr(main,'find_key_candles',lambda *a,**k: {'passed':True})
    assert main.qualify_background_first('X')['stage']=='qualified'


def test_daily_levels_special_chronology_and_unconfirmed_pool():
    from background_first import daily_quality
    ranks=[daily_quality({'state':s},{},{},{})['quality_rank'] for s in
           ['bullish_divergence','bullish_consolidation','bottom_reversal','bearish_base_or_unconfirmed']]
    assert ranks==[5,4,2,1]
    daily={'state':'bullish_consolidation'}
    four={'restart':True,'consolidation_windows':[{'end_bars_ago':2}]}
    assert daily_quality(daily,{'high_consolidation':True},four,{'passed':True,'candidates':[{'bars_ago':3}]})['quality_rank']==6
    assert daily_quality(daily,{'high_consolidation':True},four,{'passed':True,'candidates':[{'bars_ago':0}]})['quality_rank']==4
    items=[row('BASE',100,1),row('BULL',0,5)]
    assert [r['symbol'] for r in rank_background(items,'1h')]==['BULL','BASE']


def test_no_key_strength_or_stage_double_score(monkeypatch):
    from test_continuation_quality import sample
    monkeypatch.setattr(main,'load_dataframe',lambda s,tf:sample(tf))
    monkeypatch.setattr(main,'rank_btc_items',lambda *a:None)
    rows=[row('A',999,1),row('B',0,1)]
    main.rank_items(rows,'1h',None,0)
    assert rows[0]['structure_quality']==rows[1]['structure_quality']
    assert 'continuation_adjustment' not in rows[0]['structure_quality']
    assert 'key_structure_quality' not in rows[0]
    before=rows[0]['ranking_key'][:]
    rows[0]['key_candle']={'new_key_candle':True,'score':999}
    rank_background(rows,'1h')
    assert rows[0]['ranking_key']==before


def test_capital_short_covering_and_closed_periods():
    import pandas as pd
    from background_first import capital_confirmation, closed_capital_inputs
    df=pd.DataFrame({'close':[100,101,102,103,104]})
    good={'oi_delta_4h_pct':3,'cvd_proxy_6h':5,'taker_buy_sell_ratio_6h':1.2}
    bad={**good,'oi_delta_4h_pct':-3}
    assert capital_confirmation(bad,df)['short_covering']
    assert capital_confirmation(bad,df)['score'] < capital_confirmation(good,df)['score']
    assert capital_confirmation({**good,'cvd_weakening':True},df)['score'] < capital_confirmation(good,df)['score']
    h=3600000
    oi=[{'timestamp':i*h,'sumOpenInterest':100+i} for i in range(10)]
    taker=[{'timestamp':i*h,'buyVol':2,'sellVol':1} for i in range(10)]
    result=closed_capital_inputs(oi,taker,8*h+100)
    assert result['closed_boundary']==8*h and result['flow_bars']==6
    assert result['oi_delta_4h_pct']==(108/104-1)*100
    assert result['cvd_proxy_6h']==6


def test_stale_board_is_not_published_and_short_top10_not_filled():
    from scan_summary import compact_scan_feed
    from test_scan_summary import sample
    feed=sample();feed['fresh_for_1h']=False
    out=compact_scan_feed(feed)
    assert out['1h']['candidates']==[] and out['special']['formal']==[]
    feed['fresh_for_1h']=True;feed['1h']['candidates']=feed['1h']['candidates'][:2]
    assert len(compact_scan_feed(feed)['1h']['candidates'])==2


def test_4h_rank_evidence_exactly_matches_original_v57(monkeypatch):
    import json
    from pathlib import Path
    from test_continuation_quality import sample
    baseline=json.loads((Path(__file__).parent/'tests/fixtures/v56-ranking.json').read_text())
    monkeypatch.setattr(main,'load_dataframe',lambda symbol,tf:sample(tf))
    monkeypatch.setattr(main,'rank_btc_items',lambda *a:None)
    rows=[row('A',12,4),row('B',3,1),row('C',7,3)]
    assert main.rank_items(rows,'4h',None,0)==baseline


def test_missing_capital_does_not_hide_qualified_candidate(monkeypatch):
    monkeypatch.setattr(main,'read_json',lambda p:['BASE'] if p==main.SYMBOL_CACHE else {})
    monkeypatch.setattr(main,'qualify_background_first',lambda s:{'stage':'qualified','status':'qualified'})
    monkeypatch.setattr(main,'rank_items',lambda *a:None)
    result=main.build_v36_results()
    assert [r['symbol'] for r in result]==['BASE']
