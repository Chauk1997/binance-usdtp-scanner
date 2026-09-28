from copy import deepcopy
import pytest
from structure_sequence import features,daily_classify
from capital_sequence import capital_sequence,capital_score
from test_strategy_latest import frame,launch,bear_frame,NOW,H
import strategy_latest as s
from scan_summary import compact_scan_feed
from test_scan_summary import sample


def test_duration_and_confirmed_swing_no_future_pivot():
    d=frame(rise=.1)
    a=features(d)
    assert a['trend_duration']>12 and a['ema15_lead_duration']>=a['trend_duration']
    assert not a['swing']['broken']
    d.loc[len(d)-1,'high']=9999
    assert features(d)['swing']['highs']==a['swing']['highs']


def test_extension_penalty_and_defense_damage():
    d=frame(rise=.1);a=features(d)
    d.loc[99,['close','high']]=[130,130]
    assert features(d)['pullback']['overextension_atr']>a['pullback']['overextension_atr']
    d.loc[99,['close','low']]=[90,89]
    assert features(d)['ma_defense']['sma45']['close_breaches']>0


def test_daily_reserve_and_full_bear():
    assert daily_classify(frame(tf='1d'),s.trend)['pool']=='reserve'
    assert daily_classify(bear_frame('1d'),s.trend)['pool']=='excluded'
    assert daily_classify(frame(tf='1d',rise=.1),s.trend)['pool']=='formal'


def test_structure_precedes_daily_capital_and_recency_absent():
    frames={'1h':launch(frame()),'4h':frame(tf='4h',rise=.01),'1d':frame(tf='1d',rise=.01)}
    a,_=s.qualify('A','1h',frames)
    assert a['key_candle']['label']=='★ 本輪新關鍵K'
    assert 'freshness' not in a['ranking_policy']
    assert a['ranking_policy'][1]=='1h_structure_quality'
    b=deepcopy(a);b['ranking_key'][1]-=.01;b['ranking_key'][3]+=100
    a['ranking_key'] += [-100,-100];b['ranking_key'] += [100,100]
    assert a['ranking_key']>b['ranking_key']


def data():
    bars=[dict(open_time=i*H,close_time=(i+1)*H-1,close=100+i,volume=100,taker_buy_base=80 if i<4 else 55) for i in range(7)]
    oi=[dict(timestamp=i*H,sumOpenInterest=100-i) for i in range(1,8)]
    return oi,bars


def test_three_six_exact_windows_short_covering_and_weakening():
    oi,bars=data();e=capital_sequence(oi,bars,'1h',7*H)
    assert e['status']=='complete' and e['cvd_weakening']
    assert e['windows']['6']['cvd_proxy']['bar_count']==6
    assert e['windows']['3']['cvd_proxy']['bar_count']==3
    assert e['windows']['6']['oi_delta_pct']==pytest.approx((93/99-1)*100)
    assert e['windows']['6']['short_covering']
    score,warnings=capital_score(e)
    assert score<0 and 'cvd_weakening' in warnings


def test_missing_middle_oi_and_cvd_cannot_confirm():
    oi,bars=data();del oi[3];del bars[3]
    e=capital_sequence(oi,bars,'1h',7*H)
    assert e['status']=='partial'
    assert e['windows']['6']['oi_delta_pct'] is None
    assert not e['windows']['6']['cvd_proxy']['reliable']
    assert e['cvd_weakening'] is None


def test_future_data_ignored_and_4h_alignment():
    oi,bars=data();a=capital_sequence(oi,bars,'1h',7*H)
    oi.append(dict(timestamp=8*H,sumOpenInterest=9999))
    bars.append(dict(open_time=7*H,close_time=8*H-1,close=9999,volume=100,taker_buy_base=99))
    assert capital_sequence(oi,bars,'1h',7*H)==a
    assert capital_sequence(oi,bars,'4h',28*H)['status']=='partial'


def test_stale_reserve_and_intersection_not_rescored():
    feed=sample();feed['intersection']=['bogus'];feed['reserve']={'1h':{'candidates':[{'symbol':'R'}]}}
    result=compact_scan_feed(feed)
    assert result['intersection']==['S5','S6','S7','S8','S9']
    feed['fresh_for_1h']=False
    result=compact_scan_feed(feed)
    assert result['intersection']==[] and result['reserve']['1h']['candidates']==[]


def test_conditional_daily_priority_requires_key_then_expansion(monkeypatch):
    frames={'1h':launch(frame()),'4h':launch(frame(tf='4h'),98),'1d':frame(tf='1d',rise=.01)}
    real=s.features
    def seq(d):
        v=real(d);v['pullback']['reexpansion']=True
        v['pullback']['trough_close_time']=int(frames['4h'].close_time.iloc[-3])
        return v
    monkeypatch.setattr(s,'features',seq)
    monkeypatch.setattr(s,'daily_classify',lambda *a:dict(classification='high_consolidation',pool='formal',priority=3))
    row,_=s.qualify('X','1h',frames)
    assert row['daily_background']['conditional_priority']
    assert row['ranking_key'][3]==5
    frames['4h']=frame(tf='4h',rise=.01)
    row,_=s.qualify('X','1h',frames)
    assert not row['daily_background']['conditional_priority']


def test_reserve_row_never_claims_formal():
    frames={'1h':launch(frame()),'4h':launch(frame(tf='4h')),'1d':frame(tf='1d')}
    for tf in ('1h','4h'):
        row,_=s.qualify('X',tf,frames)
        assert row['pool']=='reserve'


def test_funding_has_no_positive_directional_reward():
    assert s.auxiliary_score({'funding_rate':-.01})[0]==0
    assert s.auxiliary_score({'funding_rate':.0001})[0]==0
    assert s.auxiliary_score({'funding_rate':.002})==(-1,['funding_crowded'])
