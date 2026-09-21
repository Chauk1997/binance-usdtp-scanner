import json
from pathlib import Path
import pandas as pd
from continuation_quality import continuation_quality, capital_confirmation, upside_space
from background_quality import rank_background, POLICY_1H, POLICY_4H
from test_background_quality import row


def sample(tf='1h'):
    return pd.DataFrame(json.loads((Path(__file__).parent/'tests/fixtures'/f'MUBARAKUSDT_{tf}.json').read_text()))


def test_real_consolidation_then_hot_restart():
    q=continuation_quality(sample())
    assert q['high_compression'] and q['restart'] and q['overextended']
    assert q['state']=='overextended' and q['score']==0
    daily=continuation_quality(sample('1d'))
    assert daily['established_bull'] and daily['high_consolidation'] and daily['structure_preserved']
    assert continuation_quality(sample('4h'))['state']=='healthy_bull_continuation'


def test_healthy_restart_outweighs_same_pattern_overextended():
    d=sample();d.loc[d.index[-1],'close']=float(d.ema15.iloc[-1])*1.05
    d.loc[d.index[-1],'high']=max(d.close.iloc[-1],d.open.iloc[-1])+.0001
    q=continuation_quality(d)
    assert q['state']=='high_consolidation_reexpansion' and q['score']==6
    assert continuation_quality(d,overextended=True)['score']==0
    broken=d.copy();broken.loc[broken.index[-6:],'close']=broken.sma45.iloc[-6:]*.8
    assert not continuation_quality(broken)['structure_preserved']
    low=d.copy();low['ema15']=low.sma30*.95
    assert not continuation_quality(low)['high_compression']
    assert continuation_quality(low)['score']<q['score']


def test_uniform_ranking_symbol_rename_and_policy_evidence():
    a=row('MUBARAKUSDT',10,4,four=4);b=row('OTHER',10,4,four=4)
    a['high_compression_reexpansion_quality']={'score':6}
    b['high_compression_reexpansion_quality']={'score':0}
    for tf,policy in [('1h',POLICY_1H),('4h',POLICY_4H)]:
        rank_background([a,b],tf)
        before=a['ranking_key'][:]
        a['symbol']='RENAMED'
        rank_background([a,b],tf)
        assert a['ranking_key']==before and before>b['ranking_key']
        assert a['ranking_policy']==policy and len(before)==len(policy)
        assert 'structure_cohort' not in a


def test_space_and_capital_are_distinct_from_btc():
    d=sample()
    q=upside_space(d)
    assert 0<=q['score']<=10
    positive=capital_confirmation(dict(oi_delta_1h_pct=2,cvd_proxy_6h=5,taker_buy_sell_ratio_6h=1.2),'1h')
    negative=capital_confirmation(dict(oi_delta_1h_pct=-2,cvd_proxy_6h=-5,taker_buy_sell_ratio_6h=.8),'1h')
    assert positive['score']>negative['score']
    assert capital_confirmation({},'4h')['status']=='partial_or_unavailable'
    assert continuation_quality(None)['state']=='unavailable'
