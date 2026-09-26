import asyncio
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
import pandas as pd
import pytest
import strategy_latest as s
import scanner_latest as runner
from scan_freshness import expected_bar, SCAN_TIME, freshness

H=3600000
NOW=1800000000000//(24*H)*(24*H)


def frame(n=100,tf='1h',rise=0):
    duration={'1h':H,'4h':4*H,'1d':24*H}[tf]
    end=expected_bar(tf,NOW)['close_time']+1
    bars=[dict(open_time=end-(n-i)*duration,close_time=end-(n-i-1)*duration-1,
               open=100+i*rise,close=100+i*rise,high=100.1+i*rise,low=99.9+i*rise,volume=100.0) for i in range(n)]
    return s.prepare(bars,tf,NOW)[0]


def launch(df,i=None):
    d=df.copy();i=len(d)-1 if i is None else i
    d.loc[i,['open','close','high','low','volume']]=[100,104,104.1,99.9,220]
    for col,values in [('ema15',d.close.ewm(span=15,adjust=False).mean()),('sma30',d.close.rolling(30).mean()),('sma45',d.close.rolling(45).mean())]: d[col]=values
    return d


def test_key_threshold_and_mean_excludes_current():
    d=launch(frame())
    assert s.key_at(d,99)['volume_vs_prev']==2.2
    d.loc[99,'volume']=219.999
    assert s.key_at(d,99) is None
    d.loc[75:98,'volume']=220;d.loc[98,'volume']=100
    avg=d.volume.iloc[75:99].mean();d.loc[99,'volume']=avg
    assert s.key_at(d,99) is None
    d.loc[99,'volume']=220
    assert s.key_at(d,99)


def test_shape_not_key_hard_gate_and_no_lower_wick_penalty():
    d=launch(frame());d.loc[99,'high']=150
    key=s.key_at(d,99)
    assert key and key['upper_wick_penalty']
    d.loc[99,'low']=10
    lower=s.key_at(d,99)
    assert lower['quality']==key['quality'] and lower['body_ratio']<key['body_ratio']
    d.loc[99,'close']=99
    assert s.key_at(d,99)  # Volume-only Key K does not guarantee long qualification.
    assert not s.events(d,'1h')


def test_closed_only_gap_duplicate_and_invalid():
    d=frame();bars=d.to_dict('records')
    forming={**bars[-1],'open_time':NOW,'close_time':NOW+H-1,'close':9999,'high':9999,'volume':99999}
    actual,error=s.prepare(bars+[forming],'1h',NOW)
    assert not error and len(actual)==len(d)
    for invalid in (bars[:-1],bars[:40]+bars[41:],bars+[bars[-1]]):
        assert s.prepare(invalid,'1h',NOW)[0] is None
    bars[-1]['volume']=float('nan')
    assert s.prepare(bars,'1h',NOW)[0] is None


def test_full_universe_no_extra_exclusions():
    def symbol(name,**kw):return dict(symbol=name,baseAsset=name.removesuffix('USDT'),quoteAsset='USDT',marginAsset='USDT',contractType='PERPETUAL',status='TRADING',underlyingType='COIN',**kw)
    rows=[symbol(k) for k in ('BTCUSDT','USDCUSDT','FDUSDUSDT','BTCDOMUSDT','測試USDT')]
    rows += [dict(symbol('AAPLUSDT'),underlyingType='EQUITY'),dict(symbol('FUTUSDT'),contractType='CURRENT_QUARTER'),dict(symbol('BTCUSDC'),quoteAsset='USDC')]
    selected,excluded,unknown=runner.select_universe(rows)
    assert selected==sorted(['BTCUSDT','FDUSDUSDT','BTCDOMUSDT','測試USDT']) and not unknown
    rows=[dict(symbol('NEWUSDT'),underlyingType='NEW_CLASS')]
    assert runner.select_universe(rows)[2]==['NEWUSDT']


def bear_frame(tf='4h'):
    d=frame(tf=tf)
    for i in range(len(d)):
        d.loc[i,s.MA]=[100-i*.3,110-i*.2,120-i*.1]
    return d


def test_background_veto_primary_not_daily_for_1h():
    frames={'1h':launch(frame()),'4h':frame(tf='4h',rise=.01),'1d':bear_frame('1d')}
    row,reason=s.qualify('X','1h',frames)
    assert row and not reason
    frames['4h']=bear_frame()
    assert s.qualify('X','1h',frames)[0] is None
    frames['4h']=launch(frame(tf='4h'))
    assert s.qualify('X','4h',frames)[0] is None
    flat=frame();flat['ema15']=90;flat['sma30']=95;flat['sma45']=100
    assert not s.trend(flat)['hard_exclude']


def test_all_types_and_type1_does_not_rank_automatically():
    d=launch(frame())
    assert s.events(d,'4h')[-1]['types']==['type_1','type_2']
    assert 'type_3' not in s.events(d,'4h')[-1]['types']
    d=frame(rise=.02);i=len(d)-1
    d.loc[i,['open','close','high','low','volume']]=[d.close.iloc[i-1],110,110.1,101,220]
    d['ema15']=d.close.ewm(span=15,adjust=False).mean();d['sma30']=d.close.rolling(30).mean();d['sma45']=d.close.rolling(45).mean()
    assert 'type_3' in s.events(d,'1h')[-1]['types']
    assert not any('type' in field for field in s.POLICY)


def test_auxiliary_alignment_missing_zero_and_no_proxy():
    cutoff=NOW+1000
    data={'oi':[{'timestamp':NOW-H,'sumOpenInterest':'100'},{'timestamp':NOW,'sumOpenInterest':'110'},{'timestamp':NOW+H,'sumOpenInterest':'999'}],
          'taker':[{'timestamp':NOW-H,'buyVol':'2','sellVol':'4'},{'timestamp':NOW,'buyVol':'999','sellVol':'1'}],
          'global':[{'timestamp':NOW,'longShortRatio':'1.2'}],
          'top_position':[{'timestamp':NOW-H,'longShortRatio':'100'}],
          'top_account':[], 'funding':[{'fundingTime':NOW-1,'fundingRate':'.0001'}]}
    e=runner.parse_auxiliary(data,'1h',cutoff)
    assert e['oi']==110 and e['oi_delta_pct']==pytest.approx(10)
    assert e['taker_buy_sell_ratio']==.5 and e['global_long_short_ratio']==1.2
    assert e['top_position_long_short_ratio'] is None and e['cvd']['value'] is None
    data['taker'][0]['sellVol']='0'
    assert runner.parse_auxiliary(data,'1h',cutoff)['taker_buy_sell_ratio'] is None


def warning_row(i,direct=True,weak=True):
    return {'symbol':str(i),'auxiliary':{'window_start':1,'window_end':2,'taker_buy_sell_ratio':.8 if weak else 1.2,
            'cvd':{'kind':'direct' if direct else 'proxy','reliable':True,'source':'fixture','value':-1,'window_start':1,'window_end':2}}}


def test_broad_warning_needs_reliable_cvd_and_breadth():
    weak=[warning_row(i) for i in range(6)];strong=[warning_row(i+6,weak=False) for i in range(4)]
    assert s.market_warning({'1h':weak+strong,'4h':weak+strong})['triggered']
    assert not s.market_warning({'1h':weak[:3],'4h':[]})['triggered']
    assert not s.market_warning({'1h':strong,'4h':weak})['triggered']
    assert not s.market_warning({'1h':[warning_row(i,False) for i in range(10)]})['triggered']
    row=warning_row(0);row['auxiliary']['cvd']['window_end']=3
    assert not s.valid_cvd(row['auxiliary'])


def special_frames():
    # Aligned historical 4H anchor 32h ago, then a touch and four hourly compression bars.
    four=launch(frame(tf='4h'),92)
    one=launch(frame())
    daily=frame(tf='1d',rise=.01)
    return {'1h':one,'4h':four,'1d':daily}


def test_special_real_sequence_and_approaching():
    f=special_frames()
    item=s.special('X',f)
    assert item and item['completed_count']==5
    times=[e['close_time'] for e in item['completed_stages']]
    assert times==sorted(set(times))
    f['1h']=frame()
    near=s.special('X',f)
    assert near['completed_count']==4 and near['missing_conditions']==[s.STAGES[-1]]
    f['4h']=launch(frame(tf='4h'))
    near=s.special('X',f)
    assert near['completed_count']==2  # Simultaneous signals cannot skip retracement.


def test_special_no_retroactive_daily_bull():
    f=special_frames();f['1d']['ema15']=90;f['1d']['sma30']=95;f['1d']['sma45']=100
    f['1d'].loc[99,s.MA]=[110,105,100]
    assert s.special('X',f) is None


def test_no_legacy_routes():
    import main
    from fastapi.testclient import TestClient
    with TestClient(main.app) as client:
        for path in ('/scan/resonance','/scan/1h/entry-now','/market/derivatives/update','/scan/quality/X'):
            assert client.get(path).status_code==404


def test_old_snapshot_is_not_loaded(tmp_path):
    import json
    from scan_snapshot import ScanSnapshot
    p=tmp_path/'old.json'
    p.write_text(json.dumps(dict(status='complete',scanner='V5.4_CHATGPT_FEED',strategy='V5.8',generated_at_utc='old',**{'1h':{},'4h':{},'resonance':[]})))
    store=ScanSnapshot(p)
    assert not store.feed()['feed_ready']
    store._executor.shutdown()


def test_daily_freshness_dependency():
    feed={'freshness_version':'closed-bars-v2','coverage':{tf:{'complete':True} for tf in ('1h','4h','1d')}}
    for tf in ('1h','4h','1d'):
        feed.update({f'latest_closed_{tf}_{k}':v for k,v in expected_bar(tf,NOW).items()})
    assert not freshness(feed,NOW)['stale']
    feed['latest_closed_1d_close_time']-=24*H
    assert not freshness(feed,NOW)['fresh_for_1h'] and not freshness(feed,NOW)['fresh_for_4h']


def test_full_pipeline_technical_before_auxiliary(monkeypatch):
    symbols=['BTCUSDT','GOODUSDT','BADUSDT']
    frames={name:{tf:launch(frame(tf=tf)) if name!='BADUSDT' and tf!='1d' else frame(tf=tf,rise=.01) for tf in ('1h','4h','1d')} for name in symbols}
    queried=[]
    api=SimpleNamespace(BINANCE_BASE='https://example.test',SYMBOL_CACHE='symbols',write_json=lambda *a:None,
                        interval_cache_is_current=lambda tf:True,symbol_cache_is_current=lambda *a:True,
                        cache_file=lambda symbol,tf:(symbol,tf),read_json=lambda key:frames[key[0]][key[1]].to_dict('records'))
    async def universe(*args):return symbols,{'total':3}
    async def aux(api,client,symbol,tf,cutoff):
        queried.append((symbol,tf))
        return {'oi_delta_pct':-10,'taker_buy_sell_ratio':.1,'cvd':{'value':None}}
    monkeypatch.setattr(runner,'universe',universe);monkeypatch.setattr(runner,'fetch_auxiliary',aux)
    token=SCAN_TIME.set(NOW)
    try: result=asyncio.run(runner.build(api))
    finally: SCAN_TIME.reset(token)
    assert result['status']=='complete'
    assert queried and all(symbol!='BADUSDT' for symbol,tf in queried)
    for tf in ('1h','4h'):
        assert 0<len(result['feed'][tf]['candidates'])<=2
        assert all(r['warnings'] for r in result['feed'][tf]['candidates'])
    assert 'resonance' not in result['feed'] and 'special' in result['feed']


def test_special_invalidated_chain_and_no_future_hourly_compression():
    f=special_frames()
    f['4h'].loc[94,'close']=90
    assert s.special('X',f) is None
    f=special_frames()
    f['1h']=launch(frame(),65)  # Only before the 4H retracement confirmation.
    item=s.special('X',f)
    assert item and item['completed_count']<5


def test_unknown_universe_fails_without_reusing_cache():
    writes=[]
    async def get(*a,**kw):return {'_data':{'symbols':[dict(symbol='NEWUSDT',baseAsset='NEW',quoteAsset='USDT',status='TRADING',contractType='PERPETUAL',underlyingType='UNKNOWN')]}}
    api=SimpleNamespace(safe_get=get,BINANCE_BASE='https://example.test',write_json=lambda *a:writes.append(a),SYMBOL_CACHE='unused')
    symbols,result=asyncio.run(runner.universe(api,None))
    assert symbols is None and result['stage']=='universe_classification' and not writes


def test_auxiliary_api_failure_stays_missing(monkeypatch):
    calls=[]
    async def get(*args,**kwargs):calls.append(args);return {'_error':'rate_limited'}
    async def sleep(*args):pass
    monkeypatch.setattr(runner.asyncio,'sleep',sleep)
    api=SimpleNamespace(safe_get=get,BINANCE_BASE='https://example.test')
    e=asyncio.run(runner.fetch_auxiliary(api,None,'X','1h',NOW))
    assert len(calls)==7 and e['status']=='unavailable' and len(e['errors'])==7
    assert e['cvd']['value'] is None and s.auxiliary_score(e)[0]==0


def test_insufficient_history_is_not_bearish_veto():
    result=s.trend(frame(48))
    assert not result['valid'] and not result['hard_exclude']


def test_weak_financial_data_cannot_change_technical_rank():
    frames={'1h':launch(frame()),'4h':frame(tf='4h',rise=.01),'1d':frame(tf='1d',rise=.01)}
    a,_=s.qualify('A','1h',frames)
    b=deepcopy(a);b['symbol']='B';b['types']=['type_2']
    a['ranking_key'].append(-999);b['ranking_key'].append(999)
    assert sorted([a,b],key=lambda r:r['ranking_key'],reverse=True)[0]['symbol']=='B'
    a['ranking_key'][0]+=1
    assert sorted([a,b],key=lambda r:r['ranking_key'],reverse=True)[0]['symbol']=='A'


def test_all_stale_boards_and_warning_hidden(tmp_path,monkeypatch):
    from scan_snapshot import ScanSnapshot
    monkeypatch.setattr('scan_freshness.time.time',lambda:(NOW+24*H)/1000)
    store=ScanSnapshot(tmp_path/'feed.json')
    store._feed={'strategy':s.VERSION,'freshness_version':'closed-bars-v2','coverage':{},
                 '1h':{'candidates':[{'symbol':'X'}]},'4h':{'candidates':[{'symbol':'X'}]},
                 'special':{'formal':[{'symbol':'X'}],'approaching':[{'symbol':'Y'}]},
                 'market_state':{'triggered':True}}
    feed=store.feed()
    assert feed['1h']['candidates']==feed['4h']['candidates']==feed['special']['formal']==feed['special']['approaching']==[]
    assert not feed['market_state']['triggered']
    store._executor.shutdown()


def test_summary_import_does_not_require_pandas():
    import subprocess,sys
    code="""
import sys
class NoPandas:
    def find_spec(self,fullname,path=None,target=None):
        if fullname == 'pandas': raise ImportError('MCP must not require pandas')
sys.meta_path.insert(0,NoPandas())
import scan_summary
assert 'pandas' not in sys.modules
"""
    subprocess.run([sys.executable,'-c',code],check=True)


def test_optional_btc_does_not_make_valid_v2_snapshot_stale():
    feed={'freshness_version':'closed-bars-v2','coverage':{tf:{'complete':True} for tf in ('1h','4h','1d')}}
    for tf in ('1h','4h','1d'):
        bar=expected_bar(tf,NOW)
        feed.update({f'latest_closed_{tf}_{k}':v for k,v in bar.items()})
        if tf!='1d':feed[tf]={'candidates':[{'candle':bar,'btc_candle':None}]}
    assert not freshness(feed,NOW)['stale']
