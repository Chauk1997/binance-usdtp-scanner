import copy
from dataclasses import replace
import pandas as pd
import pytest
import phase16 as p
from phase16_runner import due_timeframes, visible
from scan_freshness import DURATIONS
CFG=replace(p.CONFIG,warmup_bars=50)

def frame(n=70,tf='1h'):
    span=DURATIONS[tf];base=300*86400000
    return pd.DataFrame([dict(open_time=base+i*span,close_time=base+(i+1)*span-1,open=100.,high=101.,low=99.,close=100.5,volume=10.,ema15=100.,sma30=100.1,sma45=100.2,atr14=2.,atr_stable=2.,spread=.1,d45=.05,volume_sma24=10.) for i in range(n)])

def htf(end,tf):
    span=DURATIONS[tf];boundary=(end+1)//span*span
    d=frame(70,tf);d.open_time=[boundary-(70-i)*span for i in range(70)];d.close_time=d.open_time+span-1
    d['ema15']=103.;d['sma30']=102.;d['sma45']=101.
    return d

def setup(n=60,key_index=58):
    d=frame(n);d.loc[:49,'ema15']=103.;d.loc[:49,'sma30']=102.;d.loc[:49,'sma45']=101.
    if key_index is not None:d.loc[key_index,['open','close','high','low','volume','volume_sma24']]=[100.1,103.,103.1,99.,22.,10.5]
    t=int(d.close_time.iloc[-1]);return {'1h':d,'4h':htf(t,'4h'),'1d':htf(t,'1d')},t+60001

def test_wilder_inclusive_sma():
    d=frame(220);bars=d[['open_time','close_time','open','high','low','close','volume']].to_dict('records');bars[-1]['volume']=240
    prepared,error=p.prepare(bars,'1h',int(d.close_time.iloc[-1])+1)
    assert error is None and prepared.atr14.iloc[13]==2 and prepared.atr14.iloc[-1]==2
    assert prepared.volume_sma24.iloc[-1]==(23*10+240)/24

@pytest.mark.parametrize('mutation,expected',[('gap','gapped_or_duplicate_history'),('forming',None),('bad','invalid_ohlc')])
def test_history(mutation,expected):
    d=frame(220);bars=d[['open_time','close_time','open','high','low','close','volume']].to_dict('records');cutoff=int(d.close_time.iloc[-1])+1
    if mutation=='gap':bars.pop(80)
    if mutation=='bad':bars[-1]['high']=90
    if mutation=='forming':bars.append(dict(bars[-1],open_time=cutoff,close_time=cutoff+DURATIONS['1h']-1))
    assert p.prepare(bars,'1h',cutoff)[1]==expected

def test_confirmation_before_key_restart_idempotence(tmp_path):
    f,t=setup();s=p.Store(tmp_path/'s.db')
    with s.db:rows,c=p.process(s,'X','1h',f,None,t,CFG)
    assert c['k1']==1 and c['confirmed']==1 and len(rows)==1
    r=rows[0];assert r['confirmed_time']<r['key_time'] and r['key']['type']=='B' and r['duration']==8
    with s.db:again,c=p.process(s,'X','1h',f,None,t,CFG)
    assert c['created']==0 and len(again)==1 and again[0]['signal_id']==r['signal_id']
    s.db.close();s=p.Store(tmp_path/'s.db');assert len(s.all('signals'))==1

def test_seven_observations_early_only(tmp_path):
    f,t=setup(key_index=57);s=p.Store(tmp_path/'s.db')
    with s.db:rows,c=p.process(s,'X','1h',f,None,t,CFG)
    assert not rows and c['early']==1 and len(s.all('research'))==1

def test_no_retroactive_htf(tmp_path):
    f,t=setup();f['1d']['ema15']=99.;s=p.Store(tmp_path/'s.db')
    with s.db:rows,c=p.process(s,'X','1h',f,None,t,CFG)
    assert not rows and c['htf_rejected']==1
    f['1d']['ema15']=103.
    with s.db:rows,c=p.process(s,'X','1h',f,None,t,CFG)
    assert not rows

def test_mirror_invalidation(tmp_path):
    f,t=setup(n=63);f['1h'].loc[59,['open','close','high','low','volume','volume_sma24']]=[102.,98.,105.,97.,50.,12.]
    s=p.Store(tmp_path/'s.db')
    with s.db:rows,c=p.process(s,'X','1h',f,None,t,CFG)
    assert not rows and c['invalidated']==1 and s.all('signals')[0]['status']=='INVALIDATED'

def test_keyrun_ab_first_only(tmp_path):
    f,t=setup();f['1h'].loc[59,['open','close','high','low','volume','volume_sma24']]=[103.,107.,108.,100.,49.,13.]
    s=p.Store(tmp_path/'s.db')
    with s.db:rows,c=p.process(s,'X','1h',f,None,t,CFG)
    assert len(rows)==1 and c['created']==1
    run=s.get('states','X:1h')['run'];assert run['run_active'] and run['last_key_time']>rows[0]['key_time']
    assert rows[0]['key']['ohlcv']['close']==103.

def test_unknown_lexicographic(tmp_path):
    f,t=setup();s=p.Store(tmp_path/'s.db')
    with s.db:r=p.process(s,'X','1h',f,None,t,CFG)[0][0]
    a=copy.deepcopy(r);b=copy.deepcopy(r);c=copy.deepcopy(r)
    a['btc_resilience']=None;b['btc_resilience']=.1;c['btc_resilience']=-.1;b['key']['quality']-=1
    assert p.rank_key(c)>p.rank_key(a)>p.rank_key(b) and p.known(-100)>p.known(None)

def test_due_timeframes(tmp_path):
    s=p.Store(tmp_path/'s.db');t=16*DURATIONS['1h']+60000
    assert due_timeframes(t,s)==['1h','4h']
    with s.db:
        for tf in ('1h','4h'):s.put('boards',tf,dict(boundary_ms=t//DURATIONS[tf]*DURATIONS[tf]))
    assert due_timeframes(t+3600000,s)==['1h'] and due_timeframes(t+14400000,s)==['1h','4h'] and due_timeframes(t,s)==[]

def test_ttl_original_time():
    board=dict(scan_cutoff_ms=100,candidates=[dict(key_time=100)])
    feed={'1h':copy.deepcopy(board),'4h':copy.deepcopy(board)}
    assert visible(feed,100+86400000)['4h']['candidates'] and not visible(feed,101+86400000)['4h']['candidates']
    assert visible(feed,200)['4h']['scan_cutoff_ms']==100

@pytest.mark.parametrize('turnover,tick,expected',[(1e5,.001,'高'),(1e8,.01,'低'),(5e6,.01,'中'),(None,.01,'UNKNOWN'),(1e8,1,'高')])
def test_liquidity(turnover,tick,expected):assert p.liquidity(turnover,tick,100)==expected

def test_rollback_notify_once(tmp_path):
    f,t=setup();s=p.Store(tmp_path/'s.db')
    with pytest.raises(RuntimeError):
        with s.db:
            p.process(s,'X','1h',f,None,t,CFG);raise RuntimeError('failure')
    assert not s.all('signals') and s.get('states','X:1h') is None
    with s.db:
        rows,_=p.process(s,'X','1h',f,None,t,CFG)
        assert s.mark_notified(rows[0]['signal_id'],t) and not s.mark_notified(rows[0]['signal_id'],t)
    s.db.close();s=p.Store(tmp_path/'s.db')
    with s.db:assert not s.mark_notified(rows[0]['signal_id'],t)

def test_htf_future():
    f,t=setup();event=int(f['1h'].iloc[58].close_time);f['1d']['ema15']=99.
    d=f['1d'];future=d.iloc[-1].copy();future.close_time+=86400000;future.open_time+=86400000;future.ema15=103.
    f['1d']=pd.concat([d,pd.DataFrame([future])],ignore_index=True)
    assert not p.htf_at(f,'1h',event)['valid']

def test_24_hourly_six_four_hourly(tmp_path):
    s=p.Store(tmp_path/'s.db');count={'1h':0,'4h':0}
    for hour in range(24):
        t=hour*DURATIONS['1h']+60000
        for tf in due_timeframes(t,s):
            count[tf]+=1
            with s.db:s.put('boards',tf,dict(boundary_ms=t//DURATIONS[tf]*DURATIONS[tf]))
    assert count=={'1h':24,'4h':6}

def test_range_grades_and_no_volume_bonus():
    d=frame();i=58
    d.loc[i,['open','close','high','low','volume','volume_sma24']]=[100.,103.,104.,99.,22.,11.]
    first=p.key_at(d,i);assert first and first['range_grade']==1
    d.loc[i,'volume']=1000
    second=p.key_at(d,i);assert second['quality']==first['quality'] and second['range_grade']==first['range_grade']
    d.loc[i-1,'volume']=0;assert p.key_at(d,i) is None

def test_resistance_right_confirmation_no_future():
    d=frame(100);d['atr14']=2.;d['high']=101.;d['low']=99.
    d.loc[[30,40,79],'high']=105.
    zones=p.resistance(d,82)
    assert zones and zones[0]['tests']==2
    assert all(t<int(d.iloc[82].close_time) for t in zones[0]['confirmed_times'])
    d.loc[45,'close']=107.;assert not p.resistance(d,82)

def test_runner_keeps_fourhour_board(monkeypatch,tmp_path):
    import asyncio
    from types import SimpleNamespace
    import scanner_latest
    import phase16_runner as runner
    from scan_freshness import SCAN_TIME, expected_bar
    t=316*86400000+16*3600000+60000
    async def universe(*args):return ['BTCUSDT'],dict(total=1)
    async def safe_get(client,url,params=None):return {'_data':{'symbols':[]} if url.endswith('exchangeInfo') else []}
    monkeypatch.setattr(scanner_latest,'universe',universe)
    def prepare(bars,tf,cutoff):
        d=frame();end=expected_bar(tf,cutoff)['close_time'];d.close_time=[end-(len(d)-1-i)*DURATIONS[tf] for i in range(len(d))];d.open_time=d.close_time-DURATIONS[tf]+1
        return d,None
    monkeypatch.setattr(p,'prepare',prepare)
    calls=[]
    def process(store,symbol,tf,frames,btc,cutoff):calls.append(tf);return [],dict(created=0)
    monkeypatch.setattr(p,'process',process)
    api=SimpleNamespace(CACHE_DIR=tmp_path,BINANCE_BASE='https://example.invalid',RATE_LIMITED=False,
        safe_get=safe_get,interval_cache_is_current=lambda tf:True,
        symbol_cache_is_current=lambda symbol,tf:True,cache_file=lambda s,tf:tf,read_json=lambda path:[])
    token=SCAN_TIME.set(t)
    try:first=asyncio.run(runner.build(api))
    finally:SCAN_TIME.reset(token)
    assert calls==['1h','4h']
    before=copy.deepcopy(first['feed']['4h']);calls.clear()
    token=SCAN_TIME.set(t+3600000)
    try:second=asyncio.run(runner.build(api))
    finally:SCAN_TIME.reset(token)
    assert calls==['1h']
    assert second['feed']['4h']['scan_cutoff_ms']==before['scan_cutoff_ms']
    assert second['feed']['4h']['candidates']==before['candidates']
    state=p.Store(tmp_path/'phase16.sqlite3')
    import json
    audit=json.loads(state.db.execute('SELECT data FROM audit ORDER BY id DESC LIMIT 1').fetchone()[0])
    assert audit['feed']['4h']['scan_cutoff_ms']==before['scan_cutoff_ms']
    assert 'ranking_pool' in audit and list(audit['ranking_pool'])==['1h']

def test_phase16_summary_contract():
    from scan_summary import compact_scan_feed
    feed=dict(strategy=p.VERSION,diagnostics={'secret':'large'},research=[],candle_audit={},
              **{'1h':{'candidates':[],'scan_cutoff_ms':123},'4h':{'candidates':[],'scan_cutoff_ms':100}})
    result=compact_scan_feed(feed)
    assert result['strategy']==p.VERSION and result['4h']['scan_cutoff_ms']==100
    assert 'diagnostics' not in result and 'research' not in result

def test_all_crypto_universe_not_liquidity_or_stablecoin_filtered():
    from scanner_latest import select_phase16_universe
    def row(symbol,kind='COIN',sub=()):return dict(symbol=symbol,contractType='PERPETUAL',status='TRADING',quoteAsset='USDT',marginAsset='USDT',underlyingType=kind,underlyingSubType=list(sub))
    selected,excluded,unknown=select_phase16_universe([row('1000PEPEUSDT'),row('USDCUSDT'),row('NEWUSDT'),row('GOLDUSDT',sub=['COMMODITY']),row('EQUITYUSDT',sub=['STOCK']),row('FXUSDT',sub=['FOREX']),row('UNKNOWNUSDT','UNKNOWN')])
    assert selected==['1000PEPEUSDT','NEWUSDT','USDCUSDT'] and len(excluded)==3 and unknown==['UNKNOWNUSDT']

def test_phase16_default_and_legacy_dispatch(monkeypatch):
    import asyncio
    import scanner_latest as runner
    import phase16_runner
    async def current(api):return {'version':'phase16'}
    async def legacy(api):return {'version':'legacy'}
    monkeypatch.setattr(phase16_runner,'build',current);monkeypatch.setattr(runner,'build_legacy',legacy)
    monkeypatch.delenv('SCANNER_STRATEGY',raising=False)
    assert asyncio.run(runner.build(None))['version']=='phase16'
    monkeypatch.setenv('SCANNER_STRATEGY','legacy')
    assert asyncio.run(runner.build(None))['version']=='legacy'

@pytest.mark.parametrize('values,kind', [([102.,103.,104.,98.],'A'),([100.1,103.,104.,98.],'B'),([99.,100.15,103.,97.],'C'),([99.,103.,104.,98.],'D')])
def test_abcd_mutually_exclusive(values,kind):
    d=frame();d.loc[58,['open','close','high','low']]=values;d.loc[58,'volume']=22.;d.loc[58,'volume_sma24']=11.
    assert p.key_at(d,58)['type']==kind

def test_three_consecutive_expansion_and_sma45_threshold():
    d=frame(20)
    for i in range(20):
        d.loc[i,['ema15','sma30','sma45','atr14']]=[110+i*2,105+i,100.,2.]
    assert p.expanding(d,19,1)
    d.loc[18,'ema15']=d.loc[17,'ema15'];assert not p.expanding(d,19,1)

def test_only_next_three_bars_can_invalidate(tmp_path):
    f,t=setup(n=64);f['1h'].loc[62,['open','close','high','low','volume','volume_sma24']]=[102.,98.,105.,97.,50.,12.]
    s=p.Store(tmp_path/'s.db')
    with s.db:rows,c=p.process(s,'X','1h',f,None,t,CFG)
    assert len(rows)==1 and c['invalidated']==0
    # Expiration is a lifecycle change, not a technical invalidation.
    with s.db:rows,c=p.process(s,'X','1h',f,None,t+86400000,CFG)
    assert not rows and s.all('signals')[0]['status']=='EXPIRED'

def test_current_htf_exit_then_reentry(tmp_path):
    f,t=setup();s=p.Store(tmp_path/'s.db')
    with s.db:rows,_=p.process(s,'X','1h',f,None,t,CFG)
    sid=rows[0]['signal_id'];original=copy.deepcopy(rows[0]['event_htf'])
    f['1d']['ema15']=99.
    with s.db:rows,_=p.process(s,'X','1h',f,None,t,CFG)
    assert not rows and s.get('signals',sid)['status']=='ACTIVE'
    f['1d']['ema15']=103.
    with s.db:rows,_=p.process(s,'X','1h',f,None,t,CFG)
    assert rows[0]['signal_id']==sid and rows[0]['event_htf']==original

def test_recursive_indicators_survive_sliding_cache(tmp_path):
    import math
    raw=frame(300)
    for i in range(len(raw)):
        price=100+math.sin(i/11)
        raw.loc[i,['open','high','low','close']]=[price,price+1,price-1,price+.2]
    def frames_for(end,start=0):
        bars=raw.iloc[start:end][['open_time','close_time','open','high','low','close','volume']].to_dict('records')
        cutoff=int(raw.iloc[end-1].close_time)+1
        df,error=p.prepare(bars,'1h',cutoff);assert error is None
        return {'1h':df,'4h':htf(cutoff-1,'4h'),'1d':htf(cutoff-1,'1d')},cutoff
    s=p.Store(tmp_path/'s.db');first,t=frames_for(220)
    with s.db:p.process(s,'X','1h',first,None,t)
    full,t=frames_for(300);sliding,t=frames_for(300,80)
    with s.db:p.process(s,'X','1h',sliding,None,t)
    assert sliding['1h'].ema15.iloc[-1]==pytest.approx(full['1h'].ema15.iloc[-1],abs=1e-12)
    assert sliding['1h'].atr14.iloc[-1]==pytest.approx(full['1h'].atr14.iloc[-1],abs=1e-12)
    assert s.get('states','X:1h')['cursor']==int(raw.iloc[-1].close_time)

def test_cd_key_expansion_affects_later_structure_quality(tmp_path):
    f,t=setup(n=62);d=f['1h']
    for i in (58,60):d.loc[i,['open','close','high','low','volume','volume_sma24']]=[99.,100.15,103.,97.,22.,11.]
    d.loc[58,'spread']=1.6
    s=p.Store(tmp_path/'s.db')
    with s.db:rows,_=p.process(s,'X','1h',f,None,t,CFG)
    assert len(rows)==2
    rows.sort(key=lambda r:r['key_time'])
    assert rows[0]['key']['type']=='C' and rows[0]['consolidation_quality']==2
    assert rows[1]['expansion_cycles']==1 and rows[1]['consolidation_quality']==1

def test_one_asset_per_board_preserves_best_event():
    from phase16_runner import asset_candidates
    rows=[dict(symbol='X',signal_id='later',key_time=2,ranking_key=[p.known(2),p.known(None)]),
          dict(symbol='X',signal_id='earlier',key_time=1,ranking_key=[p.known(2),p.known(None)]),
          dict(symbol='Y',signal_id='other',key_time=3,ranking_key=[p.known(1),p.known(None)])]
    selected=asset_candidates(rows)
    assert len(selected)==2 and selected[0]['signal_id']=='earlier'
    rows[0]['ranking_key'][0]=p.known(3)
    assert asset_candidates(rows)[0]['signal_id']=='later' and len(rows)==3

def test_mcp_gateway_accepts_phase16_and_configurable_backend(monkeypatch):
    pytest.importorskip('mcp.server.mcpserver')
    import httpx
    import mcp_server as gateway
    payload={'strategy':p.VERSION,'status':'complete','feed_ready':True,'partial_scan':True,
             '1h':{'candidates':[{'symbol':'XUSDT','signal_id':'one','key_time':1}]},
             '4h':{'candidates':[]},'coverage':{}}
    def handler(request):
        assert str(request.url)=='http://local.test/scan/feed'
        return httpx.Response(200,json=payload)
    real_client=httpx.Client
    monkeypatch.setattr(gateway,'SCANNER_BASE_URL','http://local.test')
    monkeypatch.setattr(gateway.httpx,'Client',lambda **kwargs:real_client(transport=httpx.MockTransport(handler),**kwargs))
    result=gateway.get_scan_feed()
    assert result['strategy']==p.VERSION and result['feed_ready'] and result['partial_scan']
    assert result['1h']['candidates'][0]['signal_id']=='one'
