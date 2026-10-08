"""Boundary and delayed-exchange regressions for the active scan pipeline."""
import asyncio
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
import pytest
import main
import scanner_latest
import strategy_latest
import scan_freshness as f
from scan_snapshot import ScanSnapshot
from scan_summary import compact_scan_feed
from scanner_contract import VERSION
from test_strategy_latest import frame, launch


def ms(text):
    return int(datetime.fromisoformat(text).timestamp()*1000)


BOUNDARIES = [('1h', f'2026-09-27T{h:02}:00:00+08:00') for h in (0,1,8,23)] + [
    ('4h', f'2026-09-27T{h:02}:00:00+08:00') for h in (0,4,8,12,16,20)] + [
    ('1d','2026-09-27T08:00:00+08:00')]


@pytest.mark.parametrize('tf,text', BOUNDARIES)
@pytest.mark.parametrize('offset',[-1000,-1,0,1,3000,5000,60000,180000])
def test_all_boundaries_and_forming(tf,text,offset):
    boundary=ms(text);span=f.DURATIONS[tf];now=boundary+offset
    expected=f.expected_bar(tf,now)
    assert expected['close_time']==boundary-1-(span if offset<0 else 0)
    assert expected['open_time']==expected['close_time']+1-span
    closed={'open_time':boundary-span,'close_time':boundary-1}
    forming={'open_time':boundary,'close_time':boundary+span-1}
    assert f.closed_bars([closed,forming],now)==([] if offset<0 else [closed])
    assert f.expected_bar(tf,ms(datetime.fromtimestamp(now/1000,f.TAIPEI).astimezone(__import__('datetime').timezone.utc).isoformat()))==expected


def feed_at(now):
    data={'freshness_version':'closed-bars-v3','strategy':VERSION,'status':'complete',
          'scanner':'V5.4_CHATGPT_FEED','generated_at_utc':'2026-09-27T00:00:00+00:00',
          'coverage':{},'special':{'formal':[{'symbol':'X'}]},'market_state':{'triggered':True}}
    for tf in f.DURATIONS:
        bar=f.expected_bar(tf,now)
        data.update({f'latest_closed_{tf}_{k}':v for k,v in bar.items()})
        data['coverage'][tf]={'complete':True}
        if tf!='1d':data[tf]={'candidates':[{'symbol':'X','candle':bar,'btc_candle':bar}]}
    return data


@pytest.mark.parametrize('text',['2026-09-27T01:00:00+08:00','2026-09-27T04:00:00+08:00','2026-09-27T08:00:00+08:00'])
def test_snapshot_pending_then_stale_never_old_board_fresh(text,tmp_path,monkeypatch):
    boundary=ms(text);old=feed_at(boundary-1)
    store=ScanSnapshot(tmp_path/'snapshot.json');store._feed=old
    for elapsed in (0,1,5000,60000,149999,150000,180000):
        monkeypatch.setattr(f,'live_now_ms',lambda:boundary+elapsed)
        view=store.feed()
        assert not view['feed_ready'] and not view['fresh_for_1h']
        assert view['status']==('pending' if elapsed<150000 else 'stale')
        assert not view['1h']['candidates'] and not view['special']['formal']
        assert not view['market_state']['triggered']
        summary=compact_scan_feed(view)
        assert not summary['1h']['candidates']
        assert summary['special']['status']==view['status']
        assert old['1h']['candidates']  # Read projection cannot mutate persisted snapshot.
    store._feed=feed_at(boundary)
    assert store.feed()['feed_ready']
    store._executor.shutdown()


def test_grace_does_not_hide_older_missing_or_malformed_evidence():
    boundary=ms('2026-09-27T08:00:00+08:00')
    old=feed_at(boundary-1)
    for bad in ('older','coverage','row'):
        data=deepcopy(old)
        if bad=='older':data['latest_closed_1h_open_time']-=f.DURATIONS['1h']
        if bad=='coverage':data['coverage']['1d']['complete']=False
        if bad=='row':data['1h']['candidates'][0]['candle']=None
        assert f.freshness(data,boundary)['stale']


def test_cache_proof_without_next_forming_candle():
    boundary=ms('2026-09-27T12:00:00+08:00')
    bar=f.expected_bar('1h',boundary)
    for fetched in (boundary-1,boundary,boundary+59999):
        assert not f.confirmed_cache([{**bar,'fetched_after_ms':fetched}],'1h',boundary+61000)
    assert f.confirmed_cache([{**bar,'fetched_after_ms':boundary+60000}],'1h',boundary+61000)


def test_delayed_api_then_final_only_response_refreshes_cache(tmp_path,monkeypatch):
    boundary=ms('2026-09-27T12:00:00+08:00');now=boundary+61000
    bar=f.expected_bar('1h',boundary)
    prior={k:v-f.DURATIONS['1h'] for k,v in bar.items()}
    def raw(b):return [b['open_time'],'1','2','1','2','220',b['close_time'],'440',4,'120','240','0']
    disk=[{**main.clean_kline(raw(bar)),'volume':'1','fetched_after_ms':boundary-1000}]
    responses=iter([[raw(prior)],[raw(bar)]])
    async def get(*args,**kw):return {'_data':next(responses)}
    monkeypatch.setattr(main,'safe_get',get)
    monkeypatch.setattr(main,'live_now_ms',lambda:now)
    monkeypatch.setattr(main,'read_json',lambda path:deepcopy(disk))
    monkeypatch.setattr(main,'write_json',lambda path,value:disk.__setitem__(slice(None),value))
    token=f.SCAN_TIME.set(now)
    try:
        asyncio.run(main.update_one_kline_cache(None,'X','1h',asyncio.Semaphore(1)))
        assert not main.symbol_cache_is_current('X','1h')
        asyncio.run(main.update_one_kline_cache(None,'X','1h',asyncio.Semaphore(1)))
        assert main.symbol_cache_is_current('X','1h')
        assert disk[-1]['volume']=='220'  # pre-close partial value was overwritten.
        # No next forming candle was returned or needed.
        assert disk[-1]['close_time']==boundary-1
    finally:f.SCAN_TIME.reset(token)


def test_server_clock_not_host_and_explicit_boundary_wait(monkeypatch):
    boundary=ms('2026-09-27T08:00:00+08:00');mono=[100.0];waits=[];captured=[]
    monkeypatch.setattr(f,'_CLOCK',None)
    monkeypatch.setattr(f.time,'monotonic',lambda:mono[0])
    monkeypatch.setattr(f.time,'time',lambda:boundary/1000+7200)
    async def get(*args,**kw):return {'_data':{'serverTime':boundary}}
    async def sleep(seconds):waits.append(seconds);mono[0]+=seconds
    async def build(api):captured.append(f.scan_now_ms());return {'status':'complete'}
    monkeypatch.setattr(main,'safe_get',get)
    monkeypatch.setattr(main.asyncio,'sleep',sleep)
    monkeypatch.setattr(scanner_latest,'build',build)
    assert asyncio.run(main._build_complete_snapshot())['status']=='complete'
    assert waits==[60] and captured==[boundary+60000]
    assert f.SCAN_TIME.get() is None
    async def unavailable(*args,**kw):return {'_error':'timeout'}
    monkeypatch.setattr(main,'safe_get',unavailable)
    assert asyncio.run(main._build_complete_snapshot())['stage']=='server_clock'
    assert len(captured)==1


def test_zero_prior_volume_and_unaligned_history():
    df=launch(frame());df.loc[:98,'volume']=0
    key=strategy_latest.key_at(df,99)
    assert key and key['volume_vs_ma24'] is None
    bars=frame().to_dict('records')
    for b in bars:b['open_time']+=1;b['close_time']+=1
    assert strategy_latest.prepare(bars,'1h',bars[-1]['close_time']+1)[1]=='unaligned_bar_time'


@pytest.mark.parametrize('delayed_forever',[False,True])
@pytest.mark.parametrize('delayed_tf',['1h','4h','1d'])
def test_pipeline_retries_delayed_visibility_without_partial_publish(monkeypatch,delayed_forever,delayed_tf):
    from test_strategy_latest import NOW
    frames={tf:frame(tf=tf,rise=.01).to_dict('records') for tf in f.DURATIONS}
    attempts=[];waits=[]
    def current(tf):return tf!=delayed_tf or (len(attempts)>=2 and not delayed_forever)
    async def update(tf):attempts.append(tf);return {'status':'complete'}
    async def universe(*args):return ['BTCUSDT'],{'total':1}
    async def sleep(seconds):waits.append(seconds)
    api=SimpleNamespace(BINANCE_BASE='https://example.test',interval_cache_is_current=current,
                        run_incremental_update=update,symbol_cache_is_current=lambda s,tf:current(tf),
                        cache_file=lambda s,tf:tf,read_json=lambda tf:frames[tf])
    monkeypatch.setattr(scanner_latest,'universe',universe)
    monkeypatch.setattr(scanner_latest.asyncio,'sleep',sleep)
    token=f.SCAN_TIME.set(NOW)
    try:result=asyncio.run(scanner_latest.build_legacy(api))
    finally:f.SCAN_TIME.reset(token)
    assert attempts==[delayed_tf]*(4 if delayed_forever else 2)
    assert waits==([1,2,4] if delayed_forever else [1])
    if delayed_forever:
        assert result['status']=='stopped' and 'feed' not in result
        assert result['missing_symbols'][delayed_tf]==['BTCUSDT']
        audit=result['candle_audit']['BTCUSDT'][delayed_tf]
        assert audit['expected']==audit['available'] and audit['scan'] is None
    else:assert result['status']=='complete'


def test_daily_boundary_is_utc_not_taipei_midnight():
    midnight=ms('2026-09-27T00:00:00+08:00')
    assert f.expected_bar('1d',midnight)==f.expected_bar('1d',midnight-1)
    assert f.expected_bar('1d',midnight)['close_time']==ms('2026-09-26T08:00:00+08:00')-1
