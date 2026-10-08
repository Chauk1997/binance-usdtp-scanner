import asyncio
from types import SimpleNamespace
import pytest
import scanner_latest as runner
import strategy_latest as s
from cvd_proxy import calculate_cvd_proxy
from scan_freshness import SCAN_TIME, DURATIONS, expected_bar, seconds_until_scan
from test_strategy_latest import frame, launch, NOW, warning_row
from datetime import datetime

@pytest.mark.parametrize('volume,previous,older,passes',[(219,100,100,False),(220,100,100,True),(300,100,400,False)])
def test_exact_requested_key_cases(volume,previous,older,passes):
    df=launch(frame());df.loc[:97,'volume']=older;df.loc[98,'volume']=previous;df.loc[99,'volume']=volume
    assert bool(s.key_at(df,99))==passes

@pytest.mark.parametrize('hour',[0,4,8,12,16,20])
def test_taipei_four_hour_plus_one_minute(hour):
    assert seconds_until_scan(datetime.fromisoformat(f'2026-09-27T{hour:02}:00:00+08:00'))==60
    assert seconds_until_scan(datetime.fromisoformat(f'2026-09-27T{hour:02}:00:59+08:00'))==1


def test_six_passes_stay_six_and_audit_excludes_forming(monkeypatch):
    names=['BTCUSDT']+[f'X{i}USDT' for i in range(5)]+['FAILUSDT']
    def bars(symbol,tf):
        df=frame(tf=tf,rise=.01) if tf=='1d' else frame(tf=tf)
        if symbol!='FAILUSDT' and tf!='1d': df=launch(df)
        rows=df.to_dict('records');span=DURATIONS[tf]
        rows.append({**rows[-1],'open_time':NOW,'close_time':NOW+span-1,'volume':9999999})
        return rows
    api=SimpleNamespace(interval_cache_is_current=lambda tf:True,symbol_cache_is_current=lambda *a:True,
        cache_file=lambda symbol,tf:(symbol,tf),read_json=lambda key:bars(*key))
    async def universe(*args):return names,{'total':len(names)}
    seen=[]
    async def aux(api,client,symbol,tf,cutoff):seen.append(symbol);return {}
    monkeypatch.setattr(runner,'universe',universe);monkeypatch.setattr(runner,'fetch_auxiliary',aux)
    token=SCAN_TIME.set(NOW+60000)
    try: result=asyncio.run(runner.build_legacy(api))
    finally: SCAN_TIME.reset(token)
    feed=result['feed']
    assert 'FAILUSDT' not in seen
    for tf in ('1h','4h'):
        assert len(feed[tf]['candidates'])==6
        for symbol in names:
            a=feed['candle_audit'][symbol][tf]
            assert a['expected']==a['available']==a['scan']==expected_bar(tf,NOW)
    assert feed['scanned']==7 and feed['missing']==0


def test_proxy_requires_complete_window_and_is_formal_auxiliary():
    proxy=calculate_cvd_proxy([dict(open_time=0,close_time=99,volume=10,taker_buy_base=2)],0,100)
    evidence={'cvd_proxy':proxy,'window_start':0,'window_end':100,'taker_buy_sell_ratio':.5}
    assert proxy['label']=='CVD Proxy' and proxy['value']==-6
    assert s.valid_cvd_proxy(evidence) and s.auxiliary_score(evidence)[0]==-2
    proxy['complete']=False
    assert not s.valid_cvd_proxy(evidence)
