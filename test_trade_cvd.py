import asyncio
from types import SimpleNamespace
import pytest
from trade_cvd import calculate_cvd
from strategy_latest import valid_cvd, market_warning


def trade(i,t,q='1',maker=False):
    return dict(a=i,T=t,q=q,m=maker)


@pytest.fixture
def run(monkeypatch):
    async def sleep(*args):pass
    monkeypatch.setattr('trade_cvd.asyncio.sleep',sleep)
    def execute(rows,start=0,end=3600000,**kwargs):
        calls=[]
        async def get(client,url,params):
            calls.append(params)
            return {'_data':[r for r in rows if params['startTime']<=r['T']<=params['endTime']][:1000]}
        api=SimpleNamespace(safe_get=get,BINANCE_BASE='https://example.test')
        return asyncio.run(calculate_cvd(api,None,'TEST',start,end,**kwargs)),calls
    return execute


def test_exact_sign_decimal_and_half_open_boundary(run):
    r,calls=run([trade(0,0,'.3'),trade(1,1,'.1',True),trade(2,3599999,'.1',True),trade(3,3600000,'999')])
    assert r['value_decimal']=='0.1' and r['aggregate_trade_count']==3
    assert r['complete'] and r['unit']=='base_asset_quantity'
    assert valid_cvd({'cvd':r,'window_start':0,'window_end':3600000})


def test_saturated_slice_subdivision_no_duplicates(run):
    rows=[trade(i,i) for i in range(2100)]
    r,calls=run(rows,end=3000)
    assert r['complete'] and r['value']==2100 and r['aggregate_trade_count']==2100
    assert len(calls)>1 and all(p['endTime']-p['startTime']<3600000 for p in calls)


def test_four_hour_window_and_empty_zero(run):
    r,calls=run([],end=14400000)
    assert r['complete'] and r['value']==0 and len(calls)==4


@pytest.mark.parametrize('rows,reason',[
    ([trade(1,1),trade(3,2)],'trade_id_gap_or_time_regression'),
    ([trade(1,1),trade(1,2)],'duplicate_trade_id'),
    ([trade(1,1,'NaN')],'invalid_trade_quantity_or_time'),
    ([trade(1,1,'-1')],'invalid_trade_quantity_or_time'),
    ([trade(1,1,maker='false')],'invalid_trade_fields'),
])
def test_incomplete_never_used(run,rows,reason):
    r,_=run(rows)
    assert r['reason']==reason and r['value'] is None and not r['reliable']


def test_budget_and_saturated_millisecond(run):
    r,_=run([trade(i,1) for i in range(1000)],end=2,max_requests=1)
    assert r['reason']=='request_budget_exhausted' and r['value'] is None
    r,_=run([trade(i,1) for i in range(1000)],start=1,end=2)
    assert r['reason']=='saturated_millisecond'


def test_calculated_cvd_drives_breadth_warning(run):
    r,_=run([trade(1,1,'5',True)])
    rows=[{'symbol':str(i),'auxiliary':{'cvd':r,'window_start':0,'window_end':3600000,'taker_buy_sell_ratio':.8}} for i in range(6)]
    assert market_warning({'1h':rows})['triggered']
    r['complete']=False
    assert not market_warning({'1h':rows})['triggered']
