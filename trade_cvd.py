"""Window-anchored CVD calculated from exchange aggregate market trades.

Never estimate direction from candles or a taker ratio. A saturated time slice
is subdivided until every leaf is complete; failed/budgeted windows stay null.
"""
import asyncio
from decimal import Decimal, InvalidOperation
import math
import httpx

SOURCE = 'Binance USD-M /fapi/v1/aggTrades'


async def calculate_cvd(api, client, symbol, start, end, *, max_requests=128):
    result = dict(value=None, kind='calculated_from_trades', reliable=False,
                  complete=False, source=SOURCE, window_start=start, window_end=end,
                  anchor='zero_at_window_start', unit='base_asset_quantity',
                  method='sum(+q if m=false else -q)', requests=0,
                  scope='aggregate market trades including RPI; excludes ADL/insurance fund trades')
    if not isinstance(start, int) or not isinstance(end, int) or end <= start:
        return {**result, 'reason':'invalid_window'}
    trades = {}
    # API endTime is inclusive; internal windows are half-open.
    pending = [(t,min(t+3600000,end)) for t in range(start,end,3600000)]
    try:
        while pending:
            left,right = pending.pop()
            if result['requests'] >= max_requests:
                return {**result,'reason':'request_budget_exhausted'}
            result['requests'] += 1
            response = await api.safe_get(client,api.BINANCE_BASE+'/fapi/v1/aggTrades',
                                         params=dict(symbol=symbol,startTime=left,endTime=right-1,limit=1000))
            if '_error' in response:
                return {**result,'reason':str(response['_error'])}
            rows=response.get('_data')
            if not isinstance(rows,list) or len(rows)>1000:
                return {**result,'reason':'invalid_trade_response'}
            if len(rows)==1000:
                if right-left<=1:
                    return {**result,'reason':'saturated_millisecond'}
                middle=(left+right)//2
                pending.extend([(middle,right),(left,middle)])
                await asyncio.sleep(.35)
                continue  # Parent's truncated rows are not added a second time.
            for r in rows:
                ident,timestamp=r['a'],r['T']
                if type(ident) is not int or type(timestamp) is not int or type(r['m']) is not bool:
                    return {**result,'reason':'invalid_trade_fields'}
                quantity=Decimal(str(r['q']))
                if not quantity.is_finite() or quantity<0 or not left<=timestamp<right:
                    return {**result,'reason':'invalid_trade_quantity_or_time'}
                trade=(timestamp,quantity,r['m'])
                if ident in trades:
                    return {**result,'reason':'duplicate_trade_id'}
                trades[ident]=trade
            await asyncio.sleep(.35)
        ordered=sorted(trades.items())
        if any(b[0]!=a[0]+1 or b[1][0]<a[1][0] for a,b in zip(ordered,ordered[1:])):
            return {**result,'reason':'trade_id_gap_or_time_regression'}
        buy=sum((v[1] for _,v in ordered if not v[2]),Decimal(0))
        sell=sum((v[1] for _,v in ordered if v[2]),Decimal(0))
        value=float(buy-sell)
        if not all(math.isfinite(v) for v in (value,float(buy),float(sell))):
            return {**result,'reason':'numeric_overflow'}
        return {**result,'value':value,'value_decimal':str(buy-sell),'buy_volume':float(buy),
                'sell_volume':float(sell),'aggregate_trade_count':len(trades),
                'first_trade_id':ordered[0][0] if ordered else None,
                'last_trade_id':ordered[-1][0] if ordered else None,
                'reliable':True,'complete':True,'reason':None}
    except (httpx.HTTPError,KeyError,TypeError,ValueError,InvalidOperation,OverflowError) as exc:
        return {**result,'reason':'trade_data_error:'+type(exc).__name__}


def calculate_cvd_from_exchange_volume(bars, start, end):
    """Exact window delta from exchange-reported total and taker-buy volumes.

These are executed-volume aggregates, not a candle-direction estimate or a
buy/sell ratio proxy. The window's zero-anchored cumulative endpoint is B-S.
"""
    result=dict(value=None,kind='calculated_from_exchange_volume',reliable=False,
                complete=False,source='Binance USD-M /fapi/v1/klines volume+taker_buy_base',
                window_start=start,window_end=end,anchor='zero_at_window_start',
                unit='base_asset_quantity',method='sum(2*taker_buy_base-volume)',
                requests=0)
    try:
        if type(start) is not int or type(end) is not int or end<=start:
            return {**result,'reason':'invalid_window'}
        selected=sorted((b for b in (bars or []) if start<=int(b['open_time'])<end),key=lambda b:int(b['open_time']))
        cursor=start;buy=Decimal(0);total=Decimal(0)
        for row in selected:
            opening,closing=int(row['open_time']),int(row['close_time'])
            if opening!=cursor or closing<opening or closing>=end:
                return {**result,'reason':'incomplete_or_duplicate_window'}
            v,b=Decimal(str(row['volume'])),Decimal(str(row['taker_buy_base']))
            if not v.is_finite() or not b.is_finite() or v<0 or not 0<=b<=v:
                return {**result,'reason':'invalid_executed_volume'}
            total+=v;buy+=b;cursor=closing+1
        if cursor!=end:
            return {**result,'reason':'incomplete_window'}
        sell=total-buy;delta=buy-sell
        if not all(math.isfinite(float(v)) for v in (buy,sell,delta)):
            return {**result,'reason':'numeric_overflow'}
        return {**result,'value':float(delta),'value_decimal':str(delta),
                'buy_volume':float(buy),'sell_volume':float(sell),
                'total_volume':float(total),'bar_count':len(selected),
                'complete':True,'reliable':True,'reason':None}
    except (KeyError,TypeError,ValueError,InvalidOperation,OverflowError) as exc:
        return {**result,'reason':'volume_data_error:'+type(exc).__name__}
