"""1H-only Background First evidence. No entry signals or Key-K bonuses."""
import math
from continuation_quality import continuation_quality


def daily_quality(daily, continuation, four_cont, four_key):
    result = dict(daily)
    state = daily['state']
    special = (state == 'bullish_consolidation' and
               (continuation.get('high_compression') or continuation.get('high_consolidation')) and
               four_cont.get('restart') and four_key.get('passed') and
               any(k['bars_ago'] >= four_cont['consolidation_windows'][-1]['end_bars_ago']
                   for k in four_key.get('candidates', [])))
    if special:
        state, rank = 'high_consolidation_4h_key_reexpansion', 6
    elif state == 'bullish_divergence' and continuation.get('restart'):
        state, rank = 'bullish_reexpansion', 3
    else:
        rank = {'bullish_divergence':5, 'bullish_consolidation':4,
                'bottom_reversal':2}.get(state, 1)
    result.update(state=state, quality_rank=rank, special_priority=bool(special))
    return result


def structure_quality(df):
    """Skeleton only: no stage, Key K, stop signal or recency scores."""
    if df is None or len(df) < 60:
        return {'score': -1, 'status': 'insufficient_history'}
    d = df.iloc[-60:]
    c, e, m, s = (d[k].astype(float) for k in ('close','ema15','sma30','sma45'))
    slopes = {k: float(d[k].iloc[-1]/d[k].iloc[-6]-1) for k in ('ema15','sma30','sma45')}
    bull = (e > m) & (m > s)
    duration = 0
    for v in reversed(bull.tolist()):
        if not v: break
        duration += 1
    highs = [float(d.high.iloc[i]) for i in range(1,len(d)-1)
             if d.high.iloc[i] > d.high.iloc[i-1] and d.high.iloc[i] >= d.high.iloc[i+1]]
    lows = [(i,float(d.low.iloc[i])) for i in range(1,len(d)-1)
            if d.low.iloc[i] < d.low.iloc[i-1] and d.low.iloc[i] <= d.low.iloc[i+1]]
    hh = len(highs)>1 and highs[-1]>highs[-2]
    hl = len(lows)>1 and lows[-1][1]>lows[-2][1]
    support = bool(lows and min(abs(lows[-1][1]/float(d[k].iloc[lows[-1][0]])-1)
                               for k in ('ema15','sma30','sma45')) <= .02)
    intact = bool(c.iloc[-1] >= s.iloc[-1]*.97 and (not lows or c.iloc[-1]>=lows[-1][1]))
    pullback = float((d.high.iloc[-12:].max()-c.iloc[-1])/c.iloc[-1]*100)
    cont = continuation_quality(df)
    score = (2*bool(bull.iloc[-1]) + sum(v>0 for v in slopes.values()) + hh + hl +
             support + intact + min(duration,24)/24 + (0<=pullback<=5) -
             3*(not intact) - 2*cont.get('overextended',False))
    return dict(score=float(score), ma_slopes=slopes, hh=bool(hh), hl=bool(hl),
                hl_ma_support=support, structure_intact=intact, trend_bars=duration,
                pullback_pct=pullback, overextended=cont.get('overextended',False))


def structure_stage(df, cont):
    result = dict(cont)
    if cont['state'] not in ('ordinary_structure','unconfirmed_or_low_rebound','preserved_bull_consolidation'):
        return result
    e,m,s = (df[k] for k in ('ema15','sma30','sma45'))
    if cont.get('reexpanding'):
        state, score = 'initial_bull_expansion', 4
    elif cont.get('ema15_turning_up'):
        state, score = 'ema15_turning_up', 3
    elif m.iloc[-1]>m.iloc[-4] and s.iloc[-1]>s.iloc[-4]:
        state, score = 'slow_ma_follow_through', 3
    elif cont.get('structure_preserved') and df.close.iloc[-1]<df.close.iloc[-2]:
        state, score = 'healthy_pullback', 2
    else:
        state,score = result['state'],result['score']
    result.update(state=state,score=score)
    return result


def capital_confirmation(evidence, df):
    oi=evidence.get('oi_delta_4h_pct')
    flow=evidence.get('cvd_proxy_6h')
    ratio=evidence.get('taker_buy_sell_ratio_6h')
    def valid(v): return v is not None and math.isfinite(float(v))
    price=float((df.close.iloc[-1]/df.close.iloc[-5]-1)*100)
    short=bool(price>0 and valid(oi) and oi < -2)
    score=(1 if oi>0 else -1 if oi<0 else 0) if valid(oi) else 0
    score+=(1 if flow>0 else -1 if flow<0 else 0) if valid(flow) else 0
    score+=(.5 if ratio>1 else -.5 if ratio<1 else 0) if valid(ratio) else 0
    score+=.5 if price>0 and all(valid(v) for v in (oi,flow,ratio)) and oi>0 and flow>0 and ratio>1 else 0
    score-=2 if short else 0
    score-=1 if evidence.get("cvd_weakening") else 0
    return dict(score=score,status='available' if all(valid(v) for v in (oi,flow,ratio)) else 'partial_or_unavailable',
                price_change_4h_pct=price, oi_delta_pct=oi, oi_window='4h',flow_window=str(evidence.get('flow_bars',6))+'h',
                cvd_proxy_6h=flow,taker_buy_sell_ratio_6h=ratio,short_covering=short,
                cvd_kind='Binance taker-flow proxy')


def closed_capital_inputs(oi_rows, taker_rows, cutoff):
    """OI samples are boundary points; taker timestamps are interval starts."""
    hour=3600000
    boundary=cutoff//hour*hour
    oi={int(r['timestamp']):float(r['sumOpenInterest']) for r in (oi_rows if isinstance(oi_rows,list) else [])
        if int(r['timestamp'])<=boundary and r.get('sumOpenInterest') is not None}
    previous=oi.get(boundary-4*hour)
    delta=(oi[boundary]/previous-1)*100 if boundary in oi and previous else None
    flow={int(r['timestamp']):r for r in (taker_rows if isinstance(taker_rows,list) else []) if int(r['timestamp'])+hour<=boundary}
    rows=[flow[t] for t in range(boundary-6*hour,boundary,hour) if t in flow]
    buy=sum(float(r['buyVol']) for r in rows)
    sell=sum(float(r['sellVol']) for r in rows)
    cvd=buy-sell if len(rows)>=3 else None
    weakening=(sum(float(r['buyVol'])-float(r['sellVol']) for r in rows[-3:]) <
               sum(float(r['buyVol'])-float(r['sellVol']) for r in rows[-6:-3])) if len(rows)==6 else None
    return dict(oi_delta_4h_pct=delta, cvd_proxy_6h=cvd,
                taker_buy_sell_ratio_6h=buy/sell if sell and len(rows)>=3 else None,
                cvd_weakening=weakening, flow_bars=len(rows), closed_boundary=boundary)
