"""Exact closed-window 3/6-bar capital evidence; missing data stays unknown."""
from cvd_proxy import calculate_cvd_proxy
import math

def finite(v):
    try:
        v=float(v)
        return v if math.isfinite(v) else None
    except (TypeError,ValueError): return None

def capital_sequence(oi_rows,bars,tf,end):
    span={'1h':3600000,'4h':14400000}[tf]
    oi={}
    for r in (oi_rows if isinstance(oi_rows,list) else []):
        if not isinstance(r,dict): continue
        t=finite(r.get('timestamp'));v=finite(r.get('sumOpenInterest'))
        if t is not None and v is not None and v>0:
            oi[int(t)]=v
    result={'period':tf,'window_end':end,'windows':{},'short_covering_threshold_pct':-1.0}
    for n in (3,6):
        start=end-n*span
        points=[oi.get(start+i*span) for i in range(n+1)]
        cvd=calculate_cvd_proxy(bars,start,end)
        prices={int(b['close_time'])+1:finite(b.get('close')) for b in bars or []}
        returns=None
        if all(prices.get(start+i*span) not in (None,0) for i in range(n+1)):
            returns=(prices[end]/prices[start]-1)*100
        delta=(points[-1]/points[0]-1)*100 if all(v is not None for v in points) else None
        result['windows'][str(n)]=dict(bar_count=n,window_start=start,window_end=end,
             oi_delta_pct=delta,oi_points=points,price_return_pct=returns,cvd_proxy=cvd,
             short_covering=bool(returns is not None and returns>0 and delta is not None and delta<=-1))
    result['bars']=[dict(window_start=end-(6-i)*span,window_end=end-(5-i)*span,
        oi_start=oi.get(end-(6-i)*span),oi_end=oi.get(end-(5-i)*span),
        cvd_proxy=calculate_cvd_proxy(bars,end-(6-i)*span,end-(5-i)*span)) for i in range(6)]
    recent=result['windows']['3']['cvd_proxy']; prior=calculate_cvd_proxy(bars,end-6*span,end-3*span)
    # Normalized imbalance avoids confusing higher volume with directional improvement.
    def imbalance(c):
        return c['value']/c['total_volume'] if c.get('reliable') and c.get('total_volume',0)>0 else None
    a,b=imbalance(recent),imbalance(prior)
    result['cvd_weakening']=a<b if a is not None and b is not None else None
    result['recent_imbalance']=a;result['prior_imbalance']=b
    result['status']='complete' if all(w['oi_delta_pct'] is not None and w['price_return_pct'] is not None and w['cvd_proxy']['reliable'] for w in result['windows'].values()) else 'partial'
    return result

def capital_score(e):
    score=0;warnings=[]
    for n,w in e.get('windows',{}).items():
        price=w.get('price_return_pct');oi=w.get('oi_delta_pct');cvd=w.get('cvd_proxy',{})
        if price is not None and oi is not None and price>0 and oi>0: score+=1
        if w.get('short_covering'): score-=2;warnings.append(n+'bar_short_covering_or_deleveraging')
        if cvd.get('reliable'): score+=1 if cvd['value']>0 else -1 if cvd['value']<0 else 0
    if e.get('cvd_weakening'): score-=1;warnings.append('cvd_weakening')
    return score,warnings
