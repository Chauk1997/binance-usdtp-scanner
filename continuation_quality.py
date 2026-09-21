"""Symbol-independent continuation evidence, computed only from closed bars."""
import math

THRESHOLDS = dict(history_bars=60, established_bars=3, consolidation_bars=3,
                  high_zone_position=.65, max_consolidation_range_atr=3.0,
                  max_retracement=.5, slow_ma_tolerance=.03,
                  max_extension_atr=3.0, max_extension_pct=12.0)


def continuation_quality(df, overextended=False):
    result = dict(state='unavailable', score=-1, thresholds=THRESHOLDS.copy())
    required = ['close', 'high', 'low', 'ema15', 'sma30', 'sma45']
    if df is None or len(df) < 60 or not all(k in df for k in required):
        return result
    d = df.iloc[-60:]
    if not all(math.isfinite(float(v)) for v in d[required].to_numpy().ravel()):
        return result
    c, e, m, s = (d[k] for k in ['close','ema15','sma30','sma45'])
    tr = (d.high-d.low).combine((d.high-c.shift()).abs(), max).combine((d.low-c.shift()).abs(), max)
    atr = float(tr.iloc[-14:].mean())
    if atr <= 0 or min(float(c.iloc[-1]),float(e.iloc[-1]),float(s.iloc[-1])) <= 0:
        return result
    bull = (e > m) & (m > s) & (e > e.shift(3)) & (m > m.shift(3)) & (s > s.shift(3))
    spread = (e-s)/s
    # Find consolidation AFTER an established impulse, then a restart within
    # three bars. Both the chronology and pre-consolidation trend are required.
    windows = []
    for end in range(len(d)-4, len(d)):
        start = end-3
        prior = d.iloc[max(0,start-24):start]
        if len(prior) < 12:
            continue
        established = any(bool(bull.iloc[i-2:i+1].all()) for i in range(max(2,start-24), start))
        top, base = float(prior.high.max()), float(prior.low.min())
        span = top-base
        block = d.iloc[start:end]
        zone = (float(block.close.min())-base)/span if span > 0 else 0
        retracement = (top-float(block.low.min()))/span if span > 0 else 1
        width = float(block.high.max()-block.low.min())/atr
        preserved = bool((block.close >= block.sma45*.97).all() and
                         (block.sma30 >= block.sma45*.995).all())
        contraction = float(spread.iloc[end-1]) <= float(spread.iloc[start]) or width <= 2
        high_compression = established and zone >= .65 and retracement <= .5 and width <= 3 and preserved and contraction
        if high_compression:
            windows.append(dict(start_bars_ago=len(d)-1-start, end_bars_ago=len(d)-end,
                                high_zone_position=round(zone,4), retracement=round(retracement,4),
                                range_atr=round(width,4), high=float(block.high.max())))
    established = any(bool(bull.iloc[i-2:i+1].all()) for i in range(len(d)-24,len(d)-3))
    preserved = bool((c.iloc[-6:] >= s.iloc[-6:]*.97).all() and
                     (m.iloc[-6:] >= s.iloc[-6:]*.995).all() and s.iloc[-1] >= s.iloc[-6]*.998)
    turning = bool(e.iloc[-1] > e.iloc[-2] and
                   (e.iloc[-1]/e.iloc[-2]-1) > (e.iloc[-3]/e.iloc[-4]-1))
    expanding = bool(e.iloc[-1] > m.iloc[-1] > s.iloc[-1] and spread.iloc[-1] > spread.iloc[-3])
    extension_atr = float((c.iloc[-1]-e.iloc[-1])/atr)
    extension_pct = float((c.iloc[-1]/e.iloc[-1]-1)*100)
    hot = bool(overextended or extension_atr > 3 or extension_pct > 12)
    recent_top, recent_base = float(d.high.iloc[-30:].max()), float(d.low.iloc[-30:].min())
    high_position = (float(c.iloc[-1])-recent_base)/(recent_top-recent_base) if recent_top>recent_base else 0
    high_consolidation = bool(established and preserved and high_position >= .65 and not expanding)
    restart = bool(windows and preserved and turning and expanding and c.iloc[-1] > c.iloc[-2] and
                   c.iloc[-1] > e.iloc[-1])
    if hot:
        state, score = 'overextended', 0
    elif restart:
        state, score = 'high_consolidation_reexpansion', 6
    elif windows and preserved:
        state, score = 'healthy_high_compression', 5
    elif established and preserved and expanding:
        state, score = 'healthy_bull_continuation', 4
    elif established and preserved:
        state, score = 'preserved_bull_consolidation', 3
    elif expanding:
        state, score = 'unconfirmed_or_low_rebound', 1
    else:
        state, score = 'ordinary_structure', 2 if preserved else 1
    return dict(state=state, score=score, established_bull=established,
                structure_preserved=preserved, ema15_turning_up=turning,
                reexpanding=expanding, high_compression=bool(windows), high_consolidation=high_consolidation,
                high_zone_position=round(high_position,4),
                restart=restart, overextended=hot, extension_atr=round(extension_atr,4),
                extension_pct=round(extension_pct,4), consolidation_windows=windows,
                close_time=int(d.close_time.iloc[-1]) if 'close_time' in d else None,
                thresholds=THRESHOLDS.copy())


def upside_space(df):
    if df is None or len(df)<25:
        return dict(score=-1, status='unavailable')
    c=float(df.close.iloc[-1])
    previous=df.iloc[-61:-1]
    # Nearest prior local swing high above price; no known resistance is capped,
    # not represented as infinite upside.
    levels=[float(previous.high.iloc[i]) for i in range(1,len(previous)-1)
            if previous.high.iloc[i]>=previous.high.iloc[i-1] and
            previous.high.iloc[i]>=previous.high.iloc[i+1] and previous.high.iloc[i]>c]
    distance=(min(levels)/c-1)*100 if levels else None
    return dict(score=round(min(distance,10),4) if distance is not None else 10,
                status='known_resistance' if levels else 'no_prior_swing_above',
                nearest_resistance=min(levels) if levels else None, distance_pct=distance,
                lookback_bars=60, cap_pct=10)


def capital_confirmation(evidence, timeframe):
    oi=evidence.get('oi_delta_'+timeframe+'_pct')
    flow=evidence.get('cvd_proxy_6h')
    ratio=evidence.get('taker_buy_sell_ratio_6h')
    # Signs are comparable across assets; absolute OI/CVD sizes are not.
    values=[oi,flow,ratio]
    valid=[v is not None and math.isfinite(float(v)) for v in values]
    score=(1 if float(oi)>0 else -1 if float(oi)<0 else 0) if valid[0] else 0
    score+=((1 if float(flow)>0 else -1 if float(flow)<0 else 0) if valid[1] else 0)
    score+=((.5 if float(ratio)>1 else -.5 if float(ratio)<1 else 0) if valid[2] else 0)
    return dict(score=score, status='available' if all(valid) else 'partial_or_unavailable',
                oi_delta_pct=oi, cvd_proxy_6h=flow, taker_buy_sell_ratio_6h=ratio,
                flow_window='6h', oi_window=timeframe, cvd_kind='Binance taker-flow proxy')
