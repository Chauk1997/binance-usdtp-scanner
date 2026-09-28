"""Closed-bar sequence features. Swing pivots require two right-hand bars.
Thresholds are explicit heuristics, not backtested profit claims.
"""
import pandas as pd

MA=['ema15','sma30','sma45']

def tail_count(values):
    n=0
    for value in reversed(list(values)):
        if not value: break
        n+=1
    return n

def features(df):
    d=df.iloc[-96:].reset_index(drop=True)
    tr=pd.concat([d.high-d.low,(d.high-d.close.shift()).abs(),(d.low-d.close.shift()).abs()],axis=1).max(axis=1)
    atr=float(tr.iloc[-14:].mean()) or float(d.close.iloc[-1])*.000001
    bull=(d.ema15>d.sma30)&(d.sma30>d.sma45)
    lead=(d.ema15>d.sma30)&(d.ema15>d.sma45)
    highs=[]; lows=[]
    for i in range(2,len(d)-2):
        if d.high.iloc[i]>d.high.iloc[i-2:i].max() and d.high.iloc[i]>d.high.iloc[i+1:i+3].max(): highs.append(i)
        if d.low.iloc[i]<d.low.iloc[i-2:i].min() and d.low.iloc[i]<d.low.iloc[i+1:i+3].min(): lows.append(i)
    hh=None if len(highs)<2 else bool(d.high.iloc[highs[-1]]>d.high.iloc[highs[-2]])
    hl=None if len(lows)<2 else bool(d.low.iloc[lows[-1]]>d.low.iloc[lows[-2]])
    # Peak before the last three bars anchors a pullback, allowing a new breakout high.
    peak=int(d.high.iloc[-24:-3].idxmax()); pull=d.iloc[peak:]
    trough=int(pull.low.idxmin()); recovery=d.iloc[trough:]
    depth=max(0,float(d.high.iloc[peak]-pull.low.min()))/atr
    defense={m:dict(close_breaches=int((pull.close<pull[m]).sum()),wick_breaches=int((pull.low<pull[m]).sum()),
                    recovered=bool(d.close.iloc[-1]>=d[m].iloc[-1]),
                    recovery_bars=tail_count(pull.close>=pull[m])) for m in ('sma30','sma45')}
    broken=bool(lows and float(d.close.iloc[-1])<float(d.low.iloc[lows[-1]]))
    gaps=pd.DataFrame({'a':d.ema15-d.sma30,'b':d.sma30-d.sma45})/atr
    expands=(gaps.diff()>0).all(axis=1)&bull
    reexpand=bool(len(recovery)>1 and expands.iloc[-1] and (gaps.iloc[-1]>gaps.iloc[trough]).all())
    extension=max(0,float(d.close.iloc[-1]-d.ema15.iloc[-1])/atr)
    duration=tail_count(bull)
    quality=(3*int(bull.iloc[-1])+min(duration,12)/12+min(tail_count(lead),12)/12
             +int(hh is True)+int(hl is True)-2*int(broken)
             -sum(v['close_breaches']/len(pull) for v in defense.values())-max(0,extension-3)
             -max(0,depth-3)*.25+min(max(0,float(d.close.iloc[-1]-d.low.iloc[trough])/atr),depth)/max(depth,1)*.5)
    resistances=[float(d.high.iloc[i]) for i in highs if d.high.iloc[i]>d.close.iloc[-1]]
    prior=float(d.high.iloc[:-1].max())
    if prior>d.close.iloc[-1]: resistances.append(prior)
    resistance=min(resistances) if resistances else None
    space=(resistance-float(d.close.iloc[-1]))/atr if resistance else None
    return dict(atr=atr,trend_duration=duration,ema15_lead_duration=tail_count(lead),
        duration_censored=duration==len(d),ma_defense=defense,
        swing=dict(hh=hh,hl=hl,broken=broken,confirmed_right_bars=2,
                   highs=[dict(price=float(d.high.iloc[i]),close_time=int(d.close_time.iloc[i])) for i in highs],
                   lows=[dict(price=float(d.low.iloc[i]),close_time=int(d.close_time.iloc[i])) for i in lows]),
        pullback=dict(depth_atr=depth,peak_close_time=int(d.close_time.iloc[peak]),trough_close_time=int(d.close_time.iloc[trough]),
                      recovered_atr=float(d.close.iloc[-1]-d.low.iloc[trough])/atr,reexpansion=reexpand,
                      reexpansion_bars=tail_count(expands),overextension_atr=extension,structure_broken=broken),
        structure_quality=quality,reexpansion_quality=int(reexpand)+min(tail_count(expands),3)/3-max(0,extension-3),
        upside_space=dict(resistance=resistance,remaining_atr=space,status='measured' if resistance else 'no_resistance_in_96_bars',
                          score=min(space,6) if space is not None else 0))

def daily_classify(df, trend):
    base=trend(df)
    if not base['valid']: return {**base,'classification':'unconfirmed_base','pool':'reserve','priority':0}
    if base['hard_exclude']: return {**base,'classification':'bearish_divergence','pool':'excluded','priority':-1}
    bull=(df.ema15>df.sma30)&(df.sma30>df.sma45)
    gaps=pd.DataFrame({'a':df.ema15-df.sma30,'b':df.sma30-df.sma45})/df.close
    expanding=bool((gaps.diff().iloc[-3:]>0).all().all() and bull.iloc[-1])
    had_bull=bool(bull.iloc[-24:-3].any())
    high_level=bool(had_bull and df.close.iloc[-1]>=df.high.iloc[-24:].max()*.85 and df.close.iloc[-1]>=df.sma45.iloc[-1])
    prior_contraction=bool((gaps.diff().iloc[-12:-3]<0).all(axis=1).any())
    if expanding and had_bull and prior_contraction: name='bullish_reexpansion'
    elif expanding and had_bull: name='bullish_divergence'
    elif high_level: name='high_consolidation'
    elif bull.iloc[-1] and bool((df[MA].diff().iloc[-1]>0).all()): name='base_turning_bullish'
    else: name='unconfirmed_base'
    priorities={'bullish_divergence':4,'high_consolidation':3,'bullish_reexpansion':2,'base_turning_bullish':1,'unconfirmed_base':0}
    return {**base,'classification':name,'pool':'reserve' if name=='unconfirmed_base' else 'formal','priority':priorities[name]}
