"""Confirmed 2026-09-26 scanner. Pure technical decisions; no entry signals.

Numeric interpretations of qualitative shapes live in Config, not hidden gates.
Type 1 = compression launch; type 2 = structure breakout; type 3 = 1H acceleration.
"""
from dataclasses import dataclass, asdict
import math
from decimal import Decimal
import pandas as pd
from scan_freshness import DURATIONS, expected_bar

from scanner_contract import VERSION
MA = ['ema15', 'sma30', 'sma45']
STAGES = ['1D多頭', '4H第一型Key K', '4H回補均線／盤整', '1H盤整／壓縮', '新的1H第一型Key K']
POLICY = ['higher_background', 'ma_structure', 'compression', 'key_quality',
          'ma_pull', 'freshness', 'relative_strength', 'auxiliary']

@dataclass(frozen=True)
class Config:
    key_lookback: int = 12
    compression_bars: int = 4
    compression_spread: float = .02
    consolidation_range: float = .06
    contraction_ratio: float = .85
    breakout_bars: int = 24
    bear_bars: int = 3
    special_4h_lookback: int = 48
    retrace_tolerance: float = .01
    structure_floor_tolerance: float = .03
    warning_fraction: float = .60
    warning_min_symbols: int = 5

CONFIG = Config()


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError):
        return None


def prepare(bars, tf, cutoff):
    """Reject malformed/gapped histories; never silently compress time."""
    if not bars:
        return None, 'missing_history'
    try:
        df = pd.DataFrame([b for b in bars if int(b['close_time']) < cutoff])
        if df.empty:
            return None, 'missing_closed_history'
        df = df.sort_values('open_time').reset_index(drop=True)
        cols = ['open', 'high', 'low', 'close', 'volume', 'open_time', 'close_time']
        df[cols] = df[cols].apply(pd.to_numeric)
        if not all(math.isfinite(float(v)) for v in df[cols].to_numpy().flat):
            return None, 'nonfinite_history'
        if (df[['open','high','low','close']] <= 0).any().any() or (df.volume < 0).any():
            return None, 'invalid_price_or_volume'
        if ((df.high < df[['open','close','low']].max(axis=1)) |
                (df.low > df[['open','close','high']].min(axis=1))).any():
            return None, 'invalid_ohlc'
        if not (df.close_time == df.open_time + DURATIONS[tf] - 1).all():
            return None, 'invalid_bar_duration'
        if len(df) > 1 and not (df.open_time.diff().iloc[1:] == DURATIONS[tf]).all():
            return None, 'gapped_or_duplicate_history'
        if int(df.close_time.iloc[-1]) != expected_bar(tf, cutoff)['close_time']:
            return None, 'stale_history'
        df['ema15'] = df.close.ewm(span=15, adjust=False).mean()
        df['sma30'] = df.close.rolling(30).mean()
        df['sma45'] = df.close.rolling(45).mean()
        return df, None
    except (KeyError, TypeError, ValueError, OverflowError):
        return None, 'malformed_history'


def trend(df, cfg=CONFIG):
    if df is None or len(df) < 50:
        return dict(state='insufficient_history', valid=False, bullish=False, hard_exclude=False, score=-1)
    m = df[MA]
    if m.iloc[-6:].isna().any().any():
        return dict(state='invalid_ma', valid=False, bullish=False, hard_exclude=False, score=-1)
    bull = bool(m.ema15.iloc[-1] > m.sma30.iloc[-1] > m.sma45.iloc[-1])
    bear = (m.ema15 < m.sma30) & (m.sma30 < m.sma45)
    down = (m.diff() < 0).all(axis=1)
    # Both adjacent gaps must expand; ordering alone is never a hard veto.
    gap1 = (m.sma30-m.ema15)/df.close
    gap2 = (m.sma45-m.sma30)/df.close
    veto = bool((bear & down & (gap1.diff()>0) & (gap2.diff()>0)).iloc[-cfg.bear_bars:].all())
    slopes = m.iloc[-1]/m.iloc[-6]-1
    up = bool((slopes > 0).all())
    spread = (m.max(axis=1)-m.min(axis=1))/df.close
    expanding = bool(spread.iloc[-1] > spread.iloc[-4])
    state = 'bearish_divergence' if veto else 'bullish' if bull else 'compression_or_transition'
    return dict(state=state, valid=True, bullish=bull, hard_exclude=veto,
                score=0 if veto else (3+int(up and expanding) if bull else 1),
                slopes={k:float(v) for k,v in slopes.items()},
                close_time=int(df.close_time.iloc[-1]))


def compression(df, cfg=CONFIG):
    n = cfg.compression_bars
    if df is None or len(df)<45+n or df[MA].iloc[-n:].isna().any().any():
        return dict(passed=False, score=0)
    d = df.iloc[-n:]
    spread = (d[MA].max(axis=1)-d[MA].min(axis=1))/d.close
    width = float((d.high.max()-d.low.min())/d.close.iloc[-1])
    close_ma = bool((spread <= cfg.compression_spread).all())
    converging = bool(spread.iloc[-1] <= spread.iloc[0]*cfg.contraction_ratio)
    sideways = width <= cfg.consolidation_range
    passed = sideways and (close_ma or converging)
    return dict(passed=passed, score=float(max(0,1-width/cfg.consolidation_range)+max(0,1-float(spread.mean())/cfg.compression_spread)) if passed else 0,
                start_time=int(d.open_time.iloc[0]), end_time=int(d.close_time.iloc[-1]),
                spread=float(spread.iloc[-1]), range_pct=width*100,
                close_ma=close_ma, converging=converging)


def key_at(df, i):
    if i<24:
        return None
    r,p = df.iloc[i],df.iloc[i-1]
    v,pv,avg = float(r.volume),float(p.volume),float(df.volume.iloc[i-24:i].mean())
    prior_sum = sum(Decimal(str(float(x))) for x in df.volume.iloc[i-24:i])
    if not all(math.isfinite(x) and x>=0 for x in (v,pv,avg)) or Decimal(str(v)) < Decimal('2.2')*Decimal(str(pv)) or Decimal(str(v))*24 <= prior_sum:
        return None
    body = abs(float(r.close-r.open))
    upper = float(r.high-max(r.open,r.close))
    lower = float(min(r.open,r.close)-r.low)
    total = float(r.high-r.low)
    # Report true body/range, but exclude lower wick from the ranking denominator.
    # This reconciles body-quality preference with no lower-wick penalty.
    body_quality = body/(body+upper) if body+upper else 0
    up = bool(r.close>r.open)
    pull = {k:float((r[k]/p[k]-1)*100) if number(r[k]) is not None and number(p[k]) not in (None,0) else 0 for k in MA}
    above = bool(min(r.open,r.close)>max(r[k] for k in MA))
    penalty = upper > body*.5
    quality = (body_quality + int(up) + min(v/avg,10)/10 +
               min(max(float(r.close/p.close-1)*100,0),10)/10 + .25*above - .5*penalty)
    return dict(index=i, open_time=int(r.open_time), close_time=int(r.close_time),
                bars_ago=len(df)-1-i, volume=v, previous_volume=pv, preceding_mean24=avg,
                volume_vs_prev=v/pv if pv else None, volume_vs_ma24=v/avg if avg else None,
                bullish=up, body_ratio=body/total if total else 0,
                body_quality=body_quality, upper_wick=upper, lower_wick=lower,
                upper_wick_penalty=penalty, open_close_above_ma=above,
                ma_pull_pct=pull, quality=quality)


def events(df, tf, cfg=CONFIG, lookback=None):
    if df is None:
        return []
    result=[]
    for i in range(max(50, len(df)-(lookback or cfg.key_lookback)),len(df)):
        key=key_at(df,i)
        if key is None:
            continue
        before, current = df.iloc[:i],df.iloc[:i+1]
        comp=compression(before,cfg)
        r,p=df.iloc[i],df.iloc[i-1]
        pull=key['ma_pull_pct']
        upward=bool(r.close>p.close and pull['ema15']>0 and (pull['sma30']>0 or pull['sma45']>0))
        types=[]
        if upward and comp['passed'] and r.close>max(r[k] for k in MA):
            types.append('type_1')
        if upward and r.close>float(before.high.iloc[-cfg.breakout_bars:].max()):
            types.append('type_2')
        if tf=='1h' and upward and trend(before,cfg)['bullish'] and trend(current,cfg)['bullish']:
            prior_move=float(p.close/df.close.iloc[i-2]-1)
            if float(r.close/p.close-1)>max(prior_move,0) and r.ema15-r.sma45>p.ema15-p.sma45:
                types.append('type_3')
        if types:
            result.append(dict(key_candle=key, types=types, compression=comp,
                               ma_structure=trend(current,cfg)))
    return result


def qualify(symbol, tf, frames, btc=None, cfg=CONFIG):
    op=frames.get(tf)
    bg=trend(frames.get('4h' if tf=='1h' else '1d'),cfg)
    if not bg['valid'] or bg['hard_exclude']:
        return None, 'background_'+bg['state']
    current=trend(op,cfg)
    if not current['valid'] or current['hard_exclude']:
        return None, 'operation_'+current['state']
    options=events(op,tf,cfg)
    if not options:
        return None,'no_qualifying_structure_key'
    # Latest qualifying launch, not a stopped/white-arrow entry candle.
    event=options[-1]
    key=event['key_candle']
    if op.close.iloc[-1] < op.sma45.iloc[-1]*(1-cfg.structure_floor_tolerance):
        return None,'structure_broken'
    change=float(op.close.iloc[-1]/op.close.iloc[-7]-1)*100
    relative=None
    if btc is not None and len(btc)>=7 and int(btc.close_time.iloc[-1])==int(op.close_time.iloc[-1]):
        relative=change-float(btc.close.iloc[-1]/btc.close.iloc[-7]-1)*100
    daily=trend(frames.get('1d'),cfg)
    # Daily is optional for 1H and cannot veto it or outrank primary 4H context.
    context=bg['score']+(.1*daily['score'] if tf=='1h' and daily['valid'] else 0)
    rank=[context,current['score'],event['compression']['score'],key['quality'],
          sum(key['ma_pull_pct'].values()),-key['bars_ago'],relative if relative is not None else 0]
    return dict(symbol=symbol,timeframe=tf,**event,background=bg,daily_background=daily,
                ranking_key=rank,ranking_policy=POLICY,relative_strength_pct=relative,
                candle={k:int(op.iloc[-1][k]) for k in ('open_time','close_time')},
                btc_candle={k:int(btc.iloc[-1][k]) for k in ('open_time','close_time')} if btc is not None else None),None


def special(symbol, frames, cfg=CONFIG):
    one,four,daily=(frames.get(k) for k in ('1h','4h','1d'))
    if any(x is None for x in (one,four,daily)) or not trend(daily,cfg)['bullish'] or trend(four,cfg)['hard_exclude']:
        return None
    anchors=[e for e in events(four,'4h',cfg,cfg.special_4h_lookback) if 'type_1' in e['types']]
    best=None
    for anchor in anchors:
        key=anchor['key_candle']; start=key['close_time']
        # Daily must already have been bullish before the 4H launch opened.
        daily_then=daily[daily.close_time < key['open_time']]
        if not trend(daily_then,cfg)['bullish']:
            continue
        following=four.iloc[key['index']+1:]
        if (following.close < following.sma45*(1-cfg.structure_floor_tolerance)).any():
            continue
        evidence=[dict(stage=STAGES[0],close_time=int(daily_then.close_time.iloc[-1])),
                  dict(stage=STAGES[1],close_time=start)]
        final=None
        for j in range(key['index']+1,len(four)):
            r=four.iloc[j]
            touch=any(r.low<=r[k]*(1+cfg.retrace_tolerance) and r.high>=r[k]*(1-cfg.retrace_tolerance) for k in MA)
            after=four.iloc[key['index']+1:j+1]
            settled=len(after)>=cfg.compression_bars and compression(four.iloc[:j+1],cfg)['passed']
            if (touch and r.close>=r.sma45*(1-cfg.structure_floor_tolerance)) or settled:
                evidence.append(dict(stage=STAGES[2],close_time=int(r.close_time)))
                break
        if len(evidence)==3:
            after_time=evidence[-1]['close_time']
            for i in range(50,len(one)):
                # All bars in the 1H compression window occur after 4H retracement confirmation.
                comp=compression(one.iloc[:i+1],cfg)
                # The 1H compression must still belong to the 4H settling phase.
                context=four[four.close_time <= int(one.close_time.iloc[i])]
                if context.empty:
                    continue
                r=context.iloc[-1]
                touching=any(r.low<=r[k]*(1+cfg.retrace_tolerance) and r.high>=r[k]*(1-cfg.retrace_tolerance) for k in MA)
                settling=touching or compression(context,cfg)['passed']
                if comp['passed'] and comp['start_time']>after_time and settling:
                    evidence.append(dict(stage=STAGES[3],close_time=comp['end_time'],start_time=comp['start_time']))
                    break
        if len(evidence)==4:
            for e in events(one,'1h',cfg):
                k=e['key_candle']
                if ('type_1' in e['types'] and k['open_time']>evidence[-1]['close_time']
                        and e['compression']['start_time']>evidence[2]['close_time']):
                    context=four[four.close_time <= e['compression']['end_time']]
                    if context.empty:
                        continue
                    r=context.iloc[-1]
                    settling=(any(r.low<=r[m]*(1+cfg.retrace_tolerance) and r.high>=r[m]*(1-cfg.retrace_tolerance) for m in MA)
                              or compression(context,cfg)['passed'])
                    row,_=qualify(symbol,'1h',frames,cfg=cfg)
                    if row and settling:
                        final=e
            if final:
                evidence[3]=dict(stage=STAGES[3],close_time=final['compression']['end_time'],start_time=final['compression']['start_time'])
                evidence.append(dict(stage=STAGES[4],close_time=final['key_candle']['close_time']))
        item=dict(symbol=symbol,completed_stages=evidence,completed_count=len(evidence),
                  missing_conditions=STAGES[len(evidence):],next_signal=STAGES[len(evidence)] if len(evidence)<5 else None,
                  status='formal' if len(evidence)==5 else 'approaching',anchor_key=key,
                  key_candle=final['key_candle'] if final else None)
        if best is None or (item['completed_count'],start)>(best['completed_count'],best['anchor_key']['close_time']):
            best=item
    return best


def valid_cvd(evidence):
    cvd=evidence.get('cvd') or {}
    return ((cvd.get('kind')=='direct' or (cvd.get('kind') in ('calculated_from_trades','calculated_from_exchange_volume') and cvd.get('complete') is True)) and cvd.get('reliable') is True and
            bool(cvd.get('source')) and number(cvd.get('value')) is not None and
            cvd.get('window_start')==evidence.get('window_start') and
            cvd.get('window_end')==evidence.get('window_end') and
            number(cvd.get('window_start')) is not None and number(cvd.get('window_end')) is not None)


def auxiliary_score(e):
    score=0; warnings=[]
    for name,neutral in [('oi_delta_pct',0),('taker_buy_sell_ratio',1)]:
        v=number(e.get(name))
        if v is not None:
            score+=1 if v>neutral else -1 if v<neutral else 0
            if v<neutral: warnings.append(name+'_weak')
    if valid_cvd(e):
        score+=1 if e['cvd']['value']>0 else -1 if e['cvd']['value']<0 else 0
        if e['cvd']['value']<0: warnings.append('cvd_negative')
    funding=number(e.get('funding_rate'))
    if funding is not None and funding>.001:
        score-=1;warnings.append('funding_crowded')
    return score,warnings


def market_warning(boards,cfg=CONFIG):
    details={}
    for tf,rows in boards.items():
        eligible=[r for r in rows if valid_cvd(r.get('auxiliary',{})) and number(r.get('auxiliary',{}).get('taker_buy_sell_ratio')) is not None]
        weak=[r['symbol'] for r in eligible if r['auxiliary']['cvd']['value']<0 and r['auxiliary']['taker_buy_sell_ratio']<1]
        trigger=len(weak)>=cfg.warning_min_symbols and len(weak)/max(len(rows),1)>=cfg.warning_fraction
        details[tf]=dict(total=len(rows),reliable_pairs=len(eligible),weak_symbols=weak,triggered=trigger,
                         status='warning' if trigger else 'insufficient_reliable_data' if len(eligible)<len(rows) or len(rows)<cfg.warning_min_symbols else 'not_broad')
    # Global statement needs breadth on both nonempty boards, preventing old 4H/new 1H split from being called global.
    active=[d for d in details.values() if d['total']]
    global_warning=bool(active) and all(d['triggered'] for d in active)
    return dict(triggered=global_warning,message='短線主動買盤同步退潮' if global_warning else None,
                by_timeframe=details,thresholds=dict(fraction=cfg.warning_fraction,min_symbols=cfg.warning_min_symbols),
                cvd_note='成交資料計算的 CVD 不完整時，不判定 CVD＋Taker 同步退潮')


def parameters():
    return asdict(CONFIG)
