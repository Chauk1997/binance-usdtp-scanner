"""Phase 16 causal state machine. All times are Binance close timestamps (ms)."""
from dataclasses import dataclass, asdict
from hashlib import sha256
from statistics import median
from decimal import Decimal
import json
import math
import sqlite3
import os
from pathlib import Path
import pandas as pd
from scan_freshness import DURATIONS
from strategy_latest import prepare as validate_history

VERSION = 'SCANNER_V5.4_PHASE16'
PARAMETER_VERSION = 'phase16-test-defaults-1'
MA = ['ema15', 'sma30', 'sma45']
POLICY = ['ma_escape', 'key_quality', 'resistance', 'range_grade', 'consolidation_quality',
          'volume_contraction', 'duration', 'btc_resilience', 'htf_health', 'auxiliary']

@dataclass(frozen=True)
class Config:
    warmup_bars: int = 200
    resistance_lookback: int = 120
    sma45_decline: float = -.20  # three-bar slope / current ATR
    duration_bands: tuple = (12, 24, 48)  # longer consolidation preferred
    escape_bands: tuple = (0, .5, 1)
    # These are implementation defaults, not newly adopted hard strategy gates.

_overrides=json.loads(os.environ.get('SCANNER_PHASE16_DEFAULTS','{}'))
for _name in ('duration_bands','escape_bands'):
    if _name in _overrides:_overrides[_name]=tuple(_overrides[_name])
CONFIG = Config(**_overrides)
if CONFIG.warmup_bars < 50 or CONFIG.resistance_lookback < 7 or list(CONFIG.duration_bands)!=sorted(CONFIG.duration_bands) or list(CONFIG.escape_bands)!=sorted(CONFIG.escape_bands):
    raise ValueError('Invalid Phase16 implementation defaults')
if _overrides:PARAMETER_VERSION+='-'+sha256(json.dumps(asdict(CONFIG),sort_keys=True).encode()).hexdigest()[:8]


def identity(*parts):
    return sha256('|'.join(map(str, parts)).encode()).hexdigest()[:24]


def known(value):
    if value is None or not math.isfinite(float(value)):
        return [0, 0]
    return [1, float(value)]


def prepare(bars, tf, cutoff, cfg=CONFIG):
    df, error = validate_history(bars, tf, cutoff)
    if error:
        return None, error
    if len(df) < cfg.warmup_bars:
        return None, 'insufficient_indicator_warmup'
    # Explicit SMA seeds; no pandas EWM approximation of Wilder initialization.
    ema = [float('nan')] * len(df)
    close_values=df.close.to_numpy()
    ema[14] = float(df.close.iloc[:15].mean())
    for i in range(15, len(df)):
        ema[i] = ema[i-1] + (float(close_values[i])-ema[i-1]) / 8
    df['ema15'] = ema
    tr = pd.concat([df.high-df.low, (df.high-df.close.shift()).abs(),
                    (df.low-df.close.shift()).abs()], axis=1).max(axis=1)
    atr = [float('nan')] * len(df)
    tr_values=tr.to_numpy()
    atr[13] = float(tr.iloc[:14].mean())
    for i in range(14, len(df)):
        atr[i] = (atr[i-1]*13 + float(tr_values[i]))/14
    df['atr14'] = atr
    df['atr_stable'] = df.atr14.rolling(8).median()
    df['volume_sma24'] = df.volume.rolling(24).mean()
    df['spread'] = (df[MA].max(axis=1)-df[MA].min(axis=1))/df.atr_stable
    df['d45'] = pd.concat([(df.sma45-df.ema15).abs(), (df.sma45-df.sma30).abs()], axis=1).min(axis=1)/df.atr_stable
    return df, None


def key_at(df, i, bearish=False):
    if i < 24:
        return None
    cached=df.__dict__.get('_phase16_rows')
    r,p=(cached[i],cached[i-1]) if cached is not None else (df.iloc[i],df.iloc[i-1])
    span, previous = float(r.high-r.low), float(p.high-p.low)
    if span <= 0 or previous <= 0 or p.volume <= 0:
        return None
    if not (r.close < r.open if bearish else r.close > r.open):
        return None
    if Decimal(str(r.volume)) < Decimal('2.2')*Decimal(str(p.volume)):
        return None
    exact_span=Decimal(str(r.high))-Decimal(str(r.low))
    exact_previous=Decimal(str(p.high))-Decimal(str(p.low))
    if r.volume <= r.volume_sma24 or exact_span < Decimal('1.5')*exact_previous:
        return None
    averages=[float(r.ema15),float(r.sma30),float(r.sma45)]
    lo,hi=min(averages),max(averages)
    atr = float(p.atr14)
    escape = (float(r.close)-hi)/atr if atr > 0 else None
    kind = ('A' if r.open > hi and r.close > hi else
            'B' if lo <= r.open <= hi and r.close > hi and escape is not None and escape >= .5 else
            'C' if lo <= r.close <= hi else 'D')
    wick = float((r.high-r.close)/span)
    body = float(abs(r.close-r.open)/span)
    quality = 2 if body >= .7 else 1 if body >= .4 else 0
    if wick > .5:
        quality = min(quality, 1)
    multiple = span/previous
    return dict(close_time=int(r.close_time), open_time=int(r.open_time), type=kind,
                ohlcv={k:float(getattr(r,k)) for k in ('open','high','low','close','volume')},
                indicators={k:float(getattr(r,k)) for k in MA+['atr14','atr_stable','volume_sma24']},
                escape=escape, body_ratio=body, upper_wick_ratio=wick,
                close_strength=float((r.close-r.low)/span), quality=quality,
                range_multiple=multiple, range_grade=sum(multiple >= x for x in (2,3,5)),
                volume_multiple=float(r.volume/p.volume), previous_atr=atr)


def confirmed(df, start, i):
    # Current candidate is excluded; a confirmation on this bar applies next bar.
    if i-start < 8:
        return False
    d = df.iloc[i-8:i]
    return bool((d.spread <= 1).sum() >= 5 and (d.spread.iloc[-4:] <= 1).sum() >= 2
                and (d.d45 <= .5).sum() >= 3 and (d.d45.iloc[-4:] <= .5).sum() >= 1)


def htf_at(frames, tf, close_time):
    evidence = {}
    for interval in (('4h','1d') if tf == '1h' else ('1d',)):
        d = frames.get(interval)
        d = d[d.close_time <= close_time] if d is not None else None
        if d is None or len(d) < 48 or d[MA+['atr14']].iloc[-1].isna().any():
            return dict(valid=False, reason='missing_htf', evidence=evidence, health=None)
        r = d.iloc[-1]
        expected = (close_time+1)//DURATIONS[interval]*DURATIONS[interval]-1
        if int(r.close_time) != expected:
            return dict(valid=False, reason='unsynchronized_htf', evidence=evidence, health=None)
        bear = bool(r.ema15 < r.sma30 < r.sma45)
        valid = bool(r.ema15 > r.sma30 and r.ema15 > r.sma45) if tf == '1h' and interval == '1d' else not bear
        slope = [(float(r[k])-float(d.iloc[-4][k]))/float(r.atr14) if r.atr14 > 0 else None for k in MA]
        health = (None if None in slope else 2 if slope[0] >= .2 and slope[1] >= .2 and slope[2] > -.2
                  else 0 if slope[0] <= -.2 and slope[1] <= -.2 else 1)
        evidence[interval] = dict(close_time=int(r.close_time), valid=valid, health=health, slopes=slope,
                                  indicators={k:float(r[k]) for k in MA+['atr14']})
    return dict(valid=all(x['valid'] for x in evidence.values()), evidence=evidence,
                health=min(x['health'] for x in evidence.values()) if all(x['health'] is not None for x in evidence.values()) else None)


def expanding(df, i, direction, cfg=CONFIG):
    cache=df.__dict__.get('_phase16_expansion')
    if cache is None or cache['threshold']!=cfg.sma45_decline:
        e,s,a=df.ema15,df.sma30,df.sma45
        up=(e>s)&(s>a)&(e.diff()>0)&(s.diff()>0)&((e-s).diff()>0)&((s-a).diff()>0)&(a.diff(3)/df.atr14>cfg.sma45_decline)
        down=(e<s)&(s<a)&(e.diff()<0)&(s.diff()<0)&((s-e).diff()>0)&((a-s).diff()>0)&(a.diff()<=0)
        cache=dict(threshold=cfg.sma45_decline,up=(up.rolling(3).sum()==3).to_numpy(),down=(down.rolling(3).sum()==3).to_numpy())
        if '_phase16_rows' in df.__dict__:df.__dict__['_phase16_expansion']=cache
    return bool(cache['up' if direction==1 else 'down'][i])


def resistance(df, start, cfg=CONFIG):
    """Frozen at K1, right-three confirmation must strictly precede K1."""
    pivots = []
    for i in range(max(3, start-cfg.resistance_lookback), start-3):
        r = df.iloc[i]
        if r.high > df.high.iloc[i-3:i].max() and r.high >= df.high.iloc[i+1:i+4].max():
            pivots.append(i)
    zones = []
    for i in pivots:
        r = df.iloc[i]
        placed = False
        for zone in zones:
            last = zone['indices'][-1]
            highs = zone['highs']+[float(r.high)]
            if max(highs)-min(highs) <= .5*float(r.atr14) and last < i-1 and min(float(df.high.iloc[last]),float(r.high))-float(df.low.iloc[last+1:i].min()) >= .5*float(r.atr14):
                zone['indices'].append(i);zone['highs'] = highs;placed=True;break
        if not placed:
            zones.append(dict(indices=[i], highs=[float(r.high)]))
    valid = []
    for z in zones:
        if len(z['indices']) < 2:
            continue
        high = max(z['highs']);last=z['indices'][-1]
        # A close > upper edge + .5 contemporaneous ATR has removed the blocker.
        after = df.iloc[last+1:start]
        if ((after.close-high)/after.atr14 >= .5).any():
            continue
        valid.append(dict(high=high, low=min(z['highs']), tests=len(z['indices']),
                          confirmed_times=[int(df.iloc[k+3].close_time) for k in z['indices']]))
    return valid


def resilience(df, btc, i):
    if btc is None or i < 7:
        return None
    b = btc.set_index('close_time').close
    samples=[]
    for k in range(i-6, i):
        now, prev=int(df.iloc[k].close_time),int(df.iloc[k-1].close_time)
        if now not in b.index or prev not in b.index:
            return None
        br=float(b.loc[now]/b.loc[prev]-1)
        if br < 0:
            samples.append(float(df.iloc[k].close/df.iloc[k-1].close-1)-br)
    return median(samples) if samples else None


def liquidity(turnover, tick_size, price):
    if turnover is None or tick_size is None or price <= 0:
        return 'UNKNOWN'
    ratio=tick_size/price
    return '高' if turnover < 1e6 or ratio > .001 else '低' if turnover >= 1e7 and ratio <= .0005 else '中'


class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db=sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS metadata (id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS states (id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS structures (id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS signals (id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS signals_symbol_tf ON signals(json_extract(data,'$.symbol'),json_extract(data,'$.timeframe'));
        CREATE TABLE IF NOT EXISTS research (id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS boards (id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY, cutoff INTEGER, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS notifications (id TEXT PRIMARY KEY, notified_at INTEGER NOT NULL);
        ''')
        previous=self.get('metadata','parameters')
        if previous and previous['version']!=PARAMETER_VERSION:
            self.db.close()
            raise ValueError('Parameter version changed; use a new SCANNER_DATA_DIR for reproducible migration')
        with self.db:self.put('metadata','parameters',dict(version=PARAMETER_VERSION,config=asdict(CONFIG)))

    def get(self, table, key):
        row=self.db.execute(f'SELECT data FROM {table} WHERE id=?',(key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, table, key, value):
        self.db.execute(f'INSERT INTO {table}(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data',
                        (key,json.dumps(value,allow_nan=False)))

    def all(self, table):
        return [json.loads(r[0]) for r in self.db.execute(f'SELECT data FROM {table}')]

    def signals_for(self, symbol, tf):
        rows=self.db.execute("SELECT data FROM signals WHERE json_extract(data,'$.symbol')=? AND json_extract(data,'$.timeframe')=? AND json_extract(data,'$.status')='ACTIVE'",(symbol,tf))
        return [json.loads(r[0]) for r in rows]

    def active_signals(self, cutoff):
        rows=self.db.execute("SELECT data FROM signals WHERE json_extract(data,'$.status')='ACTIVE' AND json_extract(data,'$.current_htf_valid')=1 AND json_extract(data,'$.key_time')>=?",(cutoff-86400000,))
        return [json.loads(r[0]) for r in rows]

    def mark_notified(self, signal_id, time_ms):
        # Call only after a confirmed notification delivery. No automatic sender.
        if self.get('signals', signal_id) is None:
            raise ValueError('Unknown signal')
        cur=self.db.execute('INSERT OR IGNORE INTO notifications VALUES (?,?)',(signal_id,time_ms))
        return cur.rowcount == 1


def anchor_indicators(df, state):
    times={int(t):i for i,t in enumerate(df.close_time)}
    # Preserve exact recursive indicator values across sliding cache windows.
    tail=state.get('indicator_tail', [])
    for snapshot in tail:
        k=times.get(snapshot['close_time'])
        if k is not None:
            for name in ('ema15','atr14'):
                df.loc[k,name]=snapshot[name]
    cursor_index=times.get(state.get('cursor'))
    if cursor_index is not None:
        for k in range(cursor_index+1,len(df)):
            r,p=df.iloc[k],df.iloc[k-1]
            df.loc[k,'ema15']=float(p.ema15)+(float(r.close)-float(p.ema15))/8
            tr=max(float(r.high-r.low),abs(float(r.high-p.close)),abs(float(r.low-p.close)))
            df.loc[k,'atr14']=(float(p.atr14)*13+tr)/14
        df['atr_stable']=df.atr14.rolling(8).median()
        df['spread']=(df[MA].max(axis=1)-df[MA].min(axis=1))/df.atr_stable
        df['d45']=pd.concat([(df.sma45-df.ema15).abs(),(df.sma45-df.sma30).abs()],axis=1).min(axis=1)/df.atr_stable
    return cursor_index


def indicator_state(df):
    return dict(cursor=int(df.close_time.iloc[-1]),indicator_tail=[dict(close_time=int(r.close_time),ema15=float(r.ema15),atr14=float(r.atr14)) for _,r in df.iloc[-8:].iterrows()])


def process(store, symbol, tf, frames, btc, cutoff, cfg=CONFIG):
    df=frames[tf]
    state=store.get('states',symbol+':'+tf) or dict(cursor=None, bull_seen=False, structure=None, run=None)
    counts=dict(k1=0, confirmed=0, key=0, htf_rejected=0, created=0, early=0, invalidated=0)
    times={int(t):i for i,t in enumerate(df.close_time)}
    if state['cursor'] is not None and state['cursor'] < int(df.close_time.iloc[-1]) and state['cursor'] not in times:
        raise ValueError('unrecoverable_state_history_gap:'+symbol+':'+tf)
    cursor_index=anchor_indicators(df,state)
    df.__dict__['_phase16_rows']=list(df.itertuples(index=False))
    df.__dict__.pop('_phase16_expansion',None)
    cached_rows=df.__dict__['_phase16_rows']
    signals=store.signals_for(symbol,tf)
    first_new=cursor_index+1 if cursor_index is not None else cfg.warmup_bars-1
    for i in range(first_new, len(df)):
        r,p=cached_rows[i],cached_rows[i-1];t=int(r.close_time)
        if state['cursor'] is not None and t <= state['cursor']:
            continue
        bull=bool(r.ema15 > r.sma30 > r.sma45)
        key=key_at(df,i);bear=key_at(df,i,True)
        for signal in signals:
            delta=(t-signal['key_time'])//DURATIONS[tf]
            if signal['status']!='INVALIDATED' and 1 <= delta <= 3 and bear and r.close < signal['key']['ohlcv']['open']:
                signal.update(status='INVALIDATED', invalidated_time=t)
                signal.setdefault('status_history',[]).append(dict(status='INVALIDATED',cutoff=t))
                counts['invalidated']+=1
        run=state['run']
        continuation=False
        if run and run['run_active']:
            if key:
                run['last_key_time']=t;continuation=True
            else:
                run['run_active']=False
        structure=state['structure']
        if structure and structure['state'] in ('K1_PENDING','CONSOLIDATING') and t-structure['k1_time'] > 14*86400000:
            structure.update(state='EXPIRED', ended_time=t)
        active=structure and structure['state'] in ('K1_PENDING','CONSOLIDATING')
        if not active and not continuation and state['bull_seen'] and p.ema15 > p.sma30 and r.ema15 <= r.sma30:
            if structure:store.put('structures',structure['id'],structure)
            structure=dict(id=identity(symbol,tf,t),k1_time=t,state='K1_PENDING',confirmed_time=None,
                           cycles=0,shock_count=0,cycle_state=0,bear_time=None,observations=[],observation_count=0,resistance=resistance(df,i,cfg))
            state['structure']=structure;state['bull_seen']=False;active=True;counts['k1']+=1
        if active:
            start=times.get(structure['k1_time'])
            if start is None:
                raise ValueError('missing_k1_history:'+symbol+':'+tf)
            observations=structure.get('observation_count',i-start)
            if key: counts['key']+=1
            event=htf_at(frames,tf,t) if key else None
            if key and not continuation and structure['confirmed_time'] is not None and structure['confirmed_time'] < t:
                if event['valid']:
                    sid=identity(symbol,tf,structure['k1_time'],t)
                    history=df.iloc[start:i]
                    contraction=(float(history.volume.iloc[-6:].median()/history.volume.iloc[:-6].median())
                                 if len(history)>=12 and history.volume.iloc[:-6].median()>0 else None)
                    quality=2 if structure['cycles']==0 and structure['shock_count']<=1 else 1 if structure['cycles']<=1 and structure['shock_count']<=2 else 0
                    zones=[]
                    for z in structure['resistance']:
                        intervening=df.iloc[start:i]
                        if not ((intervening.close-z['high'])/intervening.atr14 >= .5).any(): zones.append(z)
                    # Highest still-valid overhead zone fixes the conservative breakout target.
                    target=max((z['high'] for z in zones),default=None)
                    strength=(float(r.close)-target)/key['previous_atr'] if target is not None and key['previous_atr']>0 else None
                    rs=resilience(df,btc,i)
                    signal=dict(strategy_version=VERSION,parameter_version=PARAMETER_VERSION,
                                symbol=symbol,contract_id=symbol,timeframe=tf,signal_id=sid,
                                consolidation_id=structure['id'],run_id=identity(sid,'run'),
                                k1_time=structure['k1_time'],confirmed_time=structure['confirmed_time'],
                                key_time=t,key=key,event_htf_valid=True,event_htf=event,
                                resistance_snapshot=zones,resistance_breakout=strength,
                                btc_resilience=rs,contraction=contraction,consolidation_quality=quality,
                                expansion_cycles=structure['cycles'],bearish_shock_count=structure['shock_count'],
                                duration=observations,status='ACTIVE',notified=False, detected_cutoff=cutoff,
                                status_history=[dict(status='DETECTED',cutoff=t),dict(status='ACTIVE',cutoff=t)],
                                confirmation_snapshot=structure.get('confirmation_snapshot'))
                    if store.get('signals',sid) is None:
                        signals.append(signal);counts['created']+=1
                    if state['run']:store.put('runs',state['run']['run_id'],state['run'])
                    state['run']=dict(run_id=signal['run_id'],signal_id=sid,consolidation_id=structure['id'],
                                      first_key_time=t,last_key_time=t,run_active=True)
                    if key['type'] in ('A','B'):
                        structure.update(state='ENDED',ended_time=t,reason=key['type'])
                else:
                    counts['htf_rejected']+=1
                    rejection_id=identity(symbol,tf,structure['k1_time'],t,'htf_rejected')
                    store.put('research',rejection_id,dict(id=rejection_id,kind='HTF_REJECTED_KEY',symbol=symbol,timeframe=tf,key=key,event_htf=event))
            elif key and not continuation and 5 <= observations <= 7 and r.spread <= 1 and r.spread <= p.spread and event['valid']:
                rid=identity(symbol,tf,structure['k1_time'],t,'early')
                store.put('research',rid,dict(id=rid,kind='EARLY_BREAKOUT',symbol=symbol,timeframe=tf,key=key,
                                            k1_time=structure['k1_time'],event_htf=event,observations=observations))
                counts['early']+=1
            # Observations exclude every potential bullish key, including rejected events.
            # Confirmation window must contain eight non-key observation bars.
            if not key and structure['state'] in ('K1_PENDING','CONSOLIDATING'):
                structure['observation_count']=structure.get('observation_count',0)+1
                window=structure.setdefault('observations',[])
                window.append(dict(time=t,spread=float(r.spread),d45=float(r.d45)))
                window[:]=window[-8:]
                if structure['confirmed_time'] is None and len(window)==8:
                    if sum(x['spread']<=1 for x in window)>=5 and sum(x['spread']<=1 for x in window[-4:])>=2 and sum(x['d45']<=.5 for x in window)>=3 and sum(x['d45']<=.5 for x in window[-4:])>=1:
                        structure.update(state='CONSOLIDATING',confirmed_time=t,
                            confirmation_snapshot=dict(times=[x['time'] for x in window],
                                spreads=[x['spread'] for x in window],d45=[x['d45'] for x in window]))
                        counts['confirmed']+=1
            if structure['state'] in ('K1_PENDING','CONSOLIDATING'):
                if r.spread <= 1:
                    if structure['cycle_state']==2: structure['cycles']+=1
                    structure['cycle_state']=1
                elif r.spread > 1.5 and structure['cycle_state']==1: structure['cycle_state']=2
                span=float(r.high-r.low);previous=float(p.high-p.low)
                if r.close < r.open and span>0 and previous>0 and span >= 1.5*previous and (r.close-r.low)/span <= .3:
                    structure['shock_count']+=1
            if structure['state'] in ('K1_PENDING','CONSOLIDATING'):
                if expanding(df,i,1,cfg):
                    structure.update(state='ENDED',ended_time=t,reason='natural')
                elif structure['bear_time'] is not None and t-structure['bear_time']==3*DURATIONS[tf] and expanding(df,i,-1,cfg):
                    structure.update(state='INVALIDATED',ended_time=t,reason='bear_expansion')
                if bear: structure['bear_time']=t
        if bull:
            state['bull_seen']=True
        state['cursor']=t
    current=htf_at(frames,tf,int(df.close_time.iloc[-1]))
    for signal in signals:
        if signal['status']!='INVALIDATED':
            signal['status']='ACTIVE' if cutoff-signal['key_time'] <= 86400000 else 'EXPIRED'
        signal['current_htf_valid']=current['valid'];signal['current_htf']=current
        signal['age_ms']=cutoff-signal['key_time']
        signal['notified']=store.db.execute('SELECT 1 FROM notifications WHERE id=?',(signal['signal_id'],)).fetchone() is not None
        previous=store.get('signals',signal['signal_id'])
        signal.setdefault('status_history',[])
        if not signal['status_history'] or signal['status_history'][-1]['status']!=signal['status']:
            signal['status_history'].append(dict(status=signal['status'],cutoff=cutoff))
        if previous is None:
            event_copy={**signal,'current_htf':signal['event_htf']}
            signal['event_ranking_key']=rank_key(event_copy)
        store.put('signals',signal['signal_id'],signal)
    state['state']=state['structure']['state'] if state['structure'] else 'WAITING'
    if state['structure']:store.put('structures',state['structure']['id'],state['structure'])
    if state['run']:store.put('runs',state['run']['run_id'],state['run'])
    state['indicator_tail']=[dict(close_time=int(r.close_time),ema15=float(r.ema15),atr14=float(r.atr14)) for _,r in df.iloc[-8:].iterrows()]
    store.put('states',symbol+':'+tf,state)
    return [s for s in signals if s['status']=='ACTIVE' and s['event_htf_valid'] and s['current_htf_valid']],counts


def rank_key(signal, special=False, synchronized_rs=None, auxiliary=None, cfg=CONFIG):
    key=signal['key'];escape=key['escape'];pressure=signal['resistance_breakout'];contraction=signal['contraction']
    escape_grade=None if escape is None else sum(escape > x if x==0 else escape >= x for x in cfg.escape_bands)
    pressure_grade=None if pressure is None else 2 if pressure >= .5 else 1 if pressure > 0 else 0
    contraction_grade=None if contraction is None else 2 if contraction <= .7 else 1 if contraction <= 1 else 0
    result=[known(escape_grade),known(key['quality']),known(pressure_grade),known(key['range_grade']),
            known(signal['consolidation_quality']),known(contraction_grade),
            known(sum(signal['duration'] >= x for x in cfg.duration_bands)),
            known(signal['btc_resilience']),known(signal['current_htf']['health']),known(auxiliary)]
    return ([known(synchronized_rs)]+result) if special else result
