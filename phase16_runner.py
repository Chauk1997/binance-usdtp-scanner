"""Full-universe Phase 16 runner. Legacy network/cache helpers remain available."""
import asyncio
import time
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import httpx
import phase16 as strategy
from scan_freshness import scan_now_ms, expected_bar, DURATIONS, latest_closed


def due_timeframes(cutoff, store):
    result=[]
    for tf in ('1h','4h'):
        boundary=cutoff//DURATIONS[tf]*DURATIONS[tf]
        prior=store.get('boards',tf)
        # Startup catch-up runs the latest closed boundary once. Reads / repeated
        # requests never re-rank a completed 4H board between its boundaries.
        if prior is None or prior['boundary_ms'] < boundary:
            result.append(tf)
    return result


def visible(feed, now):
    result=deepcopy(feed)
    for tf in ('1h','4h'):
        board=result.get(tf,{})
        rows=board.get('candidates',[])
        board['candidates']=[r for r in rows if 0 <= now-r['key_time'] <= 86400000]
        board['entry']=board['candidates']
        board['age_ms']=now-board.get('scan_cutoff_ms',now)
    return result


def freshness(feed, now):
    checks={}
    for tf in ('1h','4h'):
        boundary=now//DURATIONS[tf]*DURATIONS[tf]
        board=(feed or {}).get(tf,{})
        checks['fresh_for_'+tf]=bool(board.get('boundary_ms')==boundary)
    checks['feed_ready']=all(checks.values())
    checks['stale']=not checks['feed_ready'];checks['pending']=False
    return checks


async def build(api):
    from scanner_latest import universe, fetch_auxiliary
    from strategy_latest import auxiliary_score, special as legacy_special, market_warning
    start=time.monotonic();cutoff=scan_now_ms()
    store=strategy.Store(api.CACHE_DIR/'phase16.sqlite3')
    try:
        due=due_timeframes(cutoff,store)
        oldfeed=store.get('boards','feed')
        if not due and oldfeed:
            feed=visible(oldfeed,cutoff)
            return dict(status='complete',feed=feed,formal=feed,v52=feed,compact=feed)
        async with httpx.AsyncClient() as client:
            symbols,info=await universe(api,client)
            if symbols is None:return info
            for tf in ('1h','4h','1d'):
                if not api.interval_cache_is_current(tf):
                    update=await api.run_incremental_update(tf)
                    if update.get('status')!='complete':
                        return dict(status='stopped',stage='update_'+tf,detail=update)
            for attempt in range(3):
                missing=[tf for tf in ('1h','4h','1d') if not api.interval_cache_is_current(tf)]
                if not missing or api.RATE_LIMITED:break
                await asyncio.sleep(2**attempt)
                for tf in missing:await api.run_incremental_update(tf)
            frames={};coverage={};diagnostics={};audit={}
            for symbol in symbols:
                frames[symbol]={};diagnostics[symbol]={};audit[symbol]={}
                for tf in ('1h','4h','1d'):
                    bars=api.read_json(api.cache_file(symbol,tf))
                    df,error=strategy.prepare(bars,tf,cutoff)
                    proof=api.symbol_cache_is_current(symbol,tf)
                    if not proof:error=error or 'unconfirmed_closed_candle'
                    frames[symbol][tf]=df if not error else None
                    if error:diagnostics[symbol][tf]=error
                    audit[symbol][tf]=dict(expected=expected_bar(tf,cutoff),available=latest_closed(bars,tf,cutoff),cache_final_confirmed=proof)
            for tf in ('1h','4h','1d'):
                missing=[s for s in symbols if frames[s][tf] is None]
                coverage[tf]=dict(complete=not missing,symbols=len(symbols),scanned=len(symbols)-len(missing),missing=missing)
            # New listings with insufficient warmup are explicit hard UNKNOWNs;
            # technical coverage remains partial, never advertised as full Top10.
            hard_errors={s:d for s,d in diagnostics.items() if any(v!='insufficient_indicator_warmup' for v in d.values())}
            if hard_errors:
                return dict(status='stopped',stage='closed_candle_coverage',coverage=coverage,diagnostics=hard_errors,candle_audit=audit)
            btc=frames.get('BTCUSDT',{})
            if btc.get('1h') is None:
                return dict(status='stopped',stage='btc_history',reason='BTC 1H unavailable')
            b=btc['1h'].iloc[-1]
            special=bool(b.close < b.open and (b.high-b.low)/b.high > .025)
            # One SQLite transaction for all state, historical events and boards.
            # Any symbol failure rolls back the whole round, including cursors.
            with store.db:
                # Anchor all histories before any signal decision; 1H sees the
                # same persisted 4H/daily indicators as the 4H scanner.
                for symbol in symbols:
                    for interval,d in frames[symbol].items():
                        if d is None:continue
                        ident='indicator:'+symbol+':'+interval
                        previous=store.get('states',ident) or {}
                        strategy.anchor_indicators(d,previous)
                        store.put('states',ident,strategy.indicator_state(d))
                pools={};counts={}
                for tf in due:
                    pools[tf]=[];counts[tf]={}
                    for symbol in symbols:
                        if any(frames[symbol][k] is None for k in (tf,'1d')+ (('4h',) if tf=='1h' else ())):
                            continue
                        rows,stage_counts=strategy.process(store,symbol,tf,frames[symbol],btc.get(tf),cutoff)
                        pools[tf].extend(rows)
                        for k,v in stage_counts.items():counts[tf][k]=counts[tf].get(k,0)+v
                # Tick size comes from exchangeInfo; labels never affect ranking.
                meta=await api.safe_get(client,api.BINANCE_BASE+'/fapi/v1/exchangeInfo')
                ticks={r['symbol']:next((float(f['tickSize']) for f in r.get('filters',[]) if f.get('filterType')=='PRICE_FILTER'),None)
                       for r in meta.get('_data',{}).get('symbols',[])}
                ticker=await api.safe_get(client,api.BINANCE_BASE+'/fapi/v1/ticker/24hr')
                turnovers={r['symbol']:float(r['quoteVolume']) for r in (ticker.get('_data') or []) if isinstance(r,dict) and r.get('quoteVolume') is not None} if isinstance(ticker.get('_data'),list) else {}
                for tf,rows in pools.items():
                    for row in rows:
                        symbol=row['symbol'];d=frames[symbol][tf];last=d.iloc[-1]
                        # Special mode compares every coin to the same latest BTC
                        # 1H return, including coins ranked on the 4H board.
                        coin=frames[symbol]['1h'];sync=None
                        if coin is not None and len(coin)>1:
                            sync=float(coin.close.iloc[-1]/coin.close.iloc[-2]-1)-float(b.close/btc['1h'].close.iloc[-2]-1)
                        row['synchronized_btc_resilience']=sync
                        row['ranking_key']=strategy.rank_key(row,special,sync)
                        row['liquidity_risk']=strategy.liquidity(turnovers.get(symbol),ticks.get(symbol),float(last.close))
                        row['tags']=[]
                        if row['key']['escape'] is not None and row['key']['escape']>0 and (row['resistance_breakout'] or 0)>0:row['tags'].append('🔥結構雙突破')
                        # Existing special pattern remains label-only. No extra pool.
                        item=legacy_special(symbol,frames[symbol])
                        if item and item.get('status')=='formal':row['tags'].append('MUBARAK（既有辨識）')
                    rows.sort(key=lambda r:r['signal_id'])
                    rows.sort(key=lambda r:r['ranking_key'],reverse=True)
                    # Enrich all ties at the boundary, since only the last factor
                    # can change ordering. No volume-multiple bonus.
                    boundary=rows[9]['ranking_key'][:-1] if len(rows)>10 else None
                    frontier=[r for r in rows if boundary is None or r['ranking_key'][:-1]>=boundary]
                    for row in frontier:
                        aux=await fetch_auxiliary(api,client,row['symbol'],tf,cutoff)
                        score,_=auxiliary_score(aux)
                        available=any(aux.get(k) is not None for k in ('oi_delta_pct','taker_buy_sell_ratio','funding_rate'))
                        row['auxiliary']=aux
                        row['ranking_key'][-1]=strategy.known(score if available else None)
                    rows.sort(key=lambda r:r['ranking_key'],reverse=True)
                    board=dict(boundary_ms=cutoff//DURATIONS[tf]*DURATIONS[tf],scan_cutoff_ms=cutoff,
                               scan_time_utc=datetime.fromtimestamp(cutoff/1000,timezone.utc).isoformat(),
                               candidate_count=len(rows),candidates=rows[:10],entry=rows[:10],stage_counts=counts[tf],
                               coverage=dict(complete=all(coverage[k]['complete'] for k in (tf,'1d')+(('4h',) if tf=='1h' else ())),
                                             symbols=len(symbols),scanned=sum(not diagnostics[s] for s in symbols),
                                             missing=[s for s in symbols if diagnostics[s]]),special_mode=special)
                    store.put('boards',tf,board)
                boards={tf:deepcopy(store.get('boards',tf)) for tf in ('1h','4h')}
                live_symbols={tf:{r['symbol'] for r in board['candidates'] if cutoff-r['key_time']<=86400000} for tf,board in boards.items()}
                resonance=live_symbols['1h'] & live_symbols['4h']
                # Presentation label only; stored board and ranking remain fixed.
                for board in boards.values():
                    for row in board['candidates']:
                        if row['symbol'] in resonance and '🔥1H＋4H共振' not in row['tags']:row['tags'].append('🔥1H＋4H共振')
                feed=dict(status='complete',scanner='V5.4_CHATGPT_FEED',strategy=strategy.VERSION,
                          strategy_by_timeframe={tf:strategy.VERSION for tf in boards},parameters=as_parameters(),
                          ranking_policy=strategy.POLICY,ranking_mode='lexicographic',
                          universe=info,universe_total=len(symbols),coverage=coverage,diagnostics=diagnostics,candle_audit=audit,
                          scan_cutoff_ms=cutoff,scanned=len(symbols),missing=sum(bool(d) for d in diagnostics.values()),
                          partial_scan=any(not c['complete'] for c in coverage.values()),
                          generated_at_utc=datetime.now(timezone.utc).isoformat(),elapsed_seconds=round(time.monotonic()-start,2),
                          freshness_version='phase16',clock_source='Binance /fapi/v1/time + monotonic elapsed',
                          market_state=dict(triggered=special,label='⚠️ 此輪為 BTC 急跌「特殊版本」' if special else None,
                                            auxiliary_warning=market_warning({tf:board['candidates'] for tf,board in boards.items()})),
                          special=dict(formal=[],approaching=[]),research=[__import__('json').loads(r[0]) for r in store.db.execute('SELECT data FROM research ORDER BY rowid DESC LIMIT 100')],
                          research_total=store.db.execute('SELECT count(*) FROM research').fetchone()[0],**boards)
                for tf in ('1h','4h','1d'):
                    expected=expected_bar(tf,cutoff)
                    for k,v in expected.items():feed['latest_closed_'+tf+'_'+k]=v
                store.put('boards','feed',feed)
                store.db.execute('INSERT INTO audit(cutoff,data) VALUES(?,?)',(cutoff,__import__('json').dumps(dict(due=due,counts=counts,coverage=coverage))))
            feed=visible(feed,cutoff)
            return dict(status='complete',feed=feed,formal=feed,v52=feed,compact=feed)
    finally:
        store.db.close()


def as_parameters():
    return dict(version=strategy.PARAMETER_VERSION,defaults=__import__('dataclasses').asdict(strategy.CONFIG),
                ma=[15,30,45],atr=14,volume_sma=24,volume_multiple=2.2,range_multiple=1.5,
                consolidation_bars=8,k1_days=14,active_hours=24,implementation_defaults=True)
