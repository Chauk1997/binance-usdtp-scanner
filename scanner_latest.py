"""Active orchestration: full universe -> technical -> auxiliary -> boards."""
import asyncio
import os
import time
from datetime import datetime, timezone
import httpx
import strategy_latest as strategy
from trade_cvd import calculate_cvd_from_exchange_volume
from scan_freshness import scan_now_ms, expected_bar


def select_universe(rows, stock_symbols=()):
    selected=[]; excluded=[]; unknown=[]
    stock_symbols=set(stock_symbols)
    for x in rows:
        symbol=x.get('symbol','')
        if x.get('contractType')!='PERPETUAL' or x.get('status')!='TRADING':
            continue
        if x.get('quoteAsset')!='USDT' or x.get('marginAsset','USDT')!='USDT' or x.get('baseAsset')=='USDC':
            excluded.append(dict(symbol=symbol,reason='non_usdt_or_usdc'));continue
        tags=' '.join([str(x.get('underlyingType','')),str(x.get('underlyingSubType',[])),str(x.get('assetClass',''))]).upper()
        if symbol in stock_symbols or any(t in tags for t in ('STOCK','EQUITY','EQUITIES')):
            excluded.append(dict(symbol=symbol,reason='stock_contract'));continue
        # Do not silently claim full coverage when a newly introduced asset class is unknown.
        if x.get('underlyingType','').upper() not in ('COIN','CRYPTO','INDEX'):
            unknown.append(symbol);continue
        selected.append(symbol)
    return sorted(set(selected)),excluded,unknown


async def universe(api,client):
    result=await api.safe_get(client,api.BINANCE_BASE+'/fapi/v1/exchangeInfo')
    if '_error' in result:
        return None,dict(status='stopped',stage='universe',reason=result['_error'])
    rows=result.get('_data',{}).get('symbols',[])
    overrides=os.environ.get('SCANNER_STOCK_SYMBOLS','').split(',')
    symbols,excluded,unknown=select_universe(rows,[s.strip() for s in overrides if s.strip()])
    if unknown or not symbols:
        return None,dict(status='stopped',stage='universe_classification',unknown_symbols=unknown,
                         reason='Review exchange classification / SCANNER_STOCK_SYMBOLS; no partial universe published')
    api.write_json(api.SYMBOL_CACHE,symbols)
    return symbols,dict(source='Binance exchangeInfo',excluded=excluded,unknown_symbols=unknown,
                        total=len(symbols),sampled=False)


def parse_auxiliary(data,tf,cutoff):
    span={'1h':3600000,'4h':14400000}[tf]
    end=expected_bar(tf,cutoff)['close_time']+1
    start=end-span
    result=dict(window_start=start,window_end=end,period=tf,oi=None,oi_delta_pct=None,
                top_position_long_short_ratio=None,top_account_long_short_ratio=None,
                global_long_short_ratio=None,taker_buy_sell_ratio=None,funding_rate=None,
                cvd=dict(value=None,kind='unavailable',reliable=False,source=None,
                         reason='無可靠直接 CVD 資料'),sources={},errors={})
    def rows(name):
        value=data.get(name)
        if not isinstance(value,list):
            result['errors'][name]='unavailable';return []
        return [r for r in value if isinstance(r,dict)]
    def timestamp(r):
        return strategy.number(r.get('timestamp'))
    oi={int(timestamp(r)):strategy.number(r.get('sumOpenInterest')) for r in rows('oi') if timestamp(r) is not None}
    result['oi']=oi.get(end)
    if oi.get(start) and oi.get(end) is not None:
        result['oi_delta_pct']=(oi[end]/oi[start]-1)*100
    result['sources']['oi']=dict(endpoint='openInterestHist',timestamp=end if oi.get(end) is not None else None,unit='base_contract_quantity')
    flow=[r for r in rows('taker') if timestamp(r)==start]
    if flow:
        buy,sell=(strategy.number(flow[-1].get(k)) for k in ('buyVol','sellVol'))
        if buy is not None and sell is not None and buy>=0 and sell>0:
            result['taker_buy_sell_ratio']=buy/sell
    result['sources']['taker']=dict(endpoint='takerlongshortRatio',timestamp=start if flow else None)
    for name,field in [('top_position','top_position_long_short_ratio'),('top_account','top_account_long_short_ratio'),('global','global_long_short_ratio')]:
        # Official ratio timestamp denotes interval end; require exact boundary.
        found=[r for r in rows(name) if timestamp(r)==end]
        if found: result[field]=strategy.number(found[-1].get('longShortRatio'))
        result['sources'][name]=dict(timestamp=end if found else None)
    funding=[r for r in rows('funding') if strategy.number(r.get('fundingTime')) is not None and start-86400000<=float(r['fundingTime'])<cutoff]
    if funding:
        r=max(funding,key=lambda r:int(r['fundingTime']))
        result['funding_rate']=strategy.number(r.get('fundingRate'))
        result['sources']['funding']=dict(timestamp=int(r['fundingTime']),kind='last_settled_rate')
    result['missing_fields']=[k for k in ('oi','oi_delta_pct','top_position_long_short_ratio','top_account_long_short_ratio','global_long_short_ratio','taker_buy_sell_ratio','funding_rate') if result[k] is None]+['cvd']
    result['status']='partial' if any(result[k] is not None for k in ('oi','taker_buy_sell_ratio','funding_rate')) else 'unavailable'
    return result


async def fetch_auxiliary(api,client,symbol,tf,cutoff):
    endpoints={'oi':'/futures/data/openInterestHist','taker':'/futures/data/takerlongshortRatio',
               'top_position':'/futures/data/topLongShortPositionRatio','top_account':'/futures/data/topLongShortAccountRatio',
               'global':'/futures/data/globalLongShortAccountRatio','funding':'/fapi/v1/fundingRate'}
    data={};errors={}
    # Sequential, bounded request rate. Reuse safe_get's 418/429 cooldown.
    for name,path in endpoints.items():
        params={'symbol':symbol,'limit':8,'endTime':cutoff-1}
        if name!='funding': params['period']=tf
        try:
            response=await api.safe_get(client,api.BINANCE_BASE+path,params=params)
            data[name]=response.get('_data')
            if '_error' in response: errors[name]=response['_error']
        except (httpx.HTTPError,ValueError,TypeError) as exc:
            errors[name]=type(exc).__name__
        await asyncio.sleep(.35)
    result=parse_auxiliary(data,tf,cutoff)
    result['errors'].update(errors)
    result['cvd']=calculate_cvd_from_exchange_volume(api.read_json(api.cache_file(symbol,tf)),result['window_start'],result['window_end'])
    if result['cvd']['reliable']:
        result['missing_fields'].remove('cvd')
        result['status']='partial' if result['missing_fields'] else 'complete'
    else:
        result['errors']['cvd']=result['cvd']['reason']
    return result


async def build(api):
    started=time.monotonic();cutoff=scan_now_ms()
    headers={}
    if os.environ.get('BINANCE_API_KEY'):
        headers['X-MBX-APIKEY']=os.environ['BINANCE_API_KEY']
    async with httpx.AsyncClient(headers=headers) as client:
        symbols,universe_info=await universe(api,client)
        if symbols is None: return universe_info
        for tf in ('1h','4h','1d'):
            if not api.interval_cache_is_current(tf):
                update=await api.run_incremental_update(tf)
                if update.get('status')!='complete':
                    return dict(status='stopped',stage='update_'+tf,detail=update)
        coverage={};frames={};diagnostics={}
        for symbol in symbols:
            frames[symbol]={};diagnostics[symbol]={}
            for tf in ('1h','4h','1d'):
                df,error=strategy.prepare(api.read_json(api.cache_file(symbol,tf)),tf,cutoff)
                frames[symbol][tf]=df
                if error: diagnostics[symbol][tf]=error
        for tf in ('1h','4h','1d'):
            missing=[s for s in symbols if frames[s][tf] is None or not api.symbol_cache_is_current(s,tf)]
            coverage[tf]=dict(complete=not missing,symbols=len(symbols),missing=missing)
        if any(not c['complete'] for c in coverage.values()):
            return dict(status='stopped',stage='closed_candle_coverage',coverage=coverage,
                        missing_symbols={tf:c['missing'] for tf,c in coverage.items()},diagnostics=diagnostics)
        pools={'1h':[],'4h':[]};special_formal=[];approaching=[]
        btc=frames.get('BTCUSDT',{})
        # Scan every symbol before querying any auxiliary endpoint.
        for symbol in symbols:
            for tf in pools:
                row,reason=strategy.qualify(symbol,tf,frames[symbol],btc.get(tf))
                if row: pools[tf].append(row)
                else: diagnostics[symbol][tf]=reason
            item=strategy.special(symbol,frames[symbol])
            if item:
                (special_formal if item['status']=='formal' else approaching).append(item)
        requests={(r['symbol'],tf) for tf,rows in pools.items() for r in rows}
        requests.update((r['symbol'],'1h') for r in special_formal)
        auxiliary={}
        gate=asyncio.Semaphore(3)
        async def enrich(symbol,tf):
            async with gate:
                auxiliary[symbol,tf]=await fetch_auxiliary(api,client,symbol,tf,cutoff)
        await asyncio.gather(*(enrich(symbol,tf) for symbol,tf in sorted(requests)))
        for tf,rows in pools.items():
            for row in rows:
                row['auxiliary']=auxiliary[row['symbol'],tf]
                score,warnings=strategy.auxiliary_score(row['auxiliary'])
                row['ranking_key'].append(score);row['warnings']=warnings
            rows.sort(key=lambda r:r['symbol'])
            rows.sort(key=lambda r:r['ranking_key'],reverse=True)
        for row in special_formal:
            row['auxiliary']=auxiliary[row['symbol'],'1h']
        special_formal.sort(key=lambda r:(-r['key_candle']['close_time'],r['symbol']))
        approaching.sort(key=lambda r:(-r['completed_count'],-r['anchor_key']['close_time'],r['symbol']))
        boards={tf:rows[:10] for tf,rows in pools.items()}
        feed=dict(status='complete',scanner='V5.4_CHATGPT_FEED',strategy=strategy.VERSION,
                  strategy_by_timeframe={tf:strategy.VERSION for tf in boards},
                  parameters=strategy.parameters(),ranking_policy=strategy.POLICY,
                  universe_policy='All Binance USDT perpetuals; exclude stocks and USDC; no sampling',
                  universe=universe_info,coverage=coverage,diagnostics=diagnostics,
                  special={'formal':special_formal,'approaching':approaching},
                  market_state=strategy.market_warning(boards),
                  scan_started_at_utc=datetime.fromtimestamp(cutoff/1000,timezone.utc).isoformat(),
                  elapsed_seconds=round(time.monotonic()-started,2),freshness_version='closed-bars-v2',
                  generated_at_utc=datetime.now(timezone.utc).isoformat())
        for tf in ('1h','4h','1d'):
            for key,value in expected_bar(tf,cutoff).items():
                feed['latest_closed_'+tf+'_'+key]=value
        for tf,rows in boards.items():
            feed[tf]=dict(candidate_count=len(pools[tf]),candidates=rows)
        return dict(status='complete',feed=feed,formal=feed,v52=feed,compact=feed)
