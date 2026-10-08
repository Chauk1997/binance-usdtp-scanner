"""Presentation only. Never resurrect old strategy snapshots or intersection."""
from copy import deepcopy
from scanner_contract import VERSION


def compact_scan_feed(feed):
    if feed.get('strategy') == 'SCANNER_V5.4_PHASE16':
        result={k:deepcopy(v) for k,v in feed.items() if k not in ('diagnostics','candle_audit','research')}
        result['output_schema']='scanner-phase16-summary-v1'
        for tf in ('1h','4h'):
            result[tf]={k:deepcopy(v) for k,v in feed.get(tf,{}).items() if k!='entry'}
        result['coverage']={tf:{**{k:v for k,v in c.items() if k!='missing'},'missing_count':len(c.get('missing',[]))} for tf,c in feed.get('coverage',{}).items()}
        return result
    if feed.get('strategy') != VERSION:
        return {'status':'not_ready','feed_ready':False,'strategy':VERSION,
                'message':'Waiting for a completed current-strategy snapshot',
                '1h':{'candidates':[]},'4h':{'candidates':[]},
                'special':{'formal':[],'approaching':[]},
                'market_state':{'triggered':False,'status':'unavailable'}}
    result={k:deepcopy(v) for k,v in feed.items() if k not in ('diagnostics','candle_audit','validation_samples','resonance','intersection','1h','4h','special','market_state','reserve')}
    for tf in ('1h','4h'):
        board=feed.get(tf,{})
        fresh=bool(feed.get('fresh_for_'+tf))
        result[tf]={'fresh':fresh,'candidate_count':board.get('candidate_count',0),
                    'candidates':[{k:deepcopy(v) for k,v in row.items() if k!='diagnostics'} for row in board.get('candidates',[])[:10]] if fresh else []}
    fresh=bool(feed.get('fresh_for_1h') and feed.get('fresh_for_4h'))
    result['special']=deepcopy(feed.get('special',{'formal':[],'approaching':[]})) if fresh else {'formal':[],'approaching':[],'status':'pending' if feed.get('pending') else 'stale'}
    result['market_state']=deepcopy(feed.get('market_state',{})) if fresh else {'triggered':False,'status':'pending' if feed.get('pending') else 'stale'}
    result['coverage']={tf:{**{k:v for k,v in c.items() if k!='missing'}, 'missing_count':len(c.get('missing',[]))} for tf,c in feed.get('coverage',{}).items()}
    result['reserve']={tf:{**deepcopy(feed.get('reserve',{}).get(tf,{})), 'candidates':deepcopy(feed.get('reserve',{}).get(tf,{}).get('candidates',[])[:10]) if feed.get('fresh_for_'+tf) else []} for tf in ('1h','4h')}
    if 'intersection' in feed:
        result['intersection']=sorted({r['symbol'] for r in result['1h']['candidates']} & {r['symbol'] for r in result['4h']['candidates']})
    for name in ('formal','approaching'):
        result['special'][name]=result['special'].get(name,[])[:10]
    result['output_schema']='scanner-summary-v3'
    return result
