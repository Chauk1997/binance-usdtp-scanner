"""Presentation only. Never resurrect old strategy snapshots or intersection."""
from copy import deepcopy
from scanner_contract import VERSION


def compact_scan_feed(feed):
    if feed.get('strategy') != VERSION:
        return {'status':'not_ready','feed_ready':False,'strategy':VERSION,
                'message':'Waiting for a completed current-strategy snapshot',
                '1h':{'candidates':[]},'4h':{'candidates':[]},
                'special':{'formal':[],'approaching':[]},
                'market_state':{'triggered':False,'status':'unavailable'}}
    result={k:deepcopy(v) for k,v in feed.items() if k not in ('diagnostics','validation_samples','resonance','intersection','1h','4h','special','market_state')}
    for tf in ('1h','4h'):
        board=feed.get(tf,{})
        fresh=bool(feed.get('fresh_for_'+tf))
        result[tf]={'fresh':fresh,'candidate_count':board.get('candidate_count',0),
                    'candidates':[{k:deepcopy(v) for k,v in row.items() if k!='diagnostics'} for row in board.get('candidates',[])[:10]] if fresh else []}
    fresh=bool(feed.get('fresh_for_1h') and feed.get('fresh_for_4h'))
    result['special']=deepcopy(feed.get('special',{'formal':[],'approaching':[]})) if fresh else {'formal':[],'approaching':[],'status':'stale'}
    result['market_state']=deepcopy(feed.get('market_state',{})) if fresh else {'triggered':False,'status':'stale'}
    result['coverage']={tf:{**{k:v for k,v in c.items() if k!='missing'}, 'missing_count':len(c.get('missing',[]))} for tf,c in feed.get('coverage',{}).items()}
    result['output_schema']='scanner-summary-v2'
    return result
