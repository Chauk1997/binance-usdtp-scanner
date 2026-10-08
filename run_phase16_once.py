"""Explicit one-shot scan. No notification or cloud provisioning."""
import os
os.environ['SCANNER_SCHEDULER_ENABLED']='0'
os.environ['SCANNER_STRATEGY']='phase16'
import asyncio
import json
import sys
import main

async def run():
    result=await main.scan_snapshot.run(main._build_complete_snapshot,'formal')
    summary={k:result.get(k) for k in ('status','strategy','partial_scan','universe_total','attempted','scanned','missing','elapsed_seconds','feed_ready','stage','reason')}
    summary['boards']={tf:{k:result.get(tf,{}).get(k) for k in ('candidate_count','stage_counts','scan_time_utc','coverage')} for tf in ('1h','4h')}
    print(json.dumps(summary,ensure_ascii=False,indent=2))
    return 0 if result.get('strategy')=='SCANNER_V5.4_PHASE16' and result.get('status')=='complete' else 1

if __name__=='__main__':sys.exit(asyncio.run(run()))
