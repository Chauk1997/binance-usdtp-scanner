import asyncio
import copy
import json
from datetime import datetime
from unittest.mock import patch
import pytest
from scan_summary import compact_scan_feed
from scan_freshness import freshness
from strategy_latest import VERSION


def sample():
    def row(i):
        return {'symbol': f'S{i}', 'ranking_key': [100-i, i], 'structure_quality': {'score':100-i}, 'diagnostics': 'x'*10000}
    return {'strategy':VERSION,'special':{'formal':[], 'approaching':[]},'status':'complete', 'fresh_for_1h':True, 'fresh_for_4h':True,
            'coverage': {'1h':{'complete':True, 'symbols':521, 'missing':[]}},
            '1h':{'candidates':[row(i) for i in range(30)]},
            '4h':{'candidates':[row(i) for i in range(5,30)]},
            'validation_samples':{'diagnostics':'x'*1000000}, 'resonance':[]}


def test_bounded_order_independence_and_no_mutation():
    feed=sample(); before=copy.deepcopy(feed)
    result=compact_scan_feed(feed)
    assert feed==before
    for tf in ('1h','4h'):
        assert [r['symbol'] for r in result[tf]['candidates']]==[r['symbol'] for r in feed[tf]['candidates'][:10]]
        assert [r['ranking_key'] for r in result[tf]['candidates']]==[r['ranking_key'] for r in feed[tf]['candidates'][:10]]
    assert 'intersection' not in result and 'resonance' not in result
    assert len(json.dumps(result))<20000
    assert 'diagnostics' not in json.dumps(result).replace('diagnostics_included','')


@pytest.mark.parametrize('state', ['not_ready','running','stale','complete'])
def test_states_and_empty_boards(state):
    feed={'strategy':VERSION,'status':state, 'fresh_for_1h':False, 'fresh_for_4h':False, 'stale':True, 'stale_reasons':['missing bars']}
    result=compact_scan_feed(feed)
    assert all(result[k]==v for k,v in feed.items())
    assert result['special']['formal']==[]
    assert result['1h']['candidates']==[]
    assert result['1h']['candidate_count'] == 0


def test_incomplete_coverage_is_bounded():
    feed=sample(); feed['coverage']['1h'].update(complete=False, missing=['S']*521)
    result=compact_scan_feed(feed)
    assert result['coverage']['1h']=={'complete':False,'symbols':521,'missing_count':521}


def test_api_read_only_and_full_endpoint_unchanged(monkeypatch):
    import main
    from fastapi.testclient import TestClient
    feed=sample()
    monkeypatch.setattr(main.scan_snapshot,'feed',lambda:copy.deepcopy(feed))
    with patch.object(main,'_build_complete_snapshot',side_effect=AssertionError):
        with TestClient(main.app) as client:
            assert client.get('/scan/feed').json()==feed
            assert client.get('/scan/feed/summary').json()==compact_scan_feed(feed)


def test_mcp_tool_result(monkeypatch):
    import mcp_server
    import httpx
    feed=sample()
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self,url):
            assert url.endswith('/scan/feed')
            return httpx.Response(200,json=feed,request=httpx.Request('GET',url))
    monkeypatch.setattr(mcp_server.httpx,'Client',Client)
    assert mcp_server.get_scan_feed()==compact_scan_feed(feed)
    result=asyncio.run(mcp_server.mcp.call_tool('get_scan_feed',{}))
    # Inspect actual SDK serialization, not just the Python function.
    encoded=json.dumps(result,default=lambda x:x.model_dump())
    assert len(encoded)<40000
    assert 'S14' in encoded and 'S29' not in encoded


def test_request_time_freshness_survives_projection():
    from scan_freshness import expected_bar
    at=int(datetime.fromisoformat('2026-09-22T08:07:00+08:00').timestamp()*1000)
    feed=sample()
    for tf in ('1h','4h'):
        bar=expected_bar(tf,at)
        feed['coverage'][tf]={'complete':True,'symbols':521,'missing':[]}
        feed.update({f'latest_closed_{tf}_{k}':v for k,v in bar.items()})
        for row in feed[tf]['candidates']:
            row.update(candle=bar,btc_candle=bar)
    for offset,one,four in ((0,True,True),(3600000,False,True),(14400000,False,False)):
        checks=freshness(feed,at+offset)
        output=compact_scan_feed({**feed,**checks})
        assert output['fresh_for_1h'] is one
        assert output['fresh_for_4h'] is four
        assert 'intersection' not in output
    feed['coverage']['1h']['complete']=False
    assert compact_scan_feed({**feed,**freshness(feed,at)})['fresh_for_1h'] is False
