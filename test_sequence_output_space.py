import json
from copy import deepcopy
import importlib.util
import subprocess
import pytest
from structure_sequence import features
import strategy_latest as s
from test_strategy_latest import frame, launch
from scan_summary import compact_scan_feed
from test_scan_summary import sample

FIELDS={'trend_duration_bars','ema15_lead_bars','pullback_quality','ma_defense','swing_structure','reexpansion_quality','extension_atr','overextended'}


def test_computed_sequence_missing_swing_stays_null():
    seq=features(frame(rise=.1))
    assert FIELDS <= seq.keys()
    assert seq['trend_duration_bars']==seq['trend_duration']
    assert seq['ema15_lead_bars']==seq['ema15_lead_duration']
    assert seq['extension_atr']==seq['pullback']['overextension_atr']
    assert seq['overextended']==(seq['extension_atr']>3)
    assert seq['swing_structure']==dict(hh=None,hl=None,broken=None,status=None)
    assert all(v is None for v in seq['ma_defense']['hl'].values())
    assert {'ema15','sma30','sma45','hl'} <= seq['ma_defense'].keys()
    json.dumps(seq,allow_nan=False)


def test_real_open_and_measured_features_and_unclipped_space():
    d=frame(rise=.1)
    d.loc[99,'close']=d.high.max()+1
    opened=features(d)['upside_space']
    assert opened['open_space']==1 and opened['remaining_atr'] is None
    assert opened['ranking_key']==[1,0] and opened['score'] is None
    d.loc[70,'high']=1000
    measured=features(d)['upside_space']
    assert measured['open_space']==0 and measured['remaining_atr']>6
    assert measured['ranking_key']==[0,measured['remaining_atr']]
    assert opened['ranking_key']>measured['ranking_key']


@pytest.mark.parametrize('tf',['1h','4h'])
def test_rank_space_stays_below_every_higher_priority_and_sorts_measured(monkeypatch,tf):
    frames={'1h':launch(frame()),'4h':launch(frame(tf='4h')),'1d':frame(tf='1d',rise=.01)}
    real=s.features
    chosen=[1,0]
    def patched(d):
        seq=real(d);seq['upside_space']['ranking_key']=chosen.copy();return seq
    monkeypatch.setattr(s,'features',patched)
    opened,_=s.qualify('OPEN',tf,frames)
    chosen[:]=[0,100]
    measured,_=s.qualify('MEASURED',tf,frames)
    chosen[:]=[0,7]
    small,_=s.qualify('SMALL',tf,frames)
    idx=opened['ranking_policy'].index('upside_space')
    assert opened['ranking_key']>measured['ranking_key']>small['ranking_key']
    for i in range(idx):
        stronger=deepcopy(measured);stronger['ranking_key'][i]+=.001
        assert stronger['ranking_key']>opened['ranking_key']
    assert opened['ranking_key'][:idx]==measured['ranking_key'][:idx]


def test_api_and_summary_sequence_roundtrip(monkeypatch):
    import main
    from fastapi.testclient import TestClient
    frames={'1h':launch(frame()),'4h':launch(frame(tf='4h')),'1d':frame(tf='1d',rise=.01)}
    row,_=s.qualify('X','1h',frames)
    feed=sample();feed['1h']['candidates']=[row]
    monkeypatch.setattr(main.scan_snapshot,'feed',lambda:deepcopy(feed))
    with TestClient(main.app) as client:
        for path in ('/scan/feed','/scan/feed/summary'):
            response=client.get(path)
            assert response.status_code==200
            actual=response.json()['1h']['candidates'][0]
            assert FIELDS <= actual['sequence'].keys()
            assert actual['sequence']==row['sequence']
            assert actual['ranking_key']==row['ranking_key']


def test_confirmed_higher_low_defense_reports_breach():
    d=frame(rise=.1)
    d.loc[70,'low']=90
    d.loc[90,'low']=95
    intact=features(d)
    assert intact['swing_structure']['hl'] is True
    assert intact['ma_defense']['hl']['price']==95
    assert intact['ma_defense']['hl']['recovered'] is True
    d.loc[99,['close','low']]=[94,93]
    broken=features(d)
    assert broken['swing_structure']['status']=='broken'
    assert broken['ma_defense']['hl']['close_breaches']==1
    assert broken['ma_defense']['hl']['recovered'] is False
