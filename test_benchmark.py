from copy import deepcopy
from benchmark import gate


def valid():
    return dict(status='complete', feed_ready=True, universe_total=2, universe={'sampled': False},
                coverage={tf: dict(complete=True, symbols=2, scanned=2, missing=[]) for tf in ('1h','4h','1d')},
                fresh_for_1h=True, fresh_for_4h=True, fresh_for_1d=True,
                **{'1h': {}, '4h': {}, 'special': {}, 'market_state': {}})


def test_gate_rejects_failed_and_partial_rounds():
    assert not gate(valid())
    assert gate({'status': 'stopped'})
    for tf in ('1h','4h','1d'):
        feed = deepcopy(valid()); feed['coverage'][tf]['scanned'] = 1
        assert gate(feed)
        feed = deepcopy(valid()); feed['fresh_for_'+tf] = False
        assert gate(feed)
    feed = valid(); feed['universe']['sampled'] = True
    assert gate(feed)
