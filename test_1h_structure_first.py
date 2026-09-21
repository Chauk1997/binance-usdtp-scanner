from copy import deepcopy
from background_quality import POLICY_1H, POLICY_4H, rank_background, VERSION, VERSION_4H
from test_background_quality import row
import main


def test_every_1h_dimension_dominates_all_later_dimensions():
    expected = ['structure_quality.score', 'high_compression_reexpansion_quality.score',
                'daily_background_quality.quality_rank', 'four_hour_continuation_quality.score',
                'key_structure_quality', 'upside_space.score', 'relative_btc_resilience.score',
                'capital_confirmation.score', 'auxiliary_score']
    assert POLICY_1H == expected
    def put(item, path, value):
        parts = path.split('.')
        for part in parts[:-1]:
            item = item.setdefault(part, {})
        item[parts[-1]] = value
    for index, path in enumerate(expected):
        winner, loser = {'symbol': 'Z'}, {'symbol': 'A'}
        for j, field in enumerate(expected):
            put(winner, field, 1 if j == index else 0)
            put(loser, field, 999 if j > index else 0)
        rows = rank_background([loser, winner], '1h')
        assert rows[0]['symbol'] == 'Z'


def test_4h_policy_and_keys_remain_v56():
    expected = ['daily_background_quality.quality_rank', 'daily_continuation_quality.score',
                'structure_quality.score', 'high_compression_reexpansion_quality.score',
                'key_structure_quality', 'upside_space.score', 'relative_btc_resilience.score',
                'capital_confirmation.score', 'auxiliary_score']
    assert POLICY_4H == expected
    a = row('A', 12, 1); b = row('B', 7, 4)
    rows = rank_background([a, b], '4h')
    assert [x['symbol'] for x in rows] == ['B', 'A']
    feed = main.build_scan_feed({'status':'complete','1h':{},'4h':{}})
    assert feed['strategy'] == VERSION == 'V5.7_1H_STRUCTURE_FIRST'
    assert feed['strategy_by_timeframe']['4h'] == VERSION_4H == 'V5.6_INTEGRATED_CONTINUATION'
    assert feed['ranking_policy_by_timeframe'] == {'1h': POLICY_1H, '4h': POLICY_4H}
