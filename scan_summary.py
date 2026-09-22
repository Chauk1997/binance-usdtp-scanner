"""Read-only, bounded presentation of an already ranked completed snapshot."""
from copy import deepcopy

METADATA = '''status scanner generated_at_utc elapsed_seconds strategy strategy_by_timeframe
ranking_policy ranking_policy_by_timeframe qualification_policy universe_policy latest_closed_1h_open_time
latest_closed_1h_close_time latest_closed_4h_open_time latest_closed_4h_close_time
scan_started_at_utc freshness_version feed_ready scan_complete cache_mode
freshness_timezone stale_reasons expected_closed_1h expected_closed_4h
fresh_for_1h fresh_for_4h stale message'''.split()


def pick(value, fields):
    return {key: deepcopy(value[key]) for key in fields if key in value}


def summary_row(row, rank):
    result = pick(row, ('symbol', 'ranking_key', 'structure_stage'))
    result['rank'] = rank
    # Scores map positionally to ranking_policy_by_timeframe, without repeating
    # the same score under nine long nested keys for every candidate.
    result['context'] = {
        'daily': row.get('daily_background_quality', {}).get('state'),
        'background_4h': row.get('four_hour_background_quality', {}).get('state'),
        'compression': row.get('high_compression_reexpansion_quality', {}).get('state'),
        'continuation_4h': row.get('four_hour_continuation_quality', {}).get('state'),
        'continuation_1d': row.get('daily_continuation_quality', {}).get('state'),
    }
    result['key_candle'] = pick(row.get('key_candle', {}), ('passed', 'new_key_candle', 'label'))
    result['key_candle'].update(pick(row.get('key_candle', {}).get('latest') or {}, ('bars_ago', 'volume_vs_prev', 'volume_vs_ma24', 'ema15_pull_pct')))
    result['upside_space'] = pick(row.get('upside_space', {}), ('status', 'distance_pct', 'nearest_resistance'))
    result['btc_resilience'] = pick(row.get('relative_btc_resilience', {}), ('status', 'verdict', 'reason'))
    result['capital_status'] = row.get('capital_confirmation', {}).get('status')
    result['auxiliary_evidence'] = pick(row.get('auxiliary_evidence', {}), ('oi_delta_1h_pct', 'oi_delta_4h_pct', 'cvd_proxy_6h', 'taker_buy_sell_ratio_6h', 'funding_rate_pct'))
    return result


def compact_scan_feed(feed):
    """Take Top 10 without sorting, filtering, reranking, or changing freshness."""
    result = pick(feed, METADATA)
    result.update(output_schema='scanner-summary-v1', limit=10, diagnostics_included=False,
                  ranking_key_legend='Values correspond in order to ranking_policy_by_timeframe; preserve source order.')
    result['coverage'] = {}
    for tf in ('1h', '4h'):
        coverage = feed.get('coverage', {}).get(tf, {})
        result['coverage'][tf] = pick(coverage, ('complete', 'symbols'))
        result['coverage'][tf]['missing_count'] = len(coverage.get('missing', []))
        board = feed.get(tf, {})
        # Old snapshots without candidates must not masquerade as complete pools.
        rows = board.get('candidates', board.get('entry', []))
        result[tf] = {'candidate_count': len(rows) if 'candidates' in board else None,
                      'source': 'candidates' if 'candidates' in board else 'entry',
                      'candidates': [summary_row(row, i + 1) for i, row in enumerate(rows[:10])] if feed.get('fresh_for_' + tf) else [],
                      'fresh': feed.get('fresh_for_' + tf, False)}
    four = {row['symbol']: row['rank'] for row in result['4h']['candidates']}
    result['intersection'] = {
        'scope': '1h_top10_and_4h_top10',
        'fresh': bool(feed.get('fresh_for_1h') and feed.get('fresh_for_4h')),
        'candidates': [{'symbol': row['symbol'], 'rank_1h': row['rank'], 'rank_4h': four[row['symbol']]}
                       for row in result['1h']['candidates'] if row['symbol'] in four],
    }
    # Preserve the independent legacy resonance ranking as a separate board.
    resonance = feed.get('resonance', []) if result['intersection']['fresh'] else []
    result['resonance'] = {'count': len(resonance), 'top10': [pick(row, ('symbol', 'score', 'structure', 'structure_1h', 'structure_4h', 'daily_pct', 'vs_btc_pct', 'derivative_adjustment')) for row in resonance[:10]]}
    return result
