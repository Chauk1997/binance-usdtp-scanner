"""Versioned higher-timeframe evidence; all inputs must be closed candles."""
import math

VERSION = 'V5.6_INTEGRATED_CONTINUATION'
POLICY_1H = ['daily_background_quality.quality_rank', 'four_hour_continuation_quality.score',
             'structure_quality.score', 'high_compression_reexpansion_quality.score',
             'key_structure_quality', 'upside_space.score',
             'relative_btc_resilience.score', 'capital_confirmation.score', 'auxiliary_score']
POLICY_4H = ['daily_background_quality.quality_rank', 'daily_continuation_quality.score',
             'structure_quality.score', 'high_compression_reexpansion_quality.score',
             'key_structure_quality', 'upside_space.score',
             'relative_btc_resilience.score', 'capital_confirmation.score', 'auxiliary_score']
FIELDS = ('auxiliary_evidence', 'daily_background_quality', 'four_hour_background_quality',
          'higher_tf_quality', 'key_candle', 'structure_stage', 'key_structure_quality',
          'ranking_policy', 'daily_continuation_quality', 'four_hour_continuation_quality',
          'high_compression_reexpansion_quality', 'structure_quality', 'upside_space', 'capital_confirmation')


def background_quality(df):
    if df is None or len(df) < 60:
        return dict(state='insufficient_history', rank=-1, eligible_4h=False,
                    hard_exclude=True, reason='need_60_closed_bars')
    ma = df[['ema15', 'sma30', 'sma45']].astype(float)
    slopes = ma / ma.shift(5) - 1
    gaps = ma[['ema15', 'sma30']].copy()
    gaps['ema15'] = (ma.ema15 - ma.sma30) / ma.sma45
    gaps['sma30'] = (ma.sma30 - ma.sma45) / ma.sma45
    bull = (ma.ema15 > ma.sma30) & (ma.sma30 > ma.sma45)
    bear = (ma.ema15 < ma.sma30) & (ma.sma30 < ma.sma45)
    spread = (ma.ema15 - ma.sma45) / ma.sma45
    expanding_up = (spread > spread.shift(3)) & (ma.ema15.pct_change(5) > ma.sma45.pct_change(5))
    expanding_down = (spread < spread.shift(3)) & (ma.ema15.pct_change(5) < ma.sma45.pct_change(5))
    bull_trend = bull & (slopes > 0).all(axis=1) & expanding_up
    bear_trend = bear & (slopes < 0).all(axis=1) & expanding_down
    # Require a sustained prior regime, and never carry an old bull through
    # a more recent bear regime. This separates bull consolidation from bases.
    def last_regime(mask):
        positions = [i for i in range(max(2, len(df)-60), len(df)-1)
                     if bool(mask.iloc[i-2:i+1].all())]
        return positions[-1] if positions else -1
    prior_bull, prior_bear = last_regime(bull_trend), last_regime(bear)
    values = ma.iloc[-1]
    slope = slopes.iloc[-1]
    gap = gaps.iloc[-1]
    finite = all(math.isfinite(float(x)) for x in [*values, *slope, *gap])
    if not finite:
        return dict(state='invalid_ma', rank=-1, eligible_4h=False, hard_exclude=True)
    recent_bear = prior_bear > prior_bull
    forming = (values.ema15 > values.sma30 and values.sma30 >= values.sma45 * .9975
               and bool((slope > 0).all()) and bool(expanding_up.iloc[-1])
               and float(slope.ema15) > float(slopes.ema15.iloc[-4]))
    preserved = (float(df.close.iloc[-1]) >= values.sma45 * .97
                 and values.sma30 >= values.sma45 * .995 and slope.sma45 >= -.002)
    if bool(bear_trend.iloc[-1]):
        state, rank = 'bearish_divergence', 0
    elif bool(bull_trend.iloc[-1]):
        state, rank = ('bottom_reversal', 2) if recent_bear else ('bullish_divergence', 4)
    elif forming:
        state, rank = 'bottom_reversal', 2
    elif prior_bull >= 0 and prior_bull > prior_bear and preserved:
        state, rank = 'bullish_consolidation', 3
    else:
        state, rank = 'bearish_base_or_unconfirmed', 1
    return dict(state=state, rank=rank, eligible_4h=rank >= 2,
                hard_exclude=rank == 0,
                ema15=float(values.ema15), sma30=float(values.sma30), sma45=float(values.sma45),
                slopes_5={k: float(v) for k,v in slope.items()},
                gaps={k: float(v) for k,v in gap.items()},
                upward_divergence=bool(expanding_up.iloc[-1]),
                downward_divergence=bool(expanding_down.iloc[-1]),
                prior_bull_bars_ago=len(df)-1-prior_bull if prior_bull >= 0 else None,
                prior_bear_bars_ago=len(df)-1-prior_bear if prior_bear >= 0 else None,
                close_time=int(df.close_time.iloc[-1]) if 'close_time' in df else None,
                thresholds={'slope_bars':5, 'gap_growth_bars':3, 'history_bars':60,
                            'regime_confirmation_bars':3, 'forming_sma30_tolerance':.0025})


def rank_background(items, timeframe):
    """One uniform lexicographic policy; no cohorts, reserved slots or profiles."""
    policy = POLICY_1H if timeframe == '1h' else POLICY_4H
    for item in items:
        item.pop('structure_cohort', None)
        item['ranking_policy'] = policy
        item['ranking_key'] = []
        for path in policy:
            value = item
            for part in path.split('.'):
                value = value.get(part) if isinstance(value, dict) else None
            item['ranking_key'].append(float(value) if value is not None else 0)
    items.sort(key=lambda x: x['symbol'])
    items.sort(key=lambda x: x['ranking_key'], reverse=True)
    return items
