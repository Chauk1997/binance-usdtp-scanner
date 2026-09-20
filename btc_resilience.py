"""Closed operation-candle comparison and lexicographic ranking."""
import math

DURATIONS = {"1h": 3600000, "4h": 14400000}
BTC_LOW = 0.20
COIN_HIGH = 0.80
FIELDS = ("timeframe", "candle", "btc_candle", "relative_btc_resilience",
          "ranking_key", "auxiliary_score")


def candle_metrics(bar, timeframe, expected_open):
    if not bar:
        return None
    try:
        start, end = int(bar["open_time"]), int(bar["close_time"])
        o, h, l, c = (float(bar[k]) for k in ("open", "high", "low", "close"))
        if (start != expected_open or end != start + DURATIONS[timeframe] - 1
                or not all(math.isfinite(v) and v > 0 for v in (o, h, l, c))
                or not l <= min(o, c) <= max(o, c) <= h):
            return None
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    return dict(timeframe=timeframe, open_time=start, close_time=end,
                open=o, high=h, low=l, close=c, body_return=(c-o)/o,
                close_position=(c-l)/(h-l) if h > l else None)


def compare_candles(coin, btc):
    result = dict(status="unavailable", tier=None, score=None, verdict="unavailable")
    if coin is None or btc is None:
        return {**result, "reason": "missing_invalid_or_stale_candle"}
    if (coin["timeframe"], coin["open_time"], coin["close_time"]) != (btc["timeframe"], btc["open_time"], btc["close_time"]):
        return {**result, "reason": "time_mismatch"}
    if coin["close_position"] is None or btc["close_position"] is None:
        return {**result, "reason": "zero_range"}
    cr, br = coin["body_return"], btc["body_return"]
    cp, bp = coin["close_position"], btc["close_position"]
    if br >= 0:
        tier, verdict = 0, "btc_not_down"
    elif cr > 0 and bp <= BTC_LOW and cp >= COIN_HIGH:
        tier, verdict = 3, "highest"
    elif cr > 0 and cp > bp:
        tier, verdict = 2, "strong"
    elif cr > br and cp > bp:
        tier, verdict = 1, "resilient"
    elif cr < br and cp < bp:
        tier, verdict = -1, "weaker"
    else:
        tier, verdict = 0, "neutral"
    # Each tier occupies a disjoint interval; candle strength breaks ties.
    detail = ((cp-bp) + math.tanh(100*(cr-br))) / 4 if tier else 0
    return dict(status="available", tier=tier, score=round(tier+detail, 8),
                verdict=verdict, reason=verdict,
                thresholds={"btc_close_position_max": BTC_LOW,
                            "coin_close_position_min": COIN_HIGH})


def rank_items(items, timeframe, read_bars, now_ms):
    """Run before truncation; never fall back to daily vs-BTC returns."""
    expected = (int(now_ms)//DURATIONS[timeframe]-1)*DURATIONS[timeframe]
    def latest(symbol):
        bars = read_bars(symbol, timeframe) or []
        for bar in reversed(bars):
            if str(bar.get("open_time")) == str(expected):
                return candle_metrics(bar, timeframe, expected)
        return None
    btc = latest("BTCUSDT")
    for item in items:
        coin = latest(item["symbol"])
        relative = compare_candles(coin, btc)
        auxiliary = float((item.get("derivatives_context") or {}).get("adjustment") or 0)
        structure = float(item.get("structure_score") or 0)
        # Unknown comparison earns no bonus, like neutral; status remains explicit.
        key = [structure, relative["score"] or 0, auxiliary]
        item.update(timeframe=timeframe, candle=coin, btc_candle=btc,
                    relative_btc_resilience=relative, auxiliary_score=auxiliary,
                    ranking_key=key)
        # Legacy scalar is display-only; ranking_key is authoritative.
        item["final_score" if "final_score" in item else "watch_score"] = round(sum(key), 8)
        item["strength_component"] = relative["score"]
    items.sort(key=lambda x: x["symbol"])
    items.sort(key=lambda x: x["ranking_key"], reverse=True)
    return items


def projection(item):
    return {key: item.get(key) for key in FIELDS}
