import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
from fastapi import FastAPI

app = FastAPI(
    title="Binance USDT.P Scanner",
    version="0.3.0",
)

BINANCE_BASE = "https://fapi.binance.com"

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

SYMBOL_CACHE = CACHE_DIR / "symbols.json"

KLINE_LIMIT = 250

# 刻意保守，避免再次觸發 Binance 限制
MAX_CONCURRENCY = 3

# 每批之間稍微休息
BATCH_SIZE = 20
BATCH_SLEEP = 1.0

# exchangeInfo 快取 6 小時
SYMBOL_CACHE_SECONDS = 6 * 60 * 60

# 418 / 429 時停止整輪初始化
RATE_LIMITED = False


EXCLUDED_SYMBOLS = {
    "USDCUSDT",
    "FDUSDUSDT",
    "TUSDUSDT",
    "USDPUSDT",
    "DAIUSDT",
    "BTCDOMUSDT",
}


@app.get("/")
async def root():
    return {
        "service": "binance-usdtp-scanner",
        "version": "0.3.0",
        "status": "online",
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "0.3.0",
        "rate_limited": RATE_LIMITED,
        "time_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def cache_file(symbol, interval):
    return CACHE_DIR / f"{symbol}_{interval}.json"


def read_json(path):
    try:
        return json.loads(
            path.read_text()
        )
    except Exception:
        return None


def write_json(path, data):
    temp = path.with_suffix(
        path.suffix + ".tmp"
    )

    temp.write_text(
        json.dumps(
            data,
            separators=(",", ":"),
        )
    )

    temp.replace(path)


async def safe_get(
    client,
    url,
    params=None,
):
    global RATE_LIMITED

    if RATE_LIMITED:
        return {
            "_error": "scanner_rate_limited"
        }

    try:
        r = await client.get(
            url,
            params=params,
            timeout=30,
        )

        if r.status_code in (418, 429):
            RATE_LIMITED = True

            retry_after = r.headers.get(
                "retry-after"
            )

            return {
                "_error": "binance_rate_limit",
                "status_code": r.status_code,
                "retry_after": retry_after,
            }

        r.raise_for_status()

        return {
            "_data": r.json(),
            "_weight": r.headers.get(
                "x-mbx-used-weight-1m"
            ),
        }

    except Exception as e:
        return {
            "_error": "request_failed",
            "detail": str(e),
        }


async def get_symbols(client):
    now = time.time()

    if SYMBOL_CACHE.exists():
        age = (
            now
            - SYMBOL_CACHE.stat().st_mtime
        )

        if age < SYMBOL_CACHE_SECONDS:
            cached = read_json(
                SYMBOL_CACHE
            )

            if cached:
                return cached

    result = await safe_get(
        client,
        f"{BINANCE_BASE}/fapi/v1/exchangeInfo",
    )

    if "_error" in result:
        # API失敗時，如果舊快取存在，
        # 仍優先使用舊 Universe。
        cached = read_json(
            SYMBOL_CACHE
        )

        if cached:
            return cached

        raise RuntimeError(result)

    symbols = []

    for x in result["_data"]["symbols"]:
        symbol = x.get(
            "symbol",
            "",
        )

        if (
            x.get("quoteAsset") == "USDT"
            and x.get("contractType")
            == "PERPETUAL"
            and x.get("status")
            == "TRADING"
            and symbol.endswith("USDT")
            and symbol.isascii()
            and symbol
            not in EXCLUDED_SYMBOLS
        ):
            symbols.append(symbol)

    symbols = sorted(
        set(symbols)
    )

    write_json(
        SYMBOL_CACHE,
        symbols,
    )

    return symbols


async def download_klines(
    client,
    symbol,
    interval,
    semaphore,
):
    async with semaphore:
        result = await safe_get(
            client,
            f"{BINANCE_BASE}/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "limit": KLINE_LIMIT,
            },
        )

        if "_error" in result:
            return {
                "symbol": symbol,
                "interval": interval,
                "status": "failed",
                "error": result,
            }

        raw = result["_data"]

        if len(raw) < 200:
            return {
                "symbol": symbol,
                "interval": interval,
                "status":
                    "insufficient_history",
                "bars": len(raw),
            }

        # 只保存我們後續真正會使用的欄位
        cleaned = []

        for k in raw:
            cleaned.append({
                "open_time": k[0],
                "open": k[1],
                "high": k[2],
                "low": k[3],
                "close": k[4],
                "volume": k[5],
                "close_time": k[6],
                "quote_volume": k[7],
                "trades": k[8],
                "taker_buy_base": k[9],
                "taker_buy_quote": k[10],
            })

        write_json(
            cache_file(
                symbol,
                interval,
            ),
            cleaned,
        )

        return {
            "symbol": symbol,
            "interval": interval,
            "status": "ok",
            "bars": len(cleaned),
            "weight":
                result.get("_weight"),
        }


async def initialize_interval(
    client,
    symbols,
    interval,
):
    global RATE_LIMITED

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENCY
    )

    results = []

    for start in range(
        0,
        len(symbols),
        BATCH_SIZE,
    ):
        if RATE_LIMITED:
            break

        batch = symbols[
            start:start + BATCH_SIZE
        ]

        tasks = [
            download_klines(
                client,
                symbol,
                interval,
                semaphore,
            )
            for symbol in batch
        ]

        batch_results = (
            await asyncio.gather(
                *tasks
            )
        )

        results.extend(
            batch_results
        )

        if not RATE_LIMITED:
            await asyncio.sleep(
                BATCH_SLEEP
            )

    return results


@app.get("/cache/status")
async def cache_status():
    files = list(
        CACHE_DIR.glob("*_*.json")
    )

    one_h = [
        x for x in files
        if x.name.endswith(
            "_1h.json"
        )
    ]

    four_h = [
        x for x in files
        if x.name.endswith(
            "_4h.json"
        )
    ]

    one_d = [
        x for x in files
        if x.name.endswith(
            "_1d.json"
        )
    ]

    return {
        "version": "0.3.0",
        "1h_cached": len(one_h),
        "4h_cached": len(four_h),
        "1d_cached": len(one_d),
        "rate_limited": RATE_LIMITED,
    }


@app.get("/cache/init")
async def cache_init():
    global RATE_LIMITED

    # 新的一輪允許重新嘗試
    RATE_LIMITED = False

    async with httpx.AsyncClient() as client:
        symbols = await get_symbols(
            client
        )

        one_h = (
            await initialize_interval(
                client,
                symbols,
                "1h",
            )
        )

        if RATE_LIMITED:
            return {
                "status":
                    "stopped_rate_limit",
                "total_symbols":
                    len(symbols),
                "1h_completed":
                    sum(
                        x["status"] == "ok"
                        for x in one_h
                    ),
                "message":
                    "Binance rate limit detected. Scanner stopped automatically.",
            }

        four_h = (
            await initialize_interval(
                client,
                symbols,
                "4h",
            )
        )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "total_symbols":
            len(symbols),

        "1h_ok":
            sum(
                x["status"] == "ok"
                for x in one_h
            ),

        "1h_insufficient":
            sum(
                x["status"]
                == "insufficient_history"
                for x in one_h
            ),

        "1h_failed":
            sum(
                x["status"] == "failed"
                for x in one_h
            ),

        "4h_ok":
            sum(
                x["status"] == "ok"
                for x in four_h
            ),

        "4h_insufficient":
            sum(
                x["status"]
                == "insufficient_history"
                for x in four_h
            ),

        "4h_failed":
            sum(
                x["status"] == "failed"
                for x in four_h
            ),

        "rate_limited":
            RATE_LIMITED,
    }

@app.get("/cache/init/1d")
async def cache_init_1d():
    global RATE_LIMITED

    RATE_LIMITED = False

    async with httpx.AsyncClient() as client:
        symbols = read_json(SYMBOL_CACHE)

        if not symbols:
            symbols = await get_symbols(client)

        one_d = await initialize_interval(
            client,
            symbols,
            "1d",
        )

    return {
        "status": (
            "stopped_rate_limit"
            if RATE_LIMITED
            else "complete"
        ),
        "total_symbols": len(symbols),
        "1d_ok": sum(
            x["status"] == "ok"
            for x in one_d
        ),
        "1d_insufficient": sum(
            x["status"] == "insufficient_history"
            for x in one_d
        ),
        "1d_failed": sum(
            x["status"] == "failed"
            for x in one_d
        ),
        "rate_limited": RATE_LIMITED,
    }
def load_dataframe(
    symbol,
    interval,
):
    path = cache_file(
        symbol,
        interval,
    )

    data = read_json(path)

    if not data:
        return None

    df = pd.DataFrame(data)

    numeric = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base",
        "taker_buy_quote",
    ]

    for col in numeric:
        df[col] = pd.to_numeric(
            df[col]
        )

    df["ema15"] = (
        df["close"]
        .ewm(
            span=15,
            adjust=False,
        )
        .mean()
    )

    df["sma30"] = (
        df["close"]
        .rolling(30)
        .mean()
    )

    df["sma45"] = (
        df["close"]
        .rolling(45)
        .mean()
    )

    df["sma200"] = (
        df["close"]
        .rolling(200)
        .mean()
    )

    df["volume_ma24"] = (
        df["volume"]
        .rolling(24)
        .mean()
    )

    return df


# =========================================================
# V3.1 LOCAL STRATEGY SCANNER
# Step 1: 1H MA Hard Gate
# Higher TF: 4H bearish Hard Veto
# Step 2: Key Candle Hard Gate
# =========================================================


def pct_slope(series, bars=5):
    if len(series) <= bars:
        return 0.0

    old = float(series.iloc[-1 - bars])
    new = float(series.iloc[-1])

    if old == 0:
        return 0.0

    return (new - old) / old


def analyze_1h_ma_structure(df):
    """
    第一層只看均線/價格趨勢。
    不使用成交量、K棒、漲幅、衍生品。
    """

    # 使用最後一根已收盤K
    closed = df.iloc[:-1]

    if len(closed) < 200:
        return {
            "passed": False,
            "reason": "insufficient_history",
        }

    last = closed.iloc[-1]

    price = float(last["close"])
    ema15 = float(last["ema15"])
    sma30 = float(last["sma30"])
    sma45 = float(last["sma45"])
    sma200 = float(last["sma200"])

    ema_slope_5 = pct_slope(
        closed["ema15"], 5
    )
    sma30_slope_5 = pct_slope(
        closed["sma30"], 5
    )
    sma45_slope_5 = pct_slope(
        closed["sma45"], 5
    )

    bullish_order = (
        ema15 > sma30 > sma45
    )

    bullish_slopes = (
        ema_slope_5 > 0
        and sma30_slope_5 > 0
        and sma45_slope_5 > 0
    )

    # A：已形成多頭趨勢
    mature_bull = (
        bullish_order
        and bullish_slopes
        and price > sma45
    )

    # B：第一段多頭已出現，
    # 現在處於回補/整理，準備第二段。
    recent_bull = False

    lookback = min(
        48,
        len(closed),
    )

    recent = closed.iloc[-lookback:]

    for _, row in recent.iterrows():
        if (
            row["ema15"] > row["sma30"]
            and row["sma30"] > row["sma45"]
        ):
            recent_bull = True
            break

    # 結構不能已經明顯破壞
    structure_preserved = (
        sma30 >= sma45 * 0.995
        and sma45_slope_5 >= -0.002
        and price >= sma45 * 0.97
    )

    second_leg_candidate = (
        recent_bull
        and structure_preserved
        and not mature_bull
    )

    passed = (
        mature_bull
        or second_leg_candidate
    )

    if mature_bull:
        structure = "mature_bull"
    elif second_leg_candidate:
        structure = "second_leg_candidate"
    else:
        structure = "reject"

    return {
        "passed": passed,
        "structure": structure,
        "price": price,
        "ema15": ema15,
        "sma30": sma30,
        "sma45": sma45,
        "sma200": sma200,
        "ema15_slope_5": round(
            ema_slope_5, 6
        ),
        "sma30_slope_5": round(
            sma30_slope_5, 6
        ),
        "sma45_slope_5": round(
            sma45_slope_5, 6
        ),
    }


def analyze_4h_hard_veto(df):
    """
    1H 操作的高週期 Hard Veto。

    只有 4H 明確：
    EMA15 < SMA30 < SMA45
    且三條均線都向下
    才硬排除。

    盤整/糾結/壓縮不硬排除。
    """

    closed = df.iloc[:-1]

    if len(closed) < 200:
        return {
            "hard_veto": False,
            "reason": "insufficient_history",
        }

    last = closed.iloc[-1]

    ema15 = float(last["ema15"])
    sma30 = float(last["sma30"])
    sma45 = float(last["sma45"])

    ema_slope = pct_slope(
        closed["ema15"], 5
    )
    sma30_slope = pct_slope(
        closed["sma30"], 5
    )
    sma45_slope = pct_slope(
        closed["sma45"], 5
    )

    bearish_order = (
        ema15 < sma30 < sma45
    )

    downward = (
        ema_slope < 0
        and sma30_slope < 0
        and sma45_slope < 0
    )

    hard_veto = (
        bearish_order
        and downward
    )

    return {
        "hard_veto": hard_veto,
        "ema15": ema15,
        "sma30": sma30,
        "sma45": sma45,
        "ema15_slope_5": round(
            ema_slope, 6
        ),
        "sma30_slope_5": round(
            sma30_slope, 6
        ),
        "sma45_slope_5": round(
            sma45_slope, 6
        ),
    }


def find_key_candles(
    df,
    lookback=48,
):
    """
    已確認的關鍵K客觀條件：

    1. 成交量 >= 前一根 2.5倍
    2. 成交量 > 關鍵K之前24根平均量
    3. 多方K
    4. 有上影時，實體 >= 整根K的1/2
    5. 收盤產生向上位移
    6. EMA15 被往上拉

    搜尋最近48根已收盤K。
    """

    closed = df.iloc[:-1].copy()

    if len(closed) < 50:
        return {
            "passed": False,
            "count": 0,
            "latest": None,
            "candidates": [],
        }

    candidates = []

    last_index = len(closed) - 1
    start = max(
        25,
        last_index - lookback + 1,
    )

    for i in range(
        start,
        last_index + 1,
    ):
        row = closed.iloc[i]
        prev = closed.iloc[i - 1]

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        v = float(row["volume"])
        pv = float(prev["volume"])

        previous_close = float(
            prev["close"]
        )

        candle_range = h - l
        body = c - o

        if candle_range <= 0:
            continue

        # 關鍵K必須是向上的有效實體
        if body <= 0:
            continue

        if pv <= 0:
            continue

        volume_vs_prev = v / pv

        if volume_vs_prev < 2.5:
            continue

        volume_ma24_before = float(
            closed["volume"]
            .iloc[i - 24:i]
            .mean()
        )

        if (
            volume_ma24_before <= 0
            or v <= volume_ma24_before
        ):
            continue

        volume_vs_ma24 = (
            v / volume_ma24_before
        )

        upper_wick = h - c

        body_ratio = (
            body / candle_range
        )

        # 有上影線時實體至少整體1/2
        if (
            upper_wick > 0
            and body_ratio < 0.5
        ):
            continue

        # 必須真的向上位移
        if c <= previous_close:
            continue

        displacement = (
            c / previous_close - 1
        )

        ema_before = float(
            prev["ema15"]
        )

        ema_after = float(
            row["ema15"]
        )

        if ema_after <= ema_before:
            continue

        ema_pull = (
            ema_after / ema_before - 1
            if ema_before
            else 0
        )

        bars_ago = (
            last_index - i
        )

        candidates.append({
            "bars_ago": int(
                bars_ago
            ),

            "open_time": int(
                row["open_time"]
            ),

            "open": o,
            "high": h,
            "low": l,
            "close": c,

            "volume_vs_prev": round(
                volume_vs_prev, 3
            ),

            "volume_vs_ma24": round(
                volume_vs_ma24, 3
            ),

            "body_ratio": round(
                body_ratio, 3
            ),

            "price_displacement_pct":
                round(
                    displacement * 100,
                    4,
                ),

            "ema15_pull_pct":
                round(
                    ema_pull * 100,
                    4,
                ),
        })

    if not candidates:
        return {
            "passed": False,
            "count": 0,
            "latest": None,
            "candidates": [],
        }

    latest = min(
        candidates,
        key=lambda x: x["bars_ago"],
    )

    return {
        "passed": True,
        "count": len(candidates),
        "latest": latest,
        "candidates": candidates,
    }


@app.get("/scan/1h")
async def local_scan_1h():
    """
    完全讀本機 cache。
    此 endpoint 不向 Binance 發送請求。
    """

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:
        return {
            "status": "no_symbol_cache",
        }

    step1_reject = []
    four_h_veto = []
    insufficient = []
    key_reject = []
    passed = []

    for symbol in symbols:

        df1h = load_dataframe(
            symbol,
            "1h",
        )

        df4h = load_dataframe(
            symbol,
            "4h",
        )

        if (
            df1h is None
            or df4h is None
        ):
            insufficient.append(
                symbol
            )
            continue

        one_h = (
            analyze_1h_ma_structure(
                df1h
            )
        )

        if not one_h["passed"]:
            step1_reject.append(
                symbol
            )
            continue

        four_h = (
            analyze_4h_hard_veto(
                df4h
            )
        )

        if four_h["hard_veto"]:
            four_h_veto.append(
                symbol
            )
            continue

        key = find_key_candles(
            df1h,
            lookback=48,
        )

        if not key["passed"]:
            key_reject.append(
                symbol
            )
            continue

        passed.append({
            "symbol": symbol,
            "ma_structure": one_h,
            "higher_tf": four_h,
            "key_candle": key,
        })

    step1_survivors = (
        len(four_h_veto)
        + len(key_reject)
        + len(passed)
    )

    after_4h_veto = (
        len(key_reject)
        + len(passed)
    )

    return {
        "status": "complete",

        "source":
            "local_cache_only",

        "generated_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "total_symbols":
            len(symbols),

        "insufficient_data":
            len(insufficient),

        "step1_pass_before_4h_veto":
            step1_survivors,

        "step1_reject":
            len(step1_reject),

        "4h_hard_veto":
            len(four_h_veto),

        "after_4h_veto":
            after_4h_veto,

        "key_candle_pass":
            len(passed),

        "key_candle_reject":
            len(key_reject),

        "passed_symbols": [
            x["symbol"]
            for x in passed
        ],

        "passed": passed,
    }


# =========================================================
# V3.2 ENTRY STRUCTURE
# Key candle -> MA pullback -> stop-fall -> current location
# =========================================================


def candle_features(df, i):
    row = df.iloc[i]
    prev = df.iloc[i - 1]

    o = float(row["open"])
    h = float(row["high"])
    l = float(row["low"])
    c = float(row["close"])
    v = float(row["volume"])

    po = float(prev["open"])
    pc = float(prev["close"])

    candle_range = h - l
    body = abs(c - o)

    if candle_range <= 0:
        candle_range = 1e-12

    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)

    volume_ma24 = float(
        df["volume"]
        .iloc[max(0, i - 24):i]
        .mean()
    )

    relative_volume = (
        v / volume_ma24
        if volume_ma24 > 0
        else 0
    )

    bullish = c > o

    # 多頭吞沒
    bullish_engulfing = (
        bullish
        and pc < po
        and o <= pc
        and c >= po
    )

    # 長下影 / Hammer 類
    hammer = (
        bullish
        and lower_wick >= body * 1.5
        and lower_wick
        >= candle_range * 0.4
    )

    return {
        "bullish": bullish,
        "bullish_engulfing":
            bullish_engulfing,
        "hammer": hammer,
        "relative_volume":
            relative_volume,
        "lower_wick_ratio":
            lower_wick / candle_range,
        "body_ratio":
            body / candle_range,
        "close": c,
        "low": l,
        "high": h,
    }


def analyze_entry_structure(
    df,
    key_result,
):
    """
    分析最近一根有效關鍵K之後：
    - 是否推升
    - 是否回補均線
    - 回補哪條均線
    - 均線結構是否保持
    - 是否出現止跌訊號
    - 是否過度延伸
    """

    closed = df.iloc[:-1].copy()

    if len(closed) < 50:
        return {
            "status": "insufficient_data"
        }

    latest_key = key_result.get(
        "latest"
    )

    if not latest_key:
        return {
            "status": "no_key_candle"
        }

    bars_ago = int(
        latest_key["bars_ago"]
    )

    key_index = (
        len(closed) - 1 - bars_ago
    )

    if (
        key_index < 0
        or key_index >= len(closed)
    ):
        return {
            "status":
                "invalid_key_index"
        }

    key_row = closed.iloc[
        key_index
    ]

    key_high = float(
        key_row["high"]
    )

    key_low = float(
        key_row["low"]
    )

    key_close = float(
        key_row["close"]
    )

    after = closed.iloc[
        key_index + 1:
    ].copy()

    current = closed.iloc[-1]

    current_close = float(
        current["close"]
    )

    ema15 = float(
        current["ema15"]
    )

    sma30 = float(
        current["sma30"]
    )

    sma45 = float(
        current["sma45"]
    )

    # ---------------------------------
    # 1. 關鍵K後是否有進一步推升
    # ---------------------------------

    if len(after) > 0:
        highest_after = float(
            after["high"].max()
        )
    else:
        highest_after = key_high

    push_after_key_pct = (
        highest_after / key_close - 1
    ) * 100

    # ---------------------------------
    # 2. 是否回補均線
    # ---------------------------------

    touched_ema15 = False
    touched_sma30 = False
    touched_sma45 = False

    reclaimed_ema15 = False
    reclaimed_sma30 = False
    reclaimed_sma45 = False

    deepest_pullback = "none"

    if len(after) > 0:
        for _, row in after.iterrows():

            low = float(
                row["low"]
            )

            close = float(
                row["close"]
            )

            e15 = float(
                row["ema15"]
            )

            s30 = float(
                row["sma30"]
            )

            s45 = float(
                row["sma45"]
            )

            if low <= e15:
                touched_ema15 = True

                if close >= e15:
                    reclaimed_ema15 = True

            if low <= s30:
                touched_sma30 = True

                if close >= s30:
                    reclaimed_sma30 = True

            if low <= s45:
                touched_sma45 = True

                if close >= s45:
                    reclaimed_sma45 = True

    if touched_sma45:
        deepest_pullback = "sma45"

    elif touched_sma30:
        deepest_pullback = "sma30"

    elif touched_ema15:
        deepest_pullback = "ema15"

    # ---------------------------------
    # 3. 多頭均線結構是否仍健康
    # ---------------------------------

    ema_slope = pct_slope(
        closed["ema15"],
        5,
    )

    sma30_slope = pct_slope(
        closed["sma30"],
        5,
    )

    sma45_slope = pct_slope(
        closed["sma45"],
        5,
    )

    strict_bull_order = (
        ema15 > sma30 > sma45
    )

    # 回補過程允許短暫糾結，
    # 但中慢均線不可明顯破壞。
    structure_healthy = (
        sma30 >= sma45 * 0.995
        and sma45_slope >= -0.002
        and current_close
        >= sma45 * 0.97
    )

    # ---------------------------------
    # 4. 搜尋「回補後」止跌訊號
    # ---------------------------------

    stop_signal = None

    stop_signal_candidates = []

    search_start = max(
        key_index + 1,
        len(closed) - 12,
    )

    for i in range(
        search_start,
        len(closed),
    ):
        if i <= 0:
            continue

        f = candle_features(
            closed,
            i,
        )

        row = closed.iloc[i]

        e15 = float(
            row["ema15"]
        )

        s30 = float(
            row["sma30"]
        )

        s45 = float(
            row["sma45"]
        )

        # K棒必須出現在均線附近，
        # 不能離均線很遠還算止跌。
        distance_to_ma = min(
            abs(
                f["low"] / e15 - 1
            ),
            abs(
                f["low"] / s30 - 1
            ),
            abs(
                f["low"] / s45 - 1
            ),
        )

        near_ma = (
            distance_to_ma <= 0.02
        )

        if not near_ma:
            continue

        signal_type = None

        if f["bullish_engulfing"]:
            signal_type = (
                "bullish_engulfing"
            )

        elif f["hammer"]:
            signal_type = (
                "bullish_hammer"
            )

        # 收回 EMA15：
        # 當根最低碰到/跌破 EMA15，
        # 最後重新收在 EMA15 上。
        elif (
            f["bullish"]
            and f["low"] <= e15
            and f["close"] > e15
        ):
            signal_type = (
                "ema15_reclaim"
            )

        if signal_type:
            stop_signal_candidates.append({
                "type":
                    signal_type,

                "bars_ago":
                    len(closed)
                    - 1
                    - i,

                "relative_volume":
                    round(
                        f[
                            "relative_volume"
                        ],
                        3,
                    ),

                "lower_wick_ratio":
                    round(
                        f[
                            "lower_wick_ratio"
                        ],
                        3,
                    ),

                "close":
                    f["close"],
            })

    if stop_signal_candidates:
        # 優先最近的止跌訊號
        stop_signal = min(
            stop_signal_candidates,
            key=lambda x:
                x["bars_ago"],
        )

    # ---------------------------------
    # 5. 現價距均線
    # ---------------------------------

    distance_ema15_pct = (
        current_close / ema15 - 1
    ) * 100

    distance_sma30_pct = (
        current_close / sma30 - 1
    ) * 100

    distance_sma45_pct = (
        current_close / sma45 - 1
    ) * 100

    # 暫定客觀延伸標記。
    # 後續會拿實際案例再校準。
    overextended = (
        distance_ema15_pct > 6
        and distance_sma30_pct > 8
    )

    has_pullback = (
        touched_ema15
        or touched_sma30
        or touched_sma45
    )

    # ---------------------------------
    # 6. 狀態分類
    # ---------------------------------

    if not structure_healthy:
        trade_status = (
            "structure_damaged"
        )

    elif overextended:
        trade_status = (
            "過度延伸勿追"
        )

    elif not has_pullback:
        trade_status = (
            "等回補"
        )

    elif stop_signal is None:
        trade_status = (
            "等止跌K"
        )

    else:
        trade_status = (
            "可進場"
        )

    # ---------------------------------
    # 7. 初步品質分數
    # 只用結構，不是最終排名
    # ---------------------------------

    quality_score = 0.0

    if strict_bull_order:
        quality_score += 2.0

    if structure_healthy:
        quality_score += 2.0

    if touched_ema15:
        quality_score += 1.0

    if touched_sma30:
        quality_score += 0.5

    if stop_signal:
        quality_score += 2.0

        rv = float(
            stop_signal[
                "relative_volume"
            ]
        )

        # 止跌量越大越好
        quality_score += min(
            rv,
            3.0,
        )

    if overextended:
        quality_score -= 3.0

    return {
        "status": trade_status,

        "key_bars_ago":
            bars_ago,

        "key_high":
            key_high,

        "key_low":
            key_low,

        "push_after_key_pct":
            round(
                push_after_key_pct,
                3,
            ),

        "pullback": {
            "has_pullback":
                has_pullback,

            "deepest":
                deepest_pullback,

            "touched_ema15":
                touched_ema15,

            "touched_sma30":
                touched_sma30,

            "touched_sma45":
                touched_sma45,

            "reclaimed_ema15":
                reclaimed_ema15,

            "reclaimed_sma30":
                reclaimed_sma30,

            "reclaimed_sma45":
                reclaimed_sma45,
        },

        "ma_health": {
            "strict_bull_order":
                strict_bull_order,

            "structure_healthy":
                structure_healthy,

            "ema15_slope_5":
                round(
                    ema_slope,
                    6,
                ),

            "sma30_slope_5":
                round(
                    sma30_slope,
                    6,
                ),

            "sma45_slope_5":
                round(
                    sma45_slope,
                    6,
                ),
        },

        "stop_signal":
            stop_signal,

        "stop_signal_candidates":
            stop_signal_candidates,

        "distance_to_ma_pct": {
            "ema15":
                round(
                    distance_ema15_pct,
                    3,
                ),

            "sma30":
                round(
                    distance_sma30_pct,
                    3,
                ),

            "sma45":
                round(
                    distance_sma45_pct,
                    3,
                ),
        },

        "overextended":
            overextended,

        "structure_quality_score":
            round(
                quality_score,
                3,
            ),
    }


@app.get("/scan/1h/v32")
async def local_scan_1h_v32():

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:
        return {
            "status":
                "no_symbol_cache"
        }

    stats = {
        "total_symbols":
            len(symbols),

        "insufficient_data": 0,

        "step1_reject": 0,

        "4h_hard_veto": 0,

        "key_candle_reject": 0,

        "key_candle_pass": 0,

        "可進場": 0,

        "等回補": 0,

        "等止跌K": 0,

        "過度延伸勿追": 0,

        "structure_damaged": 0,
    }

    results = []

    for symbol in symbols:

        df1h = load_dataframe(
            symbol,
            "1h",
        )

        df4h = load_dataframe(
            symbol,
            "4h",
        )

        if (
            df1h is None
            or df4h is None
        ):
            stats[
                "insufficient_data"
            ] += 1
            continue

        one_h = (
            analyze_1h_ma_structure(
                df1h
            )
        )

        if not one_h["passed"]:
            stats[
                "step1_reject"
            ] += 1
            continue

        four_h = (
            analyze_4h_hard_veto(
                df4h
            )
        )

        if four_h["hard_veto"]:
            stats[
                "4h_hard_veto"
            ] += 1
            continue

        key = find_key_candles(
            df1h,
            lookback=48,
        )

        if not key["passed"]:
            stats[
                "key_candle_reject"
            ] += 1
            continue

        stats[
            "key_candle_pass"
        ] += 1

        entry = (
            analyze_entry_structure(
                df1h,
                key,
            )
        )

        trade_status = (
            entry["status"]
        )

        if trade_status in stats:
            stats[
                trade_status
            ] += 1

        results.append({
            "symbol": symbol,
            "status":
                trade_status,

            "structure_score":
                entry.get(
                    "structure_quality_score",
                    0,
                ),

            "ma_structure":
                one_h,

            "key_candle":
                key,

            "entry_structure":
                entry,
        })

    # 目前只是結構排序，
    # 還不是正式Top10。
    results.sort(
        key=lambda x:
            x["structure_score"],
        reverse=True,
    )

    return {
        "status":
            "complete",

        "source":
            "local_cache_only",

        "ranking_stage":
            "structure_only_not_final",

        "generated_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "stats":
            stats,

        "results":
            results,
    }


# =========================================================
# V3.2.1 CURRENT ENTRY TIMING CALIBRATION
# =========================================================


def analyze_current_entry(
    df,
    key_result,
):
    closed = df.iloc[:-1].copy()

    latest_key = key_result.get("latest")

    if latest_key is None:
        return {
            "status": "no_key_candle"
        }

    key_bars_ago = int(
        latest_key["bars_ago"]
    )

    key_index = (
        len(closed) - 1 - key_bars_ago
    )

    current = closed.iloc[-1]

    current_close = float(
        current["close"]
    )

    current_ema15 = float(
        current["ema15"]
    )

    current_sma30 = float(
        current["sma30"]
    )

    current_sma45 = float(
        current["sma45"]
    )

    # -----------------------------------
    # 1. 找「最近一次」回補均線
    # -----------------------------------

    pullbacks = []

    for i in range(
        key_index + 1,
        len(closed),
    ):
        row = closed.iloc[i]

        low = float(row["low"])
        close = float(row["close"])

        ema15 = float(row["ema15"])
        sma30 = float(row["sma30"])
        sma45 = float(row["sma45"])

        touched = []

        if low <= ema15:
            touched.append("ema15")

        if low <= sma30:
            touched.append("sma30")

        if low <= sma45:
            touched.append("sma45")

        if touched:
            if "sma45" in touched:
                deepest = "sma45"
            elif "sma30" in touched:
                deepest = "sma30"
            else:
                deepest = "ema15"

            pullbacks.append({
                "index": i,

                "bars_ago":
                    len(closed) - 1 - i,

                "deepest":
                    deepest,

                "close":
                    close,

                "ema15":
                    ema15,

                "sma30":
                    sma30,

                "sma45":
                    sma45,
            })

    latest_pullback = (
        pullbacks[-1]
        if pullbacks
        else None
    )

    # -----------------------------------
    # 2. 只在最近回補後找止跌訊號
    # -----------------------------------

    valid_stop_signals = []

    if latest_pullback:
        search_start = max(
            latest_pullback["index"],
            len(closed) - 3,
        )

        for i in range(
            search_start,
            len(closed),
        ):
            if i <= 0:
                continue

            f = candle_features(
                closed,
                i,
            )

            row = closed.iloc[i]

            ema15 = float(
                row["ema15"]
            )

            sma30 = float(
                row["sma30"]
            )

            sma45 = float(
                row["sma45"]
            )

            # 必須真的靠近均線區
            distance = min(
                abs(
                    f["low"] / ema15 - 1
                ),
                abs(
                    f["low"] / sma30 - 1
                ),
                abs(
                    f["low"] / sma45 - 1
                ),
            )

            if distance > 0.02:
                continue

            signal_type = None

            if f["bullish_engulfing"]:
                signal_type = (
                    "bullish_engulfing"
                )

            elif f["hammer"]:
                signal_type = (
                    "bullish_hammer"
                )

            elif (
                f["bullish"]
                and f["low"] <= ema15
                and f["close"] > ema15
            ):
                signal_type = (
                    "ema15_reclaim"
                )

            if signal_type:
                valid_stop_signals.append({
                    "type":
                        signal_type,

                    "bars_ago":
                        len(closed) - 1 - i,

                    "relative_volume":
                        round(
                            f[
                                "relative_volume"
                            ],
                            3,
                        ),

                    "lower_wick_ratio":
                        round(
                            f[
                                "lower_wick_ratio"
                            ],
                            3,
                        ),

                    "close":
                        f["close"],
                })

    latest_stop = (
        valid_stop_signals[-1]
        if valid_stop_signals
        else None
    )

    # -----------------------------------
    # 3. 現在均線健康度
    # -----------------------------------

    ema_slope = pct_slope(
        closed["ema15"],
        5,
    )

    sma30_slope = pct_slope(
        closed["sma30"],
        5,
    )

    sma45_slope = pct_slope(
        closed["sma45"],
        5,
    )

    structure_healthy = (
        current_sma30
        >= current_sma45 * 0.995

        and sma45_slope >= -0.002

        and current_close
        >= current_sma45 * 0.97
    )

    strict_bull_order = (
        current_ema15
        > current_sma30
        > current_sma45
    )

    # -----------------------------------
    # 4. 現在距離均線
    # -----------------------------------

    distance_ema15 = (
        current_close
        / current_ema15
        - 1
    ) * 100

    distance_sma30 = (
        current_close
        / current_sma30
        - 1
    ) * 100

    distance_sma45 = (
        current_close
        / current_sma45
        - 1
    ) * 100

    overextended = (
        distance_ema15 > 6
        and distance_sma30 > 8
    )

    # -----------------------------------
    # 5. 判斷「目前階段」
    # -----------------------------------

    if not structure_healthy:
        status = "structure_damaged"

    elif overextended:
        status = "過度延伸勿追"

    elif latest_pullback is None:
        status = "等回補"

    elif latest_stop is not None:
        # 因 search_start 限制，
        # 這裡的訊號一定只會是
        # 最近 0~2 根已收盤K。
        status = "可進場"

    else:
        status = "等止跌K"

    # -----------------------------------
    # 6. 結構分數（仍非最終排名）
    # -----------------------------------

    score = 0.0

    if strict_bull_order:
        score += 2.0

    if structure_healthy:
        score += 2.0

    if latest_pullback:
        score += 1.0

        if (
            latest_pullback["deepest"]
            == "sma30"
        ):
            score += 0.5

        elif (
            latest_pullback["deepest"]
            == "sma45"
        ):
            score += 0.25

    if latest_stop:
        score += 2.0

        score += min(
            float(
                latest_stop[
                    "relative_volume"
                ]
            ),
            3.0,
        )

        # 越新的訊號越好
        if latest_stop["bars_ago"] == 0:
            score += 1.0

        elif latest_stop["bars_ago"] == 1:
            score += 0.5

    if overextended:
        score -= 3.0

    return {
        "status": status,

        "latest_pullback":
            latest_pullback,

        "latest_stop_signal":
            latest_stop,

        "valid_stop_signals":
            valid_stop_signals,

        "structure_healthy":
            structure_healthy,

        "strict_bull_order":
            strict_bull_order,

        "distance_to_ma_pct": {
            "ema15":
                round(
                    distance_ema15,
                    3,
                ),

            "sma30":
                round(
                    distance_sma30,
                    3,
                ),

            "sma45":
                round(
                    distance_sma45,
                    3,
                ),
        },

        "overextended":
            overextended,

        "current_entry_score":
            round(
                score,
                3,
            ),
    }


def scan_one_symbol_v321(
    symbol,
):
    df1h = load_dataframe(
        symbol,
        "1h",
    )

    df4h = load_dataframe(
        symbol,
        "4h",
    )

    if (
        df1h is None
        or df4h is None
    ):
        return {
            "symbol": symbol,
            "stage": "insufficient_data",
        }

    one_h = (
        analyze_1h_ma_structure(
            df1h
        )
    )

    if not one_h["passed"]:
        return {
            "symbol": symbol,
            "stage": "step1_reject",
            "ma_structure": one_h,
        }

    four_h = (
        analyze_4h_hard_veto(
            df4h
        )
    )

    if four_h["hard_veto"]:
        return {
            "symbol": symbol,
            "stage": "4h_hard_veto",
            "ma_structure": one_h,
            "higher_tf": four_h,
        }

    key = find_key_candles(
        df1h,
        lookback=48,
    )

    if not key["passed"]:
        return {
            "symbol": symbol,
            "stage":
                "key_candle_reject",
            "ma_structure": one_h,
            "higher_tf": four_h,
        }

    entry = (
        analyze_current_entry(
            df1h,
            key,
        )
    )

    return {
        "symbol": symbol,
        "stage": "qualified",
        "status":
            entry["status"],
        "score":
            entry[
                "current_entry_score"
            ],
        "ma_structure":
            one_h,
        "higher_tf":
            four_h,
        "key_candle":
            key,
        "current_entry":
            entry,
    }


@app.get("/scan/1h/v321")
async def local_scan_1h_v321():

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:
        return {
            "status":
                "no_symbol_cache"
        }

    stats = {
        "total_symbols":
            len(symbols),

        "insufficient_data": 0,
        "step1_reject": 0,
        "4h_hard_veto": 0,
        "key_candle_reject": 0,

        "qualified": 0,

        "entry_now": 0,
        "wait_pullback": 0,
        "wait_stop_signal": 0,
        "overextended": 0,
        "structure_damaged": 0,
    }

    results = []

    for symbol in symbols:
        result = (
            scan_one_symbol_v321(
                symbol
            )
        )

        stage = result["stage"]

        if stage != "qualified":
            if stage in stats:
                stats[stage] += 1

            continue

        stats["qualified"] += 1

        status = result["status"]

        if status == "可進場":
            stats["entry_now"] += 1

        elif status == "等回補":
            stats[
                "wait_pullback"
            ] += 1

        elif status == "等止跌K":
            stats[
                "wait_stop_signal"
            ] += 1

        elif status == "過度延伸勿追":
            stats[
                "overextended"
            ] += 1

        elif status == "structure_damaged":
            stats[
                "structure_damaged"
            ] += 1

        results.append(
            result
        )

    results.sort(
        key=lambda x:
            x.get("score", 0),
        reverse=True,
    )

    return {
        "status": "complete",

        "source":
            "local_cache_only",

        "ranking_stage":
            "current_entry_structure_not_final",

        "stats":
            stats,

        "results":
            results,
    }


@app.get("/scan/1h/symbol/{symbol}")
async def debug_one_symbol(
    symbol: str,
):
    symbol = symbol.upper()

    return scan_one_symbol_v321(
        symbol
    )


@app.get("/scan/1h/entry-now")
async def entry_now_1h():

    symbols = read_json(
        SYMBOL_CACHE
    )

    results = []

    for symbol in symbols:

        result = scan_one_symbol_v321(
            symbol
        )

        if (
            result.get("stage")
            == "qualified"
            and result.get("status")
            == "可進場"
        ):
            results.append(result)

    results.sort(
        key=lambda x:
            x.get("score", 0),
        reverse=True,
    )

    return {
        "count": len(results),

        "symbols": [
            x["symbol"]
            for x in results
        ],

        "results": results,
    }


# =========================================================
# V3.2.2 COMPACT ENTRY VALIDATION
# =========================================================


@app.get("/scan/1h/entry-now/compact")
async def entry_now_compact():

    symbols = read_json(
        SYMBOL_CACHE
    )

    results = []

    for symbol in symbols:

        result = scan_one_symbol_v321(
            symbol
        )

        if (
            result.get("stage")
            != "qualified"
            or result.get("status")
            != "可進場"
        ):
            continue

        key = result["key_candle"]["latest"]

        entry = result["current_entry"]

        pullback = entry.get(
            "latest_pullback"
        )

        stop = entry.get(
            "latest_stop_signal"
        )

        ma = result["ma_structure"]

        item = {
            "symbol":
                symbol,

            "score":
                result["score"],

            "ma_structure":
                ma.get("structure"),

            "price":
                ma.get("price"),

            "key_candle": {
                "bars_ago":
                    key.get("bars_ago"),

                "volume_vs_prev":
                    key.get(
                        "volume_vs_prev"
                    ),

                "volume_vs_ma24":
                    key.get(
                        "volume_vs_ma24"
                    ),

                "body_ratio":
                    key.get(
                        "body_ratio"
                    ),

                "displacement_pct":
                    key.get(
                        "price_displacement_pct"
                    ),
            },

            "pullback": {
                "bars_ago":
                    (
                        pullback.get(
                            "bars_ago"
                        )
                        if pullback
                        else None
                    ),

                "deepest":
                    (
                        pullback.get(
                            "deepest"
                        )
                        if pullback
                        else None
                    ),
            },

            "stop_signal": {
                "type":
                    (
                        stop.get("type")
                        if stop
                        else None
                    ),

                "bars_ago":
                    (
                        stop.get(
                            "bars_ago"
                        )
                        if stop
                        else None
                    ),

                "relative_volume":
                    (
                        stop.get(
                            "relative_volume"
                        )
                        if stop
                        else None
                    ),

                "lower_wick_ratio":
                    (
                        stop.get(
                            "lower_wick_ratio"
                        )
                        if stop
                        else None
                    ),
            },

            "distance_to_ma_pct":
                entry.get(
                    "distance_to_ma_pct"
                ),

            "strict_bull_order":
                entry.get(
                    "strict_bull_order"
                ),

            "structure_healthy":
                entry.get(
                    "structure_healthy"
                ),

            "overextended":
                entry.get(
                    "overextended"
                ),
        }

        results.append(item)

    results.sort(
        key=lambda x:
            x["score"],
        reverse=True,
    )

    return {
        "count":
            len(results),

        "note":
            "structure validation only; not final ranking",

        "results":
            results,
    }


# =========================================================
# V3.3 STOP SIGNAL QUALITY
# =========================================================


def grade_stop_signal(stop):
    """
    止跌訊號品質分級。

    核心原則：
    - 吞沒 / Hammer 本身型態較強
    - EMA15 reclaim 較弱
    - 相對成交量越大越好
    - 低量不直接 Hard Reject
    """

    if not stop:
        return {
            "grade": "NONE",
            "score": 0.0,
            "reason": "no_stop_signal",
        }

    signal_type = stop.get("type")

    rv = float(
        stop.get(
            "relative_volume",
            0
        )
    )

    lower_wick = float(
        stop.get(
            "lower_wick_ratio",
            0
        )
    )

    score = 0.0

    # -----------------------------
    # 型態基礎分
    # -----------------------------

    if signal_type == "bullish_engulfing":
        score += 4.0

    elif signal_type == "bullish_hammer":
        score += 3.5

        # Hammer 下影越明確越好
        if lower_wick >= 0.60:
            score += 0.5

    elif signal_type == "ema15_reclaim":
        score += 1.5

    # -----------------------------
    # 相對成交量
    # -----------------------------

    if rv >= 3.0:
        score += 3.0

    elif rv >= 2.0:
        score += 2.5

    elif rv >= 1.5:
        score += 2.0

    elif rv >= 1.0:
        score += 1.0

    elif rv >= 0.75:
        score += 0.5

    # -----------------------------
    # 新鮮度
    # -----------------------------

    bars_ago = int(
        stop.get(
            "bars_ago",
            99
        )
    )

    if bars_ago == 0:
        score += 1.0

    elif bars_ago == 1:
        score += 0.5

    # -----------------------------
    # Grade
    # -----------------------------

    if score >= 7.0:
        grade = "A"

    elif score >= 5.0:
        grade = "B"

    else:
        grade = "C"

    return {
        "grade": grade,
        "score": round(
            score,
            3
        ),
        "type": signal_type,
        "relative_volume": rv,
        "bars_ago": bars_ago,
    }


def scan_one_symbol_v33(symbol):

    base = scan_one_symbol_v321(
        symbol
    )

    if (
        base.get("stage")
        != "qualified"
    ):
        return base

    entry = base[
        "current_entry"
    ]

    stop = entry.get(
        "latest_stop_signal"
    )

    grade = grade_stop_signal(
        stop
    )

    base[
        "stop_quality"
    ] = grade

    # V3.3 結構品質分數
    #
    # 保留原本均線/回補結構分，
    # 再加入新的止跌品質。
    base_score = float(
        base.get(
            "score",
            0
        )
    )

    old_stop_bonus = 0.0

    if stop:
        old_stop_bonus += 2.0

        old_stop_bonus += min(
            float(
                stop.get(
                    "relative_volume",
                    0
                )
            ),
            3.0,
        )

        if (
            stop.get(
                "bars_ago"
            ) == 0
        ):
            old_stop_bonus += 1.0

        elif (
            stop.get(
                "bars_ago"
            ) == 1
        ):
            old_stop_bonus += 0.5

    # 移除舊止跌加分，
    # 換成 V3.3 分級分數。
    structure_base = (
        base_score
        - old_stop_bonus
    )

    v33_score = (
        structure_base
        + grade["score"]
    )

    base[
        "v33_structure_score"
    ] = round(
        v33_score,
        3
    )

    return base


@app.get("/scan/1h/v33/entry-now")
async def scan_v33_entry_now():

    symbols = read_json(
        SYMBOL_CACHE
    )

    results = []

    grade_stats = {
        "A": 0,
        "B": 0,
        "C": 0,
    }

    for symbol in symbols:

        result = (
            scan_one_symbol_v33(
                symbol
            )
        )

        if (
            result.get("stage")
            != "qualified"
            or result.get("status")
            != "可進場"
        ):
            continue

        grade = result[
            "stop_quality"
        ]["grade"]

        if grade in grade_stats:
            grade_stats[
                grade
            ] += 1

        results.append({
            "symbol":
                symbol,

            "score":
                result[
                    "v33_structure_score"
                ],

            "stop_quality":
                result[
                    "stop_quality"
                ],

            "ma_structure":
                result[
                    "ma_structure"
                ]["structure"],

            "strict_bull_order":
                result[
                    "current_entry"
                ][
                    "strict_bull_order"
                ],

            "latest_pullback":
                result[
                    "current_entry"
                ][
                    "latest_pullback"
                ],

            "distance_to_ma_pct":
                result[
                    "current_entry"
                ][
                    "distance_to_ma_pct"
                ],

            "key_candle":
                result[
                    "key_candle"
                ]["latest"],
        })

    results.sort(
        key=lambda x:
            x["score"],
        reverse=True,
    )

    return {
        "count":
            len(results),

        "grade_stats":
            grade_stats,

        "ranking_stage":
            "stop_quality_calibrated_not_final",

        "results":
            results,
    }


# =========================================================
# V3.4 TAIWAN 08:00 DAILY RETURN + BTC RELATIVE STRENGTH
# =========================================================


DAILY_CACHE = CACHE_DIR / "daily_strength.json"


def utc_day_start_ms():
    """
    台灣 08:00 = UTC 00:00。
    取得目前 UTC 日期 00:00 timestamp(ms)。
    """
    now = datetime.now(timezone.utc)

    start = now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    return int(
        start.timestamp() * 1000
    )


async def get_today_1h(
    client,
    symbol,
    semaphore,
):
    async with semaphore:

        result = await safe_get(
            client,
            f"{BINANCE_BASE}/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": "1h",
                "startTime":
                    utc_day_start_ms(),
                "limit": 30,
            },
        )

        if "_error" in result:
            return {
                "symbol": symbol,
                "status": "failed",
                "error": result,
            }

        raw = result["_data"]

        if not raw:
            return {
                "symbol": symbol,
                "status": "no_data",
            }

        bars = []

        now_ms = int(
            datetime.now(
                timezone.utc
            ).timestamp() * 1000
        )

        for k in raw:

            # 已收盤K才拿來做抗跌分析
            closed = (
                int(k[6]) < now_ms
            )

            bars.append({
                "open_time":
                    int(k[0]),

                "open":
                    float(k[1]),

                "high":
                    float(k[2]),

                "low":
                    float(k[3]),

                "close":
                    float(k[4]),

                "closed":
                    closed,
            })

        return {
            "symbol": symbol,
            "status": "ok",
            "bars": bars,
        }


def calculate_daily_strength(
    symbol_bars,
    btc_bars,
):
    """
    計算：
    1. 台灣08:00歸零當日漲幅
    2. 相對BTC當日強弱
    3. BTC下跌1H期間的抗跌程度
    """

    if (
        not symbol_bars
        or not btc_bars
    ):
        return None

    # UTC 00:00 第一根1H的 open
    # = 台灣08:00基準價格
    base_price = float(
        symbol_bars[0]["open"]
    )

    btc_base = float(
        btc_bars[0]["open"]
    )

    # 最新價格可使用目前最新K的 close
    current_price = float(
        symbol_bars[-1]["close"]
    )

    btc_current = float(
        btc_bars[-1]["close"]
    )

    if (
        base_price <= 0
        or btc_base <= 0
    ):
        return None

    daily_return = (
        current_price / base_price - 1
    ) * 100

    btc_daily_return = (
        btc_current / btc_base - 1
    ) * 100

    relative_vs_btc = (
        daily_return
        - btc_daily_return
    )

    # -----------------------------------
    # BTC 下跌時段抗跌
    # -----------------------------------

    symbol_by_time = {
        x["open_time"]: x
        for x in symbol_bars
        if x["closed"]
    }

    btc_down_periods = []

    resistance_values = []

    for btc in btc_bars:

        if not btc["closed"]:
            continue

        btc_open = float(
            btc["open"]
        )

        btc_close = float(
            btc["close"]
        )

        if btc_open <= 0:
            continue

        btc_ret = (
            btc_close / btc_open - 1
        ) * 100

        # 只分析 BTC 下跌的1H
        if btc_ret >= 0:
            continue

        coin = symbol_by_time.get(
            btc["open_time"]
        )

        if not coin:
            continue

        coin_open = float(
            coin["open"]
        )

        coin_close = float(
            coin["close"]
        )

        if coin_open <= 0:
            continue

        coin_ret = (
            coin_close
            / coin_open
            - 1
        ) * 100

        # 正值 = 比 BTC 抗跌
        relative = (
            coin_ret - btc_ret
        )

        resistance_values.append(
            relative
        )

        btc_down_periods.append({
            "open_time":
                btc["open_time"],

            "btc_return_pct":
                round(
                    btc_ret,
                    3
                ),

            "coin_return_pct":
                round(
                    coin_ret,
                    3
                ),

            "relative_pct":
                round(
                    relative,
                    3
                ),
        })

    if resistance_values:

        avg_resistance = (
            sum(resistance_values)
            / len(resistance_values)
        )

        resistant_count = sum(
            x >= 0
            for x in resistance_values
        )

        resistance_ratio = (
            resistant_count
            / len(
                resistance_values
            )
        )

    else:
        avg_resistance = 0.0
        resistance_ratio = 0.0

    # -----------------------------------
    # 相對強弱分數
    #
    # 不作 Hard Gate。
    # 最終排名加分用。
    # -----------------------------------

    strength_score = 0.0

    # 當日漲幅相對 BTC
    strength_score += max(
        -3.0,
        min(
            relative_vs_btc,
            5.0
        )
    )

    # BTC下跌時抗跌
    strength_score += max(
        -2.0,
        min(
            avg_resistance * 2,
            4.0
        )
    )

    # 多數BTC下跌時段都抗跌
    strength_score += (
        resistance_ratio * 2
    )

    return {
        "daily_return_pct":
            round(
                daily_return,
                3
            ),

        "btc_daily_return_pct":
            round(
                btc_daily_return,
                3
            ),

        "relative_vs_btc_pct":
            round(
                relative_vs_btc,
                3
            ),

        "btc_down_hours":
            len(
                resistance_values
            ),

        "avg_resistance_vs_btc_pct":
            round(
                avg_resistance,
                3
            ),

        "resistance_ratio":
            round(
                resistance_ratio,
                3
            ),

        "strength_score":
            round(
                strength_score,
                3
            ),

        "btc_down_periods":
            btc_down_periods,
    }


@app.get("/market/daily-strength/update")
async def update_daily_strength():

    global RATE_LIMITED

    RATE_LIMITED = False

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:
        return {
            "status":
                "no_symbol_cache"
        }

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENCY
    )

    downloaded = {}

    failed = []

    async with httpx.AsyncClient() as client:

        # 每批20個，沿用保守節奏
        for start in range(
            0,
            len(symbols),
            BATCH_SIZE,
        ):

            if RATE_LIMITED:
                break

            batch = symbols[
                start:
                start + BATCH_SIZE
            ]

            tasks = [
                get_today_1h(
                    client,
                    symbol,
                    semaphore,
                )
                for symbol in batch
            ]

            batch_results = (
                await asyncio.gather(
                    *tasks
                )
            )

            for result in batch_results:

                if (
                    result["status"]
                    == "ok"
                ):
                    downloaded[
                        result["symbol"]
                    ] = result["bars"]

                else:
                    failed.append(
                        result["symbol"]
                    )

            if not RATE_LIMITED:
                await asyncio.sleep(
                    BATCH_SLEEP
                )

    if RATE_LIMITED:

        return {
            "status":
                "stopped_rate_limit",

            "downloaded":
                len(downloaded),

            "failed":
                len(failed),

            "rate_limited":
                True,
        }

    btc_bars = downloaded.get(
        "BTCUSDT"
    )

    if not btc_bars:
        return {
            "status":
                "btc_data_missing",

            "downloaded":
                len(downloaded),

            "failed":
                len(failed),
        }

    strength = {}

    for symbol, bars in (
        downloaded.items()
    ):

        metrics = (
            calculate_daily_strength(
                bars,
                btc_bars,
            )
        )

        if metrics:
            strength[
                symbol
            ] = metrics

    payload = {
        "generated_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "taiwan_reset":
            "08:00 Asia/Taipei = 00:00 UTC",

        "symbols":
            strength,
    }

    write_json(
        DAILY_CACHE,
        payload,
    )

    return {
        "status":
            "complete",

        "downloaded":
            len(downloaded),

        "calculated":
            len(strength),

        "failed":
            len(failed),

        "rate_limited":
            False,
    }


@app.get("/scan/1h/v34/entry-now")
async def scan_v34_entry_now():

    daily = read_json(
        DAILY_CACHE
    )

    if not daily:
        return {
            "status":
                "daily_strength_cache_missing",

            "next":
                "/market/daily-strength/update",
        }

    daily_symbols = daily.get(
        "symbols",
        {}
    )

    symbols = read_json(
        SYMBOL_CACHE
    )

    results = []

    for symbol in symbols:

        result = (
            scan_one_symbol_v33(
                symbol
            )
        )

        if (
            result.get("stage")
            != "qualified"
            or result.get("status")
            != "可進場"
        ):
            continue

        strength = (
            daily_symbols.get(
                symbol
            )
        )

        if not strength:
            continue

        structure_score = float(
            result.get(
                "v33_structure_score",
                0
            )
        )

        strength_score = float(
            strength.get(
                "strength_score",
                0
            )
        )

        combined_score = (
            structure_score
            + strength_score
        )

        results.append({
            "symbol":
                symbol,

            "combined_score":
                round(
                    combined_score,
                    3
                ),

            "structure_score":
                structure_score,

            "stop_quality":
                result.get(
                    "stop_quality"
                ),

            "daily_strength":
                strength,
        })

    results.sort(
        key=lambda x:
            x["combined_score"],
        reverse=True,
    )

    return {
        "count":
            len(results),

        "daily_data_generated_at":
            daily.get(
                "generated_at_utc"
            ),

        "ranking_stage":
            "structure_plus_relative_strength_not_final",

        "results":
            results,
    }


# =========================================================
# V3.5 DERIVATIVES VALIDATION
# Only query V3.4 entry-now candidates
# =========================================================


DERIVATIVES_CACHE = CACHE_DIR / "derivatives_1h.json"


def last_item(data):
    if isinstance(data, list) and data:
        return data[-1]
    return None


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def derivatives_request(
    client,
    endpoint,
    params,
    semaphore,
):
    async with semaphore:

        result = await safe_get(
            client,
            endpoint,
            params=params,
        )

        if "_error" in result:
            return None

        return result.get("_data")


async def fetch_symbol_derivatives(
    client,
    symbol,
    semaphore,
):
    """
    USDⓈ-M Futures only.

    1H period is used because this is
    the 1H scanner validation layer.
    """

    tasks = [

        # OI history
        derivatives_request(
            client,
            f"{BINANCE_BASE}/futures/data/openInterestHist",
            {
                "symbol": symbol,
                "period": "1h",
                "limit": 6,
            },
            semaphore,
        ),

        # Taker buy / sell volume
        derivatives_request(
            client,
            f"{BINANCE_BASE}/futures/data/takerlongshortRatio",
            {
                "symbol": symbol,
                "period": "1h",
                "limit": 6,
            },
            semaphore,
        ),

        # Global account L/S
        derivatives_request(
            client,
            f"{BINANCE_BASE}/futures/data/globalLongShortAccountRatio",
            {
                "symbol": symbol,
                "period": "1h",
                "limit": 3,
            },
            semaphore,
        ),

        # Top trader POSITION L/S
        derivatives_request(
            client,
            f"{BINANCE_BASE}/futures/data/topLongShortPositionRatio",
            {
                "symbol": symbol,
                "period": "1h",
                "limit": 3,
            },
            semaphore,
        ),

        # Top trader ACCOUNT L/S
        derivatives_request(
            client,
            f"{BINANCE_BASE}/futures/data/topLongShortAccountRatio",
            {
                "symbol": symbol,
                "period": "1h",
                "limit": 3,
            },
            semaphore,
        ),

        # Current mark price + funding
        derivatives_request(
            client,
            f"{BINANCE_BASE}/fapi/v1/premiumIndex",
            {
                "symbol": symbol,
            },
            semaphore,
        ),
    ]

    (
        oi_hist,
        taker,
        global_ls,
        top_position,
        top_account,
        premium,
    ) = await asyncio.gather(*tasks)

    # ---------------------------------
    # OI / OI Delta
    # ---------------------------------

    oi_current = None
    oi_delta_1h = None
    oi_delta_4h = None

    if (
        isinstance(oi_hist, list)
        and len(oi_hist) >= 2
    ):

        oi_values = []

        for row in oi_hist:
            value = safe_float(
                row.get(
                    "sumOpenInterestValue"
                )
            )

            if value is not None:
                oi_values.append(value)

        if oi_values:

            oi_current = oi_values[-1]

            if (
                len(oi_values) >= 2
                and oi_values[-2] != 0
            ):
                oi_delta_1h = (
                    oi_values[-1]
                    / oi_values[-2]
                    - 1
                ) * 100

            if (
                len(oi_values) >= 5
                and oi_values[-5] != 0
            ):
                oi_delta_4h = (
                    oi_values[-1]
                    / oi_values[-5]
                    - 1
                ) * 100

    # ---------------------------------
    # CVD proxy
    #
    # Binance takerBuySell endpoint
    # gives buy/sell volume ratio.
    #
    # We calculate:
    # buy volume - sell volume
    # across recent 1H periods.
    # ---------------------------------

    cvd_proxy = None
    taker_buy_sell_ratio = None

    if isinstance(taker, list) and taker:

        cvd = 0.0
        buy_total = 0.0
        sell_total = 0.0

        for row in taker:

            buy = safe_float(
                row.get(
                    "buyVol"
                )
            )

            sell = safe_float(
                row.get(
                    "sellVol"
                )
            )

            if (
                buy is None
                or sell is None
            ):
                continue

            buy_total += buy
            sell_total += sell

            cvd += (
                buy - sell
            )

        cvd_proxy = cvd

        if sell_total > 0:
            taker_buy_sell_ratio = (
                buy_total
                / sell_total
            )

    # ---------------------------------
    # Ratios
    # ---------------------------------

    global_row = last_item(
        global_ls
    )

    top_position_row = last_item(
        top_position
    )

    top_account_row = last_item(
        top_account
    )

    global_ratio = (
        safe_float(
            global_row.get(
                "longShortRatio"
            )
        )
        if global_row
        else None
    )

    top_position_ratio = (
        safe_float(
            top_position_row.get(
                "longShortRatio"
            )
        )
        if top_position_row
        else None
    )

    top_account_ratio = (
        safe_float(
            top_account_row.get(
                "longShortRatio"
            )
        )
        if top_account_row
        else None
    )

    # ---------------------------------
    # Funding
    # premiumIndex gives latest funding
    # ---------------------------------

    funding_rate = None
    mark_price = None

    if isinstance(premium, dict):

        funding_rate = safe_float(
            premium.get(
                "lastFundingRate"
            )
        )

        mark_price = safe_float(
            premium.get(
                "markPrice"
            )
        )

    if funding_rate is not None:
        funding_pct = (
            funding_rate * 100
        )
    else:
        funding_pct = None

    # ---------------------------------
    # Lightweight validation score
    #
    # IMPORTANT:
    # Derivatives NEVER override chart.
    # This is validation only.
    # ---------------------------------

    score = 0.0

    # Rising OI supports move
    if oi_delta_1h is not None:

        if oi_delta_1h > 1:
            score += 1.0

        elif oi_delta_1h > 0:
            score += 0.5

        elif oi_delta_1h < -2:
            score -= 0.5

    # Taker buyers dominant
    if taker_buy_sell_ratio is not None:

        if taker_buy_sell_ratio >= 1.15:
            score += 1.0

        elif taker_buy_sell_ratio >= 1.0:
            score += 0.5

        elif taker_buy_sell_ratio < 0.85:
            score -= 0.5

    # Top trader position ratio
    if top_position_ratio is not None:

        if top_position_ratio > 1:
            score += 0.5

        elif top_position_ratio < 0.8:
            score -= 0.25

    # Top trader account ratio
    if top_account_ratio is not None:

        if top_account_ratio > 1:
            score += 0.5

        elif top_account_ratio < 0.8:
            score -= 0.25

    # Funding:
    # positive is not automatically bullish.
    # Excessive positive funding is crowded.
    if funding_pct is not None:

        if funding_pct > 0.10:
            score -= 1.0

        elif funding_pct > 0.05:
            score -= 0.5

        elif (
            funding_pct >= -0.03
            and funding_pct <= 0.03
        ):
            score += 0.25

    return {
        "symbol":
            symbol,

        "oi_value":
            (
                round(oi_current, 3)
                if oi_current is not None
                else None
            ),

        "oi_delta_1h_pct":
            (
                round(oi_delta_1h, 3)
                if oi_delta_1h is not None
                else None
            ),

        "oi_delta_4h_pct":
            (
                round(oi_delta_4h, 3)
                if oi_delta_4h is not None
                else None
            ),

        "cvd_proxy_6h":
            (
                round(cvd_proxy, 3)
                if cvd_proxy is not None
                else None
            ),

        "taker_buy_sell_ratio_6h":
            (
                round(
                    taker_buy_sell_ratio,
                    3
                )
                if taker_buy_sell_ratio
                is not None
                else None
            ),

        "funding_rate_pct":
            (
                round(funding_pct, 5)
                if funding_pct is not None
                else None
            ),

        "global_long_short_ratio":
            global_ratio,

        "top_position_long_short_ratio":
            top_position_ratio,

        "top_account_long_short_ratio":
            top_account_ratio,

        "mark_price":
            mark_price,

        "derivatives_score":
            round(score, 3),
    }


@app.get("/market/derivatives/update")
async def update_derivatives():

    global RATE_LIMITED

    RATE_LIMITED = False

    # Get V3.4 candidate list locally.
    daily = read_json(
        DAILY_CACHE
    )

    if not daily:
        return {
            "status":
                "daily_cache_missing"
        }

    daily_symbols = daily.get(
        "symbols",
        {}
    )

    all_symbols = read_json(
        SYMBOL_CACHE
    )

    candidates = []

    for symbol in all_symbols:

        result = (
            scan_one_symbol_v33(
                symbol
            )
        )

        if (
            result.get("stage")
            == "qualified"
            and result.get("status")
            == "可進場"
            and symbol in daily_symbols
        ):
            candidates.append(
                symbol
            )

    semaphore = asyncio.Semaphore(
        2
    )

    results = {}

    failed = []

    async with httpx.AsyncClient() as client:

        # Only ~8 candidates.
        # Sequential symbols = conservative.
        for symbol in candidates:

            if RATE_LIMITED:
                break

            try:

                data = (
                    await fetch_symbol_derivatives(
                        client,
                        symbol,
                        semaphore,
                    )
                )

                results[
                    symbol
                ] = data

            except Exception as exc:

                failed.append({
                    "symbol":
                        symbol,

                    "error":
                        str(exc),
                })

            await asyncio.sleep(
                0.5
            )

    payload = {
        "generated_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "period":
            "1h",

        "candidate_count":
            len(candidates),

        "symbols":
            results,
    }

    write_json(
        DERIVATIVES_CACHE,
        payload,
    )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "candidate_count":
            len(candidates),

        "downloaded":
            len(results),

        "failed":
            failed,

        "rate_limited":
            RATE_LIMITED,
    }


@app.get("/scan/1h/v35/entry-now")
async def scan_v35_entry_now():

    daily = read_json(
        DAILY_CACHE
    )

    derivatives = read_json(
        DERIVATIVES_CACHE
    )

    if not daily:
        return {
            "status":
                "daily_cache_missing"
        }

    if not derivatives:
        return {
            "status":
                "derivatives_cache_missing",

            "next":
                "/market/derivatives/update",
        }

    daily_symbols = daily.get(
        "symbols",
        {}
    )

    derivative_symbols = (
        derivatives.get(
            "symbols",
            {}
        )
    )

    all_symbols = read_json(
        SYMBOL_CACHE
    )

    results = []

    for symbol in all_symbols:

        base = (
            scan_one_symbol_v33(
                symbol
            )
        )

        if (
            base.get("stage")
            != "qualified"
            or base.get("status")
            != "可進場"
        ):
            continue

        strength = (
            daily_symbols.get(
                symbol
            )
        )

        derivative = (
            derivative_symbols.get(
                symbol
            )
        )

        if (
            not strength
            or not derivative
        ):
            continue

        structure_score = float(
            base.get(
                "v33_structure_score",
                0
            )
        )

        strength_score = float(
            strength.get(
                "strength_score",
                0
            )
        )

        derivative_score = float(
            derivative.get(
                "derivatives_score",
                0
            )
        )

        # Still diagnostic only.
        # NOT the final strategy ranking.
        diagnostic_score = (
            structure_score
            + strength_score
            + derivative_score
        )

        results.append({
            "symbol":
                symbol,

            "diagnostic_score":
                round(
                    diagnostic_score,
                    3
                ),

            "structure_score":
                structure_score,

            "stop_quality":
                base.get(
                    "stop_quality"
                ),

            "daily_return_pct":
                strength.get(
                    "daily_return_pct"
                ),

            "relative_vs_btc_pct":
                strength.get(
                    "relative_vs_btc_pct"
                ),

            "avg_resistance_vs_btc_pct":
                strength.get(
                    "avg_resistance_vs_btc_pct"
                ),

            "resistance_ratio":
                strength.get(
                    "resistance_ratio"
                ),

            "derivatives":
                derivative,
        })

    results.sort(
        key=lambda x:
            x["diagnostic_score"],
        reverse=True,
    )

    return {
        "count":
            len(results),

        "ranking_stage":
            "full_data_diagnostic_not_final",

        "results":
            results,
    }


# =========================================================
# V3.6 CONTEXTUAL DERIVATIVES INTERPRETATION
# =========================================================


def interpret_derivatives(
    derivative,
    daily_return_pct,
):
    """
    Derivatives = validation layer only.
    Never overrides chart structure.

    Output adjustment is deliberately small.
    """

    oi1 = safe_float(
        derivative.get("oi_delta_1h_pct")
    )

    oi4 = safe_float(
        derivative.get("oi_delta_4h_pct")
    )

    taker = safe_float(
        derivative.get("taker_buy_sell_ratio_6h")
    )

    funding = safe_float(
        derivative.get("funding_rate_pct")
    )

    global_ls = safe_float(
        derivative.get("global_long_short_ratio")
    )

    top_pos = safe_float(
        derivative.get(
            "top_position_long_short_ratio"
        )
    )

    top_acc = safe_float(
        derivative.get(
            "top_account_long_short_ratio"
        )
    )

    adjustment = 0.0
    flags = []

    # ---------------------------------
    # OI interpretation
    # ---------------------------------

    if oi1 is not None and oi4 is not None:

        if (
            daily_return_pct > 0
            and oi1 > 0
            and oi4 > 0
        ):
            adjustment += 0.40
            flags.append(
                "price_up_oi_expanding"
            )

        elif (
            daily_return_pct > 0
            and oi1 < 0
            and oi4 < 0
        ):
            adjustment -= 0.20
            flags.append(
                "price_up_oi_contracting"
            )

        elif (
            oi1 > 0
            and oi4 < 0
        ):
            flags.append(
                "oi_short_term_recovery"
            )

        elif (
            oi1 < 0
            and oi4 > 0
        ):
            flags.append(
                "oi_short_term_cooling"
            )

    # ---------------------------------
    # Taker flow
    # ---------------------------------

    if taker is not None:

        if taker >= 1.15:
            adjustment += 0.40
            flags.append(
                "aggressive_buying"
            )

        elif taker >= 1.02:
            adjustment += 0.20
            flags.append(
                "buy_flow_positive"
            )

        elif taker <= 0.85:
            adjustment -= 0.30
            flags.append(
                "aggressive_selling"
            )

        elif taker < 0.98:
            adjustment -= 0.10
            flags.append(
                "sell_flow_dominant"
            )

    # ---------------------------------
    # Funding
    #
    # Very positive = crowded longs.
    # Very negative while price strong
    # can be squeeze fuel, not automatic bearish.
    # ---------------------------------

    if funding is not None:

        if funding >= 0.10:
            adjustment -= 0.50
            flags.append(
                "crowded_positive_funding"
            )

        elif funding >= 0.05:
            adjustment -= 0.25
            flags.append(
                "elevated_positive_funding"
            )

        elif funding <= -0.10:

            if daily_return_pct > 0:
                adjustment += 0.15
                flags.append(
                    "negative_funding_price_strong"
                )

            else:
                flags.append(
                    "strong_negative_funding"
                )

        elif (
            -0.03
            <= funding
            <= 0.03
        ):
            adjustment += 0.10
            flags.append(
                "funding_neutral"
            )

    # ---------------------------------
    # Crowd positioning
    #
    # >1 is NOT automatically bullish.
    # Extremely long-heavy = crowd risk.
    # ---------------------------------

    ratios = [
        x
        for x in [
            global_ls,
            top_pos,
            top_acc,
        ]
        if x is not None
    ]

    if ratios:

        extreme_long_count = sum(
            x >= 2.5
            for x in ratios
        )

        bullish_count = sum(
            1.05 <= x < 2.5
            for x in ratios
        )

        if extreme_long_count >= 2:
            adjustment -= 0.30
            flags.append(
                "long_crowding"
            )

        elif bullish_count >= 2:
            adjustment += 0.15
            flags.append(
                "large_traders_long_bias"
            )

    # Keep derivatives subordinate.
    adjustment = max(
        -1.0,
        min(
            adjustment,
            1.0
        )
    )

    if adjustment >= 0.50:
        verdict = "supportive"

    elif adjustment <= -0.40:
        verdict = "caution"

    else:
        verdict = "neutral"

    return {
        "verdict":
            verdict,

        "adjustment":
            round(
                adjustment,
                3
            ),

        "flags":
            flags,
    }


def strength_rank_component(
    strength,
):
    """
    Relative strength matters,
    but cannot overwhelm entry quality.
    Maximum contribution intentionally capped.
    """

    daily = safe_float(
        strength.get(
            "daily_return_pct"
        )
    ) or 0.0

    relative = safe_float(
        strength.get(
            "relative_vs_btc_pct"
        )
    ) or 0.0

    resistance = safe_float(
        strength.get(
            "avg_resistance_vs_btc_pct"
        )
    ) or 0.0

    ratio = safe_float(
        strength.get(
            "resistance_ratio"
        )
    ) or 0.0

    score = 0.0

    # Taiwan 08:00 daily gain
    score += max(
        -1.0,
        min(
            daily * 0.20,
            1.5
        )
    )

    # Relative vs BTC
    score += max(
        -1.0,
        min(
            relative * 0.15,
            1.5
        )
    )

    # Resistance during BTC down hours
    score += max(
        -0.5,
        min(
            resistance * 0.40,
            1.0
        )
    )

    score += (
        ratio * 0.75
    )

    # Relative strength can improve ranking,
    # but cannot dominate structure.
    return round(
        max(
            -2.0,
            min(
                score,
                4.0
            )
        ),
        3,
    )


@app.get("/scan/1h/v36/final")
async def scan_v36_final():

    daily = read_json(
        DAILY_CACHE
    )

    derivatives = read_json(
        DERIVATIVES_CACHE
    )

    if not daily or not derivatives:
        return {
            "status":
                "required_cache_missing"
        }

    daily_symbols = daily.get(
        "symbols",
        {}
    )

    derivative_symbols = (
        derivatives.get(
            "symbols",
            {}
        )
    )

    all_symbols = read_json(
        SYMBOL_CACHE
    )

    results = []

    for symbol in all_symbols:

        base = scan_one_symbol_v33(
            symbol
        )

        if (
            base.get("stage")
            != "qualified"
            or base.get("status")
            != "可進場"
        ):
            continue

        strength = daily_symbols.get(
            symbol
        )

        derivative = (
            derivative_symbols.get(
                symbol
            )
        )

        if (
            not strength
            or not derivative
        ):
            continue

        structure_score = float(
            base.get(
                "v33_structure_score",
                0
            )
        )

        strength_component = (
            strength_rank_component(
                strength
            )
        )

        derivative_context = (
            interpret_derivatives(
                derivative,
                float(
                    strength.get(
                        "daily_return_pct",
                        0
                    )
                ),
            )
        )

        final_score = (
            structure_score
            + strength_component
            + derivative_context[
                "adjustment"
            ]
        )

        results.append({
            "symbol":
                symbol,

            "final_score":
                round(
                    final_score,
                    3
                ),

            "structure_score":
                structure_score,

            "stop_quality":
                base.get(
                    "stop_quality"
                ),

            "strength_component":
                strength_component,

            "daily_return_pct":
                strength.get(
                    "daily_return_pct"
                ),

            "relative_vs_btc_pct":
                strength.get(
                    "relative_vs_btc_pct"
                ),

            "resistance_ratio":
                strength.get(
                    "resistance_ratio"
                ),

            "derivatives_context":
                derivative_context,

            "derivatives": {
                "oi_delta_1h_pct":
                    derivative.get(
                        "oi_delta_1h_pct"
                    ),

                "oi_delta_4h_pct":
                    derivative.get(
                        "oi_delta_4h_pct"
                    ),

                "taker_buy_sell_ratio_6h":
                    derivative.get(
                        "taker_buy_sell_ratio_6h"
                    ),

                "funding_rate_pct":
                    derivative.get(
                        "funding_rate_pct"
                    ),

                "global_long_short_ratio":
                    derivative.get(
                        "global_long_short_ratio"
                    ),

                "top_position_long_short_ratio":
                    derivative.get(
                        "top_position_long_short_ratio"
                    ),

                "top_account_long_short_ratio":
                    derivative.get(
                        "top_account_long_short_ratio"
                    ),
            },
        })

    results.sort(
        key=lambda x:
            x["final_score"],
        reverse=True,
    )

    # Strategy says never force 10.
    top = results[:10]

    return {
        "count":
            len(top),

        "ranking_stage":
            "v36_strategy_weighted",

        "priority":
            [
                "chart_structure",
                "relative_strength",
                "derivatives_validation",
            ],

        "results":
            top,
    }


# =========================================================
# V3.7 INCREMENTAL KLINE CACHE UPDATE
# =========================================================


INCREMENTAL_KLINE_LIMIT = 5

UPDATE_CONCURRENCY = 3
UPDATE_BATCH_SIZE = 20
UPDATE_BATCH_SLEEP = 1.0


def clean_kline(k):
    """
    Keep exactly the same schema as V3.0 cache/init.
    """

    return {
        "open_time": k[0],
        "open": k[1],
        "high": k[2],
        "low": k[3],
        "close": k[4],
        "volume": k[5],
        "close_time": k[6],
        "quote_volume": k[7],
        "trades": k[8],
        "taker_buy_base": k[9],
        "taker_buy_quote": k[10],
    }


def merge_kline_cache(
    old_bars,
    new_bars,
):
    """
    Merge by open_time.

    New Binance data overwrites an existing candle
    with the same open_time. This is important for
    refreshing the currently open candle.

    Keep latest KLINE_LIMIT bars only.
    """

    merged = {}

    if isinstance(old_bars, list):

        for bar in old_bars:

            try:
                open_time = int(
                    bar["open_time"]
                )
            except Exception:
                continue

            merged[open_time] = bar

    if isinstance(new_bars, list):

        for bar in new_bars:

            try:
                open_time = int(
                    bar["open_time"]
                )
            except Exception:
                continue

            merged[open_time] = bar

    ordered = [
        merged[key]
        for key in sorted(
            merged.keys()
        )
    ]

    return ordered[
        -KLINE_LIMIT:
    ]


async def update_one_kline_cache(
    client,
    symbol,
    interval,
    semaphore,
):
    """
    Lightweight incremental update.

    Does NOT download 250 bars again.
    Only gets latest few candles and merges them
    into the existing local cache.
    """

    path = cache_file(
        symbol,
        interval,
    )

    old_bars = read_json(
        path
    )

    if (
        not isinstance(old_bars, list)
        or len(old_bars) < 200
    ):
        return {
            "symbol": symbol,
            "interval": interval,
            "status":
                "cache_missing_or_insufficient",
            "old_bars":
                len(old_bars)
                if isinstance(
                    old_bars,
                    list,
                )
                else 0,
        }

    async with semaphore:

        result = await safe_get(
            client,
            f"{BINANCE_BASE}/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "limit":
                    INCREMENTAL_KLINE_LIMIT,
            },
        )

    if "_error" in result:

        return {
            "symbol": symbol,
            "interval": interval,
            "status": "failed",
            "error": result,
        }

    raw = result.get(
        "_data",
        [],
    )

    if not isinstance(
        raw,
        list,
    ) or not raw:

        return {
            "symbol": symbol,
            "interval": interval,
            "status": "empty_response",
        }

    new_bars = [
        clean_kline(k)
        for k in raw
    ]

    merged = merge_kline_cache(
        old_bars,
        new_bars,
    )

    write_json(
        path,
        merged,
    )

    old_last = (
        int(
            old_bars[-1][
                "open_time"
            ]
        )
        if old_bars
        else None
    )

    new_last = (
        int(
            merged[-1][
                "open_time"
            ]
        )
        if merged
        else None
    )

    new_open_times = {
        int(
            bar["open_time"]
        )
        for bar in new_bars
    }

    old_open_times = {
        int(
            bar["open_time"]
        )
        for bar in old_bars
    }

    added = len(
        new_open_times
        - old_open_times
    )

    return {
        "symbol": symbol,
        "interval": interval,
        "status": "ok",
        "old_bars":
            len(old_bars),
        "bars_received":
            len(new_bars),
        "new_bars_added":
            added,
        "final_bars":
            len(merged),
        "old_last_open_time":
            old_last,
        "new_last_open_time":
            new_last,
        "weight":
            result.get(
                "_weight"
            ),
    }


async def update_interval_incremental(
    client,
    symbols,
    interval,
):
    global RATE_LIMITED

    semaphore = asyncio.Semaphore(
        UPDATE_CONCURRENCY
    )

    results = []

    for start in range(
        0,
        len(symbols),
        UPDATE_BATCH_SIZE,
    ):

        if RATE_LIMITED:
            break

        batch = symbols[
            start:
            start
            + UPDATE_BATCH_SIZE
        ]

        tasks = [
            update_one_kline_cache(
                client,
                symbol,
                interval,
                semaphore,
            )
            for symbol in batch
        ]

        batch_results = (
            await asyncio.gather(
                *tasks
            )
        )

        results.extend(
            batch_results
        )

        if not RATE_LIMITED:
            await asyncio.sleep(
                UPDATE_BATCH_SLEEP
            )

    return results


def summarize_incremental_results(
    results,
):
    ok = [
        x
        for x in results
        if x.get("status")
        == "ok"
    ]

    failed = [
        x
        for x in results
        if x.get("status")
        == "failed"
    ]

    missing = [
        x
        for x in results
        if x.get("status")
        == "cache_missing_or_insufficient"
    ]

    empty = [
        x
        for x in results
        if x.get("status")
        == "empty_response"
    ]

    return {
        "processed":
            len(results),

        "ok":
            len(ok),

        "failed":
            len(failed),

        "cache_missing_or_insufficient":
            len(missing),

        "empty_response":
            len(empty),

        "new_bars_added":
            sum(
                int(
                    x.get(
                        "new_bars_added",
                        0
                    )
                )
                for x in ok
            ),
    }


@app.get("/cache/update/1h")
async def cache_update_1h():

    global RATE_LIMITED

    RATE_LIMITED = False

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:

        return {
            "status":
                "symbol_cache_missing"
        }

    async with httpx.AsyncClient() as client:

        results = (
            await update_interval_incremental(
                client,
                symbols,
                "1h",
            )
        )

    summary = (
        summarize_incremental_results(
            results
        )
    )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "interval":
            "1h",

        "symbols_expected":
            len(symbols),

        "rate_limited":
            RATE_LIMITED,

        **summary,
    }


@app.get("/cache/update/4h")
async def cache_update_4h():

    global RATE_LIMITED

    RATE_LIMITED = False

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:

        return {
            "status":
                "symbol_cache_missing"
        }

    async with httpx.AsyncClient() as client:

        results = (
            await update_interval_incremental(
                client,
                symbols,
                "4h",
            )
        )

    summary = (
        summarize_incremental_results(
            results
        )
    )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "interval":
            "4h",

        "symbols_expected":
            len(symbols),

        "rate_limited":
            RATE_LIMITED,

        **summary,
    }


@app.get("/cache/update/status/{symbol}")
async def cache_update_debug(
    symbol: str,
):

    symbol = symbol.upper()

    output = {}

    for interval in [
        "1h",
        "4h",
    ]:

        bars = read_json(
            cache_file(
                symbol,
                interval,
            )
        )

        if not isinstance(
            bars,
            list,
        ) or not bars:

            output[
                interval
            ] = {
                "exists": False
            }

            continue

        output[
            interval
        ] = {
            "exists": True,
            "bars":
                len(bars),

            "first_open_time":
                bars[0].get(
                    "open_time"
                ),

            "last_open_time":
                bars[-1].get(
                    "open_time"
                ),

            "last_close_time":
                bars[-1].get(
                    "close_time"
                ),

            "last_close":
                bars[-1].get(
                    "close"
                ),
        }

    return {
        "symbol":
            symbol,

        "data":
            output,
    }


# =========================================================
# V3.8 ONE-CLICK 1H SCAN PIPELINE
# =========================================================


def cache_latest_open_time(
    symbol,
    interval,
):
    bars = read_json(
        cache_file(
            symbol,
            interval,
        )
    )

    if (
        not isinstance(bars, list)
        or not bars
    ):
        return None

    try:
        return int(
            bars[-1]["open_time"]
        )
    except Exception:
        return None


def current_interval_open_ms(
    interval,
):
    """
    Current UTC candle open time.
    """

    now = datetime.now(
        timezone.utc
    )

    if interval == "1h":

        current = now.replace(
            minute=0,
            second=0,
            microsecond=0,
        )

    elif interval == "4h":

        hour = (
            now.hour // 4
        ) * 4

        current = now.replace(
            hour=hour,
            minute=0,
            second=0,
            microsecond=0,
        )

    elif interval == "1d":

        current = now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    else:
        raise ValueError(
            "unsupported interval"
        )

    return int(
        current.timestamp()
        * 1000
    )


def interval_cache_is_current(
    interval,
):
    """
    BTC is used as the cache clock reference.
    """

    cached = (
        cache_latest_open_time(
            "BTCUSDT",
            interval,
        )
    )

    expected = (
        current_interval_open_ms(
            interval
        )
    )

    return (
        cached is not None
        and cached >= expected
    )


async def run_incremental_update(
    interval,
):
    """
    Internal helper for V3.8.
    Same logic as /cache/update endpoints.
    """

    global RATE_LIMITED

    RATE_LIMITED = False

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:

        return {
            "status":
                "symbol_cache_missing"
        }

    async with httpx.AsyncClient() as client:

        results = (
            await update_interval_incremental(
                client,
                symbols,
                interval,
            )
        )

    summary = (
        summarize_incremental_results(
            results
        )
    )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "interval":
            interval,

        "rate_limited":
            RATE_LIMITED,

        **summary,
    }


async def build_daily_strength_cache():
    """
    Internal version of V3.4 updater.
    Only called after fresh 1H cache.

    IMPORTANT:
    Daily strength can be calculated from
    local 1H cache now, so no need to make
    another 521 Binance requests.
    """

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:
        return {
            "status":
                "symbol_cache_missing"
        }

    day_start = (
        utc_day_start_ms()
    )

    all_today = {}

    for symbol in symbols:

        bars = read_json(
            cache_file(
                symbol,
                "1h",
            )
        )

        if not isinstance(
            bars,
            list,
        ):
            continue

        today = []

        now_ms = int(
            datetime.now(
                timezone.utc
            ).timestamp()
            * 1000
        )

        for bar in bars:

            try:

                open_time = int(
                    bar["open_time"]
                )

                if open_time < day_start:
                    continue

                today.append({
                    "open_time":
                        open_time,

                    "open":
                        float(
                            bar["open"]
                        ),

                    "high":
                        float(
                            bar["high"]
                        ),

                    "low":
                        float(
                            bar["low"]
                        ),

                    "close":
                        float(
                            bar["close"]
                        ),

                    "closed":
                        int(
                            bar["close_time"]
                        ) < now_ms,
                })

            except Exception:
                continue

        if today:
            all_today[
                symbol
            ] = today

    btc_bars = all_today.get(
        "BTCUSDT"
    )

    if not btc_bars:

        return {
            "status":
                "btc_data_missing"
        }

    strength = {}

    for symbol, bars in (
        all_today.items()
    ):

        metrics = (
            calculate_daily_strength(
                bars,
                btc_bars,
            )
        )

        if metrics:
            strength[
                symbol
            ] = metrics

    payload = {
        "generated_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "taiwan_reset":
            "08:00 Asia/Taipei = 00:00 UTC",

        "source":
            "local_1h_cache",

        "symbols":
            strength,
    }

    write_json(
        DAILY_CACHE,
        payload,
    )

    return {
        "status":
            "complete",

        "calculated":
            len(strength),
    }


def get_current_entry_candidates():
    """
    Run technical Hard Gates locally.
    No Binance requests.
    """

    symbols = read_json(
        SYMBOL_CACHE
    )

    candidates = []

    if not symbols:
        return candidates

    for symbol in symbols:

        result = (
            scan_one_symbol_v33(
                symbol
            )
        )

        if (
            result.get("stage")
            == "qualified"
            and result.get("status")
            == "可進場"
        ):
            candidates.append(
                symbol
            )

    return candidates


async def update_candidate_derivatives(
    candidates,
):
    """
    Only derivatives for current technical
    entry candidates.
    """

    global RATE_LIMITED

    if not candidates:

        payload = {
            "generated_at_utc":
                datetime.now(
                    timezone.utc
                ).isoformat(),

            "period":
                "1h",

            "candidate_count":
                0,

            "symbols":
                {},
        }

        write_json(
            DERIVATIVES_CACHE,
            payload,
        )

        return {
            "status":
                "complete",

            "candidate_count":
                0,

            "downloaded":
                0,
        }

    semaphore = asyncio.Semaphore(
        2
    )

    results = {}

    failed = []

    async with httpx.AsyncClient() as client:

        for symbol in candidates:

            if RATE_LIMITED:
                break

            try:

                data = (
                    await fetch_symbol_derivatives(
                        client,
                        symbol,
                        semaphore,
                    )
                )

                results[
                    symbol
                ] = data

            except Exception as exc:

                failed.append({
                    "symbol":
                        symbol,

                    "error":
                        str(exc),
                })

            await asyncio.sleep(
                0.5
            )

    payload = {
        "generated_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "period":
            "1h",

        "candidate_count":
            len(candidates),

        "symbols":
            results,
    }

    write_json(
        DERIVATIVES_CACHE,
        payload,
    )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "candidate_count":
            len(candidates),

        "downloaded":
            len(results),

        "failed":
            failed,

        "rate_limited":
            RATE_LIMITED,
    }


def build_v36_results():
    """
    Internal final ranking builder.
    Same priority as V3.6:

    chart structure
    > relative strength
    > derivatives validation
    """

    daily = read_json(
        DAILY_CACHE
    )

    derivatives = read_json(
        DERIVATIVES_CACHE
    )

    if not daily or not derivatives:
        return []

    daily_symbols = daily.get(
        "symbols",
        {}
    )

    derivative_symbols = (
        derivatives.get(
            "symbols",
            {}
        )
    )

    symbols = read_json(
        SYMBOL_CACHE
    ) or []

    results = []

    for symbol in symbols:

        base = (
            scan_one_symbol_v33(
                symbol
            )
        )

        if (
            base.get("stage")
            != "qualified"
            or base.get("status")
            != "可進場"
        ):
            continue

        strength = (
            daily_symbols.get(
                symbol
            )
        )

        derivative = (
            derivative_symbols.get(
                symbol
            )
        )

        if (
            not strength
            or not derivative
        ):
            continue

        structure_score = float(
            base.get(
                "v33_structure_score",
                0
            )
        )

        strength_component = (
            strength_rank_component(
                strength
            )
        )

        derivative_context = (
            interpret_derivatives(
                derivative,
                float(
                    strength.get(
                        "daily_return_pct",
                        0
                    )
                ),
            )
        )

        final_score = (
            structure_score
            + strength_component
            + derivative_context[
                "adjustment"
            ]
        )

        results.append({
            "symbol":
                symbol,

            "final_score":
                round(
                    final_score,
                    3
                ),

            "status":
                "entry_now",

            "structure_score":
                structure_score,

            "stop_quality":
                base.get(
                    "stop_quality"
                ),

            "strength_component":
                strength_component,

            "daily_return_pct":
                strength.get(
                    "daily_return_pct"
                ),

            "relative_vs_btc_pct":
                strength.get(
                    "relative_vs_btc_pct"
                ),

            "avg_resistance_vs_btc_pct":
                strength.get(
                    "avg_resistance_vs_btc_pct"
                ),

            "resistance_ratio":
                strength.get(
                    "resistance_ratio"
                ),

            "derivatives_context":
                derivative_context,

            "derivatives": {
                "oi_delta_1h_pct":
                    derivative.get(
                        "oi_delta_1h_pct"
                    ),

                "oi_delta_4h_pct":
                    derivative.get(
                        "oi_delta_4h_pct"
                    ),

                "taker_buy_sell_ratio_6h":
                    derivative.get(
                        "taker_buy_sell_ratio_6h"
                    ),

                "funding_rate_pct":
                    derivative.get(
                        "funding_rate_pct"
                    ),

                "global_long_short_ratio":
                    derivative.get(
                        "global_long_short_ratio"
                    ),

                "top_position_long_short_ratio":
                    derivative.get(
                        "top_position_long_short_ratio"
                    ),

                "top_account_long_short_ratio":
                    derivative.get(
                        "top_account_long_short_ratio"
                    ),
            },
        })

    results.sort(
        key=lambda x:
            x["final_score"],
        reverse=True,
    )

    return results[:10]


@app.get("/scan/run/1h")
async def scan_run_1h():

    global RATE_LIMITED

    started = datetime.now(
        timezone.utc
    )

    pipeline = {}

    # ---------------------------------
    # STEP 1
    # Refresh 1H cache
    # ---------------------------------

    update_1h = (
        await run_incremental_update(
            "1h"
        )
    )

    pipeline[
        "update_1h"
    ] = update_1h

    if (
        update_1h.get("status")
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "update_1h",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # STEP 2
    # Refresh 4H only when candle clock
    # has moved forward.
    # ---------------------------------

    if interval_cache_is_current(
        "4h"
    ):

        pipeline[
            "update_4h"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_4h = (
            await run_incremental_update(
                "4h"
            )
        )

        pipeline[
            "update_4h"
        ] = update_4h

        if (
            update_4h.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_4h",

                "pipeline":
                    pipeline,
            }

    # ---------------------------------
    # STEP 3
    # Build Taiwan 08:00 strength
    # completely from local cache.
    # ---------------------------------

    strength_update = (
        await build_daily_strength_cache()
    )

    pipeline[
        "daily_strength"
    ] = strength_update

    if (
        strength_update.get(
            "status"
        )
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "daily_strength",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # STEP 4
    # Technical scan locally
    # ---------------------------------

    candidates = (
        get_current_entry_candidates()
    )

    pipeline[
        "technical_candidates"
    ] = {
        "count":
            len(candidates),

        "symbols":
            candidates,
    }

    # ---------------------------------
    # STEP 5
    # Derivatives only for candidates
    # ---------------------------------

    RATE_LIMITED = False

    derivative_update = (
        await update_candidate_derivatives(
            candidates
        )
    )

    pipeline[
        "derivatives"
    ] = derivative_update

    if (
        derivative_update.get(
            "status"
        )
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "derivatives",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # STEP 6
    # Final V3.6 ranking
    # ---------------------------------

    final_results = (
        build_v36_results()
    )

    finished = datetime.now(
        timezone.utc
    )

    elapsed = (
        finished - started
    ).total_seconds()

    return {
        "status":
            "complete",

        "scanner":
            "V3.8",

        "timeframe":
            "1h",

        "generated_at_utc":
            finished.isoformat(),

        "elapsed_seconds":
            round(
                elapsed,
                2
            ),

        "pipeline":
            pipeline,

        "count":
            len(final_results),

        "results":
            final_results,
    }


# =========================================================
# V4.0 DAILY CACHE FOR 4H SCANNER
# =========================================================


@app.get("/cache/init/1d")
async def cache_init_1d():
    """
    One-time initialization of 1D cache.

    Used by:
    4H operation timeframe
    -> 1D higher-timeframe Hard Veto.

    IMPORTANT:
    This is a heavy endpoint.
    Do not use for routine scans.
    """

    global RATE_LIMITED

    RATE_LIMITED = False

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:
        return {
            "status":
                "symbol_cache_missing"
        }

    async with httpx.AsyncClient() as client:

        results = (
            await initialize_interval(
                client,
                symbols,
                "1d",
            )
        )

    ok = [
        x
        for x in results
        if x.get("status")
        == "ok"
    ]

    insufficient = [
        x
        for x in results
        if x.get("status")
        == "insufficient_history"
    ]

    failed = [
        x
        for x in results
        if x.get("status")
        == "failed"
    ]

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "interval":
            "1d",

        "symbols_expected":
            len(symbols),

        "processed":
            len(results),

        "ok":
            len(ok),

        "insufficient_history":
            len(insufficient),

        "failed":
            len(failed),

        "rate_limited":
            RATE_LIMITED,

        "failed_symbols": [
            x.get("symbol")
            for x in failed
        ][:20],

        "insufficient_symbols": [
            {
                "symbol":
                    x.get("symbol"),

                "bars":
                    x.get("bars"),
            }
            for x in insufficient
        ][:20],
    }


@app.get("/cache/update/1d")
async def cache_update_1d():
    """
    Routine lightweight 1D incremental update.

    After initialization, use this endpoint.
    Never routinely call /cache/init/1d.
    """

    global RATE_LIMITED

    RATE_LIMITED = False

    symbols = read_json(
        SYMBOL_CACHE
    )

    if not symbols:
        return {
            "status":
                "symbol_cache_missing"
        }

    async with httpx.AsyncClient() as client:

        results = (
            await update_interval_incremental(
                client,
                symbols,
                "1d",
            )
        )

    summary = (
        summarize_incremental_results(
            results
        )
    )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "interval":
            "1d",

        "symbols_expected":
            len(symbols),

        "rate_limited":
            RATE_LIMITED,

        **summary,
    }


@app.get("/cache/status/1d")
async def cache_status_1d():

    symbols = read_json(
        SYMBOL_CACHE
    ) or []

    existing = 0
    sufficient = 0
    insufficient = []
    missing = []

    last_times = []

    for symbol in symbols:

        bars = read_json(
            cache_file(
                symbol,
                "1d",
            )
        )

        if not isinstance(
            bars,
            list,
        ) or not bars:

            missing.append(
                symbol
            )
            continue

        existing += 1

        if len(bars) >= 200:
            sufficient += 1

        else:
            insufficient.append({
                "symbol":
                    symbol,

                "bars":
                    len(bars),
            })

        try:
            last_times.append(
                int(
                    bars[-1][
                        "open_time"
                    ]
                )
            )
        except Exception:
            pass

    return {
        "interval":
            "1d",

        "symbols_expected":
            len(symbols),

        "cache_exists":
            existing,

        "sufficient_200_plus":
            sufficient,

        "missing":
            len(missing),

        "insufficient":
            len(insufficient),

        "latest_open_time":
            (
                max(last_times)
                if last_times
                else None
            ),

        "oldest_last_open_time":
            (
                min(last_times)
                if last_times
                else None
            ),

        "missing_symbols":
            missing[:20],

        "insufficient_symbols":
            insufficient[:20],
    }


@app.get("/cache/status/1d/{symbol}")
async def cache_status_1d_symbol(
    symbol: str,
):

    symbol = symbol.upper()

    bars = read_json(
        cache_file(
            symbol,
            "1d",
        )
    )

    if not isinstance(
        bars,
        list,
    ) or not bars:

        return {
            "symbol":
                symbol,

            "interval":
                "1d",

            "exists":
                False,
        }

    return {
        "symbol":
            symbol,

        "interval":
            "1d",

        "exists":
            True,

        "bars":
            len(bars),

        "first_open_time":
            bars[0].get(
                "open_time"
            ),

        "last_open_time":
            bars[-1].get(
                "open_time"
            ),

        "last_close_time":
            bars[-1].get(
                "close_time"
            ),

        "last_close":
            bars[-1].get(
                "close"
            ),
    }


# =========================================================
# V4.1 4H STEP 1 + 1D HARD VETO
# =========================================================


def analyze_4h_ma_structure(df):
    """
    4H operating timeframe.

    Same structural philosophy as 1H:
    A = mature bullish trend
    B = first bullish leg completed,
        preparing second leg.

    Closed candles only.
    """

    if df is None or len(df) < 60:

        return {
            "pass": False,
            "reason":
                "insufficient_4h_data",
        }

    closed = df.iloc[:-1].copy()

    if len(closed) < 55:

        return {
            "pass": False,
            "reason":
                "insufficient_closed_4h",
        }

    last = closed.iloc[-1]

    ema15 = float(
        last["ema15"]
    )

    sma30 = float(
        last["sma30"]
    )

    sma45 = float(
        last["sma45"]
    )

    close = float(
        last["close"]
    )

    ema15_slope = pct_slope(
        closed["ema15"],
        5,
    )

    sma30_slope = pct_slope(
        closed["sma30"],
        5,
    )

    sma45_slope = pct_slope(
        closed["sma45"],
        5,
    )

    # ---------------------------------
    # A. Mature bullish trend
    # ---------------------------------

    mature_bull = (
        ema15 > sma30 > sma45
        and ema15_slope > 0
        and sma30_slope > 0
        and sma45_slope > 0
        and close > sma45
    )

    if mature_bull:

        return {
            "pass":
                True,

            "structure":
                "mature_bull",

            "close":
                round(close, 10),

            "ema15":
                round(ema15, 10),

            "sma30":
                round(sma30, 10),

            "sma45":
                round(sma45, 10),

            "ema15_slope_5":
                round(
                    ema15_slope,
                    6
                ),

            "sma30_slope_5":
                round(
                    sma30_slope,
                    6
                ),

            "sma45_slope_5":
                round(
                    sma45_slope,
                    6
                ),
        }

    # ---------------------------------
    # B. First leg completed,
    # preparing second leg
    # ---------------------------------

    recent = closed.iloc[-48:]

    had_bullish_order = bool(
        (
            (
                recent["ema15"]
                > recent["sma30"]
            )
            &
            (
                recent["sma30"]
                > recent["sma45"]
            )
        ).any()
    )

    second_leg_candidate = (
        had_bullish_order
        and sma30
        >= sma45 * 0.995
        and sma45_slope
        >= -0.002
        and close
        >= sma45 * 0.97
    )

    if second_leg_candidate:

        return {
            "pass":
                True,

            "structure":
                "second_leg_candidate",

            "close":
                round(close, 10),

            "ema15":
                round(ema15, 10),

            "sma30":
                round(sma30, 10),

            "sma45":
                round(sma45, 10),

            "ema15_slope_5":
                round(
                    ema15_slope,
                    6
                ),

            "sma30_slope_5":
                round(
                    sma30_slope,
                    6
                ),

            "sma45_slope_5":
                round(
                    sma45_slope,
                    6
                ),
        }

    return {
        "pass":
            False,

        "reason":
            "no_valid_4h_bull_structure",

        "close":
            round(close, 10),

        "ema15":
            round(ema15, 10),

        "sma30":
            round(sma30, 10),

        "sma45":
            round(sma45, 10),

        "ema15_slope_5":
            round(
                ema15_slope,
                6
            ),

        "sma30_slope_5":
            round(
                sma30_slope,
                6
            ),

        "sma45_slope_5":
            round(
                sma45_slope,
                6
            ),
    }


def analyze_1d_hard_veto(df):
    """
    4H trades use 1D as higher timeframe.

    Hard veto ONLY when daily timeframe is
    clearly bearish:

    EMA15 < SMA30 < SMA45

    AND all three averages are declining.

    Neutral / tangled / compressed daily
    structure does NOT veto a 4H long.
    """

    if df is None or len(df) < 60:

        return {
            "veto":
                True,

            "reason":
                "insufficient_1d_data",
        }

    closed = df.iloc[:-1].copy()

    if len(closed) < 55:

        return {
            "veto":
                True,

            "reason":
                "insufficient_closed_1d",
        }

    last = closed.iloc[-1]

    ema15 = float(
        last["ema15"]
    )

    sma30 = float(
        last["sma30"]
    )

    sma45 = float(
        last["sma45"]
    )

    ema15_slope = pct_slope(
        closed["ema15"],
        5,
    )

    sma30_slope = pct_slope(
        closed["sma30"],
        5,
    )

    sma45_slope = pct_slope(
        closed["sma45"],
        5,
    )

    bearish_order = (
        ema15 < sma30 < sma45
    )

    downward = (
        ema15_slope < 0
        and sma30_slope < 0
        and sma45_slope < 0
    )

    veto = (
        bearish_order
        and downward
    )

    return {
        "veto":
            veto,

        "reason":
            (
                "daily_bearish_alignment"
                if veto
                else "daily_not_hard_bearish"
            ),

        "ema15":
            round(ema15, 10),

        "sma30":
            round(sma30, 10),

        "sma45":
            round(sma45, 10),

        "ema15_slope_5":
            round(
                ema15_slope,
                6
            ),

        "sma30_slope_5":
            round(
                sma30_slope,
                6
            ),

        "sma45_slope_5":
            round(
                sma45_slope,
                6
            ),
    }


def scan_one_symbol_4h_v41(
    symbol,
):
    """
    V4.1:
    4H structural Hard Gate
    +
    1D higher timeframe Hard Veto.
    """

    df4h = load_dataframe(
        symbol,
        "4h",
    )

    if df4h is None:

        return {
            "symbol":
                symbol,

            "stage":
                "insufficient_4h",
        }

    structure = (
        analyze_4h_ma_structure(
            df4h
        )
    )

    if not structure.get(
        "pass"
    ):

        return {
            "symbol":
                symbol,

            "stage":
                "step1_reject",

            "structure":
                structure,
        }

    df1d = load_dataframe(
        symbol,
        "1d",
    )

    if df1d is None:

        return {
            "symbol":
                symbol,

            "stage":
                "insufficient_1d",

            "structure":
                structure,
        }

    daily = (
        analyze_1d_hard_veto(
            df1d
        )
    )

    if daily.get(
        "veto"
    ):

        return {
            "symbol":
                symbol,

            "stage":
                "1d_hard_veto",

            "structure":
                structure,

            "daily":
                daily,
        }

    return {
        "symbol":
            symbol,

        "stage":
            "step1_pass",

        "structure":
            structure,

        "daily":
            daily,
    }


@app.get("/scan/4h/v41")
async def scan_4h_v41():

    symbols = read_json(
        SYMBOL_CACHE
    ) or []

    stats = {
        "total_symbols":
            len(symbols),

        "insufficient_4h":
            0,

        "step1_reject":
            0,

        "insufficient_1d":
            0,

        "1d_hard_veto":
            0,

        "qualified":
            0,
    }

    results = []

    for symbol in symbols:

        result = (
            scan_one_symbol_4h_v41(
                symbol
            )
        )

        stage = result.get(
            "stage"
        )

        if stage in stats:
            stats[stage] += 1

        if stage == "step1_pass":

            stats[
                "qualified"
            ] += 1

            results.append(
                result
            )

    return {
        "scanner":
            "V4.1",

        "timeframe":
            "4h",

        "stage":
            "trend_hard_gate_plus_1d_veto",

        "stats":
            stats,

        "qualified_symbols": [
            x["symbol"]
            for x in results
        ],

        "results":
            results,
    }


@app.get("/scan/4h/v41/{symbol}")
async def scan_4h_v41_symbol(
    symbol: str,
):

    return (
        scan_one_symbol_4h_v41(
            symbol.upper()
        )
    )


# =========================================================
# V4.2 4H KEY CANDLE HARD GATE
# =========================================================


def scan_one_symbol_4h_v42(
    symbol,
):
    """
    V4.2

    4H Step 1
    -> 1D Hard Veto
    -> 4H Key Candle Hard Gate
    """

    base = (
        scan_one_symbol_4h_v41(
            symbol
        )
    )

    if (
        base.get("stage")
        != "step1_pass"
    ):
        return base

    df4h = load_dataframe(
        symbol,
        "4h",
    )

    if df4h is None:

        return {
            "symbol":
                symbol,

            "stage":
                "insufficient_4h",
        }

    key = find_key_candles(
        df4h,
        lookback=48,
    )

    # find_key_candles in our 1H scanner
    # returns the key-candle result.
    # Treat absence of a valid key candle
    # as a Hard Gate rejection.

    if not key:

        return {
            "symbol":
                symbol,

            "stage":
                "key_candle_reject",

            "structure":
                base.get(
                    "structure"
                ),

            "daily":
                base.get(
                    "daily"
                ),
        }

    # find_key_candles() uses:
    # passed = True / False
    if (
        not isinstance(key, dict)
        or key.get("passed") is not True
    ):

        return {
            "symbol":
                symbol,

            "stage":
                "key_candle_reject",

            "structure":
                base.get(
                    "structure"
                ),

            "daily":
                base.get(
                    "daily"
                ),

            "key_candle":
                key,
        }

    return {
        "symbol":
            symbol,

        "stage":
            "key_candle_pass",

        "structure":
            base.get(
                "structure"
            ),

        "daily":
            base.get(
                "daily"
            ),

        "key_candle":
            key,
    }


@app.get("/scan/4h/v42")
async def scan_4h_v42():

    symbols = read_json(
        SYMBOL_CACHE
    ) or []

    stats = {
        "total_symbols":
            len(symbols),

        "insufficient_4h":
            0,

        "step1_reject":
            0,

        "insufficient_1d":
            0,

        "1d_hard_veto":
            0,

        "key_candle_reject":
            0,

        "key_candle_pass":
            0,
    }

    qualified = []

    for symbol in symbols:

        result = (
            scan_one_symbol_4h_v42(
                symbol
            )
        )

        stage = result.get(
            "stage"
        )

        if stage in stats:
            stats[stage] += 1

        if (
            stage
            == "key_candle_pass"
        ):

            qualified.append(
                result
            )

    return {
        "scanner":
            "V4.2",

        "timeframe":
            "4h",

        "stage":
            "key_candle_hard_gate",

        "stats":
            stats,

        "qualified_symbols": [
            x["symbol"]
            for x in qualified
        ],
    }


@app.get("/scan/4h/v42/{symbol}")
async def scan_4h_v42_symbol(
    symbol: str,
):

    return (
        scan_one_symbol_4h_v42(
            symbol.upper()
        )
    )


# =========================================================
# V4.3 4H CURRENT ENTRY STRUCTURE
# =========================================================


def scan_one_symbol_4h_v43(
    symbol,
):
    """
    V4.3

    4H Step 1
    -> 1D Hard Veto
    -> 4H Key Candle Hard Gate
    -> Pullback / fresh stop signal
    -> Current entry status
    """

    base = (
        scan_one_symbol_4h_v42(
            symbol
        )
    )

    if (
        base.get("stage")
        != "key_candle_pass"
    ):
        return base

    df4h = load_dataframe(
        symbol,
        "4h",
    )

    if df4h is None:

        return {
            "symbol":
                symbol,

            "stage":
                "insufficient_4h",
        }

    key_result = base.get(
        "key_candle"
    )

    if (
        not isinstance(
            key_result,
            dict,
        )
        or key_result.get(
            "passed"
        ) is not True
    ):

        return {
            "symbol":
                symbol,

            "stage":
                "key_candle_reject",
        }

    entry = (
        analyze_current_entry(
            df4h,
            key_result,
        )
    )

    status = entry.get(
        "status"
    )

    return {
        "symbol":
            symbol,

        "stage":
            "entry_structure_checked",

        "status":
            status,

        "structure":
            base.get(
                "structure"
            ),

        "daily":
            base.get(
                "daily"
            ),

        "key_candle":
            key_result,

        "entry":
            entry,
    }


@app.get("/scan/4h/v43")
async def scan_4h_v43():

    symbols = read_json(
        SYMBOL_CACHE
    ) or []

    stats = {
        "total_symbols":
            len(symbols),

        "insufficient_4h":
            0,

        "step1_reject":
            0,

        "insufficient_1d":
            0,

        "1d_hard_veto":
            0,

        "key_candle_reject":
            0,

        "key_candle_pass":
            0,

        "entry_now":
            0,

        "wait_pullback":
            0,

        "wait_stop_signal":
            0,

        "overextended":
            0,

        "structure_damaged":
            0,

        "other":
            0,
    }

    entry_now = []
    wait_pullback = []
    wait_stop = []
    overextended = []
    damaged = []

    for symbol in symbols:

        result = (
            scan_one_symbol_4h_v43(
                symbol
            )
        )

        stage = result.get(
            "stage"
        )

        # ---------------------------------
        # Earlier Hard Gate failures
        # ---------------------------------

        if stage == "insufficient_4h":
            stats[
                "insufficient_4h"
            ] += 1
            continue

        if stage == "step1_reject":
            stats[
                "step1_reject"
            ] += 1
            continue

        if stage == "insufficient_1d":
            stats[
                "insufficient_1d"
            ] += 1
            continue

        if stage == "1d_hard_veto":
            stats[
                "1d_hard_veto"
            ] += 1
            continue

        if stage == "key_candle_reject":
            stats[
                "key_candle_reject"
            ] += 1
            continue

        if stage != "entry_structure_checked":
            stats["other"] += 1
            continue

        # Reaching here means key candle passed.
        stats[
            "key_candle_pass"
        ] += 1

        status = result.get(
            "status"
        )

        compact = {
            "symbol":
                symbol,

            "structure":
                (
                    result.get(
                        "structure"
                    ) or {}
                ).get(
                    "structure"
                ),

            "key_bars_ago":
                (
                    result.get(
                        "key_candle"
                    ) or {}
                ).get(
                    "latest",
                    {}
                ).get(
                    "bars_ago"
                ),

            "key_volume_vs_prev":
                (
                    result.get(
                        "key_candle"
                    ) or {}
                ).get(
                    "latest",
                    {}
                ).get(
                    "volume_vs_prev"
                ),

            "key_volume_vs_ma24":
                (
                    result.get(
                        "key_candle"
                    ) or {}
                ).get(
                    "latest",
                    {}
                ).get(
                    "volume_vs_ma24"
                ),

            "latest_pullback":
                (
                    result.get(
                        "entry"
                    ) or {}
                ).get(
                    "latest_pullback"
                ),

            "latest_stop_signal":
                (
                    result.get(
                        "entry"
                    ) or {}
                ).get(
                    "latest_stop_signal"
                ),

            "strict_bull_order":
                (
                    result.get(
                        "entry"
                    ) or {}
                ).get(
                    "strict_bull_order"
                ),

            "structure_healthy":
                (
                    result.get(
                        "entry"
                    ) or {}
                ).get(
                    "structure_healthy"
                ),

            "distance_to_ma_pct":
                (
                    result.get(
                        "entry"
                    ) or {}
                ).get(
                    "distance_to_ma_pct"
                ),

            "entry_score":
                (
                    result.get(
                        "entry"
                    ) or {}
                ).get(
                    "current_entry_score"
                ),
        }

        if status == "可進場":

            stats[
                "entry_now"
            ] += 1

            entry_now.append(
                compact
            )

        elif status == "等回補":

            stats[
                "wait_pullback"
            ] += 1

            wait_pullback.append(
                compact
            )

        elif status == "等止跌K":

            stats[
                "wait_stop_signal"
            ] += 1

            wait_stop.append(
                compact
            )

        elif status == "過度延伸勿追":

            stats[
                "overextended"
            ] += 1

            overextended.append(
                compact
            )

        elif status == "structure_damaged":

            stats[
                "structure_damaged"
            ] += 1

            damaged.append(
                compact
            )

        else:

            stats[
                "other"
            ] += 1

    # Diagnostic ordering only.
    # NOT final ranking.
    entry_now.sort(
        key=lambda x:
            x.get(
                "entry_score"
            ) or 0,
        reverse=True,
    )

    return {
        "scanner":
            "V4.3",

        "timeframe":
            "4h",

        "stage":
            "current_entry_structure",

        "ranking_stage":
            "entry_diagnostic_not_final",

        "stats":
            stats,

        "entry_now":
            entry_now,

        "wait_pullback_symbols": [
            x["symbol"]
            for x in wait_pullback
        ],

        "wait_stop_signal_symbols": [
            x["symbol"]
            for x in wait_stop
        ],

        "overextended_symbols": [
            x["symbol"]
            for x in overextended
        ],

        "structure_damaged_symbols": [
            x["symbol"]
            for x in damaged
        ],
    }


@app.get("/scan/4h/v43/{symbol}")
async def scan_4h_v43_symbol(
    symbol: str,
):

    return (
        scan_one_symbol_4h_v43(
            symbol.upper()
        )
    )


# =========================================================
# V4.4 4H STOP SIGNAL QUALITY
# =========================================================


def scan_one_symbol_4h_v44(
    symbol,
):
    """
    V4.4

    V4.3 entry structure
    + stop signal quality grading

    Reuses the verified 1H:
    grade_stop_signal()
    """

    base = (
        scan_one_symbol_4h_v43(
            symbol
        )
    )

    if (
        base.get("stage")
        != "entry_structure_checked"
    ):
        return base

    entry = (
        base.get("entry")
        or {}
    )

    stop = entry.get(
        "latest_stop_signal"
    )

    stop_quality = (
        grade_stop_signal(
            stop
        )
    )

    result = dict(base)

    result[
        "stop_quality"
    ] = stop_quality

    return result


@app.get("/scan/4h/v44")
async def scan_4h_v44():

    symbols = (
        read_json(
            SYMBOL_CACHE
        )
        or []
    )

    stats = {
        "total_symbols":
            len(symbols),

        "insufficient_4h":
            0,

        "step1_reject":
            0,

        "insufficient_1d":
            0,

        "1d_hard_veto":
            0,

        "key_candle_reject":
            0,

        "key_candle_pass":
            0,

        "entry_now":
            0,

        "grade_A":
            0,

        "grade_B":
            0,

        "grade_C":
            0,

        "wait_pullback":
            0,

        "wait_stop_signal":
            0,

        "overextended":
            0,

        "structure_damaged":
            0,

        "other":
            0,
    }

    entry_now = []

    wait_pullback = []
    wait_stop = []
    overextended = []
    damaged = []

    for symbol in symbols:

        result = (
            scan_one_symbol_4h_v44(
                symbol
            )
        )

        stage = result.get(
            "stage"
        )

        if stage == "insufficient_4h":

            stats[
                "insufficient_4h"
            ] += 1

            continue

        if stage == "step1_reject":

            stats[
                "step1_reject"
            ] += 1

            continue

        if stage == "insufficient_1d":

            stats[
                "insufficient_1d"
            ] += 1

            continue

        if stage == "1d_hard_veto":

            stats[
                "1d_hard_veto"
            ] += 1

            continue

        if stage == "key_candle_reject":

            stats[
                "key_candle_reject"
            ] += 1

            continue

        if stage != "entry_structure_checked":

            stats[
                "other"
            ] += 1

            continue

        stats[
            "key_candle_pass"
        ] += 1

        status = result.get(
            "status"
        )

        entry = (
            result.get(
                "entry"
            )
            or {}
        )

        stop_quality = (
            result.get(
                "stop_quality"
            )
            or {}
        )

        if status == "可進場":

            stats[
                "entry_now"
            ] += 1

            grade = (
                stop_quality.get(
                    "grade"
                )
            )

            if grade == "A":
                stats[
                    "grade_A"
                ] += 1

            elif grade == "B":
                stats[
                    "grade_B"
                ] += 1

            elif grade == "C":
                stats[
                    "grade_C"
                ] += 1

            key = (
                (
                    result.get(
                        "key_candle"
                    )
                    or {}
                ).get(
                    "latest"
                )
                or {}
            )

            item = {
                "symbol":
                    symbol,

                "structure":
                    (
                        result.get(
                            "structure"
                        )
                        or {}
                    ).get(
                        "structure"
                    ),

                "key_candle": {
                    "bars_ago":
                        key.get(
                            "bars_ago"
                        ),

                    "volume_vs_prev":
                        key.get(
                            "volume_vs_prev"
                        ),

                    "volume_vs_ma24":
                        key.get(
                            "volume_vs_ma24"
                        ),

                    "body_ratio":
                        key.get(
                            "body_ratio"
                        ),

                    "displacement_pct":
                        key.get(
                            "price_displacement_pct"
                        ),
                },

                "pullback":
                    entry.get(
                        "latest_pullback"
                    ),

                "stop_signal":
                    entry.get(
                        "latest_stop_signal"
                    ),

                "stop_grade":
                    stop_quality.get(
                        "grade"
                    ),

                "stop_quality_score":
                    stop_quality.get(
                        "score"
                    ),

                "strict_bull_order":
                    entry.get(
                        "strict_bull_order"
                    ),

                "structure_healthy":
                    entry.get(
                        "structure_healthy"
                    ),

                "distance_to_ma_pct":
                    entry.get(
                        "distance_to_ma_pct"
                    ),

                "entry_score":
                    entry.get(
                        "current_entry_score"
                    ),
            }

            entry_now.append(
                item
            )

        elif status == "等回補":

            stats[
                "wait_pullback"
            ] += 1

            wait_pullback.append(
                symbol
            )

        elif status == "等止跌K":

            stats[
                "wait_stop_signal"
            ] += 1

            wait_stop.append(
                symbol
            )

        elif status == "過度延伸勿追":

            stats[
                "overextended"
            ] += 1

            overextended.append(
                symbol
            )

        elif status == "structure_damaged":

            stats[
                "structure_damaged"
            ] += 1

            damaged.append(
                symbol
            )

        else:

            stats[
                "other"
            ] += 1

    # ---------------------------------
    # Diagnostic ordering only
    # A > B > C
    # Then stop quality
    # Then entry structure
    # ---------------------------------

    grade_order = {
        "A": 3,
        "B": 2,
        "C": 1,
        "NONE": 0,
    }

    entry_now.sort(
        key=lambda x: (
            grade_order.get(
                x.get(
                    "stop_grade"
                ),
                0,
            ),

            x.get(
                "stop_quality_score"
            )
            or 0,

            x.get(
                "entry_score"
            )
            or 0,
        ),
        reverse=True,
    )

    return {
        "scanner":
            "V4.4",

        "timeframe":
            "4h",

        "stage":
            "stop_signal_quality",

        "ranking_stage":
            "technical_diagnostic_not_final",

        "stats":
            stats,

        "entry_now":
            entry_now,

        "wait_pullback_symbols":
            wait_pullback,

        "wait_stop_signal_symbols":
            wait_stop,

        "overextended_symbols":
            overextended,

        "structure_damaged_symbols":
            damaged,
    }


@app.get("/scan/4h/v44/{symbol}")
async def scan_4h_v44_symbol(
    symbol: str,
):

    return (
        scan_one_symbol_4h_v44(
            symbol.upper()
        )
    )


# =========================================================
# V4.4 4H STOP SIGNAL QUALITY
# =========================================================


def scan_one_symbol_4h_v44(
    symbol,
):
    """
    V4.4

    V4.3 entry structure
    + stop signal quality grading

    Reuses the verified 1H:
    grade_stop_signal()
    """

    base = (
        scan_one_symbol_4h_v43(
            symbol
        )
    )

    if (
        base.get("stage")
        != "entry_structure_checked"
    ):
        return base

    entry = (
        base.get("entry")
        or {}
    )

    stop = entry.get(
        "latest_stop_signal"
    )

    stop_quality = (
        grade_stop_signal(
            stop
        )
    )

    result = dict(base)

    result[
        "stop_quality"
    ] = stop_quality

    return result


@app.get("/scan/4h/v44")
async def scan_4h_v44():

    symbols = (
        read_json(
            SYMBOL_CACHE
        )
        or []
    )

    stats = {
        "total_symbols":
            len(symbols),

        "insufficient_4h":
            0,

        "step1_reject":
            0,

        "insufficient_1d":
            0,

        "1d_hard_veto":
            0,

        "key_candle_reject":
            0,

        "key_candle_pass":
            0,

        "entry_now":
            0,

        "grade_A":
            0,

        "grade_B":
            0,

        "grade_C":
            0,

        "wait_pullback":
            0,

        "wait_stop_signal":
            0,

        "overextended":
            0,

        "structure_damaged":
            0,

        "other":
            0,
    }

    entry_now = []

    wait_pullback = []
    wait_stop = []
    overextended = []
    damaged = []

    for symbol in symbols:

        result = (
            scan_one_symbol_4h_v44(
                symbol
            )
        )

        stage = result.get(
            "stage"
        )

        if stage == "insufficient_4h":

            stats[
                "insufficient_4h"
            ] += 1

            continue

        if stage == "step1_reject":

            stats[
                "step1_reject"
            ] += 1

            continue

        if stage == "insufficient_1d":

            stats[
                "insufficient_1d"
            ] += 1

            continue

        if stage == "1d_hard_veto":

            stats[
                "1d_hard_veto"
            ] += 1

            continue

        if stage == "key_candle_reject":

            stats[
                "key_candle_reject"
            ] += 1

            continue

        if stage != "entry_structure_checked":

            stats[
                "other"
            ] += 1

            continue

        stats[
            "key_candle_pass"
        ] += 1

        status = result.get(
            "status"
        )

        entry = (
            result.get(
                "entry"
            )
            or {}
        )

        stop_quality = (
            result.get(
                "stop_quality"
            )
            or {}
        )

        if status == "可進場":

            stats[
                "entry_now"
            ] += 1

            grade = (
                stop_quality.get(
                    "grade"
                )
            )

            if grade == "A":
                stats[
                    "grade_A"
                ] += 1

            elif grade == "B":
                stats[
                    "grade_B"
                ] += 1

            elif grade == "C":
                stats[
                    "grade_C"
                ] += 1

            key = (
                (
                    result.get(
                        "key_candle"
                    )
                    or {}
                ).get(
                    "latest"
                )
                or {}
            )

            item = {
                "symbol":
                    symbol,

                "structure":
                    (
                        result.get(
                            "structure"
                        )
                        or {}
                    ).get(
                        "structure"
                    ),

                "key_candle": {
                    "bars_ago":
                        key.get(
                            "bars_ago"
                        ),

                    "volume_vs_prev":
                        key.get(
                            "volume_vs_prev"
                        ),

                    "volume_vs_ma24":
                        key.get(
                            "volume_vs_ma24"
                        ),

                    "body_ratio":
                        key.get(
                            "body_ratio"
                        ),

                    "displacement_pct":
                        key.get(
                            "price_displacement_pct"
                        ),
                },

                "pullback":
                    entry.get(
                        "latest_pullback"
                    ),

                "stop_signal":
                    entry.get(
                        "latest_stop_signal"
                    ),

                "stop_grade":
                    stop_quality.get(
                        "grade"
                    ),

                "stop_quality_score":
                    stop_quality.get(
                        "score"
                    ),

                "strict_bull_order":
                    entry.get(
                        "strict_bull_order"
                    ),

                "structure_healthy":
                    entry.get(
                        "structure_healthy"
                    ),

                "distance_to_ma_pct":
                    entry.get(
                        "distance_to_ma_pct"
                    ),

                "entry_score":
                    entry.get(
                        "current_entry_score"
                    ),
            }

            entry_now.append(
                item
            )

        elif status == "等回補":

            stats[
                "wait_pullback"
            ] += 1

            wait_pullback.append(
                symbol
            )

        elif status == "等止跌K":

            stats[
                "wait_stop_signal"
            ] += 1

            wait_stop.append(
                symbol
            )

        elif status == "過度延伸勿追":

            stats[
                "overextended"
            ] += 1

            overextended.append(
                symbol
            )

        elif status == "structure_damaged":

            stats[
                "structure_damaged"
            ] += 1

            damaged.append(
                symbol
            )

        else:

            stats[
                "other"
            ] += 1

    # ---------------------------------
    # Diagnostic ordering only
    # A > B > C
    # Then stop quality
    # Then entry structure
    # ---------------------------------

    grade_order = {
        "A": 3,
        "B": 2,
        "C": 1,
        "NONE": 0,
    }

    entry_now.sort(
        key=lambda x: (
            grade_order.get(
                x.get(
                    "stop_grade"
                ),
                0,
            ),

            x.get(
                "stop_quality_score"
            )
            or 0,

            x.get(
                "entry_score"
            )
            or 0,
        ),
        reverse=True,
    )

    return {
        "scanner":
            "V4.4",

        "timeframe":
            "4h",

        "stage":
            "stop_signal_quality",

        "ranking_stage":
            "technical_diagnostic_not_final",

        "stats":
            stats,

        "entry_now":
            entry_now,

        "wait_pullback_symbols":
            wait_pullback,

        "wait_stop_signal_symbols":
            wait_stop,

        "overextended_symbols":
            overextended,

        "structure_damaged_symbols":
            damaged,
    }


@app.get("/scan/4h/v44/{symbol}")
async def scan_4h_v44_symbol(
    symbol: str,
):

    return (
        scan_one_symbol_4h_v44(
            symbol.upper()
        )
    )


# =========================================================
# V4.5 / FINAL 4H PIPELINE
# =========================================================

DERIVATIVES_4H_CACHE = (
    CACHE_DIR / "derivatives_4h.json"
)


def calculate_4h_structure_score(
    result,
):
    """
    Same philosophy as 1H V3.3:

    original entry diagnostic score
    - old stop bonus
    + verified stop-quality score

    Chart structure remains dominant.
    """

    entry = (
        result.get("entry")
        or {}
    )

    stop = entry.get(
        "latest_stop_signal"
    )

    stop_quality = (
        result.get("stop_quality")
        or grade_stop_signal(stop)
    )

    base_score = float(
        entry.get(
            "current_entry_score",
            0,
        )
        or 0
    )

    old_stop_bonus = 0.0

    if stop:

        old_stop_bonus += 2.0

        old_stop_bonus += min(
            float(
                stop.get(
                    "relative_volume",
                    0,
                )
                or 0
            ),
            3.0,
        )

        bars_ago = stop.get(
            "bars_ago"
        )

        if bars_ago == 0:
            old_stop_bonus += 1.0

        elif bars_ago == 1:
            old_stop_bonus += 0.5

    structure_base = (
        base_score
        - old_stop_bonus
    )

    structure_score = (
        structure_base
        + float(
            stop_quality.get(
                "score",
                0,
            )
            or 0
        )
    )

    return round(
        structure_score,
        3,
    )


def get_4h_entry_candidates():
    """
    Full local 4H technical gates.

    No Binance requests.

    Only symbols currently classified
    as entry-now are returned.
    """

    symbols = (
        read_json(
            SYMBOL_CACHE
        )
        or []
    )

    candidates = []

    for symbol in symbols:

        result = (
            scan_one_symbol_4h_v44(
                symbol
            )
        )

        if (
            result.get("stage")
            == "entry_structure_checked"
            and result.get("status")
            == "可進場"
        ):
            candidates.append(
                symbol
            )

    return candidates


async def update_4h_candidate_derivatives(
    candidates,
):
    """
    Derivatives only for current
    4H technical candidates.

    Uses independent cache so 1H and
    4H cannot overwrite each other.
    """

    global RATE_LIMITED

    if not candidates:

        payload = {
            "generated_at_utc":
                datetime.now(
                    timezone.utc
                ).isoformat(),

            "period":
                "4h",

            "candidate_count":
                0,

            "symbols":
                {},
        }

        write_json(
            DERIVATIVES_4H_CACHE,
            payload,
        )

        return {
            "status":
                "complete",

            "candidate_count":
                0,

            "downloaded":
                0,

            "failed":
                [],

            "rate_limited":
                RATE_LIMITED,
        }

    semaphore = asyncio.Semaphore(
        2
    )

    results = {}
    failed = []

    async with httpx.AsyncClient() as client:

        for symbol in candidates:

            if RATE_LIMITED:
                break

            try:

                data = (
                    await fetch_symbol_derivatives(
                        client,
                        symbol,
                        semaphore,
                    )
                )

                results[
                    symbol
                ] = data

            except Exception as exc:

                failed.append({
                    "symbol":
                        symbol,

                    "error":
                        str(exc),
                })

            await asyncio.sleep(
                0.5
            )

    payload = {
        "generated_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "period":
            "4h",

        "candidate_count":
            len(candidates),

        "symbols":
            results,
    }

    write_json(
        DERIVATIVES_4H_CACHE,
        payload,
    )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "candidate_count":
            len(candidates),

        "downloaded":
            len(results),

        "failed":
            failed,

        "rate_limited":
            RATE_LIMITED,
    }


def build_4h_final_results():
    """
    Final 4H ranking.

    Priority:
    chart structure
    > relative strength
    > derivatives validation
    """

    daily = read_json(
        DAILY_CACHE
    )

    derivatives = read_json(
        DERIVATIVES_4H_CACHE
    )

    if (
        not daily
        or not derivatives
    ):
        return []

    daily_symbols = (
        daily.get(
            "symbols",
            {}
        )
    )

    derivative_symbols = (
        derivatives.get(
            "symbols",
            {}
        )
    )

    symbols = (
        read_json(
            SYMBOL_CACHE
        )
        or []
    )

    results = []

    for symbol in symbols:

        base = (
            scan_one_symbol_4h_v44(
                symbol
            )
        )

        if (
            base.get("stage")
            != "entry_structure_checked"
            or base.get("status")
            != "可進場"
        ):
            continue

        strength = (
            daily_symbols.get(
                symbol
            )
        )

        derivative = (
            derivative_symbols.get(
                symbol
            )
        )

        # Final ranking requires both.
        if (
            not strength
            or not derivative
        ):
            continue

        structure_score = (
            calculate_4h_structure_score(
                base
            )
        )

        strength_component = (
            strength_rank_component(
                strength
            )
        )

        daily_return = float(
            strength.get(
                "daily_return_pct",
                0,
            )
            or 0
        )

        derivative_context = (
            interpret_derivatives(
                derivative,
                daily_return,
            )
        )

        final_score = (
            structure_score
            + strength_component
            + derivative_context[
                "adjustment"
            ]
        )

        entry = (
            base.get("entry")
            or {}
        )

        stop_quality = (
            base.get(
                "stop_quality"
            )
            or {}
        )

        structure = (
            base.get(
                "structure"
            )
            or {}
        )

        results.append({
            "symbol":
                symbol,

            "final_score":
                round(
                    final_score,
                    3,
                ),

            "status":
                "entry_now",

            "ma_structure":
                structure.get(
                    "structure"
                ),

            "structure_score":
                structure_score,

            "stop_grade":
                stop_quality.get(
                    "grade"
                ),

            "stop_quality_score":
                stop_quality.get(
                    "score"
                ),

            "stop_signal":
                entry.get(
                    "latest_stop_signal"
                ),

            "latest_pullback":
                entry.get(
                    "latest_pullback"
                ),

            "strict_bull_order":
                entry.get(
                    "strict_bull_order"
                ),

            "structure_healthy":
                entry.get(
                    "structure_healthy"
                ),

            "distance_to_ma_pct":
                entry.get(
                    "distance_to_ma_pct"
                ),

            "strength_component":
                strength_component,

            "daily_return_pct":
                strength.get(
                    "daily_return_pct"
                ),

            "btc_daily_return_pct":
                strength.get(
                    "btc_daily_return_pct"
                ),

            "relative_vs_btc_pct":
                strength.get(
                    "relative_vs_btc_pct"
                ),

            "avg_resistance_vs_btc_pct":
                strength.get(
                    "avg_resistance_vs_btc_pct"
                ),

            "resistance_ratio":
                strength.get(
                    "resistance_ratio"
                ),

            "derivatives_context":
                derivative_context,

            "derivatives": {
                "oi_delta_1h_pct":
                    derivative.get(
                        "oi_delta_1h_pct"
                    ),

                "oi_delta_4h_pct":
                    derivative.get(
                        "oi_delta_4h_pct"
                    ),

                "taker_buy_sell_ratio_6h":
                    derivative.get(
                        "taker_buy_sell_ratio_6h"
                    ),

                "funding_rate_pct":
                    derivative.get(
                        "funding_rate_pct"
                    ),

                "global_long_short_ratio":
                    derivative.get(
                        "global_long_short_ratio"
                    ),

                "top_position_long_short_ratio":
                    derivative.get(
                        "top_position_long_short_ratio"
                    ),

                "top_account_long_short_ratio":
                    derivative.get(
                        "top_account_long_short_ratio"
                    ),

                "cvd_proxy_6h":
                    derivative.get(
                        "cvd_proxy_6h"
                    ),
            },
        })

    results.sort(
        key=lambda x:
            x["final_score"],
        reverse=True,
    )

    # Never force-fill Top 10.
    return results[:10]


@app.get("/scan/4h/final")
async def scan_4h_final():

    results = (
        build_4h_final_results()
    )

    return {
        "scanner":
            "4H_FINAL",

        "ranking_priority":
            (
                "chart_structure > "
                "relative_strength > "
                "derivatives_validation"
            ),

        "count":
            len(results),

        "top":
            results,
    }


@app.get("/scan/run/4h")
async def scan_run_4h():

    global RATE_LIMITED

    started = datetime.now(
        timezone.utc
    )

    RATE_LIMITED = False

    pipeline = {}

    # ---------------------------------
    # 1. Refresh 4H cache
    # ---------------------------------

    try:

        update_4h = (
            await run_incremental_update(
                "4h"
            )
        )

        pipeline[
            "update_4h"
        ] = update_4h

    except Exception as exc:

        pipeline[
            "update_4h"
        ] = {
            "status":
                "error",

            "error":
                str(exc),
        }

    if RATE_LIMITED:

        return {
            "status":
                "stopped_rate_limit",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # 2. Refresh 1D cache
    #
    # Needed for higher-TF Hard Veto.
    # ---------------------------------

    try:

        update_1d = (
            await run_incremental_update(
                "1d"
            )
        )

        pipeline[
            "update_1d"
        ] = update_1d

    except Exception as exc:

        pipeline[
            "update_1d"
        ] = {
            "status":
                "error",

            "error":
                str(exc),
        }

    if RATE_LIMITED:

        return {
            "status":
                "stopped_rate_limit",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # 3. Fresh 1H cache
    #
    # Daily Taiwan 08:00 strength
    # is built locally from 1H cache.
    # ---------------------------------

    try:

        update_1h = (
            await run_incremental_update(
                "1h"
            )
        )

        pipeline[
            "update_1h_for_strength"
        ] = update_1h

    except Exception as exc:

        pipeline[
            "update_1h_for_strength"
        ] = {
            "status":
                "error",

            "error":
                str(exc),
        }

    if RATE_LIMITED:

        return {
            "status":
                "stopped_rate_limit",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # 4. Build Taiwan 08:00 strength
    # locally.
    # ---------------------------------

    try:

        strength_update = (
            await build_daily_strength_cache()
        )

        pipeline[
            "daily_strength"
        ] = strength_update

    except Exception as exc:

        pipeline[
            "daily_strength"
        ] = {
            "status":
                "error",

            "error":
                str(exc),
        }

    # ---------------------------------
    # 5. Full-market local 4H scan.
    # ---------------------------------

    candidates = (
        get_4h_entry_candidates()
    )

    pipeline[
        "technical_candidates"
    ] = {
        "count":
            len(candidates),

        "symbols":
            candidates,
    }

    # ---------------------------------
    # 6. Derivatives ONLY candidates.
    # ---------------------------------

    derivative_update = (
        await update_4h_candidate_derivatives(
            candidates
        )
    )

    pipeline[
        "derivatives"
    ] = derivative_update

    if RATE_LIMITED:

        return {
            "status":
                "stopped_rate_limit",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # 7. Final ranking.
    # ---------------------------------

    final_results = (
        build_4h_final_results()
    )

    finished = datetime.now(
        timezone.utc
    )

    elapsed = (
        finished - started
    ).total_seconds()

    return {
        "status":
            "complete",

        "scanner":
            "4H_ONE_CLICK_FINAL",

        "generated_at_utc":
            finished.isoformat(),

        "elapsed_seconds":
            round(
                elapsed,
                2,
            ),

        "ranking_priority":
            (
                "chart_structure > "
                "relative_strength > "
                "derivatives_validation"
            ),

        "pipeline":
            pipeline,

        "final_count":
            len(final_results),

        "top10":
            final_results,
    }


# =========================================================
# V4.6 4H ONE-CLICK PIPELINE
# CACHE CLOCK OPTIMIZED
# =========================================================


@app.get("/scan/run/4h/v46")
async def scan_run_4h_v46():

    global RATE_LIMITED

    started = datetime.now(
        timezone.utc
    )

    RATE_LIMITED = False

    pipeline = {}

    # ---------------------------------
    # STEP 1
    # Refresh 4H only when candle clock
    # has moved forward.
    # ---------------------------------

    if interval_cache_is_current(
        "4h"
    ):

        pipeline[
            "update_4h"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_4h = (
            await run_incremental_update(
                "4h"
            )
        )

        pipeline[
            "update_4h"
        ] = update_4h

        if (
            update_4h.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_4h",

                "pipeline":
                    pipeline,
            }

    # ---------------------------------
    # STEP 2
    # Refresh 1D only when UTC daily
    # candle clock has moved forward.
    #
    # BTC is clock reference.
    # New symbols with insufficient 1D
    # history do NOT trigger full refresh.
    # ---------------------------------

    if interval_cache_is_current(
        "1d"
    ):

        pipeline[
            "update_1d"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_1d = (
            await run_incremental_update(
                "1d"
            )
        )

        pipeline[
            "update_1d"
        ] = update_1d

        if (
            update_1d.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_1d",

                "pipeline":
                    pipeline,
            }

    # ---------------------------------
    # STEP 3
    # Refresh 1H only when candle clock
    # has moved forward.
    #
    # Used for Taiwan 08:00 strength.
    # ---------------------------------

    if interval_cache_is_current(
        "1h"
    ):

        pipeline[
            "update_1h_for_strength"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_1h = (
            await run_incremental_update(
                "1h"
            )
        )

        pipeline[
            "update_1h_for_strength"
        ] = update_1h

        if (
            update_1h.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_1h_for_strength",

                "pipeline":
                    pipeline,
            }

    # ---------------------------------
    # STEP 4
    # Taiwan 08:00 daily strength.
    #
    # Completely local.
    # No Binance requests.
    # ---------------------------------

    strength_update = (
        await build_daily_strength_cache()
    )

    pipeline[
        "daily_strength"
    ] = strength_update

    if (
        strength_update.get("status")
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "daily_strength",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # STEP 5
    # Full-market 4H technical scan.
    #
    # Completely local.
    # ---------------------------------

    candidates = (
        get_4h_entry_candidates()
    )

    pipeline[
        "technical_candidates"
    ] = {
        "count":
            len(candidates),

        "symbols":
            candidates,
    }

    # ---------------------------------
    # STEP 6
    # Derivatives ONLY for current
    # technical candidates.
    # ---------------------------------

    RATE_LIMITED = False

    derivative_update = (
        await update_4h_candidate_derivatives(
            candidates
        )
    )

    pipeline[
        "derivatives"
    ] = derivative_update

    if (
        derivative_update.get("status")
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "derivatives",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # STEP 7
    # Final ranking.
    #
    # chart structure
    # > relative strength
    # > derivatives validation
    # ---------------------------------

    final_results = (
        build_4h_final_results()
    )

    finished = datetime.now(
        timezone.utc
    )

    elapsed = (
        finished - started
    ).total_seconds()

    return {
        "status":
            "complete",

        "scanner":
            "V4.6",

        "timeframe":
            "4h",

        "generated_at_utc":
            finished.isoformat(),

        "elapsed_seconds":
            round(
                elapsed,
                2
            ),

        "cache_clock_optimized":
            True,

        "ranking_priority":
            (
                "chart_structure > "
                "relative_strength > "
                "derivatives_validation"
            ),

        "pipeline":
            pipeline,

        "count":
            len(final_results),

        "results":
            final_results,
    }


# =========================================================
# V3.9 1H ONE-CLICK PIPELINE
# CACHE CLOCK OPTIMIZED
# =========================================================


@app.get("/scan/run/1h/v39")
async def scan_run_1h_v39():

    global RATE_LIMITED

    started = datetime.now(
        timezone.utc
    )

    RATE_LIMITED = False
    pipeline = {}

    # ---------------------------------
    # STEP 1
    # Refresh 1H only when candle clock
    # has moved forward.
    # ---------------------------------

    if interval_cache_is_current(
        "1h"
    ):

        pipeline[
            "update_1h"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_1h = (
            await run_incremental_update(
                "1h"
            )
        )

        pipeline[
            "update_1h"
        ] = update_1h

        if (
            update_1h.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_1h",

                "pipeline":
                    pipeline,
            }

    # ---------------------------------
    # STEP 2
    # 4H higher-timeframe filter.
    # Refresh only if 4H clock moved.
    # ---------------------------------

    if interval_cache_is_current(
        "4h"
    ):

        pipeline[
            "update_4h"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_4h = (
            await run_incremental_update(
                "4h"
            )
        )

        pipeline[
            "update_4h"
        ] = update_4h

        if (
            update_4h.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_4h",

                "pipeline":
                    pipeline,
            }

    # ---------------------------------
    # STEP 3
    # Taiwan 08:00 strength.
    # Local calculation only.
    # ---------------------------------

    strength_update = (
        await build_daily_strength_cache()
    )

    pipeline[
        "daily_strength"
    ] = strength_update

    if (
        strength_update.get("status")
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "daily_strength",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # STEP 4
    # Full-market 1H technical scan.
    # Local only.
    # ---------------------------------

    candidates = (
        get_current_entry_candidates()
    )

    pipeline[
        "technical_candidates"
    ] = {
        "count":
            len(candidates),

        "symbols":
            candidates,
    }

    # ---------------------------------
    # STEP 5
    # Fresh derivatives only for
    # current technical candidates.
    # ---------------------------------

    RATE_LIMITED = False

    derivative_update = (
        await update_candidate_derivatives(
            candidates
        )
    )

    pipeline[
        "derivatives"
    ] = derivative_update

    if (
        derivative_update.get("status")
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "derivatives",

            "pipeline":
                pipeline,
        }

    # ---------------------------------
    # STEP 6
    # Existing locked V3.6 ranking.
    # ---------------------------------

    final_results = (
        build_v36_results()
    )

    finished = datetime.now(
        timezone.utc
    )

    elapsed = (
        finished - started
    ).total_seconds()

    return {
        "status":
            "complete",

        "scanner":
            "V3.9",

        "timeframe":
            "1h",

        "generated_at_utc":
            finished.isoformat(),

        "elapsed_seconds":
            round(
                elapsed,
                2
            ),

        "cache_clock_optimized":
            True,

        "ranking_priority":
            (
                "chart_structure > "
                "relative_strength > "
                "derivatives_validation"
            ),

        "pipeline":
            pipeline,

        "count":
            len(final_results),

        "results":
            final_results,
    }


# =========================================================
# 1H x 4H RESONANCE
# =========================================================


def build_1h_4h_resonance():
    """
    Timeframe ownership remains independent.

    A symbol is resonance ONLY when:
    - 1H independently passes its complete setup
      and is entry-now
    AND
    - 4H independently passes its complete setup
      and is entry-now

    No timeframe can qualify the other.
    """

    daily = (
        read_json(
            DAILY_CACHE
        )
        or {}
    )

    derivatives_1h = (
        read_json(
            DERIVATIVES_CACHE
        )
        or {}
    )

    derivatives_4h = (
        read_json(
            DERIVATIVES_4H_CACHE
        )
        or {}
    )

    daily_symbols = (
        daily.get(
            "symbols",
            {}
        )
    )

    d1_symbols = (
        derivatives_1h.get(
            "symbols",
            {}
        )
    )

    d4_symbols = (
        derivatives_4h.get(
            "symbols",
            {}
        )
    )

    symbols = (
        read_json(
            SYMBOL_CACHE
        )
        or []
    )

    results = []

    for symbol in symbols:

        # -----------------------------
        # Independent 1H qualification
        # -----------------------------

        one_h = (
            scan_one_symbol_v33(
                symbol
            )
        )

        one_h_pass = (
            one_h.get("stage")
            == "qualified"
            and one_h.get("status")
            == "可進場"
        )

        if not one_h_pass:
            continue

        # -----------------------------
        # Independent 4H qualification
        # -----------------------------

        four_h = (
            scan_one_symbol_4h_v44(
                symbol
            )
        )

        four_h_pass = (
            four_h.get("stage")
            == "entry_structure_checked"
            and four_h.get("status")
            == "可進場"
        )

        if not four_h_pass:
            continue

        strength = (
            daily_symbols.get(
                symbol
            )
            or {}
        )

        d1 = (
            d1_symbols.get(
                symbol
            )
            or {}
        )

        d4 = (
            d4_symbols.get(
                symbol
            )
            or {}
        )

        # -----------------------------
        # 1H score
        # -----------------------------

        one_h_structure = float(
            one_h.get(
                "v33_structure_score",
                0,
            )
            or 0
        )

        # -----------------------------
        # 4H score
        # -----------------------------

        four_h_structure = (
            calculate_4h_structure_score(
                four_h
            )
        )

        # -----------------------------
        # Shared relative strength
        # -----------------------------

        strength_component = (
            strength_rank_component(
                strength
            )
            if strength
            else 0.0
        )

        # -----------------------------
        # Derivatives validation
        # -----------------------------

        daily_return = float(
            strength.get(
                "daily_return_pct",
                0,
            )
            or 0
        )

        if d1:

            d1_context = (
                interpret_derivatives(
                    d1,
                    daily_return,
                )
            )

        else:

            d1_context = {
                "verdict":
                    "missing",

                "adjustment":
                    0.0,

                "flags":
                    [],
            }

        if d4:

            d4_context = (
                interpret_derivatives(
                    d4,
                    daily_return,
                )
            )

        else:

            d4_context = {
                "verdict":
                    "missing",

                "adjustment":
                    0.0,

                "flags":
                    [],
            }

        # -----------------------------
        # Resonance ranking
        #
        # Structure is dominant.
        #
        # Average the two independent
        # timeframe structure scores,
        # then add shared strength.
        #
        # Derivatives remain subordinate.
        # -----------------------------

        combined_structure = (
            (
                one_h_structure
                + four_h_structure
            )
            / 2.0
        )

        derivative_adjustment = (
            (
                float(
                    d1_context.get(
                        "adjustment",
                        0,
                    )
                )
                +
                float(
                    d4_context.get(
                        "adjustment",
                        0,
                    )
                )
            )
            / 2.0
        )

        resonance_score = (
            combined_structure
            + strength_component
            + derivative_adjustment
        )

        one_h_stop = (
            one_h.get(
                "stop_quality"
            )
            or {}
        )

        four_h_stop = (
            four_h.get(
                "stop_quality"
            )
            or {}
        )

        results.append({
            "symbol":
                symbol,

            "status":
                "resonance_entry_now",

            "resonance_score":
                round(
                    resonance_score,
                    3,
                ),

            "combined_structure_score":
                round(
                    combined_structure,
                    3,
                ),

            "1h": {
                "structure_score":
                    one_h_structure,

                "stop_grade":
                    one_h_stop.get(
                        "grade"
                    ),

                "stop_quality_score":
                    one_h_stop.get(
                        "score"
                    ),

                "stop_type":
                    one_h_stop.get(
                        "type"
                    ),
            },

            "4h": {
                "structure_score":
                    four_h_structure,

                "stop_grade":
                    four_h_stop.get(
                        "grade"
                    ),

                "stop_quality_score":
                    four_h_stop.get(
                        "score"
                    ),

                "stop_type":
                    four_h_stop.get(
                        "type"
                    ),
            },

            "relative_strength": {
                "component":
                    strength_component,

                "daily_return_pct":
                    strength.get(
                        "daily_return_pct"
                    ),

                "btc_daily_return_pct":
                    strength.get(
                        "btc_daily_return_pct"
                    ),

                "relative_vs_btc_pct":
                    strength.get(
                        "relative_vs_btc_pct"
                    ),

                "avg_resistance_vs_btc_pct":
                    strength.get(
                        "avg_resistance_vs_btc_pct"
                    ),

                "resistance_ratio":
                    strength.get(
                        "resistance_ratio"
                    ),
            },

            "derivatives_validation": {
                "1h":
                    d1_context,

                "4h":
                    d4_context,

                "combined_adjustment":
                    round(
                        derivative_adjustment,
                        3,
                    ),
            },
        })

    results.sort(
        key=lambda x:
            x["resonance_score"],
        reverse=True,
    )

    return results


@app.get("/scan/resonance")
async def scan_resonance():

    results = (
        build_1h_4h_resonance()
    )

    return {
        "scanner":
            "1H_4H_RESONANCE",

        "definition":
            (
                "1H independently qualified "
                "AND 4H independently qualified"
            ),

        "timeframe_ownership":
            "independent",

        "count":
            len(results),

        "results":
            results,
    }


# =========================================================
# V5.0 UNIFIED FULL-MARKET SCANNER
#
# 1H + 4H + resonance
#
# Strategy logic is NOT changed.
# This layer only coordinates:
# - cache clocks
# - daily strength
# - technical candidates
# - deduplicated derivatives
# - existing final builders
# =========================================================


async def update_unified_candidate_derivatives(
    candidates_1h,
    candidates_4h,
):
    """
    Download derivatives once per unique symbol.

    Then write the same fresh symbol data into
    the independent 1H and 4H derivative caches.

    This prevents resonance symbols from being
    downloaded twice.
    """

    global RATE_LIMITED

    RATE_LIMITED = False

    candidates_1h = list(
        dict.fromkeys(
            candidates_1h or []
        )
    )

    candidates_4h = list(
        dict.fromkeys(
            candidates_4h or []
        )
    )

    unique_symbols = list(
        dict.fromkeys(
            candidates_1h
            + candidates_4h
        )
    )

    results = {}
    failed = []

    if unique_symbols:

        semaphore = asyncio.Semaphore(
            2
        )

        async with httpx.AsyncClient() as client:

            for symbol in unique_symbols:

                if RATE_LIMITED:
                    break

                try:

                    data = (
                        await fetch_symbol_derivatives(
                            client,
                            symbol,
                            semaphore,
                        )
                    )

                    results[
                        symbol
                    ] = data

                except Exception as exc:

                    failed.append({
                        "symbol":
                            symbol,

                        "error":
                            str(exc),
                    })

                await asyncio.sleep(
                    0.5
                )

    generated_at = (
        datetime.now(
            timezone.utc
        ).isoformat()
    )

    symbols_1h = {
        symbol:
            results[symbol]

        for symbol in candidates_1h

        if symbol in results
    }

    symbols_4h = {
        symbol:
            results[symbol]

        for symbol in candidates_4h

        if symbol in results
    }

    payload_1h = {
        "generated_at_utc":
            generated_at,

        "period":
            "1h",

        "candidate_count":
            len(candidates_1h),

        "symbols":
            symbols_1h,
    }

    payload_4h = {
        "generated_at_utc":
            generated_at,

        "period":
            "4h",

        "candidate_count":
            len(candidates_4h),

        "symbols":
            symbols_4h,
    }

    write_json(
        DERIVATIVES_CACHE,
        payload_1h,
    )

    write_json(
        DERIVATIVES_4H_CACHE,
        payload_4h,
    )

    return {
        "status":
            (
                "stopped_rate_limit"
                if RATE_LIMITED
                else "complete"
            ),

        "1h_candidate_count":
            len(candidates_1h),

        "4h_candidate_count":
            len(candidates_4h),

        "unique_candidate_count":
            len(unique_symbols),

        "overlap_count":
            (
                len(candidates_1h)
                + len(candidates_4h)
                - len(unique_symbols)
            ),

        "downloaded_unique":
            len(results),

        "downloaded_1h":
            len(symbols_1h),

        "downloaded_4h":
            len(symbols_4h),

        "failed":
            failed,

        "rate_limited":
            RATE_LIMITED,
    }


@app.get("/scan/run/all")
async def scan_run_all():

    global RATE_LIMITED

    started = datetime.now(
        timezone.utc
    )

    RATE_LIMITED = False

    pipeline = {}

    # =====================================================
    # STEP 1
    # 1H cache clock
    # =====================================================

    if interval_cache_is_current(
        "1h"
    ):

        pipeline[
            "update_1h"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_1h = (
            await run_incremental_update(
                "1h"
            )
        )

        pipeline[
            "update_1h"
        ] = update_1h

        if (
            update_1h.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_1h",

                "pipeline":
                    pipeline,
            }

    # =====================================================
    # STEP 2
    # 4H cache clock
    # =====================================================

    if interval_cache_is_current(
        "4h"
    ):

        pipeline[
            "update_4h"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_4h = (
            await run_incremental_update(
                "4h"
            )
        )

        pipeline[
            "update_4h"
        ] = update_4h

        if (
            update_4h.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_4h",

                "pipeline":
                    pipeline,
            }

    # =====================================================
    # STEP 3
    # 1D cache clock
    # =====================================================

    if interval_cache_is_current(
        "1d"
    ):

        pipeline[
            "update_1d"
        ] = {
            "status":
                "skipped_current"
        }

    else:

        update_1d = (
            await run_incremental_update(
                "1d"
            )
        )

        pipeline[
            "update_1d"
        ] = update_1d

        if (
            update_1d.get("status")
            != "complete"
        ):
            return {
                "status":
                    "stopped",

                "stage":
                    "update_1d",

                "pipeline":
                    pipeline,
            }

    # =====================================================
    # STEP 4
    # Taiwan 08:00 daily strength
    #
    # Local calculation.
    # =====================================================

    strength_update = (
        await build_daily_strength_cache()
    )

    pipeline[
        "daily_strength"
    ] = strength_update

    if (
        strength_update.get("status")
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "daily_strength",

            "pipeline":
                pipeline,
        }

    # =====================================================
    # STEP 5
    # Independent technical qualification
    #
    # No Binance requests.
    # =====================================================

    candidates_1h = (
        get_current_entry_candidates()
    )

    candidates_4h = (
        get_4h_entry_candidates()
    )

    technical_overlap = sorted(
        set(candidates_1h)
        &
        set(candidates_4h)
    )

    pipeline[
        "technical_candidates"
    ] = {
        "1h_count":
            len(candidates_1h),

        "4h_count":
            len(candidates_4h),

        "1h_symbols":
            candidates_1h,

        "4h_symbols":
            candidates_4h,

        "overlap_count":
            len(technical_overlap),

        "overlap_symbols":
            technical_overlap,
    }

    # =====================================================
    # STEP 6
    # Deduplicated derivatives
    #
    # A symbol appearing in both timeframes
    # is downloaded only once.
    # =====================================================

    derivative_update = (
        await update_unified_candidate_derivatives(
            candidates_1h,
            candidates_4h,
        )
    )

    pipeline[
        "derivatives"
    ] = derivative_update

    if (
        derivative_update.get("status")
        != "complete"
    ):
        return {
            "status":
                "stopped",

            "stage":
                "derivatives",

            "pipeline":
                pipeline,
        }

    # =====================================================
    # STEP 7
    # Existing LOCKED ranking builders.
    #
    # No strategy changes.
    # =====================================================

    results_1h = (
        build_v36_results()
    )

    results_4h = (
        build_4h_final_results()
    )

    # =====================================================
    # STEP 8
    # Independent 1H x 4H resonance.
    # =====================================================

    resonance = (
        build_1h_4h_resonance()
    )

    finished = datetime.now(
        timezone.utc
    )

    elapsed = (
        finished - started
    ).total_seconds()

    return {
        "status":
            "complete",

        "scanner":
            "V5.0_UNIFIED",

        "generated_at_utc":
            finished.isoformat(),

        "elapsed_seconds":
            round(
                elapsed,
                2
            ),

        "cache_clock_optimized":
            True,

        "derivatives_deduplicated":
            True,

        "ranking_priority":
            (
                "chart_structure > "
                "relative_strength > "
                "derivatives_validation"
            ),

        "pipeline":
            pipeline,

        "1h": {
            "count":
                len(results_1h),

            "top10":
                results_1h[:10],
        },

        "4h": {
            "count":
                len(results_4h),

            "top10":
                results_4h[:10],
        },

        "resonance": {
            "count":
                len(resonance),

            "results":
                resonance,
        },
    }


# =========================================================
# V5.1 WATCHLIST
#
# Uses EXISTING locked strategy classifications only.
#
# No Binance requests.
# No new Hard Gate.
# No strategy rule changes.
# =========================================================


def build_watchlist_for_timeframe(
    timeframe,
):
    """
    Build timing watchlist only from symbols that
    already passed the required technical gates.

    Categories:
    - 等止跌K
    - 等回補
    - 過度延伸勿追

    可進場 is NOT included here because it belongs
    to the formal final ranking.
    """

    symbols = (
        read_json(
            SYMBOL_CACHE
        )
        or []
    )

    daily = (
        read_json(
            DAILY_CACHE
        )
        or {}
    )

    daily_symbols = (
        daily.get(
            "symbols",
            {}
        )
    )

    wait_stop = []
    wait_pullback = []
    overextended = []

    for symbol in symbols:

        # -----------------------------------------
        # Run existing locked scanner
        # -----------------------------------------

        if timeframe == "1h":

            result = (
                scan_one_symbol_v33(
                    symbol
                )
            )

            # Only symbols that passed the
            # 1H technical gates.
            if (
                result.get("stage")
                != "qualified"
            ):
                continue

            entry = (
                result.get(
                    "current_entry"
                )
                or {}
            )

            structure_score = float(
                result.get(
                    "v33_structure_score",
                    0
                )
                or 0
            )

        elif timeframe == "4h":

            result = (
                scan_one_symbol_4h_v44(
                    symbol
                )
            )

            # Only symbols that passed:
            # 4H Step1
            # + 1D veto
            # + key candle
            # + entry structure analysis.
            if (
                result.get("stage")
                != "entry_structure_checked"
            ):
                continue

            entry = (
                result.get(
                    "entry"
                )
                or {}
            )

            structure_score = float(
                calculate_4h_structure_score(
                    result
                )
                or 0
            )

        else:

            continue

        status = result.get(
            "status"
        )

        # -----------------------------------------
        # Entry-now belongs to final ranking,
        # not watchlist.
        # -----------------------------------------

        if status == "可進場":
            continue

        # -----------------------------------------
        # Only the three existing timing states.
        # -----------------------------------------

        if status not in (
            "等止跌K",
            "等回補",
            "過度延伸勿追",
        ):
            continue

        strength = (
            daily_symbols.get(
                symbol
            )
            or {}
        )

        strength_component = (
            strength_rank_component(
                strength
            )
            if strength
            else 0.0
        )

        # Watchlist ranking deliberately does NOT
        # require derivatives.
        #
        # These symbols are not entry-now yet.
        # We avoid extra Binance derivative calls
        # just for observation.
        watch_score = (
            structure_score
            + strength_component
        )

        stop_quality = (
            result.get(
                "stop_quality"
            )
            or {}
        )

        latest_stop = (
            entry.get(
                "latest_stop_signal"
            )
        )

        latest_pullback = (
            entry.get(
                "latest_pullback"
            )
        )

        item = {
            "symbol":
                symbol,

            "status":
                status,

            "watch_score":
                round(
                    watch_score,
                    3
                ),

            "structure_score":
                round(
                    structure_score,
                    3
                ),

            "strength_component":
                round(
                    strength_component,
                    3
                ),

            "daily_return_pct":
                strength.get(
                    "daily_return_pct"
                ),

            "btc_daily_return_pct":
                strength.get(
                    "btc_daily_return_pct"
                ),

            "relative_vs_btc_pct":
                strength.get(
                    "relative_vs_btc_pct"
                ),

            "avg_resistance_vs_btc_pct":
                strength.get(
                    "avg_resistance_vs_btc_pct"
                ),

            "resistance_ratio":
                strength.get(
                    "resistance_ratio"
                ),

            "stop_quality":
                stop_quality,

            "latest_stop_signal":
                latest_stop,

            "latest_pullback":
                latest_pullback,
        }

        # -----------------------------------------
        # Preserve useful existing diagnostics
        # when available.
        # -----------------------------------------

        for field in (
            "strict_bull_order",
            "structure_healthy",
            "overextended",
            "distance_to_ma_pct",
            "ma_structure",
        ):

            if field in entry:

                item[field] = (
                    entry.get(field)
                )

            elif field in result:

                item[field] = (
                    result.get(field)
                )

        # -----------------------------------------
        # Classification
        # -----------------------------------------

        if status == "等止跌K":

            wait_stop.append(
                item
            )

        elif status == "等回補":

            wait_pullback.append(
                item
            )

        elif status == "過度延伸勿追":

            overextended.append(
                item
            )

    # ---------------------------------------------
    # Ranking:
    #
    # chart structure
    # > relative strength
    #
    # No derivatives because not entry-now.
    # ---------------------------------------------

    for group in (
        wait_stop,
        wait_pullback,
        overextended,
    ):

        group.sort(
            key=lambda x:
                x["watch_score"],
            reverse=True,
        )

    return {
        "timeframe":
            timeframe,

        "wait_stop": {
            "label":
                "等止跌K",

            "count":
                len(wait_stop),

            "results":
                wait_stop,
        },

        "wait_pullback": {
            "label":
                "等回補",

            "count":
                len(wait_pullback),

            "results":
                wait_pullback,
        },

        "overextended": {
            "label":
                "過度延伸勿追",

            "count":
                len(overextended),

            "results":
                overextended,
        },
    }


@app.get("/scan/watchlist")
async def scan_watchlist():

    started = datetime.now(
        timezone.utc
    )

    watch_1h = (
        build_watchlist_for_timeframe(
            "1h"
        )
    )

    watch_4h = (
        build_watchlist_for_timeframe(
            "4h"
        )
    )

    finished = datetime.now(
        timezone.utc
    )

    return {
        "status":
            "complete",

        "scanner":
            "V5.1_WATCHLIST",

        "generated_at_utc":
            finished.isoformat(),

        "elapsed_seconds":
            round(
                (
                    finished
                    - started
                ).total_seconds(),
                2,
            ),

        "binance_requests":
            0,

        "ranking_priority":
            (
                "chart_structure > "
                "relative_strength"
            ),

        "derivatives":
            "not_required_until_entry_now",

        "1h":
            watch_1h,

        "4h":
            watch_4h,
    }


# =========================================================
# V5.2 UNIFIED FULL SCAN
#
# V5.0 formal scan
# + V5.1 watchlists
#
# No strategy changes.
# No ranking changes.
# Watchlist adds zero Binance requests.
# =========================================================


@app.get("/scan/run/all/v52")
async def scan_run_all_v52():

    started = datetime.now(
        timezone.utc
    )

    # =====================================================
    # STEP 1
    # Run existing LOCKED V5.0 unified scanner.
    #
    # This already handles:
    # - cache clocks
    # - daily strength
    # - 1H technical candidates
    # - 4H technical candidates
    # - deduplicated derivatives
    # - 1H final ranking
    # - 4H final ranking
    # - resonance
    # =====================================================

    formal = await scan_run_all()

    if (
        formal.get("status")
        != "complete"
    ):

        return {
            "status":
                "stopped",

            "scanner":
                "V5.2_UNIFIED",

            "stage":
                "formal_scan",

            "formal":
                formal,
        }

    # =====================================================
    # STEP 2
    # Build V5.1 watchlists locally.
    #
    # No Binance requests.
    # =====================================================

    watch_1h = (
        build_watchlist_for_timeframe(
            "1h"
        )
    )

    watch_4h = (
        build_watchlist_for_timeframe(
            "4h"
        )
    )

    # =====================================================
    # STEP 3
    # Safety check:
    # formal entry symbols should not appear in watchlists.
    #
    # We do NOT change classification.
    # We only report overlap if something unexpected occurs.
    # =====================================================

    formal_1h_symbols = {
        item.get("symbol")

        for item in (
            formal.get(
                "1h",
                {}
            ).get(
                "top10",
                []
            )
            or []
        )

        if item.get("symbol")
    }

    formal_4h_symbols = {
        item.get("symbol")

        for item in (
            formal.get(
                "4h",
                {}
            ).get(
                "top10",
                []
            )
            or []
        )

        if item.get("symbol")
    }

    watch_1h_symbols = set()
    watch_4h_symbols = set()

    for group_name in (
        "wait_stop",
        "wait_pullback",
        "overextended",
    ):

        for item in (
            watch_1h.get(
                group_name,
                {}
            ).get(
                "results",
                []
            )
            or []
        ):

            symbol = item.get(
                "symbol"
            )

            if symbol:
                watch_1h_symbols.add(
                    symbol
                )

        for item in (
            watch_4h.get(
                group_name,
                {}
            ).get(
                "results",
                []
            )
            or []
        ):

            symbol = item.get(
                "symbol"
            )

            if symbol:
                watch_4h_symbols.add(
                    symbol
                )

    overlap_1h = sorted(
        formal_1h_symbols
        &
        watch_1h_symbols
    )

    overlap_4h = sorted(
        formal_4h_symbols
        &
        watch_4h_symbols
    )

    finished = datetime.now(
        timezone.utc
    )

    elapsed = (
        finished
        - started
    ).total_seconds()

    # =====================================================
    # FINAL
    # =====================================================

    return {
        "status":
            "complete",

        "scanner":
            "V5.2_UNIFIED",

        "generated_at_utc":
            finished.isoformat(),

        "elapsed_seconds":
            round(
                elapsed,
                2
            ),

        "strategy": {
            "formal_entry":
                "V5.0_LOCKED",

            "watchlist":
                "V5.1_LOCKED",

            "strategy_changed":
                False,

            "ranking_changed":
                False,

            "watchlist_extra_binance_requests":
                0,
        },

        "formal": {
            "1h":
                formal.get(
                    "1h"
                ),

            "4h":
                formal.get(
                    "4h"
                ),

            "resonance":
                formal.get(
                    "resonance"
                ),
        },

        "watchlist": {
            "1h":
                watch_1h,

            "4h":
                watch_4h,
        },

        "safety_check": {
            "1h_formal_watchlist_overlap":
                overlap_1h,

            "4h_formal_watchlist_overlap":
                overlap_4h,

            "passed":
                (
                    len(overlap_1h) == 0
                    and
                    len(overlap_4h) == 0
                ),
        },

        "pipeline":
            formal.get(
                "pipeline"
            ),
    }



# ============================================================
# V5.3 COMPACT SCAN OUTPUT
# Display / transport layer only.
# Strategy logic remains V5.0 LOCKED + V5.1 LOCKED.
# ============================================================

DISPLAY_ENTRY = "\u53ef\u9032\u5834"
DISPLAY_WAIT_STOP = "\u7b49\u6b62\u8dccK"
DISPLAY_WAIT_PULLBACK = "\u7b49\u56de\u88dc"
DISPLAY_OVEREXTENDED = "\u904e\u5ea6\u5ef6\u4f38\u52ff\u8ffd"
DISPLAY_RESONANCE = "1H\u00d74H \u5171\u632f"


def compact_stop(stop_quality=None, stop=None):
    stop_quality = stop_quality or {}
    stop = stop or {}

    return {
        "grade": stop_quality.get("grade"),
        "score": stop_quality.get("score"),
        "type": (
            stop_quality.get("type")
            or stop.get("type")
        ),
        "relative_volume": (
            stop_quality.get("relative_volume")
            if stop_quality.get("relative_volume") is not None
            else stop.get("relative_volume")
        ),
        "bars_ago": (
            stop_quality.get("bars_ago")
            if stop_quality.get("bars_ago") is not None
            else stop.get("bars_ago")
        ),
    }


def compact_formal_item(item):
    item = item or {}

    derivative = (
        item.get("derivative_context")
        or item.get("derivatives_context")
        or {}
    )

    stop_quality = (
        item.get("stop_quality")
        or {}
    )

    stop = (
        item.get("stop")
        or item.get("latest_stop_signal")
        or {}
    )

    return {
        "symbol": item.get("symbol"),
        "status": DISPLAY_ENTRY,

        "final_score": item.get("final_score"),

        "structure_score": (
            item.get("v33_structure_score")
            if item.get("v33_structure_score") is not None
            else item.get("structure_score")
        ),

        "daily_return_pct":
            item.get("daily_return_pct"),

        "relative_vs_btc_pct":
            item.get("relative_vs_btc_pct"),

        "avg_resistance_vs_btc_pct":
            item.get("avg_resistance_vs_btc_pct"),

        "resistance_ratio":
            item.get("resistance_ratio"),

        "stop":
            compact_stop(
                stop_quality,
                stop,
            ),

        "derivatives": {
            "verdict":
                derivative.get("verdict"),

            "adjustment":
                derivative.get("adjustment"),

            "flags":
                derivative.get("flags"),
        },
    }


def compact_watch_item(item, display_status):
    item = item or {}

    stop_quality = (
        item.get("stop_quality")
        or {}
    )

    stop = (
        item.get("latest_stop_signal")
        or item.get("stop")
        or {}
    )

    return {
        "symbol": item.get("symbol"),
        "status": display_status,

        "watch_score":
            item.get("watch_score"),

        "structure_score":
            item.get("structure_score"),

        "strength_component":
            item.get("strength_component"),

        "daily_return_pct":
            item.get("daily_return_pct"),

        "relative_vs_btc_pct":
            item.get("relative_vs_btc_pct"),

        "avg_resistance_vs_btc_pct":
            item.get("avg_resistance_vs_btc_pct"),

        "resistance_ratio":
            item.get("resistance_ratio"),

        "stop":
            compact_stop(
                stop_quality,
                stop,
            ),

        "latest_pullback":
            item.get("latest_pullback"),
    }


def compact_watchlist(watch):
    """
    Convert the LOCKED V5.1 watchlist structure
    into V5.3 compact display output.

    V5.1 structure:
    category -> {label, count, results}
    """

    watch = watch or {}

    wait_stop_group = (
        watch.get("wait_stop")
        or {}
    )

    wait_pullback_group = (
        watch.get("wait_pullback")
        or {}
    )

    overextended_group = (
        watch.get("overextended")
        or {}
    )

    wait_stop_results = (
        wait_stop_group.get("results")
        if isinstance(
            wait_stop_group,
            dict,
        )
        else []
    ) or []

    wait_pullback_results = (
        wait_pullback_group.get("results")
        if isinstance(
            wait_pullback_group,
            dict,
        )
        else []
    ) or []

    overextended_results = (
        overextended_group.get("results")
        if isinstance(
            overextended_group,
            dict,
        )
        else []
    ) or []

    return {
        "wait_stop": {
            "label":
                DISPLAY_WAIT_STOP,

            "count":
                len(wait_stop_results),

            "results": [
                compact_watch_item(
                    x,
                    DISPLAY_WAIT_STOP,
                )
                for x in wait_stop_results
            ],
        },

        "wait_pullback": {
            "label":
                DISPLAY_WAIT_PULLBACK,

            "count":
                len(wait_pullback_results),

            "results": [
                compact_watch_item(
                    x,
                    DISPLAY_WAIT_PULLBACK,
                )
                for x in wait_pullback_results
            ],
        },

        "overextended": {
            "label":
                DISPLAY_OVEREXTENDED,

            "count":
                len(overextended_results),

            "results": [
                compact_watch_item(
                    x,
                    DISPLAY_OVEREXTENDED,
                )
                for x in overextended_results
            ],
        },
    }


def compact_resonance_item(item):
    item = item or {}

    return {
        "symbol":
            item.get("symbol"),

        "status":
            DISPLAY_RESONANCE,

        "resonance_score":
            item.get("resonance_score"),

        "combined_structure_score":
            item.get("combined_structure_score"),

        "structure_1h":
            item.get("structure_1h"),

        "structure_4h":
            item.get("structure_4h"),

        "strength_component":
            item.get("strength_component"),

        "daily_return_pct":
            item.get("daily_return_pct"),

        "relative_vs_btc_pct":
            item.get("relative_vs_btc_pct"),

        "derivative_adjustment":
            item.get("derivative_adjustment"),

        "stop_1h":
            item.get("stop_1h"),

        "stop_4h":
            item.get("stop_4h"),
    }


@app.get("/scan/run/compact")
async def scan_run_compact():
    """
    V5.3 compact output.

    Executes LOCKED V5.2 and only transforms
    the outgoing response.

    No strategy changes.
    No ranking changes.
    No extra Binance requests beyond V5.2.
    """

    full = await scan_run_all_v52()

    if full.get("status") != "complete":
        return {
            "status": "stopped",
            "scanner": "V5.3_COMPACT",
            "source_status":
                full.get("status"),
            "source":
                full,
        }

    formal = full.get("formal") or {}
    watchlist = full.get("watchlist") or {}

    formal_1h = (
        formal.get("1h")
        or {}
    )

    formal_4h = (
        formal.get("4h")
        or {}
    )

    resonance = (
        formal.get("resonance")
        or {}
    )

    top_1h = (
        formal_1h.get("top10")
        or formal_1h.get("results")
        or []
    )

    top_4h = (
        formal_4h.get("top10")
        or formal_4h.get("results")
        or []
    )

    resonance_results = (
        resonance.get("results")
        or resonance.get("resonance")
        or []
    )

    compact_1h = [
        compact_formal_item(x)
        for x in top_1h
    ]

    compact_4h = [
        compact_formal_item(x)
        for x in top_4h
    ]

    compact_resonance = [
        compact_resonance_item(x)
        for x in resonance_results
    ]

    return {
        "status": "complete",
        "scanner": "V5.3_COMPACT",

        "generated_at_utc":
            full.get("generated_at_utc"),

        "elapsed_seconds":
            full.get("elapsed_seconds"),

        "strategy": {
            "formal":
                "V5.0_LOCKED",

            "watchlist":
                "V5.1_LOCKED",

            "unified":
                "V5.2_LOCKED",

            "compact":
                "V5.3_DISPLAY_ONLY",

            "strategy_changed":
                False,

            "ranking_changed":
                False,
        },

        "1h": {
            "entry_label":
                DISPLAY_ENTRY,

            "count":
                len(compact_1h),

            "top10":
                compact_1h,

            "watchlist":
                compact_watchlist(
                    watchlist.get("1h")
                ),
        },

        "4h": {
            "entry_label":
                DISPLAY_ENTRY,

            "count":
                len(compact_4h),

            "top10":
                compact_4h,

            "watchlist":
                compact_watchlist(
                    watchlist.get("4h")
                ),
        },

        "resonance": {
            "label":
                DISPLAY_RESONANCE,

            "count":
                len(compact_resonance),

            "results":
                compact_resonance,
        },

        "safety_check":
            full.get("safety_check"),
    }


# ============================================================
# V5.4 CHATGPT SCAN FEED
# Compact transport layer for ChatGPT.
# NO strategy / ranking changes.
# ============================================================

def v54_formal_item(x):
    x = x or {}
    stop = x.get("stop") or {}
    derivatives = x.get("derivatives") or {}

    return {
        "symbol": x.get("symbol"),
        "score": x.get("final_score"),
        "structure": x.get("structure_score"),
        "daily_pct": x.get("daily_return_pct"),
        "vs_btc_pct": x.get("relative_vs_btc_pct"),
        "resistance_pct": x.get("avg_resistance_vs_btc_pct"),

        "stop": {
            "grade": stop.get("grade"),
            "type": stop.get("type"),
            "rv": stop.get("relative_volume"),
            "bars_ago": stop.get("bars_ago"),
        },

        "derivatives": {
            "verdict": derivatives.get("verdict"),
            "adjustment": derivatives.get("adjustment"),
        },
    }


def v54_watch_item(x):
    x = x or {}
    stop = x.get("stop") or {}

    return {
        "symbol": x.get("symbol"),
        "score": x.get("watch_score"),
        "structure": x.get("structure_score"),
        "daily_pct": x.get("daily_return_pct"),
        "vs_btc_pct": x.get("relative_vs_btc_pct"),
        "resistance_pct": x.get("avg_resistance_vs_btc_pct"),

        "stop": {
            "grade": stop.get("grade"),
            "type": stop.get("type"),
            "rv": stop.get("relative_volume"),
            "bars_ago": stop.get("bars_ago"),
        },
    }


def v54_watch_group(watch, key):
    watch = watch or {}
    group = watch.get(key) or {}

    if not isinstance(group, dict):
        return []

    return [
        v54_watch_item(x)
        for x in (group.get("results") or [])
    ]


def v54_resonance_item(x):
    x = x or {}

    return {
        "symbol": x.get("symbol"),
        "score": x.get("resonance_score"),
        "structure": x.get("combined_structure_score"),
        "structure_1h": x.get("structure_1h"),
        "structure_4h": x.get("structure_4h"),
        "daily_pct": x.get("daily_return_pct"),
        "vs_btc_pct": x.get("relative_vs_btc_pct"),
        "derivative_adjustment": x.get("derivative_adjustment"),
        "stop_1h": x.get("stop_1h"),
        "stop_4h": x.get("stop_4h"),
    }


@app.get("/scan/feed")
async def scan_feed():
    """
    V5.4 ChatGPT feed.

    Runs V5.3 / V5.2 locked scanner and removes
    fields not needed for normal scan presentation.

    Strategy unchanged.
    Ranking unchanged.
    """

    data = await scan_run_compact()

    if data.get("status") != "complete":
        return {
            "status": "stopped",
            "scanner": "V5.4_CHATGPT_FEED",
            "source_status": data.get("status"),
        }

    one_h = data.get("1h") or {}
    four_h = data.get("4h") or {}
    resonance = data.get("resonance") or {}

    one_watch = one_h.get("watchlist") or {}
    four_watch = four_h.get("watchlist") or {}

    return {
        "status": "complete",
        "scanner": "V5.4_CHATGPT_FEED",

        "generated_at_utc":
            data.get("generated_at_utc"),

        "elapsed_seconds":
            data.get("elapsed_seconds"),

        "strategy": "V5.2_LOCKED",

        "1h": {
            "entry": [
                v54_formal_item(x)
                for x in (one_h.get("top10") or [])
            ],

            "wait_stop":
                v54_watch_group(
                    one_watch,
                    "wait_stop",
                ),

            "wait_pullback":
                v54_watch_group(
                    one_watch,
                    "wait_pullback",
                ),

            "overextended":
                v54_watch_group(
                    one_watch,
                    "overextended",
                ),
        },

        "4h": {
            "entry": [
                v54_formal_item(x)
                for x in (four_h.get("top10") or [])
            ],

            "wait_stop":
                v54_watch_group(
                    four_watch,
                    "wait_stop",
                ),

            "wait_pullback":
                v54_watch_group(
                    four_watch,
                    "wait_pullback",
                ),

            "overextended":
                v54_watch_group(
                    four_watch,
                    "overextended",
                ),
        },

        "resonance": [
            v54_resonance_item(x)
            for x in (
                resonance.get("results")
                or []
            )
        ],
    }

