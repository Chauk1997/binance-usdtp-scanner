"""Official strategy proxy: closed-window taker buy volume minus sell volume."""
from trade_cvd import calculate_cvd_from_exchange_volume


def calculate_cvd_proxy(bars, start, end):
    result = calculate_cvd_from_exchange_volume(bars, start, end)
    return {**result, 'kind': 'proxy', 'label': 'CVD Proxy'}
