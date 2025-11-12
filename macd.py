from typing import List, Tuple, Optional

def compute_ema_full(prices: List[float], period: int) -> List[Optional[float]]:
    if not prices or period <= 0:
        return []
    k = 2.0 / (period + 1)
    ema_vals: List[Optional[float]] = []
    prev: Optional[float] = None
    for i, price in enumerate(prices):
        if i + 1 < period:
            ema_vals.append(None)
        elif i + 1 == period:
            sma = sum(prices[:period]) / period
            prev = sma
            ema_vals.append(prev)
        else:
            prev = (price - prev) * k + prev
            ema_vals.append(prev)
    return ema_vals

def compute_macd_histogram(closes: List[float], fast=12, slow=26, signal=9) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    if len(closes) < slow + signal:
        return [], [], []
    ema_fast = compute_ema_full(closes, fast)
    ema_slow = compute_ema_full(closes, slow)
    macd_line: List[Optional[float]] = []
    for e_f, e_s in zip(ema_fast, ema_slow):
        if e_f is None or e_s is None:
            macd_line.append(None)
        else:
            macd_line.append(e_f - e_s)
    macd_clean = [x for x in macd_line if x is not None]
    signal_ema_full = compute_ema_full(macd_clean, signal)
    macd_signal = [None] * len(macd_line)
    first_macd_index = next((i for i, x in enumerate(macd_line) if x is not None), None)
    if first_macd_index is not None:
        for i, val in enumerate(signal_ema_full):
            macd_signal[first_macd_index + i] = val
    macd_hist: List[Optional[float]] = []
    for m, s in zip(macd_line, macd_signal):
        if m is None or s is None:
            macd_hist.append(None)
        else:
            macd_hist.append(m - s)
    return macd_line, macd_signal, macd_hist

def update_ema(prev_ema: float, price: float, period: int) -> float:
    k = 2.0 / (period + 1)
    return (price - prev_ema) * k + prev_ema

def flipped_negative_to_positive_at_open(macd_hist: List[Optional[float]], open_index: int) -> bool:
    if open_index <= 0 or open_index >= len(macd_hist):
        return False
    prev = macd_hist[open_index - 1]
    cur = macd_hist[open_index]
    if prev is None or cur is None:
        return False
    return prev < 0 and cur >= 0
