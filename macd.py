# macd.py
def ema(series, periods):
    values = []
    k = 2 / (periods + 1)
    ema_prev = series[0]

    for price in series:
        ema_current = (price - ema_prev) * k + ema_prev
        ema_prev = ema_current
        values.append(ema_current)

    return values

def compute_macd_histogram(closes, fast=12, slow=26, signal=9):
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = [a - b for a, b in zip(ema_fast, ema_slow)]
    macd_signal = ema(macd_line, signal)
    macd_hist = [a - b for a, b in zip(macd_line, macd_signal)]
    return macd_line, macd_signal, macd_hist

def flipped_negative_to_positive_at_open(macd_hist, idx):
    if idx < 1:
        return False
    prev = macd_hist[idx - 1]
    cur = macd_hist[idx]
    return prev < 0 and cur >= 0
