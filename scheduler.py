import asyncio
import os
import math
import time
from datetime import datetime, timezone
from typing import List

from macd import compute_macd_histogram, flipped_negative_to_positive_at_open

TF_TO_SECONDS = {
    "5": 5*60,
    "15": 15*60,
    "60": 60*60,
    "240": 4*60*60,
    "1440": 24*60*60,
}

class Scheduler:
    def __init__(self, bybit_client, store, notifier, macd_params, token_bucket):
        self.bybit = bybit_client
        self.store = store
        self.notifier = notifier
        self.macd_fast, self.macd_slow, self.macd_signal = macd_params
        self.token_bucket = token_bucket
        self._running = False
        self.symbols = []
        self._task = None
        self._scanner_task = None
        self._trim_task = None

        self.batch = int(os.getenv("BATCH_SIZE", "40"))
        self.backfill_limit = int(os.getenv("BACKFILL_LIMIT", "200"))

    async def start(self):
        print("[Scheduler] Starting scheduler...")
        await self.token_bucket.start()
        self._running = True

        self.symbols = await self.bybit.get_symbols()
        print(f"[Scheduler] Loaded {len(self.symbols)} symbols from Bybit")

        await self._backfill_all()
        print("[Scheduler] Launching background loops (hourly, 5min, trim)")

        self._task = asyncio.create_task(self._candle_open_loops())
        self._scanner_task = asyncio.create_task(self._five_min_scanner())
        self._trim_task = asyncio.create_task(self._trim_loop())

    async def stop(self):
        self._running = False
        for t in (self._task, self._scanner_task, self._trim_task):
            if t:
                t.cancel()

    async def _backfill_all(self):
        tfs = [x.strip() for x in os.getenv("BACKFILL_TFS", "1440,240,60").split(",")]
        batch = self.batch
        for i in range(0, len(self.symbols), batch):
            tasks = []
            for sym in self.symbols[i:i+batch]:
                for tf in tfs:
                    tasks.append(self._fetch_and_store(sym, tf))
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)

    async def _fetch_and_store(self, symbol, tf, limit=None):
        try:
            limit = limit or self.backfill_limit
            klines = await self.bybit.get_klines(symbol, tf, limit=limit)
            if klines:
                await asyncio.to_thread(self.store.save_candles, symbol, tf, klines)
        except Exception:
            return

    async def _wait_until_next(self, period_seconds: int):
        now = datetime.now(timezone.utc)
        now_ts = now.timestamp()
        next_ts = (math.floor(now_ts / period_seconds) + 1) * period_seconds
        wait = max(0, next_ts - now_ts)
        await asyncio.sleep(wait + 0.25)

    async def _candle_open_loops(self):
        print("[Scheduler] Entered candle open loop")

        # ✅ Run immediately on startup
        await self._run_tf_check("60")
        now = datetime.now(timezone.utc)
        if now.hour % 4 == 0:
            await self._run_tf_check("240")

        # ✅ Then continue every hour afterward
        while self._running:
            await self._wait_until_next(TF_TO_SECONDS["60"])
            await self._run_tf_check("60")
            now = datetime.now(timezone.utc)
            if now.hour % 4 == 0:
                await self._run_tf_check("240")

    async def _run_tf_check(self, tf: str):
        batch = self.batch
        for i in range(0, len(self.symbols), batch):
            tasks = [self._process_symbol_tf(sym, tf) for sym in self.symbols[i:i+batch]]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)

    async def _process_symbol_tf(self, symbol: str, tf: str):
        try:
            klines = await asyncio.to_thread(self.store.get_candles, symbol, tf)
            if not klines:
                await self._fetch_and_store(symbol, tf)
                klines = await asyncio.to_thread(self.store.get_candles, symbol, tf)
            if not klines:
                return
            closes = [float(k["close"]) for k in klines]
            macd_line, macd_signal, macd_hist = compute_macd_histogram(
                closes, self.macd_fast, self.macd_slow, self.macd_signal
            )
            open_index = len(macd_hist) - 1
            if flipped_negative_to_positive_at_open(macd_hist, open_index):
                ttl = TF_TO_SECONDS.get(tf, 3600)
                meta = {
                    "symbol": symbol,
                    "tf": tf,
                    "open_ts": int(time.time()),
                    "expiry": int(time.time()) + ttl,
                    "macd_hist_open": macd_hist[open_index],
                    "last_notified": None,
                }
                await asyncio.to_thread(self.store.create_signal, symbol, tf, meta, ttl)
                for lazy_tf in os.getenv("LAZY_TFS", "5,15").split(","):
                    lazy_tf = lazy_tf.strip()
                    if lazy_tf:
                        await self._fetch_and_store(symbol, lazy_tf, limit=200)
        except Exception:
            return

    async def _five_min_scanner(self):
        while self._running:
            await self._wait_until_next(TF_TO_SECONDS["5"])
            await self._scan_active_signals()

    async def _scan_active_signals(self):
        tasks = []
        for root_tf in ["60", "240"]:
            symbols = await asyncio.to_thread(self.store.get_active_signals, root_tf)
            for sym in symbols:
                tasks.append(self._check_alignment(sym, root_tf))
        if tasks:
            await asyncio.gather(*tasks)

    async def _check_alignment(self, symbol: str, root_tf: str):
        try:
            required = ["1440", "15", "5"]
            if root_tf == "60":
                required.insert(0, "240")
            else:
                required.insert(0, "60")
            aligned = True
            last_flipped_tf = None
            last_flip_ts = None
            for tf in required:
                klines = await asyncio.to_thread(self.store.get_candles, symbol, tf)
                if not klines:
                    await self._fetch_and_store(symbol, tf, limit=200)
                    klines = await asyncio.to_thread(self.store.get_candles, symbol, tf)
                if not klines:
                    aligned = False
                    break
                closes = [float(k["close"]) for k in klines]
                macd_line, macd_signal, macd_hist = compute_macd_histogram(
                    closes, self.macd_fast, self.macd_slow, self.macd_signal
                )
                idx = len(macd_hist) - 1
                if not flipped_negative_to_positive_at_open(macd_hist, idx):
                    aligned = False
                    break
                else:
                    open_time = (
                        int(klines[-1]["open_time"])
                        if klines[-1].get("open_time")
                        else int(time.time())
                    )
                    last_flipped_tf = tf
                    last_flip_ts = open_time
            if aligned:
                sig = await asyncio.to_thread(self.store.get_signal, symbol, root_tf)
                last_notified = sig.get("last_notified") if sig else None
                if last_notified != last_flip_ts:
                    sig60 = await asyncio.to_thread(self.store.get_signal, symbol, "60")
                    sig240 = await asyncio.to_thread(self.store.get_signal, symbol, "240")
                    is_super = (sig60 is not None and root_tf == "240") or (
                        sig240 is not None and root_tf == "60"
                    )
                    title = "SUPER" if is_super else "ALIGNED"
                    ts_str = datetime.fromtimestamp(last_flip_ts, timezone.utc).isoformat()
                    msg = f"[{title}] {symbol} — root={root_tf} aligned on open {ts_str} UTC. last flipped timeframe: {last_flipped_tf}"
                    await self.notifier.send(msg)
                    await asyncio.to_thread(
                        self.store.set_last_notified, symbol, root_tf, last_flip_ts
                    )
        except Exception:
            return

    async def _trim_loop(self):
        interval = int(os.getenv("TRIM_INTERVAL_MINUTES", "60")) * 60
        while self._running:
            try:
                await asyncio.to_thread(self.store.trim_all)
            except Exception:
                pass
            await asyncio.sleep(interval)
