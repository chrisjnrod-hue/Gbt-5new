import asyncio
import os
import math
import time
from datetime import datetime, timezone
from typing import List

from macd import compute_macd_histogram, flipped_negative_to_positive_at_open
from bybit_ws_client import BybitWebSocketClient

TF_TO_SECONDS = {"5": 300, "15": 900, "60": 3600, "240": 14400, "1440": 86400}


class Scheduler:
    """Main orchestrator for MACD scanning, alignment, and signal generation."""

    def __init__(self, bybit_client, store, notifier, macd_params, token_bucket):
        self.bybit = bybit_client
        self.store = store
        self.notifier = notifier
        self.macd_fast, self.macd_slow, self.macd_signal = macd_params
        self.token_bucket = token_bucket
        self._running = False
        self.symbols: List[str] = []
        self.ws_client = None
        self.batch = int(os.getenv("BATCH_SIZE", "40"))
        self.backfill_limit = int(os.getenv("BACKFILL_LIMIT", "200"))
        self._task = None
        self._scanner_task = None
        self._trim_task = None

    async def start(self):
        await self.token_bucket.start()
        self._running = True
        print("[Scheduler] Loading symbols from Bybit…")
        self.symbols = await self.bybit.get_symbols()
        print(f"[Scheduler] Loaded {len(self.symbols)} symbols from Bybit")

        # WebSocket client
        self.ws_client = BybitWebSocketClient(category="linear")
        await self.ws_client.connect()

        # Kline callback
        async def on_kline(msg):
            try:
                topic = msg.get("topic", "")
                parts = topic.split(".")
                if len(parts) < 3:
                    print(f"[WS] Unexpected topic format: {topic}")
                    return
                _, tf, symbol = parts
                data = msg.get("data", [])
                if not data:
                    return
                k = data[0]
                start_raw = int(k.get("start", time.time()))
                start = start_raw // 1000 if start_raw > 10_000_000_000 else start_raw
                if start < 1_260_000_000 or start > 2_070_000_0000:
                    print(f"[WARN] Skipping truly abnormal WS timestamp {start_raw} for {symbol}-{tf}")
                    return
                candle = {"open_time": start, "close": float(k.get("close", 0.0)), "_raw": k}
                await asyncio.to_thread(self.store.save_candles, symbol, tf, [candle])
            except Exception as e:
                print(f"[WS] Kline callback error: {e}")

        # Subscribe
        limit = int(os.getenv("WS_SYMBOL_LIMIT", "50"))
        for sym in self.symbols[:limit]:
            await self.ws_client.subscribe_kline(sym, "5", on_kline)
            await self.ws_client.subscribe_kline(sym, "60", on_kline)

        # Backfill and initial scan
        await self._backfill_all()
        print("[Scheduler] Running initial hourly scan after backfill…")
        await self._run_tf_check("60")
        await self._run_tf_check("240")

        # Start loops
        self._task = asyncio.create_task(self._candle_open_loops())
        self._scanner_task = asyncio.create_task(self._five_min_scanner())
        self._trim_task = asyncio.create_task(self._trim_loop())
        print("[Scheduler] Background loops running (hourly, 5min, trim) ✅")

    async def stop(self):
        self._running = False
        for t in (self._task, self._scanner_task, self._trim_task):
            if t:
                t.cancel()
        if self.ws_client:
            await self.ws_client.close()

    async def _backfill_all(self):
        tfs = [x.strip() for x in os.getenv("BACKFILL_TFS", "1440,240,60").split(",")]
        for i in range(0, len(self.symbols), self.batch):
            tasks = []
            for sym in self.symbols[i:i + self.batch]:
                for tf in tfs:
                    tasks.append(self._fetch_and_store(sym, tf))
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.25)

    async def _fetch_and_store(self, symbol, tf, limit=None):
        try:
            limit = limit or self.backfill_limit
            klines = await self.bybit.get_klines(symbol, tf, limit=limit)
            if klines:
                await asyncio.to_thread(self.store.save_candles, symbol, tf, klines)
        except Exception as e:
            print(f"[Scheduler] fetch_and_store error for {symbol}-{tf}: {e}")

    async def _wait_until_next(self, seconds):
        now = datetime.now(timezone.utc).timestamp()
        next_ts = (math.floor(now / seconds) + 1) * seconds
        await asyncio.sleep(max(0, next_ts - now) + 0.25)

    async def _candle_open_loops(self):
        print("[Scheduler] Candle loop active (runs 1h/4h checks)")
        while self._running:
            try:
                await self._run_tf_check("60")
                now = datetime.now(timezone.utc)
                if now.hour % 4 == 0:
                    await self._run_tf_check("240")
                await self._wait_until_next(TF_TO_SECONDS["60"])
            except Exception as e:
                print(f"[Scheduler] candle_open_loops error: {e}")
                await asyncio.sleep(10)

    async def _run_tf_check(self, tf: str):
        print(f"[Scheduler] Running timeframe check for {tf}")
        for i in range(0, len(self.symbols), self.batch):
            tasks = [self._process_symbol_tf(sym, tf) for sym in self.symbols[i:i + self.batch]]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.25)

    async def _process_symbol_tf(self, symbol: str, tf: str):
        try:
            klines = await asyncio.to_thread(self.store.get_candles, symbol, tf)
            if not klines:
                await self._fetch_and_store(symbol, tf)
                klines = await asyncio.to_thread(self.store.get_candles, symbol, tf)
            if not klines:
                return

            closes = [float(k["close"]) for k in klines]
            _, _, macd_hist = compute_macd_histogram(
                closes, self.macd_fast, self.macd_slow, self.macd_signal
            )
            idx = len(macd_hist) - 1
            open_ts = int(klines[-1].get("open_time", time.time()))
            if open_ts < 1_260_000_000 or open_ts > 2_070_000_0000:
                print(f"[WARN] Abnormal timestamp {open_ts} for {symbol}-{tf}, skipping this TF.")
                return

            flipped = flipped_negative_to_positive_at_open(macd_hist, idx)
            if tf in ("60", "240") and flipped:
                ts_str = datetime.utcfromtimestamp(open_ts).strftime("%Y-%m-%d %H:%M")
                print(f"[MACD] {symbol} ({tf}) flipped positive on {ts_str} UTC")
                ttl = TF_TO_SECONDS.get(tf, 3600)
                meta = {
                    "symbol": symbol,
                    "tf": tf,
                    "open_ts": open_ts,
                    "expiry": open_ts + ttl,
                    "macd_hist_open": macd_hist[idx],
                    "last_notified": None,
                }
                await asyncio.to_thread(self.store.create_signal, symbol, tf, meta, ttl)
        except Exception as e:
            print(f"[Scheduler] process_symbol_tf error for {symbol}-{tf}: {e}")

    async def _five_min_scanner(self):
        while self._running:
            await self._wait_until_next(TF_TO_SECONDS["5"])
            print("[Scheduler] Running 5-min scan")
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
            all_tfs = ["1440", "240", "60", "15", "5"]
            tfs_to_check = [tf for tf in all_tfs if tf != root_tf]
            root_signal = await asyncio.to_thread(self.store.get_signal, symbol, root_tf)
            if not root_signal:
                return

            aligned = True
            last_flipped_tf = None
            last_flip_ts = None

            for tf in tfs_to_check:
                klines = await asyncio.to_thread(self.store.get_candles, symbol, tf)
                if not klines:
                    await self._fetch_and_store(symbol, tf, limit=200)
                    klines = await asyncio.to_thread(self.store.get_candles, symbol, tf)
                if not klines:
                    aligned = False
                    break

                closes = [float(k["close"]) for k in klines]
                _, _, macd_hist = compute_macd_histogram(
                    closes, self.macd_fast, self.macd_slow, self.macd_signal
                )
                idx = len(macd_hist) - 1
                open_time = int(klines[-1].get("open_time", time.time()))
                flipped = flipped_negative_to_positive_at_open(macd_hist, idx)

                if flipped:
                    last_flipped_tf = tf
                    last_flip_ts = open_time
                elif macd_hist[-1] <= 0:
                    aligned = False
                    break

            if aligned and last_flipped_tf and last_flip_ts:
                last_notified = root_signal.get("last_notified")
                if last_notified != last_flip_ts:
                    title = "ENTRY" if root_tf == "60" else "SIGNAL"
                    ts_str = datetime.fromtimestamp(last_flip_ts, timezone.utc).strftime("%Y-%m-%d %H:%M")
                    msg = f"[{title}] {symbol} — root={root_tf} confirmed by {last_flipped_tf} flip at {ts_str} UTC"
                    print(f"[ALERT] {msg}")
                    await self.notifier.send(msg)
                    await asyncio.to_thread(self.store.set_last_notified, symbol, root_tf, last_flip_ts)
        except Exception as e:
            print(f"[Scheduler] check_alignment error for {symbol}-{root_tf}: {e}")

    async def _trim_loop(self):
        interval = int(os.getenv("TRIM_INTERVAL_MINUTES", "60")) * 60
        while self._running:
            try:
                await asyncio.to_thread(self.store.trim_all)
                print("[Scheduler] Trimmed old data ✅")
            except Exception as e:
                print(f"[Scheduler] trim_loop error: {e}")
            await asyncio.sleep(interval)
