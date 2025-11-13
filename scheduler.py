# scheduler.py
import asyncio
import os
import math
import time
from datetime import datetime, timezone
from typing import List, Set

from macd import compute_macd_histogram, flipped_negative_to_positive_at_open
from bybit_ws_client import BybitWebSocketClient

TF_TO_SECONDS = {"5": 300, "15": 900, "60": 3600, "240": 14400, "1440": 86400}


def chunked(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


class Scheduler:
    """Scheduler with dynamic WS subscriptions for real-time entry confirmations (5m + 15m)."""

    def __init__(self, bybit_client, store, notifier, macd_params, token_bucket):
        self.bybit = bybit_client
        self.store = store
        self.notifier = notifier
        self.macd_fast, self.macd_slow, self.macd_signal = macd_params
        self.token_bucket = token_bucket

        self._running = False
        self.symbols: List[str] = []
        self.ws_client: BybitWebSocketClient | None = None

        self.batch = int(os.getenv("BATCH_SIZE", "40"))
        self.backfill_limit = int(os.getenv("BACKFILL_LIMIT", "200"))

        # no bulk startup subscriptions by default
        self.ws_symbol_limit = int(os.getenv("WS_SYMBOL_LIMIT", "0"))
        self.ws_sub_batch = int(os.getenv("WS_SUB_BATCH", "20"))

        # live-subscriptions tracked by symbol (we subscribe to 5m & 15m per symbol)
        self.live_subscriptions: Set[str] = set()

        self._task = None
        self._scanner_task = None
        self._trim_task = None
        self._dynamic_task = None

        self._ws_on_kline = None

    async def start(self):
        await self.token_bucket.start()
        self._running = True

        print("[Scheduler] Loading symbols from Bybit…")
        self.symbols = await self.bybit.get_symbols()
        print(f"[Scheduler] Loaded {len(self.symbols)} symbols from Bybit")

        # Create and connect WS client
        self.ws_client = BybitWebSocketClient(category="linear")
        await self.ws_client.connect()

        # ws_on_kline handler used by dynamic subscriptions
        async def on_kline(msg: dict):
            try:
                topic = msg.get("topic", "")
                parts = topic.split(".")
                if len(parts) < 3:
                    return
                _, tf, symbol = parts
                data = msg.get("data", [])
                if not data:
                    return
                k = data[0]
                start_raw = int(k.get("start", time.time()))
                start = start_raw // 1000 if start_raw > 10_000_000_000 else start_raw
                if start < 1_260_000_000 or start > 2_070_000_0000:
                    return
                candle = {"open_time": start, "close": float(k.get("close", 0.0)), "_raw": k}
                await asyncio.to_thread(self.store.save_candles, symbol, tf, [candle])
            except Exception as e:
                print(f"[WS] Kline callback error: {e}")

        self._ws_on_kline = on_kline

        # Optional small startup subscription for top symbols if configured
        limit = min(self.ws_symbol_limit, len(self.symbols))
        if limit > 0:
            subscribe_coros = []
            for sym in self.symbols[:limit]:
                subscribe_coros.append(self.ws_client.subscribe_kline(sym, "5", self._ws_on_kline))
                subscribe_coros.append(self.ws_client.subscribe_kline(sym, "60", self._ws_on_kline))

            print(f"[Scheduler] Performing startup WS subscribe to {limit} symbols...")
            for chunk in chunked(subscribe_coros, self.ws_sub_batch):
                try:
                    await asyncio.gather(*chunk)
                except Exception as e:
                    print(f"[Scheduler] WS subscribe batch error: {e}")
                await asyncio.sleep(0.1)
            print("[Scheduler] Startup WS subscribe complete.")

        # Backfill
        await self._backfill_all()

        # Initial scans
        print("[Scheduler] Running initial hourly scan after backfill…")
        await self._run_tf_check("60")
        await self._run_tf_check("240")

        # Startup hello
        try:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            hello_msg = f"🤖 Scheduler started successfully — system live and ready to scan! ({ts})"
            print("[Scheduler] Sending startup hello message…")
            await self.notifier.send(hello_msg)
        except Exception as e:
            print(f"[Scheduler] notifier send error at startup: {e}")

        # Start loops
        self._task = asyncio.create_task(self._candle_open_loops())
        self._scanner_task = asyncio.create_task(self._five_min_scanner())
        self._trim_task = asyncio.create_task(self._trim_loop())
        self._dynamic_task = asyncio.create_task(self._dynamic_ws_manager())
        print("[Scheduler] Background loops running (hourly, 5min, trim) ✅")

    async def stop(self):
        self._running = False
        for t in (self._task, self._scanner_task, self._trim_task, self._dynamic_task):
            if t:
                t.cancel()
        if self.ws_client:
            await self.ws_client.close()

    async def _backfill_all(self):
        tfs = [x.strip() for x in os.getenv("BACKFILL_TFS", "1440,240,60").split(",")]
        for i in range(0, len(self.symbols), self.batch):
            tasks = []
            for sym in self.symbols[i : i + self.batch]:
                for tf in tfs:
                    tasks.append(self._fetch_and_store(sym, tf))
            if tasks:
                await asyncio.gather(*tasks)
            await asyncio.sleep(0.25)

    async def _fetch_and_store(self, symbol: str, tf: str, limit: int | None = None):
        try:
            limit = limit or self.backfill_limit
            klines = await self.bybit.get_klines(symbol, tf, limit=limit)
            if klines:
                await asyncio.to_thread(self.store.save_candles, symbol, tf, klines)
        except Exception as e:
            print(f"[Scheduler] fetch_and_store error for {symbol}-{tf}: {e}")

    async def _wait_until_next(self, seconds: int):
        now = datetime.now(timezone.utc).timestamp()
        next_ts = (math.floor(now / seconds) + 1) * seconds
        await asyncio.sleep(max(0, next_ts - now) + 0.25)

    async def _candle_open_loops(self):
        print("[Scheduler] Candle loop active (runs 1 h / 4 h checks)")
        while self._running:
            try:
                await self._wait_until_next(TF_TO_SECONDS["60"])
                await self._run_tf_check("60")
                now = datetime.now(timezone.utc)
                if now.hour % 4 == 0:
                    await self._run_tf_check("240")
            except Exception as e:
                print(f"[Scheduler] candle_open_loops error: {e}")
                await asyncio.sleep(10)

    async def _run_tf_check(self, tf: str):
        print(f"[Scheduler] Running timeframe check for {tf}")
        for i in range(0, len(self.symbols), self.batch):
            tasks = [self._process_symbol_tf(sym, tf) for sym in self.symbols[i : i + self.batch]]
            if tasks:
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
                # Immediately subscribe live for entry confirmation
                asyncio.create_task(self._subscribe_symbol_live(symbol))
        except Exception as e:
            print(f"[Scheduler] process_symbol_tf error for {symbol}-{tf}: {e}")

    async def _subscribe_symbol_live(self, symbol: str):
        if not self.ws_client:
            return
        if symbol in self.live_subscriptions:
            return
        try:
            await self.ws_client.subscribe_kline(symbol, "5", self._ws_on_kline)
            await self.ws_client.subscribe_kline(symbol, "15", self._ws_on_kline)
            self.live_subscriptions.add(symbol)
            print(f"[WS] Subscribed live: {symbol} (5m,15m)")
        except Exception as e:
            print(f"[Scheduler] subscribe_symbol_live error for {symbol}: {e}")

    async def _unsubscribe_symbol_live(self, symbol: str):
        if not self.ws_client:
            return
        if symbol not in self.live_subscriptions:
            return
        try:
            try:
                await self.ws_client.unsubscribe_kline(symbol, "5")
            except Exception:
                pass
            try:
                await self.ws_client.unsubscribe_kline(symbol, "15")
            except Exception:
                pass
            self.live_subscriptions.discard(symbol)
            print(f"[WS] Unsubscribed live: {symbol} (5m,15m)")
        except Exception as e:
            print(f"[Scheduler] unsubscribe_symbol_live error for {symbol}: {e}")

    async def _dynamic_ws_manager(self):
        while self._running:
            try:
                active60 = await asyncio.to_thread(self.store.get_active_signals, "60") or []
                active240 = await asyncio.to_thread(self.store.get_active_signals, "240") or []
                active_set = set(active60) | set(active240)

                to_sub = [s for s in active_set if s not in self.live_subscriptions]
                for symbol in to_sub:
                    asyncio.create_task(self._subscribe_symbol_live(symbol))
                    await asyncio.sleep(0.05)

                to_unsub = [s for s in list(self.live_subscriptions) if s not in active_set]
                for symbol in to_unsub:
                    asyncio.create_task(self._unsubscribe_symbol_live(symbol))
                    await asyncio.sleep(0.02)

            except Exception as e:
                print(f"[Scheduler] dynamic_ws_manager error: {e}")

            await asyncio.sleep(30)

    async def _five_min_scanner(self):
        while self._running:
            await self._wait_until_next(TF_TO_SECONDS["5"])
            print("[Scheduler] Running 5 min scan")
            await self._scan_active_signals()

    async def _scan_active_signals(self):
        tasks = []
        for root_tf in ("60", "240"):
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

                if macd_hist[-1] <= 0:
                    aligned = False
                    break

                flipped = flipped_negative_to_positive_at_open(macd_hist, idx)
                if flipped:
                    last_flipped_tf = tf
                    last_flip_ts = open_time

            if aligned and last_flipped_tf and last_flip_ts:
                root_open_ts = root_signal.get("open_ts", 0)
                if last_flip_ts > root_open_ts:
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
