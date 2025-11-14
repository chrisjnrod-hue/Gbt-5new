# scheduler.py
"""Scheduler for processing market data, detecting MACD flips, and managing signals and subscriptions."""
import asyncio
import time
import os
from datetime import datetime, timezone

from macd import compute_macd_histogram, flipped_negative_to_positive_at_open
from store_sqlite import Store
from bybit_ws_client import BybitWebSocketClient
from bybit_client import BybitClientV5
from notifier import Notifier

class Scheduler:
    def __init__(self):
        # Core components
        self.store = Store("market.db")
        self.rest = BybitClientV5()
        self.ws = BybitWebSocketClient()
        self.notifier = Notifier()

        # List of USDT perpetual symbols
        self.symbols = []

        # Real-time WS intervals for confirmation
        self.ws_intervals = ["5", "15"]

        # Track which symbols are currently subscribed
        self.active_ws = {}

        # Background tasks
        self._tf_loop_task = None
        self._ws_manager_task = None

    # ========================================================
    # STARTUP
    # ========================================================
    async def start(self):
        # Connect WS first (persistent connection)
        await self.ws.connect()

        print("[Scheduler] Loading symbols from Bybit…")
        syms = await self.rest.fetch_usdt_perpetuals()
        self.symbols = syms
        print(f"[Scheduler] Loaded {len(self.symbols)} symbols.")

        # Telegram startup notification
        try:
            await self.notifier.send("🤖 Scheduler started — system running.")
        except:
            pass

        # Clear old signals
        self.store.clear_signals()
        print("[Scheduler] Old signals cleared.")

        # Backfill 1H + 4H
        await self._initial_backfill()

        # Initial MACD root scan
        await self._scan_root_tf("60")
        await self._scan_root_tf("240")

        # Launch background loops
        self._tf_loop_task = asyncio.create_task(self._tf_loop())
        self._ws_manager_task = asyncio.create_task(self._ws_manager())

        print("[Scheduler] Background loops running.")

    # ========================================================
    #          INITIAL BACKFILL OF ROOT TFs
    # ========================================================
    async def _initial_backfill(self):
        print("[Scheduler] Backfilling 1H + 4H…")

        for symbol in self.symbols:
            try:
                # 1H
                kl1h = await self.rest.get_klines(symbol, "60", limit=200)
                if kl1h:
                    self.store.save_candles(symbol, "60", kl1h)

                # 4H
                kl4h = await self.rest.get_klines(symbol, "240", limit=200)
                if kl4h:
                    self.store.save_candles(symbol, "240", kl4h)

            except Exception as e:
                print(f"[Backfill] Error {symbol}: {e}")

        print("[Scheduler] Backfill complete.")

    # ========================================================
    #     ROOT TF SCAN (1H / 4H) — DETECT FLIPS
    # ========================================================
    async def _scan_root_tf(self, tf: str):
        print(f"[ROOT] Scanning {tf}…")

        for symbol in self.symbols:
            try:
                kl = self.store.get_candles(symbol, tf)
                if not kl or len(kl) < 35:
                    continue

                kl = sorted(kl, key=lambda x: x["open"])
                closes = [float(k["close"]) for k in kl]

                _, _, hist = compute_macd_histogram(closes)
                idx = len(hist) - 1
                open_ts = kl[idx]["open"]
                now_ms = int(time.time() * 1000)

                # Only treat candle as current if within 35 seconds
                if now_ms - open_ts > 35000:
                    continue

                # Flip detection
                if flipped_negative_to_positive_at_open(hist, idx):
                    # Create signal with TTL (e.g., 3600 seconds)
                    self.store.create_signal(symbol, tf, {"open": open_ts}, ttl_seconds=3600)
                    print(f"[ROOT FLIP] {symbol} TF={tf} @ {ts_to_dt(open_ts)}")

            except Exception as e:
                print(f"[ROOT] Error {symbol}: {e}")

    # ========================================================
    #     DETECT 1H and 4H CANDLE OPENS
    # ========================================================
    async def _tf_loop(self):
        print("[TF LOOP] Started.")
        last_1h = None
        last_4h = None

        while True:
            now = datetime.now(timezone.utc)
            minute = now.minute

            # 1H candle open
            if minute == 0:
                key = now.replace(minute=0, second=0, microsecond=0)
                if key != last_1h:
                    last_1h = key
                    print("[TF LOOP] New 1H candle.")
                    await self._scan_root_tf("60")

            # 4H candle open
            if minute == 0 and now.hour % 4 == 0:
                key4 = now.replace(minute=0, second=0, microsecond=0)
                if key4 != last_4h:
                    last_4h = key4
                    print("[TF LOOP] New 4H candle.")
                    await self._scan_root_tf("240")

            await asyncio.sleep(1)

    # ========================================================
    #       WS MANAGER — SUBSCRIBE / UNSUBSCRIBE
    # ========================================================
    async def _ws_manager(self):
        print("[WS MANAGER] Started.")

        while True:
            try:
                root60 = self.store.get_active_signals("60")
                root240 = self.store.get_active_signals("240")

                # Determine symbols needed to subscribe (union of both lists)
                needed = set(root60) | set(root240)

                # Subscribe to new symbols
                for sym in needed:
                    if sym not in self.active_ws:
                        await self._subscribe_symbol_live(sym)

                # Unsubscribe expired symbols
                for sym in list(self.active_ws.keys()):
                    if sym not in needed:
                        await self._unsubscribe_symbol_live(sym)

            except Exception as e:
                print(f"[WS MANAGER] Error: {e}")

            await asyncio.sleep(30)

    # ========================================================
    # Subscribe to 5m + 15m
    # ========================================================
    async def _subscribe_symbol_live(self, symbol: str):
        self.active_ws[symbol] = True
        print(f"[WS] Live-Subscribe: {symbol}")

        for tf in self.ws_intervals:
            await self.ws.subscribe_kline(symbol, tf, self._on_kline)

    async def _unsubscribe_symbol_live(self, symbol: str):
        if symbol in self.active_ws:
            del self.active_ws[symbol]
        print(f"[WS] Live-Unsubscribe: {symbol}")

        for tf in self.ws_intervals:
            await self.ws.unsubscribe_kline(symbol, tf)
