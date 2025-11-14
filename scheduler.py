# ============================================================
# scheduler.py  — FULL, FINAL VERSION
# ============================================================

import asyncio
import time
import os
from datetime import datetime, timezone

from macd import compute_macd_histogram, flipped_negative_to_positive_at_open
from store_sqlite import Store
from bybit_ws_client import BybitWebSocketClient
from bybit_client import BybitClientV5
from notifier import Notifier


# Convert milliseconds → datetime
def ts_to_dt(ts_ms: int):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


# ============================================================
#                    SCHEDULER CLASS
# ============================================================

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

        # Telegram startup
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
                    self.store.replace_klines(symbol, "60", kl1h)

                # 4H
                kl4h = await self.rest.get_klines(symbol, "240", limit=200)
                if kl4h:
                    self.store.replace_klines(symbol, "240", kl4h)

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
                kl = self.store.get_klines(symbol, tf)
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
                    self.store.save_signal(symbol, tf, open_ts)
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

                needed = set(root60.keys()) | set(root240.keys())

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


    # Subscribe to 5m + 15m
    async def _subscribe_symbol_live(self, symbol: str):
        self.active_ws[symbol] = True
        print(f"[WS] Live-Subscribe: {symbol}")

        for tf in self.ws_intervals:
            await self.ws.subscribe_kline(
                symbol, tf,
                lambda msg, s=symbol, t=tf: asyncio.create_task(self._on_ws_kline(s, t, msg))
            )


    # Unsubscribe symbol
    async def _unsubscribe_symbol_live(self, symbol: str):
        print(f"[WS] Live-Unsubscribe: {symbol}")

        for tf in self.ws_intervals:
            await self.ws.unsubscribe_kline(symbol, tf)

        self.active_ws.pop(symbol, None)


    # ========================================================
    #       WS KLINE CALLBACK (5m / 15m)
    # ========================================================
    async def _on_ws_kline(self, symbol: str, tf: str, msg: dict):
        try:
            data = msg["data"][0]
            open_ts = int(data["start"])
            close = float(data["close"])

            now_ms = int(time.time() * 1000)
            if open_ts > now_ms + 60000:
                print(f"[WARN] Skipping abnormal TS {open_ts} for {symbol}-{tf}")
                return

            # Save new candle
            self.store.update_ws_kline(symbol, tf, open_ts, close)

            # Check root signal
            root_ts = self._get_root_signal(symbol)
            if not root_ts:
                return

            # Must be the most recent open
            if now_ms - open_ts > 35000:
                return

            # Confirm entry flip
            if await self._check_entry_confirmation(symbol, tf):
                await self._send_entry_alert(symbol, root_ts, tf, open_ts)

        except Exception as e:
            print(f"[WS ERROR] {symbol}-{tf}: {e}")


    # ========================================================
    #        ENTRY CONFIRMATION LOGIC
    # ========================================================
    async def _check_entry_confirmation(self, symbol: str, tf: str):
        try:
            kl = self.store.get_klines(symbol, tf)
            if not kl or len(kl) < 35:
                return False

            kl = sorted(kl, key=lambda x: x["open"])
            closes = [float(k["close"]) for k in kl]

            _, _, hist = compute_macd_histogram(closes)
            idx = len(hist) - 1

            # Only count OPEN flip
            if flipped_negative_to_positive_at_open(hist, idx):
                print(f"[ENTRY] {symbol} confirmed on {tf}m @ {ts_to_dt(kl[idx]['open'])}")
                return True

        except Exception as e:
            print(f"[ENTRY] Error {symbol}: {e}")

        return False


    # ========================================================
    #              SEND TELEGRAM ENTRY ALERT
    # ========================================================
    async def _send_entry_alert(self, symbol, root_ts, entry_tf, entry_ts):
        try:
            msg = (
                f"🚀 **ENTRY SIGNAL**\n"
                f"Symbol: {symbol}\n"
                f"Root TF: {self._root_tf_of(symbol)}\n"
                f"Root Flip: {ts_to_dt(root_ts)}\n"
                f"Entry TF: {entry_tf}m\n"
                f"Entry Flip: {ts_to_dt(entry_ts)}"
            )

            print(msg)
            await self.notifier.send(msg)

            # Clean up
            await self._unsubscribe_symbol_live(symbol)
            self.store.delete_signal(symbol)

        except Exception as e:
            print(f"[ALERT] Error sending alert: {e}")


    # ========================================================
    #                  ROOT TF HELPER
    # ========================================================
    def _root_tf_of(self, symbol: str):
        if self.store.get_signal(symbol, "60"):
            return "1H"
        if self.store.get_signal(symbol, "240"):
            return "4H"
        return "?"


    # ========================================================
    #                GET ROOT SIGNAL
    # ========================================================
    def _get_root_signal(self, symbol: str):
        sig1 = self.store.get_signal(symbol, "60")
        if sig1:
            return sig1["ts"]

        sig4 = self.store.get_signal(symbol, "240")
        if sig4:
            return sig4["ts"]

        return None


# ============================================================
# END OF FILE
# ============================================================
