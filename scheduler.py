# ============================
# scheduler.py  (PART 1 OF 4)
# ============================

import asyncio
import time
import os
from datetime import datetime, timezone

from macd import compute_macd_histogram, flipped_negative_to_positive_at_open
from store_sqlite import Store
from bybit_ws_client import BybitWebSocketClient
from bybit_client import BybitClientV5  # REST Backfill
from notifier import Notifier


# ============================================================
# Utility Helpers
# ============================================================

def ts_to_dt(ts_ms: int):
    """Convert integer milliseconds → datetime."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


# ============================================================
# Main Scheduler Class
# ============================================================

class Scheduler:
    def __init__(self):
        # Database
        self.store = Store("market.db")

        # REST Backfill Client
        self.rest = BybitClientV5()

        # Real-Time WS Client
        self.ws = BybitWebSocketClient()

        # Telegram notifier
        self.notifier = Notifier()

        # All USDT symbols
        self.symbols = []

        # Dynamic WS state
        self.active_ws = {}       # symbol -> True
        self.ws_intervals = ["5", "15"]   # Only real-time entry TFs

        # Make sure WS limit is 0 (dynamic mode)
        self.ws_symbol_limit = int(os.getenv("WS_SYMBOL_LIMIT", "0"))

        # Background loop tasks
        self._tf_loop_task = None
        self._ws_manager_task = None
        self._start_done = False


    # ------------------------------------------------------------
    # STARTUP ENTRY
    # ------------------------------------------------------------
    async def start(self):
        await self.ws.connect()

        # Load symbols
        print("[Scheduler] Loading symbols from Bybit…")
        syms = await self.rest.fetch_usdt_perpetuals()
        self.symbols = syms
        print(f"[Scheduler] Loaded {len(self.symbols)} symbols from Bybit")

        # Startup Telegram
        try:
            await self.notifier.send("🤖 Scheduler started — system live & scanning.")
            print("[Scheduler] Startup Telegram sent.")
        except Exception:
            print("[Scheduler] Could not send Telegram startup message.")

        # CLEAN UP OLD SIGNALS (requested)
        self.store.clear_signals()
        print("[Scheduler] Old signals cleared.")

        # Initial backfill + initial MACD root scan
        await self._initial_backfill()

        # Start background loops
        self._tf_loop_task = asyncio.create_task(self._tf_loop())
        self._ws_manager_task = asyncio.create_task(self._ws_manager())

        self._start_done = True
        print("[Scheduler] Background loops running (tf_loop + ws_manager).")


    # ------------------------------------------------------------
    # INITIAL BACKFILL: 1H + 4H ONLY (root TFs)
    # ------------------------------------------------------------
    async def _initial_backfill(self):
        print("[Scheduler] Running initial backfill (1H + 4H).")

        for symbol in self.symbols:
            try:
                # backfill 1H
                kl_1h = await self.rest.get_klines(symbol, "60", limit=200)
                if kl_1h:
                    self.store.replace_klines(symbol, "60", kl_1h)

                # backfill 4H
                kl_4h = await self.rest.get_klines(symbol, "240", limit=200)
                if kl_4h:
                    self.store.replace_klines(symbol, "240", kl_4h)

            except Exception as e:
                print(f"[Scheduler] Backfill error {symbol}: {e}")

        print("[Scheduler] Backfill finished.")
        print("[Scheduler] Running initial root MACD scan…")

        await self._scan_root_tf("60")
        await self._scan_root_tf("240")

# ============================
# scheduler.py  (PART 2 OF 4)
# ============================

    # ------------------------------------------------------------
    # ROOT TF SCAN (1H and 4H only)
    # ------------------------------------------------------------
    async def _scan_root_tf(self, tf: str):
        """
        Scan 1H or 4H timeframe for MACD negative→positive flips on the CURRENT candle.
        Root timeframe: "60" or "240".
        """
        print(f"[Scheduler] Scanning ROOT TF = {tf}")

        for symbol in self.symbols:
            try:
                klines = self.store.get_klines(symbol, tf)
                if not klines or len(klines) < 35:
                    continue

                # sort by open time
                klines = sorted(klines, key=lambda x: x["open"])

                closes = [float(k["close"]) for k in klines]
                _, _, macd_hist = compute_macd_histogram(closes)

                idx = len(klines) - 1  # newest completed candle
                cur_open_ts = klines[idx]["open"]

                # Candle must be "current" (within 30 sec of open)
                now_ts = int(time.time() * 1000)
                if now_ts - cur_open_ts > 35_000:
                    continue

                # MACD flip?
                if flipped_negative_to_positive_at_open(macd_hist, idx):
                    # Save this ROOT signal
                    self.store.save_signal(symbol, tf, cur_open_ts)
                    print(f"[ROOT FLIP] {symbol} TF={tf} flipped @ {ts_to_dt(cur_open_ts)}")

            except Exception as e:
                print(f"[Scheduler] Root TF error {symbol}-{tf}: {e}")


    # ------------------------------------------------------------
    # BACKGROUND LOOP — Fires on each new 1H/4H candle
    # ------------------------------------------------------------
    async def _tf_loop(self):
        """
        Runs forever, detecting new 1H and 4H candles.
        At each new candle: run a new ROOT TF scan.
        """
        print("[TF LOOP] Candle-based TF loop started.")

        last_1h = None
        last_4h = None

        while True:
            try:
                now = datetime.now(tz=timezone.utc)
                minute = now.minute

                # detect 1H candle open
                if minute == 0:
                    current_key = now.replace(minute=0, second=0, microsecond=0)
                    if last_1h != current_key:
                        last_1h = current_key
                        print(f"[TF LOOP] New 1H candle @ {now}")
                        await self._scan_root_tf("60")

                # detect 4H candle open
                if minute == 0 and now.hour % 4 == 0:
                    current_key4 = now.replace(minute=0, second=0, microsecond=0)
                    if last_4h != current_key4:
                        last_4h = current_key4
                        print(f"[TF LOOP] New 4H candle @ {now}")
                        await self._scan_root_tf("240")

            except Exception as e:
                print(f"[TF LOOP] Error: {e}")

            await asyncio.sleep(1)

# ============================
# scheduler.py  (PART 3 OF 4)
# ============================

    # ------------------------------------------------------------
    # WS MANAGER LOOP — Maintain dynamic subscriptions
    # ------------------------------------------------------------
    async def _ws_manager(self):
        """
        Every 30 seconds, ensure we are:
        - Subscribed to WS for symbols that HAVE a root signal (1H or 4H)
        - Unsubscribed for symbols that NO LONGER have root signals
        """
        print("[WS MANAGER] Started.")
        while True:
            try:
                root_60 = self.store.get_active_signals("60")
                root_240 = self.store.get_active_signals("240")

                # union of symbols with active root signal
                needed = set(root_60.keys()) | set(root_240.keys())

                # Subscribe missing ones
                for symbol in needed:
                    if symbol not in self.active_ws:
                        await self._subscribe_symbol_live(symbol)

                # Unsubscribe stale ones
                for symbol in list(self.active_ws.keys()):
                    if symbol not in needed:
                        await self._unsubscribe_symbol_live(symbol)

            except Exception as e:
                print(f"[WS MANAGER] Error: {e}")

            await asyncio.sleep(30)


    # ------------------------------------------------------------
    # SUBSCRIBE (& register 5m/15m WS handlers)
    # ------------------------------------------------------------
    async def _subscribe_symbol_live(self, symbol: str):
        """
        Subscribes real-time WS only for:
        - 5m
        - 15m
        These confirm the final entry flip after the root flip.
        """
        print(f"[WS-MANAGER] Live-Subscribe → {symbol}")
        self.active_ws[symbol] = True

        for iv in self.ws_intervals:
            try:
                await self.ws.subscribe_kline(
                    symbol,
                    iv,
                    lambda msg, s=symbol, tf=iv: asyncio.create_task(self._on_ws_kline(s, tf, msg))
                )
            except Exception as e:
                print(f"[WS] Subscribe error {symbol}-{iv}: {e}")


    # ------------------------------------------------------------
    # UNSUBSCRIBE (when root signal expires)
    # ------------------------------------------------------------
    async def _unsubscribe_symbol_live(self, symbol: str):
        print(f"[WS-MANAGER] Live-Unsubscribe → {symbol}")

        for iv in self.ws_intervals:
            try:
                await self.ws.unsubscribe_kline(symbol, iv)
            except Exception as e:
                print(f"[WS] Unsubscribe error {symbol}-{iv}: {e}")

        self.active_ws.pop(symbol, None)


    # ------------------------------------------------------------
    # WS KLINE CALLBACK
    # ------------------------------------------------------------
    async def _on_ws_kline(self, symbol: str, tf: str, msg: dict):
        """
        WS pushes candles in real-time:
            msg["data"] = [{
                "symbol": "...",
                "interval": "...",
                "start": TS,
                "close": price
            }]
        We treat ONLY candle-open flips on 5m/15m as entry confirmation.
        """
        try:
            data = msg["data"][0]
            open_ts = int(data["start"])
            close_price = float(data["close"])

            # Timestamp sanity check
            now_ms = int(time.time() * 1000)
            if open_ts > now_ms + 60_000:
                print(f"[WARN] Skipping abnormal WS ts {open_ts} for {symbol}-{tf}")
                return

            # Append or replace newest candle
            self.store.update_ws_kline(symbol, tf, open_ts, close_price)

            # Check if this symbol has root signal
            root_ts = self._get_root_signal(symbol)
            if not root_ts:
                return

            # Require WS candle to be "new"
            if now_ms - open_ts > 35_000:
                return

            # Check 5m/15m entry confirmation
            if await self._check_entry_confirmation(symbol, tf):
                await self._send_entry_alert(symbol, root_ts, tf, open_ts)

        except Exception as e:
            print(f"[WS-KLINE] Error {symbol}-{tf}: {e}")


    # ------------------------------------------------------------
    # Determine if coin has a ROOT MACD signal (1H or 4H)
    # ------------------------------------------------------------
    def _get_root_signal(self, symbol: str):
        sig_60 = self.store.get_signal(symbol, "60")
        sig_240 = self.store.get_signal(symbol, "240")

        if sig_60:
            return sig_60["ts"]
        if sig_240:
            return sig_240["ts"]
        return None

# ============================
# scheduler.py  (PART 4 OF 4)
# ============================

    # ------------------------------------------------------------
    # ENTRY CONFIRMATION (5m or 15m flip)
    # ------------------------------------------------------------
    async def _check_entry_confirmation(self, symbol: str, tf: str):
        """
        Once root signal exists (1H or 4H),
        we wait for a 5m/15m MACD flip to confirm entry.
        """
        try:
            kl = self.store.get_klines(symbol, tf)
            if not kl or len(kl) < 35:
                return False

            kl = sorted(kl, key=lambda x: x["open"])
            closes = [float(k["close"]) for k in kl]

            _, _, hist = compute_macd_histogram(closes)
            idx = len(hist) - 1

            # Only treat candle-open flips as confirmation
            if flipped_negative_to_positive_at_open(hist, idx):
                print(f"[ENTRY] {symbol} confirmed on {tf}m @ {ts_to_dt(kl[idx]['open'])}")
                return True

        except Exception as e:
            print(f"[ENTRY] Error checking entry for {symbol}-{tf}: {e}")

        return False


    # ------------------------------------------------------------
    # SEND ENTRY ALERT TO TELEGRAM
    # ------------------------------------------------------------
    async def _send_entry_alert(self, symbol: str, root_ts, entry_tf, entry_ts):
        try:
            root_dt = ts_to_dt(root_ts)
            entry_dt = ts_to_dt(entry_ts)

            msg = (
                f"🚀 **ENTRY SIGNAL**\n"
                f"**Symbol:** {symbol}\n"
                f"**Root TF:** {self._root_tf_of(symbol)}\n"
                f"**Root Flip:** {root_dt}\n"
                f"**Entry TF:** {entry_tf}m\n"
                f"**Entry Flip:** {entry_dt}\n"
            )

            print(msg)
            await self.notifier.send(msg)

            # After entry alert, unsubscribe WS for this symbol
            await self._unsubscribe_symbol_live(symbol)

            # Also clear signal so we don't re-alert
            self.store.delete_signal(symbol)

        except Exception as e:
            print(f"[ALERT] Error sending alert for {symbol}: {e}")


    # ------------------------------------------------------------
    # Helper to check which root TF (60 or 240) is active
    # ------------------------------------------------------------
    def _root_tf_of(self, symbol: str):
        if self.store.get_signal(symbol, "60"):
            return "1H"
        if self.store.get_signal(symbol, "240"):
            return "4H"
        return "?"

    # ------------------------------------------------------------
    # TRIM OLD CANDLES
    # ------------------------------------------------------------
    async def trim_loop(self):
        """
        Trims all stored candles to prevent DB bloat.
        Keeps last 500 candles per TF.
        """
        print("[TRIM] Trim loop started.")
        while True:
            try:
                self.store.trim_klines(max_keep=500)
            except Exception as e:
                print(f"[TRIM] Error: {e}")

            await asyncio.sleep(3600)  # run once per hour


# ============================
# END OF FILE
# ============================
