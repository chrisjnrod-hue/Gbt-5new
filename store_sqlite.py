# store_sqlite.py
"""SQLite-based persistence store for candle and signal data."""
import sqlite3
import json
import time
from typing import List, Optional
import threading
import os

DB_PATH = os.getenv("SQLITE_DB_PATH", "data.db")
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "100"))
LAZY_TF_MAX_CANDLES = int(os.getenv("LAZY_TF_MAX_CANDLES", "300"))

class Store:
    """
    SQLite-based persistence store for candle data and trading signals.
    """
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # tuned pragmas for WAL and performance on small deployments
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            # minimal candles: only open_time and close
            c.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    tf TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    close REAL NOT NULL
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_candles_sym_tf_time ON candles(symbol, tf, open_time);")
            # EMA state for incremental MACD updates
            c.execute("""
                CREATE TABLE IF NOT EXISTS ema_state (
                    symbol TEXT NOT NULL,
                    tf TEXT NOT NULL,
                    ema_fast REAL,
                    ema_slow REAL,
                    ema_signal REAL,
                    last_open_time INTEGER,
                    PRIMARY KEY(symbol, tf)
                )
            """)
            # signals: store meta as json, expiry integer
            c.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    symbol TEXT NOT NULL,
                    tf TEXT NOT NULL,
                    meta TEXT,
                    expiry INTEGER,
                    PRIMARY KEY(symbol, tf)
                )
            """)
            conn.commit()
            conn.close()

    def save_candles(self, symbol: str, tf: str, candles: List[dict], maxlen: int = None):
        """
        Save a list of candles (each with open_time and close) to the database.
        """
        maxlen = maxlen or (LAZY_TF_MAX_CANDLES if tf in self._lazy_tfs() else MAX_CANDLES)
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            for candle in candles:
                if candle.get("open_time") is None:
                    continue
                c.execute("INSERT INTO candles (symbol, tf, open_time, close) VALUES (?, ?, ?, ?)",
                          (symbol, tf, int(candle["open_time"]), float(candle["close"])))
            # trim down to maxlen (keep newest)
            c.execute("SELECT COUNT(*) FROM candles WHERE symbol=? AND tf=?", (symbol, tf))
            count = c.fetchone()[0]
            if count > maxlen:
                to_delete = count - maxlen
                c.execute("""
                    DELETE FROM candles WHERE id IN (
                        SELECT id FROM candles WHERE symbol=? AND tf=? ORDER BY open_time ASC LIMIT ?
                    )
                """, (symbol, tf, to_delete))
            conn.commit()
            conn.close()

    def get_candles(self, symbol: str, tf: str) -> List[dict]:
        """
        Retrieve stored candles for a symbol and timeframe, ordered by time.
        """
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("SELECT open_time, close FROM candles WHERE symbol=? AND tf=? ORDER BY open_time ASC", (symbol, tf))
            rows = c.fetchall()
            conn.close()
            # Return list of dicts with 'open' and 'close'
            return [{"open": r[0], "close": r[1]} for r in rows] if rows else []

    # EMA state methods
    def get_ema_state(self, symbol: str, tf: str) -> Optional[dict]:
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("SELECT ema_fast, ema_slow, ema_signal, last_open_time FROM ema_state WHERE symbol=? AND tf=?", (symbol, tf))
            row = c.fetchone()
            conn.close()
            if not row:
                return None
            return {"ema_fast": row[0], "ema_slow": row[1], "ema_signal": row[2], "last_open_time": row[3]}

    def set_ema_state(self, symbol: str, tf: str, ema_fast: float, ema_slow: float, ema_signal: float, last_open_time: int):
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("REPLACE INTO ema_state (symbol, tf, ema_fast, ema_slow, ema_signal, last_open_time) VALUES (?, ?, ?, ?, ?, ?)",
                      (symbol, tf, ema_fast, ema_slow, ema_signal, last_open_time))
            conn.commit()
            conn.close()

    # signal methods
    def create_signal(self, symbol: str, tf: str, meta: dict, ttl_seconds: int):
        """
        Create or replace a signal entry with given metadata and time-to-live.
        """
        expiry = int(time.time()) + ttl_seconds
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("REPLACE INTO signals (symbol, tf, meta, expiry) VALUES (?, ?, ?, ?)",
                      (symbol, tf, json.dumps(meta), expiry))
            conn.commit()
            conn.close()

    def delete_signal(self, symbol: str, tf: str):
        """
        Delete a signal entry for a symbol and timeframe.
        """
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("DELETE FROM signals WHERE symbol=? AND tf=?", (symbol, tf))
            conn.commit()
            conn.close()

    def get_active_signals(self, tf: str) -> List[str]:
        """
        Get list of symbols with active (non-expired) signals for a given timeframe.
        """
        now = int(time.time())
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("SELECT symbol FROM signals WHERE tf=? AND expiry>?", (tf, now))
            rows = c.fetchall()
            conn.close()
            return [r[0] for r in rows]

    def clear_signals(self):
        """
        Delete all signals from the database.
        """
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("DELETE FROM signals")
            conn.commit()
            conn.close()

    def get_signal(self, symbol: str, tf: str) -> Optional[dict]:
        """
        Retrieve the metadata for a signal if it is still active (not expired).
        """
        now = int(time.time())
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("SELECT meta, expiry FROM signals WHERE symbol=? AND tf=?", (symbol, tf))
            row = c.fetchone()
            conn.close()
            if not row:
                return None
            meta = json.loads(row[0])
            if row[1] <= now:
                return None
            return meta

    def set_last_notified(self, symbol: str, tf: str, ts: int):
        """
        Update the 'last_notified' timestamp in the signal metadata.
        """
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            c.execute("SELECT meta, expiry FROM signals WHERE symbol=? AND tf=?", (symbol, tf))
            row = c.fetchone()
            if not row:
                conn.close()
                return
            meta = json.loads(row[0])
            expiry = row[1]
            meta["last_notified"] = ts
            c.execute("REPLACE INTO signals (symbol, tf, meta, expiry) VALUES (?, ?, ?, ?)",
                      (symbol, tf, json.dumps(meta), expiry))
            conn.commit()
            conn.close()

    def trim_all(self, default_keep: int = MAX_CANDLES):
        """
        Trims candle tables for all symbol/timeframe combos to the default keep length.
        """
        with self._lock:
            conn = self._connect()
            c = conn.cursor()
            # find symbol/tf combos
            c.execute("SELECT DISTINCT symbol, tf FROM candles")
            combos = c.fetchall()
            for symbol, tf in combos:
                maxlen = LAZY_TF_MAX_CANDLES if tf in self._lazy_tfs() else default_keep
                c.execute("SELECT COUNT(*) FROM candles WHERE symbol=? AND tf=?", (symbol, tf))
                count = c.fetchone()[0]
                if count > maxlen:
                    to_delete = count - maxlen
                    c.execute("""
                        DELETE FROM candles
                        WHERE id IN (
                            SELECT id FROM candles WHERE symbol=? AND tf=? ORDER BY open_time ASC LIMIT ?
                        )
                    """, (symbol, tf, to_delete))
            conn.commit()
            conn.close()

    def _lazy_tfs(self) -> List[str]:
        env = os.getenv("LAZY_TFS", "5,15")
        return [x.strip() for x in env.split(",") if x.strip()]
