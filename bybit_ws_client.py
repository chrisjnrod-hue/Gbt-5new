# bybit_ws_client.py
import asyncio
import json
import traceback
import websockets
from typing import Callable

WS_URL = "wss://stream.bybit.com/v5/public/linear"

class BybitWebSocketClient:
    def __init__(self, category="linear"):
        self.category = category
        self.ws = None
        self._connected = False
        self._lock = asyncio.Lock()

        # topic -> callback
        self._topic_handlers = {}

    # --------------------------
    # Connect
    # --------------------------
    async def connect(self):
        async with self._lock:
            if self._connected:
                return

            print(f"[WS] Connecting to {WS_URL} ...")
            self.ws = await websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=20
            )
            self._connected = True
            asyncio.create_task(self._reader())
            print("[WS] Connected OK")

    async def close(self):
        async with self._lock:
            if self.ws:
                await self.ws.close()
            self._connected = False

    # --------------------------
    # Subscribe / Unsubscribe
    # --------------------------
    async def subscribe_kline(self, symbol: str, interval: str, callback: Callable):
        topic = f"kline.{interval}.{symbol}"

        if topic in self._topic_handlers:
            return

        await self._send({
            "op": "subscribe",
            "args": [topic]
        })

        self._topic_handlers[topic] = callback
        print(f"[WS] Subscribed to {topic}")

    async def unsubscribe_kline(self, symbol: str, interval: str):
        topic = f"kline.{interval}.{symbol}"

        if topic not in self._topic_handlers:
            return

        await self._send({
            "op": "unsubscribe",
            "args": [topic]
        })

        self._topic_handlers.pop(topic, None)
        print(f"[WS] Unsubscribed from {topic}")

    # --------------------------
    # Internal Send
    # --------------------------
    async def _send(self, payload: dict):
        if not self._connected:
            await self.connect()

        try:
            await self.ws.send(json.dumps(payload))
        except Exception:
            traceback.print_exc()
            self._connected = False
            await asyncio.sleep(1)
            await self.connect()
            await self.ws.send(json.dumps(payload))

    # --------------------------
    # Reader Loop
    # --------------------------
    async def _reader(self):
        while True:
            try:
                msg = await self.ws.recv()
                data = json.loads(msg)

                topic = data.get("topic")
                if topic in self._topic_handlers:
                    cb = self._topic_handlers[topic]

                    raw = data.get("data")
                    if not raw:
                        continue

                    if isinstance(raw, list):
                        item = raw[0]
                    else:
                        item = raw

                    normalized = {
                        "symbol": item.get("symbol"),
                        "interval": item.get("interval") or item.get("period"),
                        "start": int(item.get("start") or item.get("t") or 0),
                        "close": float(item.get("close") or item.get("c") or 0),
                    }

                    try:
                        await cb({"data": [normalized]})
                    except Exception:
                        traceback.print_exc()

            except websockets.exceptions.ConnectionClosed:
                print("[WS] Connection closed. Reconnecting...")
                self._connected = False
                await asyncio.sleep(1)
                await self.connect()

            except Exception:
                traceback.print_exc()
                await asyncio.sleep(0.25)
