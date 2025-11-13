# bybit_ws_client.py
import asyncio
import aiohttp
import json
import time
from typing import Callable, Dict, Optional

class BybitWebSocketClient:
    """
    WebSocket client for Bybit V5 public streams (linear).
    - subscribe_kline(symbol, interval, callback): subscribe and register callback
    - unsubscribe_kline(symbol, interval): unsubscribe and remove callback
    """

    def __init__(self, category: str = "linear", reconnect_delay: float = 3.0):
        self.category = category
        self.url = f"wss://stream.bybit.com/v5/public/{category}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        # topic -> callback coroutine
        self.callbacks: Dict[str, Callable] = {}
        # track active topics
        self.topics: set = set()
        self._listen_task: Optional[asyncio.Task] = None
        self._running = False
        self.reconnect_delay = reconnect_delay
        self._lock = asyncio.Lock()

    # -------------------- Connection -------------------- #
    async def connect(self):
        async with self._lock:
            if self.ws and not self.ws.closed:
                return
            if self.session is None:
                self.session = aiohttp.ClientSession()
            # try to connect
            try:
                self.ws = await self.session.ws_connect(self.url, heartbeat=20)
                self._running = True
                # start listen loop
                if not self._listen_task or self._listen_task.done():
                    self._listen_task = asyncio.create_task(self._listen())
                print(f"[WS] Connected to {self.url}")
            except Exception as e:
                print(f"[WS] Connect error: {e}")
                self._running = False
                # do not raise; caller can retry

    async def close(self):
        self._running = False
        if self.ws and not self.ws.closed:
            try:
                await self.ws.close()
            except Exception:
                pass
        if self.session:
            try:
                await self.session.close()
            except Exception:
                pass
        print("[WS] Closed connection")

    # -------------------- Subscribe / Unsubscribe -------------------- #
    async def subscribe_kline(self, symbol: str, interval: str, callback):
        """
        Subscribe to kline.{interval}.{symbol} and register callback.
        callback receives the raw message dict (the JSON object received from server).
        """
        topic = f"kline.{interval}.{symbol}"
        # ensure connection
        if not self.ws or self.ws.closed:
            await self.connect()
            # small wait to stabilize
            await asyncio.sleep(0.05)

        msg = {"op": "subscribe", "args": [topic]}
        try:
            await self.ws.send_json(msg)
            # register callback immediately (even if ack not arrived)
            self.callbacks[topic] = callback
            self.topics.add(topic)
            print(f"[WS] Subscribed to {topic}")
        except Exception as e:
            print(f"[WS] subscribe_kline error for {topic}: {e}")
            raise

    async def unsubscribe_kline(self, symbol: str, interval: str):
        """
        Unsubscribe from kline.{interval}.{symbol}. Safe if subscription not present.
        """
        topic = f"kline.{interval}.{symbol}"
        if not self.ws or self.ws.closed:
            # still attempt to connect so unsubscribe can be sent; if not, just cleanup locally
            try:
                await self.connect()
            except Exception:
                pass

        msg = {"op": "unsubscribe", "args": [topic]}
        try:
            # send unsubscribe; ignore failures
            await self.ws.send_json(msg)
            print(f"[WS] Unsubscribed from {topic}")
        except Exception as e:
            print(f"[WS] unsubscribe_kline send error for {topic}: {e}")

        # remove callback registration locally
        if topic in self.callbacks:
            del self.callbacks[topic]
        if topic in self.topics:
            self.topics.discard(topic)

    # -------------------- Listen loop -------------------- #
    async def _listen(self):
        """Listen for incoming WS messages, dispatch to callbacks, and auto-reconnect."""
        while True:
            try:
                if not self.ws:
                    await asyncio.sleep(self.reconnect_delay)
                    await self.connect()
                    continue

                async for msg in self.ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                        except Exception:
                            continue

                        # direct heartbeat / system messages may not have 'topic'
                        topic = data.get("topic")
                        # dispatch only when topic exists and registered callback exists
                        if topic and topic in self.callbacks and "data" in data:
                            cb = self.callbacks.get(topic)
                            # call callback but do not await blocking the loop
                            try:
                                # ensure callback is awaited (it is an async fn)
                                asyncio.create_task(cb(data))
                            except Exception as e:
                                print(f"[WS] Callback dispatch error for {topic}: {e}")
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f"[WS] WSMsgType.ERROR: {msg.data}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("[WS] WebSocket CLOSED")
                        break
            except Exception as e:
                print(f"[WS] Listen loop exception: {e}")
            # if loop exits, attempt reconnect after delay
            try:
                await asyncio.sleep(self.reconnect_delay)
                await self.connect()
            except Exception as e:
                print(f"[WS] Reconnect attempt failed: {e}")
                await asyncio.sleep(self.reconnect_delay)
