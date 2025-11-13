# bybit_ws_client.py
import asyncio
import aiohttp
import json
import time

class BybitWebSocketClient:
    """
    Simple public WebSocket client for Bybit V5.

    Streams live klines for given symbols and intervals, then
    triggers user-supplied callbacks for each update.
    """

    def __init__(self, category="linear"):
        # category: "linear" (futures) or "spot"
        self.url = f"wss://stream.bybit.com/v5/public/{category}"
        self.session = None
        self.ws = None
        self.callbacks = {}   # topic -> callback coroutine
        self._running = False

    async def connect(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        self.ws = await self.session.ws_connect(self.url, heartbeat=20)
        self._running = True
        asyncio.create_task(self._listen())
        print(f"[WS] Connected to {self.url}")

    async def subscribe_kline(self, symbol: str, interval: str, callback):
        """
        Subscribe to kline updates for one symbol & interval.
        e.g. interval="5" for 5-minute candles, or "60" for 1-hour.
        """
        topic = f"kline.{interval}.{symbol}"
        sub_msg = {"op": "subscribe", "args": [topic]}
        await self.ws.send_json(sub_msg)
        self.callbacks[topic] = callback
        print(f"[WS] Subscribed to {topic}")

    async def _listen(self):
        """Continuously receive messages and dispatch to callbacks."""
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    topic = data.get("topic")
                    if not topic:
                        continue
                    cb = self.callbacks.get(topic)
                    if cb and "data" in data and isinstance(data["data"], list):
                        await cb(data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    print(f"[WS] Error: {msg.data}")
        except Exception as e:
            print(f"[WS] Listen loop stopped: {e}")
            self._running = False

    async def close(self):
        self._running = False
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()
        print("[WS] Closed connection")
