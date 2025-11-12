import aiohttp
import asyncio
import time

class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, max_rate_per_sec: float = 10.0):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.session = aiohttp.ClientSession()
        self._lock = asyncio.Lock()
        self._last_send = 0.0
        self.min_interval = 1.0 / max_rate_per_sec

    async def close(self):
        await self.session.close()

    async def send(self, text: str):
        async with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last_send)
            if wait > 0:
                await asyncio.sleep(wait)
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text}
            async with self.session.post(url, json=payload, timeout=10) as resp:
                self._last_send = time.monotonic()
                try:
                    return await resp.json()
                except Exception:
                    return None
