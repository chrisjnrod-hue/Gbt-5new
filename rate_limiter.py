import asyncio

class TokenBucket:
    def __init__(self, capacity: int, refill_interval: float):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_interval = refill_interval
        self._lock = asyncio.Lock()
        self._refill_task = None

    async def start(self):
        if self._refill_task is None:
            self._refill_task = asyncio.create_task(self._refill_loop())

    async def _refill_loop(self):
        while True:
            await asyncio.sleep(self.refill_interval)
            async with self._lock:
                self.tokens = self.capacity

    async def consume(self, amount: int = 1):
        while True:
            async with self._lock:
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
            await asyncio.sleep(0.01)
