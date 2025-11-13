import aiohttp
import asyncio
from typing import Dict, List, Optional


class BybitClientV5:
    def __init__(self, base_url: str, token_bucket):
        self.base_url = base_url.rstrip("/")
        self.session: Optional[aiohttp.ClientSession] = None
        self.token_bucket = token_bucket

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def _get(self, path: str, params: Dict = None, timeout: int = 20) -> Dict:
        """Generic GET request with rate limiting."""
        await self._ensure_session()
        await self.token_bucket.consume(1)
        url = f"{self.base_url}{path}"
        try:
            async with self.session.get(url, params=params, timeout=timeout) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    print(f"[BybitClientV5] HTTP {resp.status} for {url} params={params}")
                return data
        except Exception as e:
            print(f"[BybitClientV5] _get error for {url}: {e}")
            return {}

    async def get_symbols(self) -> List[str]:
        """Return a list of active linear USDT symbols."""
        out: List[str] = []
        try:
            params = {"category": "linear", "baseCoin": "USDT"}
            resp = await self._get("/v5/market/instruments-info", params)
            result = resp.get("result") or {}
            items = result.get("list") or result.get("rows") or []

            # If no items returned, retry without baseCoin filter
            if not items:
                print("[BybitClientV5] No instruments found with baseCoin=USDT, retrying without filter...")
                resp = await self._get("/v5/market/instruments-info", {"category": "linear"})
                result = resp.get("result") or {}
                items = result.get("list") or result.get("rows") or []

            print(f"[BybitClientV5] Fetched {len(items)} instruments from Bybit.")

            for it in items:
                symbol = it.get("symbol") or it.get("name")
                quote = it.get("quote_currency") or it.get("quoteCoin") or it.get("quote")
                status = it.get("status") or it.get("state") or it.get("status_code")
                if not symbol:
                    continue
                if quote and quote.upper() == "USDT" and (not status or str(status).lower() in ("trading", "active", "list")):
                    out.append(symbol)

            print(f"[BybitClientV5] Parsed {len(out)} active USDT symbols.")
        except Exception as e:
            print(f"[BybitClientV5] get_symbols error: {e}")
            return []

        return out

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[Dict]:
        """Fetch kline (candlestick) data for a symbol and interval."""
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": str(interval),
            "limit": str(limit),
        }
        try:
            resp = await self._get("/v5/market/kline", params)
            result = resp.get("result") or {}
            items = result.get("list") or []
            parsed = []
            for k in items:
                # Bybit returns [startTime, open, high, low, close, volume, turnover]
                candle = {
                    "open_time": int(k[0]) // 1000,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                }
                parsed.append(candle)
            return list(reversed(parsed))
        except Exception as e:
            print(f"[BybitClientV5] get_klines error for {symbol}-{interval}: {e}")
            return []

    async def close(self):
        """Close aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
