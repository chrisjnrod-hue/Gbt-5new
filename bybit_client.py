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
        """Perform safe GET request with rate limiting, retries, and JSON fallback."""
        await self._ensure_session()
        await self.token_bucket.consume(1)
        url = f"{self.base_url}{path}"
        headers = {"User-Agent": "Mozilla/5.0 (compatible; BybitBot/1.0)"}

        for attempt in range(3):
            try:
                async with self.session.get(url, params=params, headers=headers, timeout=timeout) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        print(f"[BybitClientV5] HTTP {resp.status} for {url} params={params}")
                    try:
                        data = await resp.json(content_type=None)
                        return data
                    except Exception:
                        print(f"[BybitClientV5] Non-JSON response on attempt {attempt+1}: {text[:120]}")
                        await asyncio.sleep(1)
            except Exception as e:
                print(f"[BybitClientV5] _get error for {url}: {e}")
                await asyncio.sleep(1)
        return {}

    async def get_symbols(self) -> List[str]:
        """Return a list of active Linear USDT Perpetual trading pairs."""
        out: List[str] = []
        try:
            params = {"category": "linear", "baseCoin": "USDT"}
            resp = await self._get("/v5/market/instruments-info", params)
            result = resp.get("result") or {}
            items = result.get("list") or result.get("rows") or []

            if not items:
                print("[BybitClientV5] No instruments found with baseCoin=USDT, retrying without filter...")
                resp = await self._get("/v5/market/instruments-info", {"category": "linear"})
                result = resp.get("result") or {}
                items = result.get("list") or result.get("rows") or []

            clean = []
            for it in items:
                symbol = it.get("symbol", "")
                quote = it.get("quoteCoin", "")
                contract_type = it.get("contractType", "")
                status = (it.get("status") or it.get("state") or "").lower()

                if (
                    quote == "USDT"
                    and "perpetual" in contract_type.lower()
                    and status == "trading"
                    and not symbol.startswith(("100", "TEST", "BULL", "BEAR"))
                ):
                    clean.append(symbol)

            print(f"[BybitClientV5] Fetched {len(items)} raw instruments, {len(clean)} valid USDT perpetuals.")
            out = clean
        except Exception as e:
            print(f"[BybitClientV5] get_symbols error: {e}")
            return []

        return out

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[Dict]:
        """Fetch historical kline data for a given symbol and timeframe."""
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
                start = int(k[0]) // 1000
                # ✅ Skip impossible timestamps (roughly <2001 or > 2033)
                if start < 1_000_000_000 or start > 2_000_000_0000:
                    print(f"[WARN] Skipping abnormal candle timestamp {start} for {symbol}-{interval}")
                    continue

                candle = {
                    "open_time": start,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                }
                parsed.append(candle)

            # Ensure chronological order
            return list(reversed(parsed))
        except Exception as e:
            print(f"[BybitClientV5] get_klines error for {symbol}-{interval}: {e}")
            return []

    async def close(self):
        """Close aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
