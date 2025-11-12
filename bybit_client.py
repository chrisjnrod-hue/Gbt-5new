import aiohttp
from typing import List, Dict

TF_TO_PERIOD = {
    "1": "1",
    "3": "3",
    "5": "5",
    "15": "15",
    "30": "30",
    "60": "60",
    "240": "240",
    "1440": "D",
}

class BybitClientV5:
    def __init__(self, base_url: str, token_bucket):
        self.base_url = base_url.rstrip('/')
        self.session = aiohttp.ClientSession()
        self.bucket = token_bucket

    async def close(self):
        await self.session.close()

    async def _get(self, path: str, params: Dict = None, timeout: int = 20) -> Dict:
        await self.bucket.consume(1)
        params = params or {}
        url = f"{self.base_url}{path}"
        async with self.session.get(url, params=params, timeout=timeout) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_symbols(self) -> List[str]:
        out: List[str] = []
        try:
            resp = await self._get("/v5/market/instruments-info", {"category": "linear"})
            result = resp.get("result") or {}
            items = result.get("list") or result.get("rows") or []
            for it in items:
                quote = it.get("quote_currency") or it.get("quoteCoin") or it.get("quote")
                status = it.get("status") or it.get("state") or it.get("status_code")
                symbol = it.get("symbol") or it.get("name")
                if not symbol:
                    continue
                if quote and quote.upper() == "USDT" and (not status or str(status).lower() in ("trading", "active", "list")):
                    out.append(symbol)
        except Exception:
            return []
        return out

    async def get_klines(self, symbol: str, tf: str, limit: int = 200) -> List[dict]:
        period = TF_TO_PERIOD.get(tf)
        if period is None:
            raise ValueError(f"Unsupported timeframe: {tf}")
        params = {"symbol": symbol, "interval": period, "limit": limit}
        try:
            resp = await self._get("/v5/market/kline", params)
            result = resp.get("result") or {}
            data_list = result.get("list") or result.get("rows") or []
            normalized = []
            for d in data_list:
                open_time = d.get("start") or d.get("open_time") or d.get("t") or d.get("open_time_ms")
                try:
                    open_time = int(open_time) if open_time is not None else None
                    if open_time and open_time > 10**12:
                        open_time = open_time // 1000
                except Exception:
                    open_time = None
                normalized.append({
                    "open_time": open_time,
                    "close": float(d.get("close") or d.get("close_price") or d.get("c") or 0.0),
                    "_raw": d
                })
            if len(normalized) >= 2 and normalized[0]["open_time"] and normalized[1]["open_time"]:
                if normalized[0]["open_time"] > normalized[1]["open_time"]:
                    normalized.reverse()
            return normalized
        except Exception:
            return []
