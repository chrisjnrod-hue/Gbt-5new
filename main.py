import os
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from typing import Optional

from rate_limiter import TokenBucket
from notifier import TelegramNotifier
from scheduler import Scheduler
from bybit_client import BybitClientV5
from store_sqlite import SQLiteStore

load_dotenv()

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "data.db")
BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bybit.com")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
REST_MAX_TOKENS = int(os.getenv("REST_MAX_TOKENS", "500"))
REST_REFILL_SECONDS = float(os.getenv("REST_REFILL_SECONDS", "7"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
CRON_SECRET = os.getenv("CRON_SECRET", "")

app = FastAPI()

store = SQLiteStore(SQLITE_DB_PATH)

token_bucket = TokenBucket(capacity=REST_MAX_TOKENS, refill_interval=REST_REFILL_SECONDS)
bybit = BybitClientV5(BYBIT_REST_BASE, token_bucket)
notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
scheduler = Scheduler(bybit, store, notifier, (MACD_FAST, MACD_SLOW, MACD_SIGNAL), token_bucket)

@app.on_event("startup")
async def startup_event():
    await token_bucket.start()
    await scheduler.start()
    # light backfill re-check to catch missed opens while sleeping
    try:
        await scheduler._backfill_all()
    except Exception:
        pass

@app.on_event("shutdown")
async def shutdown_event():
    try:
        await scheduler.stop()
    except Exception:
        pass
    try:
        await bybit.close()
    except Exception:
        pass
    try:
        await notifier.close()
    except Exception:
        pass

@app.get("/health")
async def health(x_cron_secret: Optional[str] = Header(None)):
    # If CRON_SECRET is set in env, require the header X-Cron-Secret to match.
    # cron-job.org can be configured to send this header.
    if CRON_SECRET:
        if not x_cron_secret or x_cron_secret != CRON_SECRET:
            raise HTTPException(status_code=403, detail="Forbidden")
    return {"status": "ok"}
  from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse

app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "note": "Service running. Use /health for scheduler pings."}

@app.get("/favicon.ico")
async def favicon():
    # If you have a static/favicon.ico file in your repo; otherwise return 404 intentionally removed
    try:
        return FileResponse("static/favicon.ico")
    except Exception:
        # fallback simple 204 so browser stops retrying
        from fastapi.responses import Response
        return Response(status_code=204)
