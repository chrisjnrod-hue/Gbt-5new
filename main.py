# main.py
"""Entry point for the FastAPI server and scheduler."""
import os
from fastapi import FastAPI
from dotenv import load_dotenv

from scheduler import Scheduler
from rate_limiter import TokenBucket

load_dotenv()

REST_MAX_TOKENS = int(os.getenv("REST_MAX_TOKENS", "500"))
REST_REFILL_SECONDS = float(os.getenv("REST_REFILL_SECONDS", "7"))

app = FastAPI()

# Token bucket is optional, used only for REST backfill
token_bucket = TokenBucket(capacity=REST_MAX_TOKENS, refill_interval=REST_REFILL_SECONDS)

# New scheduler (self-contained)
scheduler = Scheduler()

@app.on_event("startup")
async def startup_event():
    print("[Main] Starting token bucket...")
    await token_bucket.start()

    print("[Main] Starting scheduler...")
    await scheduler.start()

    print("[Main] Scheduler launched in background ✔")

@app.on_event("shutdown")
async def shutdown_event():
    print("[Main] Shutting down scheduler...")
    try:
        await scheduler.ws.close()
    except:
        pass

    try:
        await token_bucket.stop()
    except:
        pass

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "ok", "service": "running"}
