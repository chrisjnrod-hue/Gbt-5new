--- README.md ---
```markdown
# Bybit MACD Signal Scanner (Render-friendly)

Overview
- Scans Bybit USDT linear perpetuals for MACD-histogram negative→positive flips on 1h and 4h opens.
- Stores minimal candle data (open_time + close) and EMA state to reduce storage and CPU needs.
- Eagerly backfills 1d/4h/1h; lazily fetches 15m/5m only for symbols with active root TF signals.
- Designed for single-instance deployments (SQLite) on Render's free tier, with an option to migrate to Redis.

Highlights of recent updates
- Startup is non-blocking: scheduler initialization and backfill run in background so the app responds to /health immediately. This reduces 503s from monitors when the Dyno/instance wakes.
- Root (/) route added to avoid noisy 404s from browsers/probes.
- /favicon.ico handler returns a 204 fallback if no favicon is present (reduces repeated browser requests).
- Optional CRON_SECRET support: when set, /health requires the header X-Cron-Secret to match. Use this to secure pings from external monitors or Render Cron Jobs.

Required endpoints
- GET /health — canonical endpoint for uptime pings. When CRON_SECRET is set, you must call it with header:
  X-Cron-Secret: <your_secret>
- GET / — simple root response to avoid 404 noise.
- GET /favicon.ico — returns static/favicon.ico if present or 204.

Important environment variables (minimal set to run)
- STORE (default: sqlite) — "sqlite" or "redis"
- SQLITE_DB_PATH (default: data.db) — path to SQLite file when STORE=sqlite
- REDIS_URL — Redis connection string if STORE=redis
- BYBIT_REST_BASE (default: https://api.bybit.com)
- CRON_SECRET — optional but recommended. If set, /health requires X-Cron-Secret header.
- TELEGRAM_BOT_TOKEN & TELEGRAM_CHAT_ID — optional, for alerts.

Minimal .env example
```
STORE=sqlite
SQLITE_DB_PATH=data.db
BYBIT_REST_BASE=https://api.bybit.com
CRON_SECRET=your_cron_secret_here      # set to require X-Cron-Secret on /health
TELEGRAM_BOT_TOKEN=                    # optional
TELEGRAM_CHAT_ID=                      # optional
```

Keeping free Render instances awake
Option A — Render Cron Job (recommended)
- Advantages: runs inside Render infra, easier to keep free instances awake, no external account required.
- Example Cron Job command (set schedule to `*/5 * * * *`):
  ```
  curl -fsS https://<your-render-app>.onrender.com/health -H "X-Cron-Secret: $CRON_SECRET" || true
  ```
- In Render dashboard:
  - Set CRON_SECRET in Service Environment (same secret used by the Cron Job).
  - Create Cron Job and either add CRON_SECRET to the Cron Job environment or reference the service env.

render.yaml snippet (optional)
```yaml
services:
  - type: web
    name: my-app
    env: production
    startCommand: uvicorn main:app --host 0.0.0.0 --port $PORT

cron_jobs:
  - name: keep-awake-health
    schedule: "*/5 * * * *"
    command: curl -fsS https://my-app.onrender.com/health -H "X-Cron-Secret: $CRON_SECRET" || true
    # make sure CRON_SECRET is available to the Cron Job environment
```

Option B — External monitor (cron-job.org / UptimeRobot)
- Create a monitor for: `https://<your-render-app>.onrender.com/health`
- Interval: 5 minutes (free minimum)
- Add header: `X-Cron-Secret: <your_secret>` (if you set CRON_SECRET)
- Note: The very first ping after an instance sleeps may return 503 while the instance boots. Consider tolerating one failed check before alerting.

Why non-blocking startup matters
- If the FastAPI startup handler blocks (awaits heavy backfill or network calls), the instance may not respond to readiness pings quickly and monitors will see 503/timeouts.
- The app now starts the Scheduler in background (asyncio.create_task), and the Scheduler performs symbol fetch/backfill in its own background task. This makes /health responsive immediately after the process is started.

Troubleshooting
- 503 from UptimeRobot or cron-job.org:
  - Usually the instance was asleep and is booting. The first request can fail; subsequent checks should succeed.
  - Ensure startup is non-blocking (this repo includes that change). If heavy work still runs on startup, move it into background tasks.
  - Use Render Cron Jobs (internal) for more reliable keep-awake pings.
- 404 for GET / in logs:
  - Occurs when a client requests root `/` and there’s no route — now fixed by adding a simple root route. If you still see 404s, ensure you deployed the updated main.py.
- 404 for /favicon.ico:
  - Browsers request this. Return a 204 or serve a static/favicon.ico file to avoid repeated logs.
- 403 on /health:
  - Happens when CRON_SECRET is set but the caller did not send the correct header. Verify header name (`X-Cron-Secret`) and value match exactly.
- Slow first response after wake:
  - Check Render logs to see boot time and any long-running tasks. Reduce startup work or offload to background jobs.

Testing examples
- Without header (should be 403 when CRON_SECRET is set):
  ```
  curl -i https://<your-render-app>.onrender.com/health
  ```
- With header (should be 200):
  ```
  curl -i -H "X-Cron-Secret: <your_secret>" https://<your-render-app>.onrender.com/health
  ```
- Check root:
  ```
  curl -i https://<your-render-app>.onrender.com/
  ```
- Check favicon:
  ```
  curl -i https://<your-render-app>.onrender.com/favicon.ico
  ```

Best practices & recommendations
- Set CRON_SECRET and configure your monitor/cron to send X-Cron-Secret — cheap and effective protection.
- Use Render Cron Jobs to keep free instances awake (internal pings are often more reliable).
- Keep startup lightweight: avoid long awaited calls in @app.on_event("startup"). Use background tasks for heavy work.
- Monitor logs for repeated 503/404/403 and adjust monitor settings (tolerate 1 failure) to avoid alert noise.
- Store secrets in Render Environment / Secrets — never commit them to source control.

Migration to Redis
- A one-off script (migrate_sqlite_to_redis.py) is included to export SQLite rows to Redis lists.
- Switch STORE environment variable to `redis` and set REDIS_URL to a reachable Redis instance.

Contact / next steps
- If you want, I can:
  - produce an updated render.yaml using your exact service name,
  - generate a secure CRON_SECRET for you,
  - or provide a small shell script that writes the README and updated files to disk.

```
