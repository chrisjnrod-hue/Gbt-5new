Bybit MACD Signal Scanner (single-instance, Render-friendly)

Overview
- Scans all Bybit USDT linear perpetuals for MACD-histogram negative→positive flips at 1h and 4h opens.
- Stores minimal candle data (open_time + close) to save space.
- Eagerly backfills only 1d, 4h, and 1h for all symbols.
- Lazily fetches/stores 15m and 5m only for symbols with active 1h/4h signals.
- Uses SQLite with WAL and tuned PRAGMAs for a simple single-instance deployment (Render free tier).
- Stores EMA state to allow incremental MACD updates and reduce history needs.

Render notes
- In Render, set environment variables in the service settings:
  - TELEGRAM_BOT_TOKEN
  - TELEGRAM_CHAT_ID
  - SQLITE_DB_PATH (optional)
  - CRON_SECRET (optional; recommended for security)
- Render free services may sleep; use an external uptime monitor (cron-job.org / UptimeRobot) pointing to /health every 5 minutes to reduce sleeps.

Cron-job.org usage with secret header
1. Create a cron-job.org account and new cron job:
   - URL: https://<your-render-app>.onrender.com/health
   - Method: GET
   - Interval: Every 5 minutes
2. Add HTTP header:
   - Header name: X-Cron-Secret
   - Header value: the value you set in CRON_SECRET in Render settings
3. In your Render service, set CRON_SECRET as an environment variable. The app will reject /health requests missing or not matching the header when CRON_SECRET is set.
4. Test: use "Run now" in cron-job.org and check your Render logs; /health should return 200 if header matches.

Quick start
1. Copy .env.example -> set environment variables in Render service settings (do NOT commit secrets).
2. Ensure STORE=sqlite for single-instance use.
3. Deploy to Render (connect repo / use Docker).
4. Use cron-job.org to hit /health every 5 minutes; configure X-Cron-Secret header if you want to require a secret.

Migration to Redis (low complexity)
- Use migrate_sqlite_to_redis.py to export candle lists to Redis.
- Switch STORE to redis and implement a Redis-backed Store (simple mapping to the same store methods).
- Upstash / RedisCloud provide simple free tiers.

Config recommendations for free tier
- STORE=sqlite
- MAX_CANDLES=100
- LAZY_TF_MAX_CANDLES=300
- LAZY_TFS=5,15
- BACKFILL_TFS=1440,240,60
- TRIM_INTERVAL_MINUTES=60
- Use cron-job.org to ping /health every 5 minutes; set CRON_SECRET and add header X-Cron-Secret in cron-job.org to secure the endpoint.
