# ctyun-logpush-backfill

Standalone Cloudflare Worker for replaying historical Logpush logs in a precise time window `[A, B]`.

This repository is designed for **historical backfill only**. It runs independently from the production `ctyun-logpush` pipeline, but it still sends to the **same customer endpoint**, so the default sender rate stays conservative.

## Pre-Deployment Checklist

Before deploying, review and adjust `wrangler-backfill.toml`:

| Field | Current Value (example) | What to Change |
|---|---|---|
| `name` | `ctyun-logpush-backfill` | Your Worker name |
| `account_id` | `0297df3199a9...` | **Must change** to your own Cloudflare Account ID |
| `bucket_name` | `cdn-logs-raw` | Your R2 bucket name |
| `R2_BUCKET_NAME` | `cdn-logs-raw` | Must match `bucket_name` |
| `BACKFILL_START_TIME` | `""` | Required start time (ISO 8601) |
| `BACKFILL_END_TIME` | `""` | Required end time (ISO 8601, must be <= now) |
| `BACKFILL_RATE` | `"20"` | Raw files scanned per cron minute |
| `SEND_TIMEOUT_MS` | `"300000"` | Max wait for customer ACK before retry |

Then set the three Worker secrets:

```bash
wrangler secret put CTYUN_ENDPOINT --config wrangler-backfill.toml
wrangler secret put CTYUN_PRIVATE_KEY --config wrangler-backfill.toml
wrangler secret put CTYUN_URI_EDGE --config wrangler-backfill.toml
```

## One-Time Setup

```bash
wrangler queues create parse-queue-backfill
wrangler queues create send-queue-backfill
wrangler queues create parse-dlq-backfill
wrangler queues create send-dlq-backfill
```

## Deployment

Push to `main` triggers automatic deployment via GitHub Actions.

Or deploy manually:

```bash
wrangler deploy --config wrangler-backfill.toml
```

## Architecture

```text
R2 logs/ -> parse-queue-backfill -> Parser -> processed-backfill/<run-id>/
         -> send-queue-backfill -> Sender -> customer endpoint
```

## Monitoring

```bash
curl https://ctyun-logpush-backfill.<your-subdomain>.workers.dev/backfill/status
wrangler tail ctyun-logpush-backfill
```

Key status fields:

- `status`
- `phase`
- `run_id`
- `enqueued_count`
- `send_stats.success_count`
- `send_stats.timeout_count`
- `send_stats.ack_ms_avg`
- `send_stats.queue_wait_ms_avg`

## Safety Defaults

- Precise record-level replay inside `[BACKFILL_START_TIME, BACKFILL_END_TIME]`
- Sender hard-capped at `<= 5,000 lines/s` by default
- Temporary artifacts under `processed-backfill/<run-id>/` are auto-cleaned after a successful run with a long safety delay
- Sender evidence (`ack_ms`, `queue_wait_ms`) is exposed for troubleshooting and customer communication

## Documentation

| Language | File |
|---|---|
| English | [CTYun Logpush Backfill Guide](https://cfchinanetwork.github.io/ctyun-logpush-backfill/docs/CTYun-Logpush-Backfill-Guide.html) |
| 中文 | [天翼云历史日志补传指南](https://cfchinanetwork.github.io/ctyun-logpush-backfill/docs/CTYun-Logpush-Backfill-Guide.zh-CN.html) |

## Related

- Production worker: [CFChinaNetwork/ctyun-logpush-worker](https://github.com/CFChinaNetwork/ctyun-logpush-worker)
