# ctyun-logpush-backfill

Standalone Cloudflare Worker for replaying historical Logpush logs in a precise time window `[A, B]`.

Runs fully independently from the production [`ctyun-logpush-worker`](https://github.com/CFChinaNetwork/ctyun-logpush-worker) — own queues, own R2 prefix, own concurrency budget — but sends to the **same customer endpoint**, so the sender is rate-capped explicitly.

## Customer Constraints

Three customer-side requirements drive the entire design:

| Requirement | How it is enforced |
|---|---|
| **Receiver throughput cap (~100,000 lines/s)** | Sender hard-capped via `max_concurrency=20` × `MIN_SENDER_INVOCATION_MS=200ms` × `BATCH_SIZE=1000` |
| **Receiver does NOT dedupe** | Delivery is **at-least-once**. Worker-side `.done` / `.queued` markers minimize duplicates, but a residual rate of < 0.001% under fetch-abort scenarios is unavoidable |
| **No Worker subrequest logs** | Source-side filter: only `ParentRayID == "00"` AND `WorkerSubrequest != true` records are forwarded |

The production worker `ctyun-logpush-worker` shares the same customer endpoint at a similar rate. Backfill runs in its own Worker / queues / concurrency budget — it does not consume production capacity, but both share the receiver's downstream bandwidth. See the [docs](#documentation) for full coexistence details.

## Pre-Deployment Checklist

Before deploying, review and adjust `wrangler-backfill.toml`:

| Field | Current Value (example) | What to Change |
|---|---|---|
| `account_id` | `0297df3199a9...` | **Must change** to your own Cloudflare Account ID |
| `bucket_name` / `R2_BUCKET_NAME` | `cdn-logs-raw` | Your R2 bucket name |
| `BACKFILL_START_TIME` | `""` | Replay start time (ISO 8601) |
| `BACKFILL_END_TIME` | `""` | Replay end time (ISO 8601, must be `<= now`) |
| `BACKFILL_ENABLED` | `"false"` | Set to `"true"` only when ready to run |
| `BACKFILL_RATE` | `"60"` | Raw files scanned per cron minute |

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

## Architecture

```text
R2 logs/ -> parse-queue-backfill -> Parser
         -> processed-backfill/<run-id>/
         -> send-queue-backfill  -> Sender -> customer endpoint
```

## Deploy

```bash
wrangler deploy --config wrangler-backfill.toml
```

Do not change `BACKFILL_START_TIME` / `BACKFILL_END_TIME` while a run is active. Wait until `/backfill/status` reports `fully_completed = true` first.

## Monitoring

```bash
curl https://ctyun-logpush-backfill.<your-subdomain>.workers.dev/backfill/status
wrangler tail ctyun-logpush-backfill
```

The status payload is human-friendly (Beijing time, plain-language summary) and is also persisted to `cdn-logs-raw/backfill-state/status.json` for direct R2 UI viewing. Use `?view=raw` for low-level state and artifact stats. See the guides below for the full field list and re-run / cleanup details.

## Documentation

| Language | File |
|---|---|
| English | [CTYun Logpush Backfill Guide](https://cfchinanetwork.github.io/ctyun-logpush-backfill/docs/CTYun-Logpush-Backfill-Guide.html) |
| 中文 | [天翼云历史日志补传指南](https://cfchinanetwork.github.io/ctyun-logpush-backfill/docs/CTYun-Logpush-Backfill-Guide.zh-CN.html) |

## Related

- Production worker: [`CFChinaNetwork/ctyun-logpush-worker`](https://github.com/CFChinaNetwork/ctyun-logpush-worker)
