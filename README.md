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
| `RUN_AGGREGATOR` | `RunAggregator` | Durable Object binding used for cross-file batching |
| `BACKFILL_START_TIME` | `""` | Required start time (ISO 8601) |
| `BACKFILL_END_TIME` | `""` | Required end time (ISO 8601, must be <= now) |
| `BACKFILL_ENABLED` | `"false"` | Set to `"true"` only when you are ready to run the replay |
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

No extra manual setup is required for the Durable Object. It is created automatically by Wrangler using the migration in `wrangler-backfill.toml`.

## Deployment

Deploy manually:

```bash
wrangler deploy --config wrangler-backfill.toml
```

Do not change `BACKFILL_START_TIME` / `BACKFILL_END_TIME` while an existing run is still active. Wait until `/backfill/status` reports `status = "cleaned"` first.

## Architecture

```text
R2 logs/ -> parse-queue-backfill -> Parser
         -> RunAggregator Durable Object
         -> processed-backfill/<run-id>/
         -> send-queue-backfill -> Sender -> customer endpoint
```

Architecture diagram:

- Direct preview: [Flow Chart (PNG)](docs/Flow%20Chart.png)
- Editable source: [Flow Chart (draw.io)](docs/Flow%20Chart.drawio)

## Monitoring

```bash
curl https://ctyun-logpush-backfill.<your-subdomain>.workers.dev/backfill/status
wrangler tail ctyun-logpush-backfill
```

For the original internal state shape, use `GET /backfill/status?view=raw`.

Key status fields:

- `status`
- `stage`
- `message`
- `run_id`
- `window_start`
- `window_end`
- `raw_files_enqueued`
- `batches_handed_to_send_queue`
- `log_lines_handed_to_send_queue`
- `line_count_status`
- `average_lines_per_batch`
- `smallest_batch_lines`
- `largest_batch_lines`
- `batches_still_buffered_in_worker`
- `lines_still_buffered_in_worker`

If you deploy this change in the middle of an older in-flight run, `line_count_status` may be `not_available_for_legacy_run`; exact line totals start with runs that begin on this version.

## Safety Defaults

- Precise record-level replay inside `[BACKFILL_START_TIME, BACKFILL_END_TIME]`
- Only top-level requests are sent (`ParentRayID = "00"` and `WorkerSubrequest != true`)
- Sender hard-capped at `<= 50,000 lines/s` with the current checked-in configuration
- Temporary artifacts under `processed-backfill/<run-id>/` are auto-cleaned after a successful run with a long safety delay
- Exact batch and line totals emitted to `send-queue-backfill` are exposed in `progress.json.aggregate`; no attempt-level ACK/queue-wait metrics are persisted

## Documentation

| Language | File |
|---|---|
| English | [CTYun Logpush Backfill Guide](https://cfchinanetwork.github.io/ctyun-logpush-backfill/docs/CTYun-Logpush-Backfill-Guide.html) |
| 中文 | [天翼云历史日志补传指南](https://cfchinanetwork.github.io/ctyun-logpush-backfill/docs/CTYun-Logpush-Backfill-Guide.zh-CN.html) |

## Related

- Production worker: [CFChinaNetwork/ctyun-logpush-worker](https://github.com/CFChinaNetwork/ctyun-logpush-worker)
