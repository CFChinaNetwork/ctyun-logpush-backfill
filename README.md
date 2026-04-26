# ctyun-logpush-backfill

Standalone Cloudflare Worker for replaying historical Logpush logs in a precise time window `[A, B]`.

This repository is designed for **historical backfill only**. It runs independently from the production `ctyun-logpush` pipeline, but it still sends to the **same customer endpoint**, so the sender is rate-capped explicitly.

The repository now contains a single production-ready queue-only implementation in `src/index.js`.

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
| `BACKFILL_ENABLED` | `"false"` | Set to `"true"` only when you are ready to run the replay |
| `BACKFILL_RATE` | `"100"` | Raw files scanned per cron minute |
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

Deploy manually:

```bash
wrangler deploy --config wrangler-backfill.toml
```

Do not change `BACKFILL_START_TIME` / `BACKFILL_END_TIME` while an existing run is still active. Wait until `/backfill/status` reports `fully_completed = true` first.

If you must replay the same time window again, delete `cdn-logs-raw/backfill-state/progress.json` and `cdn-logs-raw/backfill-state/status.json` first, then redeploy (or keep `BACKFILL_ENABLED = "true"` and wait for the next cron tick). Old `processed-backfill/<run-id>/` prefixes do not block a new run because each run uses a fresh `run_id`.

## Architecture

```text
R2 logs/ -> parse-queue-backfill -> Parser
         -> processed-backfill/<run-id>/
         -> send-queue-backfill -> Sender -> customer endpoint
```

## Monitoring

```bash
curl https://ctyun-logpush-backfill.<your-subdomain>.workers.dev/backfill/status
wrangler tail ctyun-logpush-backfill
```

Key status fields:

- `summary`
- `status_code` / `status_explained`
- `stage_code` / `stage_explained`
- `delivery_completed`
- `fully_completed`
- `run_id`
- `replay_window_beijing`
- `task_started_beijing`
- `raw_file_scan_finished_beijing`
- `last_refresh_beijing`
- `matched_raw_files`
- `batches_sent` / `batches_pending`
- `log_lines_sent` / `log_lines_pending`
- `batch_lines_avg` / `batch_lines_min` / `batch_lines_max`
- `batch_size_note`
- `cleanup`
- `rerun_same_window_how`
- `r2_console_note`

`/backfill/status` is also persisted to `cdn-logs-raw/backfill-state/status.json` for direct viewing in the R2 UI.
Use `GET /backfill/status?view=raw` when you want the raw state file plus low-level artifact stats.

## Safety Defaults

- Precise record-level replay inside `[BACKFILL_START_TIME, BACKFILL_END_TIME]`
- Only top-level requests are sent (`ParentRayID = "00"` and `WorkerSubrequest != true`)
- Sender hard-capped at `<= 100,000 lines/s` with the current checked-in configuration when batches are near the configured `BATCH_SIZE = 1000`
- Delivery semantics are **at-least-once**, not exactly-once. If a customer POST succeeds but the `.done` marker cannot be persisted, the batch is retried and the receiver should dedupe by batch identity if strict exactly-once is required.
- Temporary artifacts under `processed-backfill/<run-id>/` are auto-cleaned after a successful run with a long safety delay
- Sender evidence (`ack_ms`, `queue_wait_ms`) is emitted in Worker logs; `/backfill/status` now also gives a direct batch/log-line reconciliation view without adding a shared hot-path counter

## Documentation

| Language | File |
|---|---|
| English | [CTYun Logpush Backfill Guide](https://cfchinanetwork.github.io/ctyun-logpush-backfill/docs/CTYun-Logpush-Backfill-Guide.html) |
| 中文 | [天翼云历史日志补传指南](https://cfchinanetwork.github.io/ctyun-logpush-backfill/docs/CTYun-Logpush-Backfill-Guide.zh-CN.html) |

## Related

- Production worker: [CFChinaNetwork/ctyun-logpush-worker](https://github.com/CFChinaNetwork/ctyun-logpush-worker)
