# ctyun-logpush-backfill

Cloudflare Worker for historical log backfill to CDN partner log ingestion endpoint.

**Fully independent** from production `ctyun-logpush`: dedicated queues, separate R2 prefix (`processed-backfill/`), rate-limited Sender at a fixed **~10 batches/s (= ~10,000 log lines/s)**. Safe to deploy alongside production without any interference.

## Use Case

When production `ctyun-logpush` missed a specific time range `[A, B]` (e.g. after an upstream outage), deploy this worker to replay those logs to the customer endpoint. Deploy → wait → `wrangler delete`.

## Architecture

```
R2 bucket: cdn-logs-raw (shared with production, prefix-isolated)
  logs/YYYYMMDD/...                 ← Logpush raw files (read-only)
  processed/...                     ← Production worker writes (untouched)
  processed-backfill/...            ← This worker writes
  backfill-state/progress.json      ← This worker's state

Backfill pipeline:
  scheduled (1 min cron) → scan logs/ → rate-limited enqueue
         ↓
  parse-queue-backfill → Parser → processed-backfill/
         ↓
  send-queue-backfill → Sender (max_concurrency=1, ~10 batch/s)
         ↓
  Customer log ingestion endpoint (same as production)
```

## Pre-Deployment Checklist

Before deploying, review and adjust `wrangler-backfill.toml`:

| Field | Current Value | What to Change |
|---|---|---|
| `account_id` | `0297df3199a9...` | Your Cloudflare Account ID (`wrangler whoami`) |
| `bucket_name` (R2) | `cdn-logs-raw` | Your R2 bucket name (must match production) |
| `R2_BUCKET_NAME` (vars) | `cdn-logs-raw` | Must match `bucket_name` above |
| `BACKFILL_START_TIME` | `""` | e.g. `"2026-04-22T14:00:00Z"` (UTC) or `"2026-04-22T22:00:00+08:00"` (Beijing) |
| `BACKFILL_END_TIME` | `""` | Must be ≤ now, range ≤ 48h |
| `BACKFILL_RATE` | `"5"` | Raw files per minute (default 5, max 100) |

## One-Time Setup

### 1. Create the 4 queues

```bash
wrangler queues create parse-queue-backfill
wrangler queues create send-queue-backfill
wrangler queues create parse-dlq-backfill
wrangler queues create send-dlq-backfill
```

### 2. Set secrets (values identical to production)

```bash
wrangler secret put CTYUN_ENDPOINT     --config wrangler-backfill.toml
wrangler secret put CTYUN_PRIVATE_KEY  --config wrangler-backfill.toml
wrangler secret put CTYUN_URI_EDGE     --config wrangler-backfill.toml
```

## Deployment

Push to `main` triggers automatic deployment via GitHub Actions.

Or manual:
```bash
wrangler deploy --config wrangler-backfill.toml
```

## Monitoring Progress

```bash
# HTTP endpoint (state JSON)
curl https://ctyun-logpush-backfill.<your-subdomain>.workers.dev/backfill/status

# Live log
wrangler tail ctyun-logpush-backfill
```

Key fields in status JSON:
- `phase`: `"enqueue"` or `"done"`
- `status`: `"running"` or `"done"`
- `enqueued_count`: total raw files enqueued so far
- `enqueue_progress`: per-day-prefix progress
- `completed_at`: timestamp when enqueue phase finished

**Important**: `status=done` means all raw files have been **enqueued**. Actual delivery to customer is asynchronous via the rate-limited Sender. Monitor `send-queue-backfill` backlog drain for real completion.

Delivery time = `total_batches ÷ ~10 batch/s`, where `total_batches ≈ enqueued_count × avg_batches_per_file`. `avg_batches_per_file` depends on your Logpush `max_upload_records` setting and actual traffic (default 100K records = 100 batches at `BATCH_SIZE=1000`, but small files with low traffic may produce far fewer). Watch the CF Dashboard → Queues → `send-queue-backfill` panel for real-time backlog.

## Pausing / Resuming

```bash
# Pause: edit wrangler-backfill.toml, set BACKFILL_ENABLED = "false", then redeploy
# State is preserved. To resume, set back to "true" and redeploy.
```

## Re-Running for a Different Time Range

1. Edit `BACKFILL_START_TIME` and `BACKFILL_END_TIME` in `wrangler-backfill.toml`
2. `wrangler deploy --config wrangler-backfill.toml`
3. Worker auto-detects config change and re-initializes state

Alternatively, manually delete R2 object `backfill-state/progress.json`.

## Cleanup

After `status=done` and `send-queue-backfill` backlog is drained:

```bash
wrangler delete --config wrangler-backfill.toml
```

### What's left behind in R2

Same behavior as production: **after each successful HTTP POST**, Sender:
1. Writes a tiny `.done` marker file (content = `"1"`, ~1 byte) under `processed-backfill/{safeKey}-{N}.txt.done` — used to prevent double-sending on Queue retry
2. **Deletes** the original batch `.txt` file — no longer needed, saves R2 storage

So when backfill completes, `processed-backfill/` contains only `.done` markers (tiny, no actual log data), plus `backfill-state/progress.json` (one state file). These can be manually removed via CF Dashboard / wrangler if desired, but are harmless to leave in place.

The 4 backfill queues can be left as-is (empty and idle) — next backfill run will reuse them without re-creation.

## Environment Variables

| Name | Type | Description |
|---|---|---|
| `CLOUDFLARE_API_TOKEN` | Secret (GitHub) | CF API token for GitHub Actions deploy |
| `CTYUN_ENDPOINT` | Secret | Customer log server base URL (same as production) |
| `CTYUN_PRIVATE_KEY` | Secret | Authentication private key (same as production) |
| `CTYUN_URI_EDGE` | Secret | Log POST URI path (same as production) |
| `BACKFILL_START_TIME` | Var | ISO 8601 start time (required) |
| `BACKFILL_END_TIME` | Var | ISO 8601 end time (required, must be ≤ now) |
| `BACKFILL_RATE` | Var | Raw files per cron minute (default 5, max 100) |
| `BACKFILL_ENABLED` | Var | Set to `"false"` to pause |
| `BATCH_SIZE` | Var | Log lines per POST (default 1000, same as production) |
| `LOG_PREFIX` | Var | R2 prefix for raw Logpush files (default `"logs/"`) |
| `LOG_LEVEL` | Var | `info` or `debug` |
| `PARSE_QUEUE_NAME` | Var | Must match `parse-queue-backfill` |
| `SEND_QUEUE_NAME` | Var | Must match `send-queue-backfill` |
| `R2_BUCKET_NAME` | Var | Must match `[[r2_buckets]].bucket_name` |

## Throughput & Duration

The Sender is the binding constraint — **fixed at ~10 batches/s (= ~10,000 log lines/s)**, regardless of which domain or its normal traffic level. Controlled by `max_batch_size=2, max_concurrency=1` in `wrangler-backfill.toml`.

**Duration formula**:
```
backfill_time ≈ total_log_lines_in_range / 10,000 lines/s
```

where `total_log_lines_in_range` = number of requests the domain actually served during `[A, B]`.

### Examples

| Scenario (domain's normal traffic × gap duration) | Log lines to replay | Backfill time |
|---|---|---|
| 1K req/s × 1h gap (small domain / off-peak) | 3.6M | ~6 min |
| 10K req/s × 1h gap (medium domain) | 36M | ~1 h |
| 100K req/s × 1h gap (large domain at peak) | 360M | ~10 h |
| 100K req/s × 4h gap | 1.44B | ~40 h |

### How to speed up

If delivery is too slow, edit `wrangler-backfill.toml`:

| Change | New rate | Multiplier |
|---|---|---|
| `max_batch_size=5, max_concurrency=1` | ~25 batch/s | 2.5× |
| `max_batch_size=5, max_concurrency=2` | ~50 batch/s | 5× |
| `max_batch_size=10, max_concurrency=2` | ~100 batch/s | 10× |

Then `wrangler deploy --config wrangler-backfill.toml`.

**⚠️ Confirm the customer's receiving endpoint can handle the higher load first.** The default slow rate is intentional — to stay gentle on the endpoint while production traffic is also flowing in. Tuning `BACKFILL_RATE` (the Producer rate) alone does NOT speed up delivery; it only grows the `send-queue-backfill` backlog.

## Design Rationale

1. **Why independent queues** — guarantee zero interference with production pipeline.
2. **Why shared R2 bucket** — raw files are the same data source; only `logs/` prefix is read (no write there).
3. **Why prefix-isolated `processed-backfill/`** — Sender's `.done` idempotency markers must not collide with production's.
4. **Why `max_concurrency=1` on Sender** — ensures strictly stable rate to the customer endpoint, no burst traffic even if parse-queue-backfill has a backlog.
5. **Why `END ≤ now` enforced** — prevents misconfigure from scanning current real-time data.
6. **Why `startAfter` pagination** — mid-execution interruption (worker wall-time budget) resumes without losing files.
7. **Why config-change auto-reset** — editing `BACKFILL_START/END_TIME` and redeploying is the intuitive way to re-run, no manual state cleanup needed.

## FAQ

**Q: Does this worker affect production `ctyun-logpush`?**
A: No. It runs as a separate worker with its own queues, its own R2 prefix. The only shared resource is the R2 bucket (but only `logs/` is read by both; writes go to different prefixes).

**Q: What if the customer endpoint returns 503 during backfill?**
A: Sender catches the error and calls `msg.retry()`. CF Queue retries up to 5 times with `retry_delay=30s` between attempts. After 5 failures, the message goes to `send-dlq-backfill`. Monitor this DLQ for stuck messages.

**Q: Can I run backfill for a future time range?**
A: No. `END_TIME` must be ≤ current time. Attempting future time returns a validation error.

**Q: What if Logpush hasn't yet written files in my desired range?**
A: The worker will scan R2 and find zero matching files, resulting in `status=done` with `enqueued_count=0`. Check that files actually exist for your range under `logs/YYYYMMDD/`.

**Q: How do I know when real delivery (not just enqueue) is complete?**
A: When `send-queue-backfill` is empty. Check via CF Dashboard → Queues. Total time ≈ `(enqueued_count × avg_batches_per_file) ÷ 10 batch/s` — varies significantly with your Logpush file sizes.

## Related

- Production worker: [`CFChinaNetwork/ctyun-logpush-worker`](https://github.com/CFChinaNetwork/ctyun-logpush-worker)
