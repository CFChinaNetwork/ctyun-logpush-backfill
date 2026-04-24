# ctyun-logpush-backfill

Cloudflare Worker for historical log backfill to CDN partner log ingestion endpoint.

**CF-side fully independent** from production `ctyun-logpush`: dedicated queues, per-run R2 prefix (`processed-backfill/<run-id>/`), rate-limited Sender hard-capped at **≤ 5 batches/s (= ≤ 5,000 log lines/s)** via a code-level throttle. This keeps the backfill pipeline isolated from the production CF pipeline. The customer ingestion endpoint is still shared, so backfill intentionally stays conservative and adds at most **1 extra in-flight POST**.

## Use Case

When production `ctyun-logpush` missed a specific time range `[A, B]` (e.g. after an upstream outage), deploy this worker to replay only the log records whose `EdgeStartTimestamp` falls inside that historical window. Deploy → wait → `wrangler delete`.

## Architecture

```
R2 bucket: cdn-logs-raw (shared with production, prefix-isolated)
  logs/YYYYMMDD/...                 ← Logpush raw files (read-only)
  processed/...                     ← Production worker writes (untouched)
  processed-backfill/<run-id>/...   ← This worker writes (run-id = window + this run instance suffix)
  backfill-state/progress.json      ← This worker's state

Backfill pipeline:
  scheduled (1 min cron) → scan logs/ → rate-limited enqueue
         ↓
  parse-queue-backfill → Parser → processed-backfill/<run-id>/
         ↓
  send-queue-backfill → Sender (max_concurrency=1, max_batch_size=1, throttled ≤ 5 batch/s)
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
| `SEND_TIMEOUT_MS` | `"300000"` | Max wait for customer ACK before retrying |

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
- `run_id`: current backfill run identifier (`<window-start>_<window-end>_<run-started-at>`)
- `phase`: `"enqueue"` or `"done"`
- `status`: `"running"` or `"done"`
- `enqueued_count`: total raw files enqueued so far
- `enqueue_progress`: per-day-prefix progress
- `completed_at`: timestamp when enqueue phase finished
- `send_stats`: sender-side evidence including `ack_ms_avg/max`, `queue_wait_ms_avg/max`, `timeout_count`, `http_error_count`

`queue_wait_ms` is the Worker-side per-message approximation of the Queue dashboard's `Average Consumer Lag Time` for `send-queue-backfill`: both describe how long a message waited between being written to the queue and being consumed. The dashboard metric is queue-native and bucket-averaged; `queue_wait_ms` is emitted per send attempt and correlated with `ack_ms`.

**Important**: `status=done` means all raw files have been **enqueued**. Actual delivery to customer is asynchronous via the rate-limited Sender. Monitor `send-queue-backfill` backlog drain for real completion.

Delivery time ≥ `total_batches ÷ 5 batch/s` (lower bound; actual may be longer if the endpoint is slower than the 200ms-per-invocation floor). `total_batches ≈ enqueued_count × avg_batches_per_file`. `avg_batches_per_file` depends on your Logpush `max_upload_records` setting and actual traffic (default 100K records = 100 batches at `BATCH_SIZE=1000`, but small files with low traffic may produce far fewer). Watch the CF Dashboard → Queues → `send-queue-backfill` panel for real-time backlog.

## Pausing / Resuming

```bash
# Pause: edit wrangler-backfill.toml, set BACKFILL_ENABLED = "false", then redeploy
# State is preserved. To resume, set back to "true" and redeploy.
```

## Re-Running for a Different Time Range

1. Edit `BACKFILL_START_TIME` and `BACKFILL_END_TIME` in `wrangler-backfill.toml`
2. `wrangler deploy --config wrangler-backfill.toml`
3. Worker auto-detects config change and re-initializes state

Each run writes to its own `processed-backfill/<run-id>/` prefix, so old `.done` markers do not suppress a new run for the same or overlapping time range.

For clean observability, let the previous `send-queue-backfill` backlog drain before starting a new run.

Alternatively, manually delete R2 object `backfill-state/progress.json`.

## Cleanup

After `status=done` and `send-queue-backfill` backlog is drained:

```bash
wrangler delete --config wrangler-backfill.toml
```

### What's left behind in R2

Same behavior as production: **after each successful HTTP POST**, Sender:
1. Writes a tiny `.done` marker file (content = `"1"`, ~1 byte) under `processed-backfill/<run-id>/{safeKey}-{N}.txt.done` — used to prevent double-sending on Queue retry
2. **Deletes** the original batch `.txt` file — no longer needed, saves R2 storage

The transient `.queued` marker used to suppress duplicate parser re-enqueue is removed on successful send. If a batch is stuck in retries / DLQ, you may temporarily see both the batch file and its `.queued` marker in that run's prefix.

So when backfill completes, `processed-backfill/<run-id>/` contains only `.done` markers (tiny, no actual log data), plus `backfill-state/progress.json` (one state file). These can be manually removed via CF Dashboard / wrangler if desired, but are harmless to leave in place.

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
| `SEND_TIMEOUT_MS` | Var | Max time to wait for customer ACK before retrying (default 300000) |
| `LOG_PREFIX` | Var | R2 prefix for raw Logpush files (default `"logs/"`) |
| `LOG_LEVEL` | Var | `info` or `debug` |
| `PARSE_QUEUE_NAME` | Var | Must match `parse-queue-backfill` |
| `SEND_QUEUE_NAME` | Var | Must match `send-queue-backfill` |
| `R2_BUCKET_NAME` | Var | Must match `[[r2_buckets]].bucket_name` |

## Throughput & Duration

The Sender is hard-capped at **≤ 5 batches/s (= ≤ 5,000 log lines/s)**, regardless of which domain, its normal traffic level, or how fast the receiving endpoint responds.

**How the cap is enforced** (two layers):
1. **Config layer** (`wrangler-backfill.toml`): `max_batch_size=1, max_concurrency=1` → serial, single-message processing, no burst
2. **Code layer** (`src/index.js`): `handleSendQueue` ensures each invocation takes at least 200ms (`MIN_SENDER_INVOCATION_MS`); if `sendBatch` finishes faster, it sleeps the remainder. This guarantees the cap even when the endpoint replies very quickly.

Per the customer-supplied formula `concurrency × requests/sec × lines/request`:

```
1 concurrent POST × ≤ 5 req/s × 1,000 lines/request = ≤ 5,000 lines/s
```

**Actual rate may be lower** if the endpoint is slow (e.g. 500ms sendBatch → only 2 batch/s). The cap is a ceiling, not a floor.

Because `max_concurrency=1`, the extra load added to the customer endpoint is also capped by ACK latency:

```
extra_req_per_sec = min(5, 1000 / ack_ms)
extra_in_flight_requests = 1
```

Example: if the customer endpoint takes 2s to ACK, backfill naturally drops to ~0.5 req/s.

Backfill also records sender evidence in two places:

```text
- Worker logs: ack_ms=<client ACK latency>, queue_wait_ms=<time spent waiting in send-queue-backfill>
- /backfill/status: aggregated send_stats for the current run
```

## Tuning Order

If backfill is slower than expected, adjust in this order:

1. **Increase `SEND_TIMEOUT_MS`** if `ack_ms_max` is close to the timeout and you want to reduce duplicate-causing retries.
2. **Increase `BATCH_SIZE`** if the customer can accept larger request bodies. This is the safest way to raise lines/sec without increasing requests/sec.
3. **Increase Sender concurrency cautiously** (`send-queue-backfill max_concurrency` from `1` to `2`, then re-test) only if the customer confirms they can handle more than one extra in-flight POST.

Do **not** expect `BACKFILL_RATE` to improve drain speed; it only makes the backlog grow faster.

**Duration formula (upper-bound estimate)**:
```
backfill_time ≳ total_log_lines_in_range / 5,000 lines/s
```

where `total_log_lines_in_range` = number of requests the domain actually served during `[A, B]`.

### Examples

| Scenario (domain's normal traffic × gap duration) | Log lines to replay | Backfill time |
|---|---|---|
| 1K req/s × 1h gap (small domain / off-peak) | 3.6M | ~12 min |
| 10K req/s × 1h gap (medium domain) | 36M | ~2 h |
| 100K req/s × 1h gap (large domain at peak) | 360M | ~20 h |
| 100K req/s × 4h gap | 1.44B | ~80 h |

### How to speed up

The ceiling = `max_batch_size × max_concurrency × (1000 / MIN_SENDER_INVOCATION_MS)` msgs/s. With the 200ms floor, each invocation still caps at 5/s, so scaling comes from batch size and/or concurrency:

| Change (`wrangler-backfill.toml`) | Cap becomes | Multiplier |
|---|---|---|
| `max_batch_size=2, max_concurrency=1` | ≤ 10 batch/s | 2× |
| `max_batch_size=5, max_concurrency=1` | ≤ 25 batch/s | 5× |
| `max_batch_size=5, max_concurrency=2` | ≤ 50 batch/s | 10× |
| `max_batch_size=10, max_concurrency=2` | ≤ 100 batch/s | 20× |

Then `wrangler deploy --config wrangler-backfill.toml`.

To change the 200ms floor itself (e.g. lift to 400ms or reduce to 100ms), edit `MIN_SENDER_INVOCATION_MS` in `src/index.js` and redeploy.

**⚠️ Confirm the customer's receiving endpoint can handle the higher load first.** The default slow rate is intentional — to stay gentle on the endpoint while production traffic is also flowing in. Tuning `BACKFILL_RATE` (the Producer rate) alone does NOT speed up delivery; it only grows the `send-queue-backfill` backlog.

## Design Rationale

1. **Why independent queues** — isolate backfill from the production CF pipeline.
2. **Why shared R2 bucket** — raw files are the same data source; only `logs/` prefix is read (no write there).
3. **Why per-run `processed-backfill/<run-id>/`** — Sender's `.done` markers from an old run must not collide with a new run.
4. **Why record-level clipping inside Parser** — backfill enqueues overlapping files, but only emits records whose timestamps are inside `[A, B]`.
5. **Why single Parser consumer** — sender is the real bottleneck; serializing parser avoids duplicate batch enqueue races under retry / duplicate queue delivery.
6. **Why `max_concurrency=1` on Sender** — ensures strictly stable rate to the customer endpoint, no burst traffic even if parse-queue-backfill has a backlog.
7. **Why `END ≤ now` enforced** — prevents misconfigure from scanning current real-time data.
8. **Why `startAfter` pagination** — mid-execution interruption (worker wall-time budget) resumes without losing files.
9. **Why config-change auto-reset** — editing `BACKFILL_START/END_TIME` and redeploying is the intuitive way to re-run, no manual state cleanup needed.

## FAQ

**Q: Does this worker affect production `ctyun-logpush`?**
A: It does not share queues or processed prefixes with production, so it cannot block or corrupt the production CF pipeline. However, it still POSTs to the same customer endpoint, so it adds backfill traffic there. Keep the Sender throttle conservative if the endpoint is slow.

**Q: What if the customer endpoint returns 503 during backfill?**
A: Sender catches the error and calls `msg.retry()`. CF Queue retries up to 5 times with `retry_delay=30s` between attempts. After 5 failures, the message goes to `send-dlq-backfill`. Monitor this DLQ for stuck messages.

**Q: Can I run backfill for a future time range?**
A: No. `END_TIME` must be ≤ current time. Attempting future time returns a validation error.

**Q: What if Logpush hasn't yet written files in my desired range?**
A: The worker will scan R2 and find zero matching files, resulting in `status=done` with `enqueued_count=0`. Check that files actually exist for your range under `logs/YYYYMMDD/`.

**Q: How do I know when real delivery (not just enqueue) is complete?**
A: When `send-queue-backfill` is empty. Check via CF Dashboard → Queues. Total time ≥ `(enqueued_count × avg_batches_per_file) ÷ 5 batch/s` (lower bound; actual may be longer if endpoint is slow). Varies significantly with your Logpush file sizes.

## Related

- Production worker: [`CFChinaNetwork/ctyun-logpush-worker`](https://github.com/CFChinaNetwork/ctyun-logpush-worker)
