# ctyun-logpush-backfill 客户部署检查清单

## 1. 方案边界

- 这套 backfill Worker 与生产 `ctyun-logpush` 在 Cloudflare 内部链路上隔离：独立 Worker、独立 Queues、独立 `processed-backfill/<run-id>/` 前缀。
- 这套 backfill Worker **仍然会 POST 到同一个客户接收 endpoint**，所以它不是“客户侧零影响”，而是“受控、低速、额外 1 个 in-flight POST”的补传方案。
- 不要在前一个 backfill 的 `send-queue-backfill` 还没 drain 完时，再启动另一个重叠时间窗的 backfill run。

## 2. 部署前必须确认

- 确认补传窗口：`BACKFILL_START_TIME` 和 `BACKFILL_END_TIME`
- 确认时间格式：支持 UTC 和北京时间 ISO 8601
- 确认时间范围：`END_TIME <= now`，且总跨度 `<= 48h`
- 确认 R2 bucket 与生产一致
- 确认客户 endpoint 与生产一致
- 确认客户 ACK 慢时的超时设置：默认建议 `SEND_TIMEOUT_MS="180000"`

## 3. wrangler-backfill.toml 需要改的字段

- `account_id`
- `bucket_name`
- `R2_BUCKET_NAME`
- `BACKFILL_START_TIME`
- `BACKFILL_END_TIME`
- `SEND_TIMEOUT_MS`
- 如需更保守或更激进，再调整 `BACKFILL_RATE`

建议值：

```toml
BACKFILL_START_TIME = "2026-04-22T15:00:00+08:00"
BACKFILL_END_TIME   = "2026-04-22T19:00:00+08:00"
BACKFILL_RATE       = "5"
SEND_TIMEOUT_MS     = "180000"
```

## 4. 一次性初始化

```bash
wrangler queues create parse-queue-backfill
wrangler queues create send-queue-backfill
wrangler queues create parse-dlq-backfill
wrangler queues create send-dlq-backfill

wrangler secret put CTYUN_ENDPOINT     --config wrangler-backfill.toml
wrangler secret put CTYUN_PRIVATE_KEY  --config wrangler-backfill.toml
wrangler secret put CTYUN_URI_EDGE     --config wrangler-backfill.toml
```

## 5. 部署

```bash
wrangler deploy --config wrangler-backfill.toml
```

## 6. 运行中要看什么

### A. 看补传状态

```bash
curl https://ctyun-logpush-backfill.<your-subdomain>.workers.dev/backfill/status
```

重点字段：

- `run_id`
- `status`
- `phase`
- `enqueued_count`
- `send_stats.success_count`
- `send_stats.timeout_count`
- `send_stats.http_error_count`
- `send_stats.ack_ms_avg`
- `send_stats.ack_ms_max`
- `send_stats.queue_wait_ms_avg`
- `send_stats.queue_wait_ms_max`

解释：

- `ack_ms_*`：客户接收端从我们发出 POST 到返回 ACK 的时间
- `queue_wait_ms_*`：消息在 `send-queue-backfill` 里排队等待被发送的时间

### B. 看实时日志

```bash
wrangler tail ctyun-logpush-backfill
```

重点搜索：

- `ack_ms=`
- `queue_wait_ms=`
- `HTTP 503`
- `Send timeout`

## 7. 如何解释“是客户 ACK 慢还是 CF 自己慢”

判断方式：

- 如果 `ack_ms` 高，但 `queue_wait_ms` 低：说明是客户接收端 ACK 慢
- 如果 `queue_wait_ms` 高，但 `ack_ms` 低：说明是 send queue backlog 大，发送前排队久
- 如果两者都高：说明客户 ACK 慢，同时我们的 send queue 也在堆积

这两组数据可以直接拿去和客户沟通，不需要只靠肉眼看 dashboard 曲线猜。

## 8. 什么时候算补传完成

必须同时满足：

- `/backfill/status` 里 `status="done"`
- Cloudflare Dashboard 里 `send-queue-backfill` backlog 归零

只看到 `status=done` 还不够，那只表示 raw files 已经全部入队，不代表已经全部送达客户。

## 9. 清理

```bash
wrangler delete --config wrangler-backfill.toml
```

R2 会保留：

- `backfill-state/progress.json`
- `backfill-state/send-stats.json`
- `processed-backfill/<run-id>/*.done`

这些都可以后续手动删，也可以先保留作为审计证据。
