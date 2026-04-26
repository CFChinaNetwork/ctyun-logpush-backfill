'use strict';

/**
 * Cloudflare Workers — Logpush Historical Backfill Worker (queue-only)
 *
 * 专用于补传指定时间窗 [BACKFILL_START_TIME, BACKFILL_END_TIME] 内的 Logpush 日志。
 * 与生产 worker (ctyun-logpush) 完全独立：独立 Worker / 独立 queues / 独立
 * R2 prefix（processed-backfill/）/ 独立 concurrency 配额。共享同一个 R2 bucket
 * (只读 logs/ 前缀) 和同一个客户接收端（共担下游带宽）。
 *
 * Architecture:
 *   scheduled (cron, 1/min) → 扫 R2 logs/YYYYMMDD/ → rate-limited 入 parse-queue-backfill
 *                                                       ↓
 *                          Parser: gzip 流式解码 + 逐条按窗口 + ParentRayID 过滤 +
 *                                  transformEdge 转 partner 格式 + 每 BATCH_SIZE 行
 *                                  写入 processed-backfill/<run-id>/{batch}.txt
 *                                                       ↓
 *                                           send-queue-backfill
 *                                                       ↓
 *                          Sender: gzip + MD5 签名 POST + .done 标记 + 清理临时 batch
 *                                                       ↓
 *                                            客户接收端（与生产共享）
 *
 * Customer constraints (drives the entire design):
 *   1. Receiver cap ~100,000 lines/s. Sender hard-capped via:
 *      max_concurrency × (1000 / MIN_SENDER_INVOCATION_MS) × BATCH_SIZE
 *      = 20 × (1000 / 200) × 1000 = 100,000 lines/s (theoretical ceiling)
 *   2. Receiver does NOT dedupe. Delivery is at-least-once. .done / .queued
 *      markers minimize duplicates; residual fetch-abort cases are < 0.001%.
 *   3. Drop Worker subrequests. Source-side filter:
 *      ParentRayID === "00" AND WorkerSubrequest !== true.
 *
 * Real-world bottleneck (vivo asia-main-appstore measured run, 651 files):
 *   Parser single-file wall time ≈ 150 s (50 batches × 5 sequential R2 ops × ~588 ms/op
 *   = ~147 s I/O + ~3 s CPU). Parser sustained ≈ 3 batch/s at 93% concurrency utilization.
 *   Sender single-invocation ≈ 0.55 s (incl. ACK ~400 ms); ceiling ≈ 36 batch/s but
 *   actual ≈ 3 batch/s at 8% utilization (Sender is mostly idle, waiting on Parser).
 *   Total throughput ≈ 3 batch/s ≈ 3K lines/s, far below the Sender's 100K nameplate.
 *   To raise throughput, raise PARSER max_concurrency in wrangler-backfill.toml
 *   (10 → 20 cuts duration ~50%). Do NOT raise Sender max_concurrency — Sender
 *   already has plenty of headroom; the bottleneck is upstream.
 *
 * Design notes:
 *   - 热路径不使用 Durable Object（旧版有 RunAggregator，已通过 v2 migration 移除）
 *   - 每个 raw .gz 文件独立解析，最后一个 batch 可能不满 BATCH_SIZE（可接受）
 *   - 幂等保护：parser 端 head(.queued) + sender 端 head(.done)
 *   - 数据丢失保护：parser 失败 rollback (delete batchKey + .queued)
 *
 * Env Secrets : CTYUN_ENDPOINT, CTYUN_PRIVATE_KEY, CTYUN_URI_EDGE
 * Env Vars    : BACKFILL_START_TIME, BACKFILL_END_TIME, BACKFILL_ENABLED,
 *               BACKFILL_RATE, BATCH_SIZE, SEND_TIMEOUT_MS,
 *               LOG_PREFIX, LOG_LEVEL, PARSE_QUEUE_NAME, SEND_QUEUE_NAME,
 *               R2_BUCKET_NAME
 *
 * HTTP Endpoints:
 *   GET  /backfill/status            人话版进度 (Beijing time)
 *   GET  /backfill/status?view=raw   原始 state + artifact 统计
 *
 * Related:
 *   Production worker: https://github.com/CFChinaNetwork/ctyun-logpush-worker
 *   Docs (EN/中):      https://cfchinanetwork.github.io/ctyun-logpush-backfill/
 */

// ─── IATA airport code → ISO country code（用于 partner 格式 #45 country 字段）
//     coloToCountry() 优先用 EdgeColoCode 查表，失败则回退到 ClientCountry，
//     再失败则默认 'CN'。覆盖 200+ 主流 colo，未列出的 colo 走 fallback。
const IATA_TO_COUNTRY = Object.freeze({
  'HGH':'CN','SHA':'CN','PEK':'CN','PVG':'CN','CAN':'CN','SZX':'CN',
  'CTU':'CN','CKG':'CN','XIY':'CN','WUH':'CN','NKG':'CN','TSN':'CN',
  'TAO':'CN','CGO':'CN','CSX':'CN','HRB':'CN','DLC':'CN','URC':'CN',
  'KMG':'CN','FOC':'CN','HAK':'CN','SHE':'CN','TNA':'CN','XMN':'CN',
  'NNG':'CN','INC':'CN','LHW':'CN','TYN':'CN','CGQ':'CN','HET':'CN',
  'HKG':'HK','MFM':'MO',
  'TPE':'TW','TSA':'TW','KHH':'TW','RMQ':'TW',
  'NRT':'JP','HND':'JP','KIX':'JP','ITM':'JP','NGO':'JP','FUK':'JP',
  'CTS':'JP','OKA':'JP','HIJ':'JP','KOJ':'JP','SDJ':'JP',
  'ICN':'KR','GMP':'KR','PUS':'KR','CJU':'KR','CJJ':'KR',
  'SIN':'SG',
  'KUL':'MY','PEN':'MY','BKI':'MY','KCH':'MY',
  'BKK':'TH','DMK':'TH','HKT':'TH','CNX':'TH',
  'CGK':'ID','DPS':'ID','SUB':'ID','MDC':'ID','UPG':'ID',
  'MNL':'PH','CEB':'PH','DVO':'PH',
  'SGN':'VN','HAN':'VN','DAD':'VN',
  'BOM':'IN','DEL':'IN','MAA':'IN','BLR':'IN','CCU':'IN','HYD':'IN',
  'AMD':'IN','COK':'IN','PNQ':'IN','GAU':'IN','JAI':'IN','LKO':'IN',
  'KHI':'PK','LHE':'PK','ISB':'PK',
  'DAC':'BD','CMB':'LK','RGN':'MM','PNH':'KH','KTM':'NP',
  'SYD':'AU','MEL':'AU','BNE':'AU','PER':'AU','ADL':'AU','CBR':'AU',
  'AKL':'NZ','CHC':'NZ','WLG':'NZ',
  'LAX':'US','SFO':'US','SEA':'US','ORD':'US','DFW':'US','JFK':'US',
  'EWR':'US','MIA':'US','ATL':'US','IAD':'US','DEN':'US','PHX':'US',
  'MSP':'US','DTW':'US','BOS':'US','CLT':'US','LAS':'US','SLC':'US',
  'PDX':'US','SAN':'US','AUS':'US','CMH':'US','IND':'US','MCI':'US',
  'STL':'US','RIC':'US','BUF':'US','HNL':'US','OMA':'US','TUL':'US',
  'OKC':'US','ELP':'US','ABQ':'US','BHM':'US','LIT':'US','GRR':'US',
  'ICT':'US','CID':'US','DSM':'US','FAR':'US','RAP':'US','BIS':'US',
  'SMF':'US','BUR':'US','LGB':'US','ONT':'US','TUS':'US',
  'YYZ':'CA','YVR':'CA','YUL':'CA','YYC':'CA','YEG':'CA','YOW':'CA',
  'MEX':'MX','GDL':'MX','MTY':'MX','CUN':'MX',
  'GRU':'BR','GIG':'BR','SSA':'BR','FOR':'BR','REC':'BR','POA':'BR',
  'EZE':'AR','SCL':'CL','BOG':'CO','MDE':'CO','LIM':'PE',
  'UIO':'EC','CCS':'VE','PTY':'PA','SJO':'CR','GUA':'GT','SDQ':'DO',
  'ASU':'PY','MVD':'UY','VVI':'BO',
  'LHR':'GB','LGW':'GB','MAN':'GB','EDI':'GB','BHX':'GB','STN':'GB',
  'CDG':'FR','ORY':'FR','LYS':'FR','NCE':'FR','MRS':'FR',
  'FRA':'DE','MUC':'DE','DUS':'DE','BER':'DE','HAM':'DE','STR':'DE',
  'CGN':'DE','NUE':'DE','LEJ':'DE',
  'AMS':'NL','BRU':'BE',
  'MAD':'ES','BCN':'ES','VLC':'ES','AGP':'ES','PMI':'ES',
  'LIS':'PT','OPO':'PT',
  'FCO':'IT','MXP':'IT','LIN':'IT','NAP':'IT','VCE':'IT',
  'ZRH':'CH','GVA':'CH','VIE':'AT',
  'WAW':'PL','KRK':'PL','PRG':'CZ','BUD':'HU',
  'OTP':'RO','SOF':'BG','ATH':'GR','SKG':'GR',
  'IST':'TR','SAW':'TR','ESB':'TR','ADB':'TR',
  'TLV':'IL',
  'DXB':'AE','AUH':'AE','SHJ':'AE',
  'RUH':'SA','JED':'SA','DMM':'SA',
  'KWI':'KW','DOH':'QA','BAH':'BH','MCT':'OM','AMM':'JO',
  'CAI':'EG','JNB':'ZA','CPT':'ZA','DUR':'ZA',
  'LOS':'NG','NBO':'KE','ADD':'ET','DAR':'TZ','ACC':'GH','DKR':'SN',
  'CMN':'MA','TUN':'TN','ALG':'DZ',
  'SVO':'RU','DME':'RU','LED':'RU','OVB':'RU','SVX':'RU',
  'KBP':'UA','ARN':'SE','OSL':'NO','CPH':'DK','HEL':'FI',
  'DUB':'IE','KEF':'IS','LUX':'LU',
  'RIX':'LV','VNO':'LT','TLL':'EE',
  'ZAG':'HR','BEG':'RS','BTS':'SK',
  'ALA':'KZ','TAS':'UZ','GYD':'AZ','TBS':'GE','EVN':'AM',
  'MLA':'MT','LCA':'CY','TGD':'ME',
  'GUM':'GU','NAN':'FJ','POM':'PG','MLE':'MV',
});

function coloToCountry(coloCode, clientCountry) {
  if (coloCode) {
    const c = IATA_TO_COUNTRY[coloCode.toUpperCase()];
    if (c) return c;
  }
  return clientCountry ? clientCountry.toUpperCase() : 'CN';
}

// ─── Partner log format constants (CDN partner spec v3.0, 145 fields, SOH-delimited)
const SEP = '\u0001';                 // SOH (0x01) field separator per partner spec
const MONTH_ABBR = Object.freeze([
  'Jan','Feb','Mar','Apr','May','Jun',
  'Jul','Aug','Sep','Oct','Nov','Dec',
]);
const VERSION_EDGE = 'cf_vod_v3.0';   // partner spec version tag (field #1)
// Pre-frozen "all dashes" arrays for unused trailing fields (cheap to spread)
const DASHES_9  = Object.freeze(Array(9).fill('-'));
const DASHES_4  = Object.freeze(Array(4).fill('-'));
const DASHES_2  = Object.freeze(Array(2).fill('-'));
const DASHES_16 = Object.freeze(Array(16).fill('-'));
const DASHES_15 = Object.freeze(Array(15).fill('-'));
const DASHES_50 = Object.freeze(Array(50).fill('-'));
// Field-level truncation limits (matches production worker)
const MAX_URL_LEN = 4096;
const MAX_UA_LEN  = 1024;
const MAX_REF_LEN = 2048;

// ─── Runtime constants
const BATCH_ROOT_PREFIX       = 'processed-backfill/';     // 所有 batch artifact 写在这个前缀下
const STATE_KEY               = 'backfill-state/progress.json'; // 内部 progress state
const STATUS_KEY              = 'backfill-state/status.json';   // 对外人话版 status
const MAX_RANGE_HOURS         = 24 * 7;                    // 最大 7 天回放窗口
const WALL_TIME_BUDGET_MS     = 55_000;                    // 单次 cron 不超过 55s（cron 周期 60s）
const CLEANUP_GRACE_MS        = 24 * 60 * 60_000;          // delivery_completed 后等 24h 才删 artifact
const MAX_RATE                = 100;                       // BACKFILL_RATE 上限（files/cron-min）
const DEFAULT_RATE            = 5;                         // BACKFILL_RATE 默认值（保守）
const LIST_LIMIT              = 1000;                      // R2 list 单页 size
const MAX_DAY_PREFIXES        = 10;                        // 防止跨日 prefix 失控
const DEFAULT_SEND_TIMEOUT_MS = 300_000;                   // 5 min — 故意宽松，最小化 abort 引起的翻倍
const MIN_SEND_TIMEOUT_MS     = 1_000;
const DEFAULT_BATCH_SIZE      = 1000;                      // 每次 POST 1000 行
const LOG_LEVELS             = Object.freeze({ debug: 0, info: 1, warn: 2, error: 3 });

// ─── Sender 限速：单 invocation 至少 200ms（设计来防止 ACK 极快时超过 receiver cap）
//
// 理论 ceiling = max_concurrency × max_batch_size × (1000 / 200) × BATCH_SIZE
//              = 20 × 1 × 5 × 1000 = 100,000 lines/s
//
// 注意：基于 vivo asia-main-appstore 实测，客户接收端 ACK 平均 ~400ms，floor 在
// ACK>200ms 时不起作用。Sender 单 invocation 实际 ~0.55s，理论吞吐 ~36 batch/s，
// 但实测只跑 ~3 batch/s（Sender 大部分时间 idle，等 Parser 喂数据）。
// 要提高总吞吐，调 PARSER max_concurrency（不是 Sender），详见顶部 banner。
const MIN_SENDER_INVOCATION_MS = 200;

// ─── Worker 入口 ────────────────────────────────────────────────────────────
// queue:     被 parse-queue-backfill / send-queue-backfill 触发，按 queue 名分发
// scheduled: cron 每分钟一次，runBackfillScan 负责 enqueue + cleanup
// fetch:     HTTP 入口，主要处理 GET /backfill/status
export default {
  async queue(batch, env) {
    if (batch.queue === env.PARSE_QUEUE_NAME) await handleParseQueue(batch, env);
    else if (batch.queue === env.SEND_QUEUE_NAME) await handleSendQueue(batch, env);
    else log(env, 'warn', `Unknown queue: ${batch.queue}`);
  },

  async scheduled(event, env, ctx) {
    // ctx.waitUntil 让 cron 在 background 跑，不阻塞 scheduled handler 返回
    ctx.waitUntil(runBackfillScan(env));
  },

  async fetch(request, env) {
    return handleFetch(request, env);
  },
};

// ─── Parser ─────────────────────────────────────────────────────────────────
// 消费 parse-queue-backfill：每 message 是一个 R2 raw .gz 文件 key。
// 流式解压 ndjson → 逐条按时间窗口和 ParentRayID 过滤 → transformEdge 转格式 →
// 每 BATCH_SIZE (1000) 行写一个 batch 文件到 processed-backfill/<run-id>/ 并入
// send-queue-backfill。
//
// max_batch_size = 1（wrangler-backfill.toml 配置）：单 invocation 处理一个文件，
// 避免大 raw .gz 文件之间共享 CPU 预算。
//
// 实测单文件 wall time（vivo asia-main-appstore 实测，100K 行 / 26MB gz / 50.9% 命中率）：
//   - JSON.parse + filter + transform：~3 s CPU
//   - 50 × writeBatchAndEnqueue × 5 串行 R2 ops，平均每 op ~588 ms：~147 s I/O
//   - 总 ~150 s/file → 0.4 file/min 单 consumer → 4 file/min 总（max_concurrency=10）
//
// I/O 慢的根因不是单个 R2 op 本身慢，而是 long-running invocation 内
// 大量串行 R2 调用的累积开销。详见顶部 banner 的 "Real-world bottleneck"。
// 这是整个 pipeline 的实际瓶颈（Parser 93% utilization vs Sender 8%）。
async function handleParseQueue(batch, env) {
  // max_batch_size=1，所以 messages 数组实际只有 1 条；用 allSettled 是防御性的
  await Promise.allSettled(batch.messages.map((msg) => processFile(msg, env)));
}

// 单个 raw 文件处理：
//   1. 验证 run 元数据 + 跳过已 cleaned 的 run（防止 late retry 重新处理）
//   2. R2.get 流式拉取 .gz
//   3. streamParseNdjsonGzip 逐行回调（边解压边解析，避免 OOM）
//   4. 三层过滤：timestamp 窗口 → ParentRayID/WorkerSubrequest → transform 异常
//   5. 累计到 BATCH_SIZE 行就 flush 一个 batch 到 R2 + send-queue
//   6. 文件末尾把不满 BATCH_SIZE 的 tail batch 也 flush（这就是为什么
//      一个 raw 文件可能有 1 个 underfilled batch）
async function processFile(msg, env) {
  const key = msg.body?.object?.key;
  if (!key) {
    log(env, 'warn', `No object.key: ${JSON.stringify(msg.body)}`);
    msg.ack();
    return;
  }

  const run = await resolveRunContext(msg.body?.run, env);
  if (!run.valid) {
    log(env, 'error', `Invalid backfill run for ${key}: ${run.error}`);
    msg.retry();
    return;
  }
  if (await isRunCleaned(env, run.id)) {
    log(env, 'info', `Run already cleaned (skip late parse duplicate): ${key}`);
    msg.ack();
    return;
  }

  log(env, 'info', `Parsing: ${key}`);
  try {
    const object = await env.RAW_BUCKET.get(key);
    if (!object) {
      log(env, 'warn', `Not in R2: ${key}`);
      msg.ack();
      return;
    }

    const batchSize = parsePositiveInt(env.BATCH_SIZE, DEFAULT_BATCH_SIZE, 1);
    let lines = [];
    let batchIdx = 0;
    let lineCount = 0;
    let errCount = 0;
    let skipped = 0;
    let invalidTs = 0;
    let skippedWorker = 0;
    let kept = 0;

    await streamParseNdjsonGzip(object.body, async (record) => {
      lineCount++;
      const recordMs = parseTimestamp(record.EdgeStartTimestamp);
      if (recordMs === null) {
        skipped++;
        invalidTs++;
        return;
      }
      if (!isRecordInRunWindow(recordMs, run)) {
        skipped++;
        return;
      }
      if (!isTopLevelParentRequest(record)) {
        skipped++;
        skippedWorker++;
        return;
      }
      try {
        lines.push(transformEdge(record));
        kept++;
      } catch (error) {
        errCount++;
        log(env, 'warn', `Transform err line ${lineCount}: ${error.message}`);
        return;
      }
      if (lines.length >= batchSize) {
        await writeBatchAndEnqueue(lines, key, batchIdx++, env, run);
        lines = [];
      }
    });

    if (lines.length > 0) await writeBatchAndEnqueue(lines, key, batchIdx++, env, run);
    log(env, 'info', `Done: ${key} | lines=${lineCount} kept=${kept} batches=${batchIdx} errors=${errCount} skipped=${skipped} invalid_ts=${invalidTs} skipped_worker=${skippedWorker} run=${run.id}`);
    msg.ack();
  } catch (error) {
    log(env, 'error', `Failed: ${key}: ${error.message}`);
    msg.retry();
  }
}

// 写一个 batch 到 R2 并入 send-queue。
//
// 幂等保护（关键）：
//   - head(.done)   命中：上次 sender 已发送，跳过避免重复发到客户
//   - head(.queued) 命中：上次 parser retry 已入过队，跳过避免重复入队
//
// 失败回滚（关键）：
//   - 任何一步失败（put batch/put queued/send queue）都 delete batch+queued
//   - 不能留孤立 batchKey 或 .queued（否则 cleanup 阶段会误判）
//
// 5 个串行 await 是 Parser 单文件 wall time 的主导来源（实测占 >95%：
// 50 batch × 5 ops × ~588 ms/op ≈ 147 s I/O，CPU 仅 ~3 s）。
// 历史上讨论过用 Promise.all 并行化，但因 race condition 风险（一边 put
// 一边 catch+delete 可能产生孤立资源 → 数据丢失）暂未实施。
async function writeBatchAndEnqueue(lines, sourceKey, index, env, run) {
  const batchKey = buildBatchKey(sourceKey, index, run.id, lines.length);
  const queuedMarkerKey = `${batchKey}.queued`;

  // 幂等检查 1：本 batch 是否已被 sender 成功发送
  const doneMarker = await env.RAW_BUCKET.head(`${batchKey}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'debug', `Batch already sent (skip): ${batchKey}`);
    return;
  }

  // 幂等检查 2：本 batch 是否已被 parser 入过 send-queue（防 parser retry 翻倍）
  const queuedMarker = await env.RAW_BUCKET.head(queuedMarkerKey).catch(() => null);
  if (queuedMarker) {
    log(env, 'debug', `Batch already queued (skip duplicate enqueue): ${batchKey}`);
    return;
  }

  const body = `${lines.join('\n')}\n`;
  try {
    // Step 1：写 batch 内容（sender 后续 R2.get 用）
    await env.RAW_BUCKET.put(batchKey, body, {
      httpMetadata: { contentType: 'text/plain; charset=utf-8' },
    });
    // Step 2：写 .queued 标记（parser retry 时跳过本 batch 用）
    await env.RAW_BUCKET.put(queuedMarkerKey, '1', {
      httpMetadata: { contentType: 'text/plain' },
    });
    // Step 3：入 send-queue（sender 异步消费）
    await env.SEND_QUEUE.send({
      key: batchKey,
      queuedAtMs: Date.now(),
      runId: run.id,
      lineCount: lines.length,
    });
  } catch (error) {
    // 任何一步失败都 rollback，避免留孤立资源
    await Promise.allSettled([
      env.RAW_BUCKET.delete(batchKey),
      env.RAW_BUCKET.delete(queuedMarkerKey),
    ]);
    throw error;
  }

  log(env, 'debug', `Queued: ${batchKey} (${lines.length} lines)`);
}

// ─── Sender ─────────────────────────────────────────────────────────────────
// 消费 send-queue-backfill：每 message 是一个待发送的 batch R2 key。
// R2.get → gzip → MD5 签名 POST → 客户 ACK → 写 .done → delete batch + .queued。
//
// max_batch_size = 1, max_concurrency = 20（wrangler-backfill.toml）。
//
// 实测吞吐（vivo asia-main-appstore，27908 batch / ~150 min 跑完）：
//   单 invocation 实际 ~0.55s（含 ACK 平均 ~400ms + R2 ops ~150ms）
//   理论吞吐 = 20 / 0.55 ≈ 36 batch/s
//   但实测仅 ~3 batch/s（utilization 仅 8%）— Sender 大部分时间 idle，等 Parser 喂数据
//
// 真正的瓶颈在 Parser（参见 handleParseQueue / writeBatchAndEnqueue）。
// Sender 已经有大量富余，调 Sender max_concurrency 没有意义；要加速请调 Parser。
// 不要调 MIN_SENDER_INVOCATION_MS 或 max_batch_size。
async function handleSendQueue(batch, env) {
  const invocationStart = Date.now();
  // max_batch_size=1，所以 messages 只有 1 条；用 allSettled 是防御性的
  const results = await Promise.allSettled(batch.messages.map((msg) => sendBatch(msg, env)));

  // 单条失败不影响其他：成功 ack，失败 retry（CF queue at-least-once 语义）
  results.forEach((result, index) => {
    if (result.status === 'fulfilled') batch.messages[index].ack();
    else {
      log(env, 'warn', `Send failed, retry: ${result.reason}`);
      batch.messages[index].retry();
    }
  });

  // 200ms 限速 floor：保护客户接收端在 ACK 极快情况下也不会被 burst 打爆。
  // 注意：客户实测 ACK ~1s，floor 在生产环境实际不起作用（已经 >> 200ms）。
  const elapsed = Date.now() - invocationStart;
  if (elapsed < MIN_SENDER_INVOCATION_MS) {
    await new Promise((resolve) => setTimeout(resolve, MIN_SENDER_INVOCATION_MS - elapsed));
  }
}

// 单 batch 发送热路径。完成顺序对幂等性至关重要：
//   1. head(.done) 早返回 — 防 queue 重复投递时翻倍
//   2. R2.get 取 batch 内容（如果文件已被 cleanup 删了且 run 已 cleaned，静默 ack）
//   3. CompressionStream gzip 边读边压缩（不一次性加载到内存）
//   4. fetch POST + AbortController 5min timeout（默认值刻意宽松，最小化
//      abort 触发概率 → 因为客户接收端不能 dedupe，abort 会引起翻倍）
//   5. resp.body.cancel() 显式释放 — 防 CF "stalled HTTP response" 保护
//   6. 写 .done 标记（3 次重试，关键步骤）
//   7. delete batchKey + .queued（用 catch+warn 不抛异常 — delete 失败不能让
//      已成功的 batch 重发）
//
// 如果 .done 写失败：throw → msg.retry() → 下次 retry 时 head(.done)=null →
// 重新 fetch → 客户可能收到第 2 份（< 0.0001% 概率，已 3 次内部重试覆盖）。
async function sendBatch(msg, env) {
  const { key } = msg.body;
  if (!key) throw new Error(`Invalid message: ${JSON.stringify(msg.body)}`);

  const runId = typeof msg.body?.runId === 'string' && msg.body.runId.trim()
    ? msg.body.runId.trim()
    : extractRunIdFromBatchKey(key);
  const lineCount = parseBatchLineCount(msg.body?.lineCount);
  const queueWaitMs = parseQueueWaitMs(msg.body?.queuedAtMs);
  const queuedMarkerKey = `${key}.queued`;

  const doneMarker = await env.RAW_BUCKET.head(`${key}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'info', `Already sent (skip duplicate): ${key}`);
    return;
  }

  const object = await env.RAW_BUCKET.get(key);
  if (!object) {
    if (runId && await isRunCleaned(env, runId)) {
      log(env, 'info', `Batch already cleaned after completion (skip late duplicate): ${key}`);
      return;
    }
    throw new Error(`Batch missing before successful send: ${key} (no .done marker present)`);
  }

  const uri = env.CTYUN_URI_EDGE;
  const endpoint = env.CTYUN_ENDPOINT;
  const privateKey = env.CTYUN_PRIVATE_KEY;
  if (!endpoint || !privateKey || !uri) {
    throw new Error('Missing CTYUN_ENDPOINT, CTYUN_PRIVATE_KEY or CTYUN_URI_EDGE');
  }

  const compressed = await new Response(
    object.body.pipeThrough(new CompressionStream('gzip'))
  ).arrayBuffer();

  const timeoutMs = parseSendTimeoutMs(env);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const fetchStartedAt = Date.now();
  let resp;

  try {
    resp = await fetch(buildAuthUrl(endpoint, uri, privateKey), {
      method: 'POST',
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Encoding': 'gzip',
      },
      body: compressed,
      signal: controller.signal,
    });
  } catch (error) {
    clearTimeout(timeout);
    if (controller.signal.aborted) {
      throw new Error(`Send timeout after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }

  const ackMs = Date.now() - fetchStartedAt;
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`HTTP ${resp.status} ${resp.statusText} | ${text.substring(0, 200)}`);
  }

  await resp.body?.cancel().catch(() => {});
  log(env, 'info', `Sent batch lines=${lineCount ?? '?'} bytes=${object.size ?? '?'} (uncompressed) → HTTP ${resp.status} | ack_ms=${ackMs}${queueWaitMs === null ? '' : ` queue_wait_ms=${queueWaitMs}`} | ${key}`);

  const wroteDone = await writeDoneMarkerWithRetry(env, key);

  if (wroteDone) {
    await Promise.allSettled([
      env.RAW_BUCKET.delete(key),
      env.RAW_BUCKET.delete(queuedMarkerKey),
    ]).then((results) => {
      if (results[0].status === 'rejected') {
        log(env, 'warn', `Delete failed (will be cleaned by lifecycle): ${key}: ${results[0].reason?.message || results[0].reason}`);
      }
      if (results[1].status === 'rejected') {
        log(env, 'warn', `Queued marker cleanup failed: ${queuedMarkerKey}: ${results[1].reason?.message || results[1].reason}`);
      }
    });
  } else {
    log(env, 'error', `Successful POST but failed to persist .done marker after retries: ${key}`);
    throw new Error(`POST succeeded but .done marker could not be persisted for ${key}`);
  }
}

// ─── Cron / Scheduled Handler ───────────────────────────────────────────────
// 每分钟一次。两个职责：
//   (a) phase=enqueue 阶段：扫 R2 logs/YYYYMMDD/ 把窗口内文件入 parse-queue
//   (b) status=done 阶段：跑 cleanup 状态机（inspect → ready → 24h grace → delete）
//
// 单次 cron 不超过 WALL_TIME_BUDGET_MS = 55s（cron 周期 60s）。
// 跨 cron 用 R2 上的 backfill-state/progress.json 持久化进度，中断不丢数据。
async function runBackfillScan(env) {
  const startedAt = Date.now();
  try {
    const config = parseConfig(env);
    if (!config.valid) {
      log(env, 'error', config.error);
      return;
    }

    if (env.BACKFILL_ENABLED !== 'true') {
      log(env, 'info', 'Paused until BACKFILL_ENABLED=true');
      return;
    }

    const state = await loadState(env, config);
    if (state.status === 'cleaned') {
      log(env, 'info', `Backfill cleaned. run_id=${state.run_id}, completed_at=${state.completed_at}. Change BACKFILL_START_TIME/BACKFILL_END_TIME and redeploy to run a new window.`);
      return;
    }

    let changed = false;
    if (state.phase === 'enqueue' && Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
      changed = (await runEnqueue(env, state, config, startedAt)) || changed;
    }
    if (state.status === 'done' && Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
      changed = (await maybeAutoCleanup(env, state, startedAt)) || changed;
    }

    if (changed) {
      await saveState(env, state).catch((error) => {
        log(env, 'warn', `Failed to save state (will retry next cron): ${error.message}`);
      });
    }

  } catch (error) {
    log(env, 'error', `Backfill cron crashed: ${error.message}\n${error.stack}`);
  }
}

// 校验环境变量并构造 cron config。所有错误都进 log，cron 跳过本次执行。
// 关键约束：END 必须 ≤ 当前时间；窗口跨度 ≤ MAX_RANGE_HOURS (7 天)。
function parseConfig(env) {
  const start = (env.BACKFILL_START_TIME || '').trim();
  const end = (env.BACKFILL_END_TIME || '').trim();
  if (!start || !end) {
    return {
      valid: false,
      error: 'BACKFILL_START_TIME and BACKFILL_END_TIME must be set.',
    };
  }

  const startMs = new Date(start).getTime();
  const endMs = new Date(end).getTime();
  if (isNaN(startMs) || isNaN(endMs)) {
    return { valid: false, error: `Invalid time format. START="${start}" END="${end}".` };
  }
  if (startMs >= endMs) {
    return { valid: false, error: `START (${start}) must be earlier than END (${end}).` };
  }
  if (endMs > Date.now()) {
    return { valid: false, error: `END (${end}) must not be in the future.` };
  }

  const spanHours = (endMs - startMs) / 3600000;
  if (spanHours > MAX_RANGE_HOURS) {
    return {
      valid: false,
      error: `Range span ${spanHours.toFixed(1)}h exceeds max ${MAX_RANGE_HOURS}h.`,
    };
  }

  const rateRaw = parseInt(env.BACKFILL_RATE || String(DEFAULT_RATE), 10);
  const rate = Math.min(MAX_RATE, Math.max(1, isNaN(rateRaw) ? DEFAULT_RATE : rateRaw));

  return {
    valid: true,
    start,
    end,
    startMs,
    endMs,
    rate,
    windowId: buildRunId(startMs, endMs),
    bucketName: env.R2_BUCKET_NAME || 'cdn-logs-raw',
    logPrefix: env.LOG_PREFIX || 'logs/',
  };
}

// 从 R2 读 progress.json，三种情况：
//   (a) 同一 config (start/end 都未变)：继续这个 run
//   (b) config 变了 + 旧 run 已 cleaned：reinitialize 一个新 run
//   (c) config 变了 + 旧 run 还在跑：抛错（防止用户 race condition 重写 state）
// (c) 的处理方式：要重跑同一时间窗，必须先手动删 progress.json + status.json。
async function loadState(env, config) {
  const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
  if (obj) {
    try {
      const state = JSON.parse(await obj.text());
      if (state.config?.start === config.start && state.config?.end === config.end) {
        if (!state.run_id) state.run_id = buildRunInstanceId(config.startMs, config.endMs);
        state.cleanup = normalizeCleanupState(state.cleanup);
        return state;
      }
      if (!canReinitializeFromState(state)) {
        throw new Error(`Existing backfill run ${state.run_id} is still ${state.status}. Wait until status="cleaned" before changing BACKFILL_START_TIME/BACKFILL_END_TIME.`);
      }
      log(env, 'info', `Config changed (was ${state.config?.start}→${state.config?.end}, now ${config.start}→${config.end}). Re-initializing state.`);
    } catch (error) {
      if (String(error.message || '').includes('Existing backfill run')) throw error;
      log(env, 'warn', `State file corrupted, re-initializing: ${error.message}`);
    }
  }

  return {
    config: { start: config.start, end: config.end, rate: config.rate },
    run_id: buildRunInstanceId(config.startMs, config.endMs),
    phase: 'enqueue',
    status: 'running',
    started_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    enqueue_progress: {},
    enqueued_count: 0,
    last_cron_at: null,
    completed_at: null,
    cleanup: createInitialCleanupState(),
  };
}

async function saveState(env, state) {
  state.updated_at = new Date().toISOString();
  state.last_cron_at = new Date().toISOString();
  await env.RAW_BUCKET.put(STATE_KEY, JSON.stringify(state, null, 2), {
    httpMetadata: { contentType: 'application/json; charset=utf-8' },
  });
}

// Enqueue Phase：扫 R2 logs/YYYYMMDD/ 把窗口内文件入 parse-queue。
//
// 双层时间过滤（保险）：
//   (1) prefix 级别：getR2PrefixesByDay 限定到具体日期前缀，缩小 list 范围
//   (2) 文件名级别：extractFileTimeRange 解析 startMs/endMs，跳过窗口外文件
//   (3) 记录级别：Parser 端再按 EdgeStartTimestamp 精确裁切（不在这里做）
//
// rate-limited：每次 cron 最多入队 BACKFILL_RATE 个文件，防止 parse-queue 瞬时爆量。
// 增量保存：用 prog.start_after 记录扫到哪了，下次 cron 直接续上。
//
// 跳过特殊 key：BATCH_ROOT_PREFIX (本工具产物)、backfill-state/、生产 worker 的
// .recover-done- 标记 / processed/ 前缀（避免误把它们当作 raw 文件处理）。
async function runEnqueue(env, state, config, startedAt) {
  const prefixes = getR2PrefixesByDay(config.startMs, config.endMs, config.logPrefix);
  for (const prefix of prefixes) {
    if (!state.enqueue_progress[prefix]) {
      state.enqueue_progress[prefix] = { start_after: null, done: false, enqueued: 0 };
    }
  }

  let enqueuedThisCron = 0;
  for (const prefix of prefixes) {
    if (enqueuedThisCron >= config.rate) break;
    if (Date.now() - startedAt >= WALL_TIME_BUDGET_MS) break;

    const prog = state.enqueue_progress[prefix];
    if (prog.done) continue;

    while (enqueuedThisCron < config.rate && Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
      const list = await env.RAW_BUCKET.list({
        prefix,
        startAfter: prog.start_after || undefined,
        limit: LIST_LIMIT,
      });

      if (list.objects.length === 0) {
        prog.done = true;
        break;
      }

      let pageExhausted = true;
      for (const obj of list.objects) {
        if (enqueuedThisCron >= config.rate) {
          pageExhausted = false;
          break;
        }

        const key = obj.key;
        prog.start_after = key;
        if (key.startsWith(BATCH_ROOT_PREFIX) || key.startsWith('backfill-state/') || key.startsWith('.recover-done-') || key.startsWith('processed/')) {
          continue;
        }

        const range = extractFileTimeRange(key);
        if (!range) continue;
        if (range.startMs > config.endMs) {
          prog.done = true;
          break;
        }
        if (range.endMs < config.startMs) continue;

        try {
          await env.PARSE_QUEUE.send(buildParseQueueMessage(key, config, state.run_id));
          enqueuedThisCron++;
          prog.enqueued++;
        } catch (error) {
          log(env, 'error', `Enqueue failed for ${key}: ${error.message}. Cron will retry next minute.`);
          throw error;
        }
      }

      if (!pageExhausted) break;
      if (prog.done) break;
      if (isR2ListComplete(list)) {
        prog.done = true;
        break;
      }
    }
  }

  state.enqueued_count += enqueuedThisCron;
  const allDone = prefixes.every((prefix) => state.enqueue_progress[prefix].done);
  if (allDone) {
    state.phase = 'done';
    state.status = 'done';
    state.completed_at = new Date().toISOString();
  }

  await saveState(env, state).catch((error) => {
    log(env, 'warn', `Incremental state save failed inside enqueue loop: ${error.message}`);
  });

  log(env, 'info', `[Enqueue] +${enqueuedThisCron} files this cron. total=${state.enqueued_count}`);
  return true;
}

// ─── Cleanup State Machine ──────────────────────────────────────────────────
// 在 enqueue 完成后，按以下状态机清理 processed-backfill/<run-id>/ 临时文件：
//
//   pending  → inspectRunArtifacts: 列所有 artifact 并验证 .done 都存在
//   ready    → 24h grace period 等待运维方检查
//   deleting → deleteRunArtifacts: 批量删除所有 artifact
//   done     → 标记 state.status='cleaned'，run 彻底完成
//
// 任何阶段都遵守 WALL_TIME_BUDGET_MS，超时则把进度持久化等下次 cron 续。
async function maybeAutoCleanup(env, state, startedAt) {
  state.cleanup = normalizeCleanupState(state.cleanup);

  // done → 把外层 state.status 也升级为 cleaned（双向同步）
  if (state.cleanup.status === 'done') {
    if (state.status !== 'cleaned') {
      state.status = 'cleaned';
      return true;
    }
    return false;
  }

  // ready → 等 24h grace 后才进入 deleting
  if (state.cleanup.status === 'ready') {
    const readyAt = Date.parse(state.cleanup.ready_at || '');
    if (Number.isFinite(readyAt) && Date.now() - readyAt < CLEANUP_GRACE_MS) return false;
    state.cleanup.status = 'deleting';
    state.cleanup.delete_start_after = null;
    return true;
  }

  if (state.cleanup.status === 'deleting') {
    return deleteRunArtifacts(env, state, startedAt);
  }

  // 默认 (pending) → 走 inspect
  return inspectRunArtifacts(env, state, startedAt);
}

// inspect 阶段：list processed-backfill/<run-id>/ 所有对象，验证每个 .txt 都
// 有对应 .done 标记。如果发现还有 pending 的 batch（.txt 但没 .done），重置
// cleanup state 让 sender 继续发送。增量保存 inspect_start_after 支持续跑。
async function inspectRunArtifacts(env, state, startedAt) {
  const prefix = getBatchPrefix(state.run_id);
  let changed = false;
  let startAfter = state.cleanup.inspect_start_after || undefined;

  while (Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
    const page = await env.RAW_BUCKET.list({ prefix, startAfter, limit: LIST_LIMIT });
    if (page.objects.length === 0) {
      if (state.cleanup.status !== 'ready') {
        state.cleanup.status = 'ready';
        state.cleanup.ready_at = state.cleanup.ready_at || new Date().toISOString();
        state.cleanup.inspect_start_after = null;
        changed = true;
      }
      return changed;
    }

    for (const obj of page.objects) {
      if (await isPendingCleanupArtifact(env, obj.key)) {
        const hadCleanupState = state.cleanup.status !== 'pending' || state.cleanup.ready_at !== null || state.cleanup.inspect_start_after !== null;
        state.cleanup = createInitialCleanupState();
        return hadCleanupState;
      }
    }

    state.cleanup.inspect_start_after = page.objects[page.objects.length - 1].key;
    changed = true;
    if (isR2ListComplete(page)) {
      state.cleanup.status = 'ready';
      state.cleanup.ready_at = state.cleanup.ready_at || new Date().toISOString();
      state.cleanup.inspect_start_after = null;
      return true;
    }

    startAfter = state.cleanup.inspect_start_after || undefined;
  }

  return changed;
}

// delete 阶段：批量删除 processed-backfill/<run-id>/ 下所有对象。
// R2 batch delete API 一次最多 1000 个 key（LIST_LIMIT）。整批失败时降级到逐个删除。
async function deleteRunArtifacts(env, state, startedAt) {
  const prefix = getBatchPrefix(state.run_id);
  let changed = false;
  let startAfter = state.cleanup.delete_start_after || undefined;

  while (Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
    const page = await env.RAW_BUCKET.list({ prefix, startAfter, limit: LIST_LIMIT });
    if (page.objects.length === 0) {
      Object.assign(state, compactStateAfterCleanup(state));
      return true;
    }

    const keys = page.objects.map((obj) => obj.key);
    await deleteR2Keys(env, keys);
    state.cleanup.deleted_objects += keys.length;
    state.cleanup.delete_start_after = page.objects[page.objects.length - 1].key;
    changed = true;
    if (isR2ListComplete(page)) {
      Object.assign(state, compactStateAfterCleanup(state));
      return true;
    }

    startAfter = state.cleanup.delete_start_after || undefined;
  }

  return changed;
}

// 优先用 batch delete (一次 API call 删多个)，失败则降级为单个并行 delete。
// 单个 delete 的失败用 allSettled 吞掉（cleanup 阶段允许部分失败，下次 cron 继续）。
async function deleteR2Keys(env, keys) {
  if (keys.length === 0) return;
  try {
    await env.RAW_BUCKET.delete(keys);
  } catch {
    await Promise.allSettled(keys.map((key) => env.RAW_BUCKET.delete(key)));
  }
}

// ─── HTTP Endpoints ─────────────────────────────────────────────────────────
// GET  /                       — banner 文本，纯 placeholder
// GET  /backfill/status        — 人话版 status JSON（buildPublicStatusResponse）
// GET  /backfill/status?view=raw — 原始 progress.json + artifact 统计
//
// /backfill/status 同时把 payload 写到 R2 backfill-state/status.json，方便在
// R2 UI 直接看（不需要每次 curl）。这个写入是只在 GET /backfill/status 触发，
// 不是高频热路径。
async function handleFetch(request, env) {
  const url = new URL(request.url);

  if (url.pathname === '/backfill/status') {
    const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
    if (!obj) {
      const notStarted = {
        status: 'not_started',
        message: 'No backfill state found yet. Ensure BACKFILL_START_TIME and BACKFILL_END_TIME are set and wait for the next cron trigger.',
        config_hint: {
          BACKFILL_START_TIME: env.BACKFILL_START_TIME || '(unset)',
          BACKFILL_END_TIME: env.BACKFILL_END_TIME || '(unset)',
          BACKFILL_RATE: env.BACKFILL_RATE || String(DEFAULT_RATE),
          BACKFILL_ENABLED: env.BACKFILL_ENABLED || '(default: false)',
        },
      };
      await env.RAW_BUCKET.put(STATUS_KEY, JSON.stringify(notStarted, null, 2), {
        httpMetadata: { contentType: 'application/json; charset=utf-8' },
      }).catch(() => {});
      return jsonResponse(notStarted);
    }

    try {
      const data = JSON.parse(await obj.text());
      const artifactStats = await collectRunArtifactStats(env, data.run_id, parsePositiveInt(env.BATCH_SIZE, DEFAULT_BATCH_SIZE, 1));
      if (url.searchParams.get('view') === 'raw') {
        return jsonResponse({ ...data, artifact_stats: artifactStats });
      }
      const publicStatus = buildPublicStatusResponse(data, artifactStats, env);
      await env.RAW_BUCKET.put(STATUS_KEY, JSON.stringify(publicStatus, null, 2), {
        httpMetadata: { contentType: 'application/json; charset=utf-8' },
      }).catch(() => {});
      return jsonResponse(publicStatus);
    } catch (error) {
      return jsonResponse({ status: 'error', message: `State file corrupted: ${error.message}` }, 500);
    }
  }

  if (url.pathname === '/' || url.pathname === '') {
    return new Response('ctyun-logpush-backfill worker\nGET /backfill/status — view backfill progress\n', {
      status: 200,
      headers: { 'content-type': 'text/plain; charset=utf-8' },
    });
  }

  return new Response('Not Found', { status: 404 });
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
}

// ─── 流式解析: gzip + ndjson → 逐行 JSON 回调 ──────────────────────────────
// 关键点：
//   - DecompressionStream 边读边解压（不一次性加载到内存，128MB 安全）
//   - TextDecoder { stream: true } 处理跨 chunk 的 UTF-8 字节边界
//   - buffer.split('\n') + buffer = lines.pop() ?? '' 是经典的"保留最后一行"
//     语义（最后一行可能不完整，留给下个 chunk 拼接）
async function streamParseNdjsonGzip(inputStream, onRecord) {
  const reader = inputStream.pipeThrough(new DecompressionStream('gzip')).getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        const last = buffer.trim();
        if (last) await tryParse(last, onRecord);
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed) await tryParse(trimmed, onRecord);
      }
    }
  } finally {
    reader.releaseLock();
  }
}

async function tryParse(line, onRecord) {
  try {
    await onRecord(JSON.parse(line));
  } catch {
    console.warn(`[WARN] JSON parse failed: ${line.substring(0, 100)}`);
  }
}

// ─── 格式转换: CF http_requests → CDN partner format v3.0（145 字段, SOH 分隔）─
// 字段顺序严格按 partner spec 排列。空值/缺失统一输出 '-'，避免空字段被误解析。
// 长字段（URL/UA/Referer）按 partner 限制截断。
//
// sf = "string field"：null/empty → '-'；其余按 maxLen 截断
function sf(val, maxLen) {
  if (val == null || val === '') return '-';
  const s = String(val);
  return maxLen && s.length > maxLen ? s.substring(0, maxLen) : s;
}

// 单条记录转换：返回 SOH 分隔的字符串。字段顺序就是 partner spec 字段索引。
// DASHES_N 是预冻结的纯 '-' 数组（性能：避免每条记录都 new Array+fill）。
function transformEdge(r) {
  return [
    VERSION_EDGE,
    fmtTimeLocal(r.EdgeStartTimestamp),
    sf(r.RayID),
    sf(r.EdgeResponseStatus),
    fmtMsec(r.EdgeStartTimestamp),
    fmtSec(r.EdgeTimeToFirstByteMs),
    fmtSec(r.OriginResponseHeaderReceiveDurationMs),
    fmtSec(r.OriginRequestHeaderSendDurationMs),
    fmtSec(r.EdgeTimeToFirstByteMs),
    '-',
    sf(r.EdgeServerIP),
    schemeToPort(r.ClientRequestScheme),
    sf(r.ClientIP),
    sf(r.ClientSrcPort),
    sf(r.ClientRequestMethod),
    sf(r.ClientRequestScheme),
    sf(r.ClientRequestHost),
    sf(buildFullUrl(r), MAX_URL_LEN),
    sf(r.ClientRequestProtocol),
    sf(r.ClientRequestBytes),
    responseContentLength(r),
    sf(r.EdgeResponseBytes),
    sf(r.EdgeResponseBodyBytes),
    sf(r.OriginIP),
    sf(r.OriginResponseStatus),
    fmtSec(r.OriginResponseDurationMs),
    mapCache(r.CacheCacheStatus),
    mapCache(r.CacheCacheStatus),
    sf(r.OriginIP),
    sf(r.OriginResponseStatus),
    '-',
    '-',
    sf(r.EdgeResponseContentType),
    sf(r.ClientRequestReferer, MAX_REF_LEN),
    sf(r.ClientRequestUserAgent, MAX_UA_LEN),
    sf(r.ClientIP),
    '-',
    '-',
    '-',
    sf(r.ClientIP),
    '-',
    mapDysta(r.CacheCacheStatus),
    '-',
    fmtSec(r.OriginTLSHandshakeDurationMs),
    coloToCountry(r.EdgeColoCode, r.ClientCountry),
    ...DASHES_9,
    fmtTimeLocalSimple(r.EdgeStartTimestamp),
    '-',
    '-',
    '-',
    '-',
    sf(r.ClientRequestHost),
    '-',
    sf(r.ClientSSLProtocol),
    ...DASHES_2,
    ...DASHES_16,
    ...DASHES_15,
    ...DASHES_50,
  ].join(SEP);
}

function responseContentLength(r) {
  if (r.ResponseHeaders && r.ResponseHeaders['content-length']) {
    return sf(r.ResponseHeaders['content-length']);
  }
  return '-';
}

// CF CacheCacheStatus → partner HIT/MISS 二元映射（partner 不区分细分状态）
function mapCache(s) {
  if (!s) return '-';
  const l = s.toLowerCase();
  if (['hit','stale','revalidated','updating'].includes(l)) return 'HIT';
  if (['miss','expired','bypass','dynamic','none'].includes(l)) return 'MISS';
  return '-';
}

// CF CacheCacheStatus → partner static/dynamic 标记（用于动静分离统计）
function mapDysta(s) {
  if (!s) return '-';
  const l = s.toLowerCase();
  return l === 'hit' ? 'static' : l === 'dynamic' ? 'dynamic' : '-';
}

// ─── 鉴权: auth_key={ts}-{rand}-md5({uri}-{ts}-{rand}-{key}) ────────────────
// 协议要求：ts 为未来时间（now + 300s 留 buffer 容忍时钟偏差），rand 为随机数。
// 接收端用同样的 privateKey 重新计算 md5 验证签名。
function buildAuthUrl(endpoint, uri, privateKey) {
  const ts = Math.floor(Date.now() / 1000) + 300;
  const rand = Math.floor(Math.random() * 99999);
  return `${endpoint}${uri}?auth_key=${ts}-${rand}-${md5(`${uri}-${ts}-${rand}-${privateKey}`)}`;
}

// ─── Time / Format helpers ─────────────────────────────────────────────────

// Logpush EdgeStartTimestamp 可能是数字（秒/毫秒）或字符串（数字/ISO 日期），
// 这里统一返回 ms epoch（>1e12 视为已是 ms，否则视为秒 × 1000）
function parseTimestamp(ts) {
  if (ts == null) return null;
  if (typeof ts === 'number') return ts > 1e12 ? ts : ts * 1000;
  if (typeof ts === 'string') {
    const n = Number(ts);
    if (!isNaN(n) && n > 0) return n > 1e12 ? n : n * 1000;
    const d = new Date(ts).getTime();
    return isNaN(d) ? null : d;
  }
  return null;
}

// Apache log format 时间："[DD/Mon/YYYY:HH:MM:SS +0800]"（partner 强制北京时间）
function fmtTimeLocal(ts) {
  const ms = parseTimestamp(ts);
  if (ms == null) return '-';
  const d = new Date(ms + 8 * 3600 * 1000);
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const mo = MONTH_ABBR[d.getUTCMonth()];
  const yy = d.getUTCFullYear();
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mi = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `[${dd}/${mo}/${yy}:${hh}:${mi}:${ss} +0800]`;
}

function fmtTimeLocalSimple(ts) {
  const s = fmtTimeLocal(ts);
  return s === '-' ? '-' : s.slice(1, -1);
}

function fmtMsec(ts) {
  const ms = parseTimestamp(ts);
  if (ms == null) return '-';
  return `${Math.floor(ms / 1000)}.${String(Math.floor(ms) % 1000).padStart(3, '0')}`;
}

function fmtSec(ms) {
  if (ms == null) return '-';
  return (ms / 1000).toFixed(3);
}

function buildFullUrl(r) {
  return `${r.ClientRequestScheme || 'http'}://${r.ClientRequestHost || ''}${r.ClientRequestURI || '/'}`;
}

function schemeToPort(scheme) {
  if (!scheme) return '-';
  return scheme.toLowerCase() === 'https' ? '443' : '80';
}

// ─── 时间范围提取（用于 enqueue 阶段过滤窗口外文件）────────────────────────
// Logpush R2 文件名两种格式：
//   双时间戳（默认）: 20260422T140000Z_20260422T140500Z_abc.log.gz
//   单时间戳（fallback）: 20260422T140000Z_abc.log.gz
//
// runEnqueue 用此函数判断"文件时间范围与 [A,B] 窗口是否有交集"：
//   - file.startMs > B → 跳过（已过窗口；list 顺序保证后面也都过窗口，可以 break）
//   - file.endMs < A   → 跳过（在窗口前）
//   - 其余 → 加入 parse-queue（Parser 端再按记录 EdgeStartTimestamp 精确裁切）
function extractFileTimeRange(key) {
  const m = key.match(/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z[_-](\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/);
  if (m) {
    const s = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
    const e = Date.UTC(+m[7], +m[8] - 1, +m[9], +m[10], +m[11], +m[12]);
    if (!isNaN(s) && !isNaN(e)) return { startMs: s, endMs: e };
  }
  const m2 = key.match(/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/);
  if (m2) {
    const t = Date.UTC(+m2[1], +m2[2] - 1, +m2[3], +m2[4], +m2[5], +m2[6]);
    if (!isNaN(t)) return { startMs: t, endMs: t };
  }
  return null;
}

// 记录级（Parser 端）的窗口判断：用 EdgeStartTimestamp 精确裁切。
function isRecordInRunWindow(recordMs, run) {
  return recordMs >= run.startMs && recordMs <= run.endMs;
}

// 解析 parse-queue 消息中的 run 元数据。如果消息里没带或字段缺失，回退到读
// R2 progress.json 取当前 run_id（兼容旧版本入队的 message 缺字段的情况）。
async function resolveRunContext(run, env) {
  const normalized = normalizeRunContext(run);
  if (normalized.valid) return normalized;

  const config = parseConfig(env);
  if (!config.valid) {
    return { valid: false, error: normalized.error || config.error };
  }
  const stateRunId = await loadExistingStateRunId(env, config);
  if (!stateRunId) {
    return { valid: false, error: normalized.error || 'Missing run metadata and no active state.run_id to fall back to.' };
  }

  return {
    valid: true,
    id: stateRunId,
    start: config.start,
    end: config.end,
    startMs: config.startMs,
    endMs: config.endMs,
  };
}

function normalizeRunContext(run) {
  if (!run || typeof run !== 'object') {
    return { valid: false, error: 'Missing run metadata in queue message.' };
  }

  const id = typeof run.id === 'string' ? run.id.trim() : '';
  const startMs = Number(run.startMs);
  const endMs = Number(run.endMs);
  if (!id || !Number.isFinite(startMs) || !Number.isFinite(endMs) || startMs >= endMs) {
    return { valid: false, error: `Malformed run metadata: ${JSON.stringify(run)}` };
  }

  return {
    valid: true,
    id,
    start: typeof run.start === 'string' ? run.start : new Date(startMs).toISOString(),
    end: typeof run.end === 'string' ? run.end : new Date(endMs).toISOString(),
    startMs,
    endMs,
  };
}

// ─── Run ID helpers ─────────────────────────────────────────────────────────
// run_id 格式：{startUTC}_{endUTC}_{createdAtUTC}，例如：
//   20260422T070500Z_20260422T084000Z_20260425T050927Z
// 同一时间窗口的不同 run（删了 state 重跑）会有不同的 createdAt 后缀，所以
// 旧 run 的 processed-backfill/<run-id>/ 不会与新 run 冲突。
function buildRunId(startMs, endMs) {
  return `${fmtCompactUtc(startMs)}_${fmtCompactUtc(endMs)}`;
}

function buildRunInstanceId(startMs, endMs, createdAtMs = Date.now()) {
  return `${buildRunId(startMs, endMs)}_${fmtCompactUtc(createdAtMs)}`;
}

function fmtCompactUtc(ms) {
  const d = new Date(ms);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mi = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `${yyyy}${mm}${dd}T${hh}${mi}${ss}Z`;
}

// batchKey 格式：processed-backfill/<run-id>/<safeTail>-<md5>-<idx>-n<lineCount>.txt
//   - safeTail：source 文件名安全化（只留 [a-zA-Z0-9_-]，截断 80 字符）
//   - md5(sourceKey)：避免不同源文件 safeTail 撞名
//   - idx：同一 sourceKey 内的 batch 序号
//   - n<lineCount>：batch 实际行数（用于 collectRunArtifactStats 统计）
function buildBatchKey(sourceKey, index, runId, lineCount = 0) {
  const tail = sourceKey.split('/').pop() || 'raw';
  const safeTail = tail.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 80);
  return `${getBatchPrefix(runId)}${safeTail}-${md5(sourceKey)}-${index}-n${lineCount}.txt`;
}

function getBatchPrefix(runId) {
  return `${BATCH_ROOT_PREFIX}${runId}/`;
}

function buildParseQueueMessage(key, config, runId) {
  return {
    bucket: config.bucketName,
    object: { key },
    run: {
      id: runId,
      start: config.start,
      end: config.end,
      startMs: config.startMs,
      endMs: config.endMs,
    },
  };
}

function extractRunIdFromBatchKey(key) {
  if (typeof key !== 'string' || !key.startsWith(BATCH_ROOT_PREFIX)) return null;
  const suffix = key.slice(BATCH_ROOT_PREFIX.length);
  const slash = suffix.indexOf('/');
  return slash === -1 ? null : suffix.slice(0, slash) || null;
}

function parseSendTimeoutMs(env) {
  return parsePositiveInt(env.SEND_TIMEOUT_MS, DEFAULT_SEND_TIMEOUT_MS, MIN_SEND_TIMEOUT_MS);
}

function parseBatchLineCount(raw) {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return Math.floor(parsed);
}

function extractBatchLineCountFromKey(key) {
  const match = String(key || '').match(/-n(\d+)\.txt(?:\.done|\.queued)?$/);
  if (!match) return null;
  const parsed = Number(match[1]);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeBatchArtifactKey(key) {
  if (typeof key !== 'string') return null;
  if (key.endsWith('.done')) return key.slice(0, -'.done'.length);
  if (key.endsWith('.queued')) return key.slice(0, -'.queued'.length);
  if (key.endsWith('.txt')) return key;
  return null;
}

function parsePositiveInt(raw, fallback, min) {
  const parsed = parseInt(raw ?? '', 10);
  if (isNaN(parsed) || parsed < min) return fallback;
  return parsed;
}

// 客户业务约束：只发送顶层请求（不发 Worker 子请求 / fetch subrequest 日志）。
// 两个判据同时成立才保留：
//   - ParentRayID === "00"：表示这是一个父请求（无 parent ray）
//   - WorkerSubrequest !== true：未被显式标记为 subrequest
// 注意 WorkerSubrequest 可能是 boolean 或 string，所以做两次比较。
function isTopLevelParentRequest(record) {
  return String(record?.ParentRayID ?? '') === '00' && record?.WorkerSubrequest !== true && record?.WorkerSubrequest !== 'true';
}

function isR2ListComplete(listResult) {
  return listResult?.truncated !== true;
}

function createInitialCleanupState() {
  return {
    status: 'pending',
    ready_at: null,
    inspect_start_after: null,
    delete_start_after: null,
    deleted_objects: 0,
    cleaned_at: null,
  };
}

function normalizeCleanupState(cleanup) {
  return {
    ...createInitialCleanupState(),
    ...(cleanup && typeof cleanup === 'object' ? cleanup : {}),
  };
}

function isPendingRunArtifact(key) {
  return key.endsWith('.txt') || key.endsWith('.queued');
}

// inspect 阶段判断单个 artifact 是否还 pending（用 head(.done) 验证）：
//   - .queued 文件：对应的 .done 不存在 → pending（sender 还没成功发送）
//   - .txt 文件：对应的 .done 不存在 → pending
//   - 其他扩展名：不是 batch artifact，跳过
// 任何一个 pending 都会让 cleanup 退回 pending 状态等 sender 完成。
async function isPendingCleanupArtifact(env, key) {
  if (key.endsWith('.queued')) {
    const batchKey = key.slice(0, -'.queued'.length);
    const done = await env.RAW_BUCKET.head(`${batchKey}.done`).catch(() => null);
    return !done;
  }
  if (key.endsWith('.txt')) {
    const done = await env.RAW_BUCKET.head(`${key}.done`).catch(() => null);
    return !done;
  }
  return false;
}

function compactStateAfterCleanup(state) {
  const cleanedAt = new Date().toISOString();
  return {
    config: state.config,
    run_id: state.run_id,
    phase: 'done',
    status: 'cleaned',
    started_at: state.started_at,
    completed_at: state.completed_at,
    enqueued_count: state.enqueued_count,
    last_cron_at: state.last_cron_at,
    cleanup: {
      status: 'done',
      ready_at: state.cleanup.ready_at,
      inspect_start_after: null,
      delete_start_after: null,
      deleted_objects: state.cleanup.deleted_objects,
      cleaned_at: cleanedAt,
    },
  };
}

function canReinitializeFromState(state) {
  return state?.status === 'cleaned';
}

async function loadExistingStateRunId(env, config) {
  const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
  if (!obj) return null;
  try {
    const state = JSON.parse(await obj.text());
    if (state.config?.start !== config.start || state.config?.end !== config.end) return null;
    return typeof state.run_id === 'string' && state.run_id.trim() ? state.run_id.trim() : null;
  } catch {
    return null;
  }
}

async function isRunCleaned(env, runId) {
  if (!runId) return false;
  const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
  if (!obj) return false;
  try {
    const state = JSON.parse(await obj.text());
    return state.run_id === runId && state.status === 'cleaned';
  } catch {
    return false;
  }
}

// 按日期生成 R2 prefix（限定 list 扫描范围，避免扫整个 bucket）。
// 例如窗口跨 2 天 → 返回 ["logs/20260422/", "logs/20260423/"]。
// 防爆设计：MAX_DAY_PREFIXES=10 限制最多生成 10 个前缀，超出窗口直接放弃。
function getR2PrefixesByDay(startMs, endMs, basePrefix) {
  const prefixes = [];
  const d = new Date(startMs);
  d.setUTCHours(0, 0, 0, 0);
  const endDay = new Date(endMs);
  endDay.setUTCHours(0, 0, 0, 0);
  let iter = 0;
  while (d.getTime() <= endDay.getTime() && iter++ < MAX_DAY_PREFIXES) {
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    prefixes.push(`${basePrefix}${yyyy}${mm}${dd}/`);
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return prefixes;
}

// 统一日志函数。LOG_LEVEL=info 时丢弃 debug；error/warn 走 console.error/warn 便于
// 在 wrangler tail 里区分级别。前缀 [BACKFILL][LEVEL] 便于 grep。
function log(env, level, msg) {
  if ((LOG_LEVELS[level] ?? 1) >= (LOG_LEVELS[env?.LOG_LEVEL] ?? 1)) {
    const fn = level === 'error' ? console.error : level === 'warn' ? console.warn : console.log;
    fn(`[BACKFILL][${level.toUpperCase()}] ${new Date().toISOString()} ${msg}`);
  }
}

function parseQueueWaitMs(queuedAtMs) {
  const queuedAt = Number(queuedAtMs);
  if (!Number.isFinite(queuedAt) || queuedAt <= 0) return null;
  return Math.max(0, Date.now() - queuedAt);
}

// 扫 processed-backfill/<run-id>/ 下所有 artifact，按 batch 分组（baseKey 是
// .txt 不带后缀），返回总 batch 数 / 已发数（有 .done）/ 待发数 / 行数分布。
//
// 仅在 GET /backfill/status 时调用，不是热路径。大 run（>10K batches）时这个
// 接口可能慢几秒（要 list 全部对象）；不影响 cron 主流程。
async function collectRunArtifactStats(env, runId, batchSize) {
  if (!runId) return createEmptyArtifactStats(batchSize);

  const prefix = getBatchPrefix(runId);
  const batches = new Map();
  let startAfter;

  while (true) {
    const page = await env.RAW_BUCKET.list({ prefix, startAfter, limit: LIST_LIMIT });

    for (const obj of page.objects) {
      const baseKey = normalizeBatchArtifactKey(obj.key);
      if (!baseKey) continue;
      const lineCount = extractBatchLineCountFromKey(baseKey);
      const entry = batches.get(baseKey) || {
        key: baseKey,
        lineCount,
        hasTxt: false,
        hasDone: false,
        hasQueued: false,
      };
      entry.lineCount = entry.lineCount ?? lineCount;
      if (obj.key.endsWith('.done')) entry.hasDone = true;
      else if (obj.key.endsWith('.queued')) entry.hasQueued = true;
      else if (obj.key.endsWith('.txt')) entry.hasTxt = true;
      batches.set(baseKey, entry);
    }

    if (isR2ListComplete(page) || page.objects.length === 0) break;
    startAfter = page.objects[page.objects.length - 1].key;
  }

  let totalBatches = 0;
  let sentBatches = 0;
  let pendingBatches = 0;
  let totalLines = 0;
  let sentLines = 0;
  let pendingLines = 0;
  let minLines = null;
  let maxLines = null;

  for (const entry of batches.values()) {
    totalBatches++;
    const lineCount = parseBatchLineCount(entry.lineCount);
    if (lineCount !== null) {
      totalLines += lineCount;
      minLines = minLines === null ? lineCount : Math.min(minLines, lineCount);
      maxLines = maxLines === null ? lineCount : Math.max(maxLines, lineCount);
    }
    if (entry.hasDone) {
      sentBatches++;
      if (lineCount !== null) sentLines += lineCount;
    } else {
      pendingBatches++;
      if (lineCount !== null) pendingLines += lineCount;
    }
  }

  return {
    total_batches: totalBatches,
    sent_batches: sentBatches,
    pending_batches: pendingBatches,
    total_lines: totalLines,
    sent_lines: sentLines,
    pending_lines: pendingLines,
    min_batch_lines: minLines,
    max_batch_lines: maxLines,
    avg_batch_lines: totalBatches > 0 ? Number((totalLines / totalBatches).toFixed(2)) : null,
    configured_batch_size: batchSize,
  };
}

function createEmptyArtifactStats(batchSize) {
  return {
    total_batches: 0,
    sent_batches: 0,
    pending_batches: 0,
    total_lines: 0,
    sent_lines: 0,
    pending_lines: 0,
    min_batch_lines: null,
    max_batch_lines: null,
    avg_batch_lines: null,
    configured_batch_size: batchSize,
  };
}

function derivePublicStage(state, artifactStats) {
  if (state.status === 'cleaned' || state.cleanup?.status === 'done') return 'cleaned';
  if (artifactStats.pending_batches > 0) return 'sending';
  if (state.status === 'done' && state.cleanup?.status === 'ready') return 'sent_waiting_cleanup';
  if (state.phase === 'enqueue') return 'scanning_and_queueing';
  return 'running';
}

function buildHumanMessage(state, artifactStats, stage) {
  const linesInfo = `${artifactStats.sent_lines}/${artifactStats.total_lines} 条日志`;
  const batchInfo = `${artifactStats.sent_batches}/${artifactStats.total_batches} 个 batch`;
  if (stage === 'cleaned') {
    return `这次补传已经完成并清理完临时文件。共匹配 ${state.enqueued_count ?? 0} 个原始文件，发送 ${batchInfo}，约 ${linesInfo}。`;
  }
  if (stage === 'sent_waiting_cleanup') {
    return `这次补传已经发送完成。共匹配 ${state.enqueued_count ?? 0} 个原始文件，发送 ${batchInfo}，约 ${linesInfo}，当前只是等待自动清理临时文件。`;
  }
  if (stage === 'sending') {
    return `这次补传已经匹配 ${state.enqueued_count ?? 0} 个原始文件，已发送 ${batchInfo}，约 ${linesInfo}，还有 ${artifactStats.pending_batches} 个 batch 待发送。`;
  }
  return `这次补传正在扫描并排队，当前已匹配 ${state.enqueued_count ?? 0} 个原始文件。`;
}

function toBeijingTime(value) {
  if (!value) return null;
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) return null;
  const d = new Date(ms + 8 * 3600 * 1000);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mi = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss} GMT+8`;
}

// 把内部 progress.json 转成对外友好的 status payload。增加了：
//   - summary：一句人话总结
//   - delivery_completed / fully_completed 双口径（前者是 batch 都发完，后者是含
//     cleanup grace 也彻底完成）
//   - 北京时间字段（避免运维方误读 UTC）
//   - explained 字段（每个状态码都附人话解释）
function buildPublicStatusResponse(state, artifactStats, env) {
  const stage = derivePublicStage(state, artifactStats);
  const lastUpdatedAt = state.updated_at || state.last_cron_at || state.completed_at || state.started_at || null;
  const isDeliveryCompleted = artifactStats.pending_batches === 0 && ['done', 'cleaned'].includes(state.status);
  const isFullyCompleted = state.status === 'cleaned' || state.cleanup?.status === 'done';
  return {
    summary: buildHumanMessage(state, artifactStats, stage),
    status_code: state.status,
    status_explained: explainStatus(state.status),
    stage_code: stage,
    stage_explained: explainStage(stage),
    delivery_completed: isDeliveryCompleted,
    delivery_completed_explained: isDeliveryCompleted
      ? '这表示所有待发送 batch 都已经发完，没有 batch pending 了。'
      : '这表示还有 batch 尚未发完，补传还在进行中。',
    fully_completed: isFullyCompleted,
    fully_completed_explained: isFullyCompleted
      ? '这表示补传已经彻底完成，临时文件也已经清理完。'
      : '只有 fully_completed=true 才算彻底补传完成。当前如果只是 delivery_completed=true，说明日志已经发完，但临时文件可能还在等待自动清理。',
    run_id: state.run_id,
    replay_window_beijing: `${toBeijingTime(state.config?.start)} ~ ${toBeijingTime(state.config?.end)}`,
    task_started_beijing: toBeijingTime(state.started_at),
    raw_file_scan_finished_beijing: toBeijingTime(state.completed_at),
    last_refresh_beijing: toBeijingTime(lastUpdatedAt),
    matched_raw_files: state.enqueued_count ?? 0,
    batches_sent: artifactStats.sent_batches,
    batches_pending: artifactStats.pending_batches,
    log_lines_sent: artifactStats.sent_lines,
    batch_size_max_configured: artifactStats.configured_batch_size,
    batch_lines_avg: artifactStats.avg_batch_lines,
    batch_lines_min: artifactStats.min_batch_lines,
    batch_lines_max: artifactStats.max_batch_lines,
    batch_size_note: buildBatchSizeNote(artifactStats),
    cleanup_status: state.cleanup?.status || null,
    cleanup_status_explained: explainCleanupStatus(state.cleanup?.status),
    rerun_same_window_how: '如果同一时间窗需要重新跑，请先删除 R2 里的 backfill-state/progress.json 和 backfill-state/status.json，再保持 BACKFILL_ENABLED=true 重新部署或等待下一次 cron。旧 run 的 processed-backfill/<run-id>/ 不会影响新 run。',
    r2_console_note: 'R2 Dashboard 里 progress.json 或 status.json 的 Date Created 不是这次 run 是否最新的判断依据。请看 task_started_beijing、raw_file_scan_finished_beijing、last_refresh_beijing。',
  };
}

async function writePublicStatusSnapshot(env, state) {
  const artifactStats = await collectRunArtifactStats(env, state.run_id, parsePositiveInt(env.BATCH_SIZE, DEFAULT_BATCH_SIZE, 1));
  const publicStatus = buildPublicStatusResponse(state, artifactStats, env);
  await env.RAW_BUCKET.put(STATUS_KEY, JSON.stringify(publicStatus, null, 2), {
    httpMetadata: { contentType: 'application/json; charset=utf-8' },
  });
}

function explainStatus(status) {
  if (status === 'running') return '还在执行中，尚未全部完成。';
  if (status === 'done') return '日志已经发完了，但临时文件可能还在等待自动清理。';
  if (status === 'cleaned') return '补传彻底完成，临时文件也已经清理完。';
  return '未知状态。';
}

function explainStage(stage) {
  if (stage === 'scanning_and_queueing') return '正在扫描 R2 原始文件，并把命中的文件送进 parse queue。';
  if (stage === 'sending') return '原始文件已经匹配到一部分，当前 send worker 还在继续发送 batch。';
  if (stage === 'sent_waiting_cleanup') return '所有 batch 都已经发完，当前只是等待自动清理临时文件。';
  if (stage === 'cleaned') return '临时文件也已经清理完，这次 backfill 已经彻底结束。';
  return '当前阶段未知。';
}

function explainCleanupStatus(status) {
  if (status === 'pending') return '临时文件还没进入清理等待阶段。';
  if (status === 'ready') return '日志已经发完，当前只是在等待自动清理临时文件。';
  if (status === 'deleting') return '系统正在删除临时文件。';
  if (status === 'done') return '临时文件已经清理完。';
  return '当前没有额外清理信息。';
}

function buildBatchSizeNote(artifactStats) {
  const maxConfigured = artifactStats.configured_batch_size;
  const avg = artifactStats.avg_batch_lines;
  if (!artifactStats.total_batches) {
    return `当前还没有产生 batch。单个 batch 的理论上限是 ${maxConfigured} 条。`;
  }
  if (avg === null) {
    return `单个 batch 的理论上限是 ${maxConfigured} 条。`;
  }
  if (avg >= maxConfigured * 0.8) {
    return `这次 batch 大多接近上限 ${maxConfigured} 条，只有最后的尾 batch 可能更小。`;
  }
  if (avg >= maxConfigured * 0.3) {
    return `这次 batch 不是固定 1000 条。平均 ${avg} 条，说明有不少 batch 在未装满时就结束了。`;
  }
  return `这次 batch 明显不是固定 ${maxConfigured} 条。平均只有 ${avg} 条，说明这个时间窗里的日志比较分散，按文件切出来的 batch 普遍偏小。`;
}

// 写 .done 标记，3 次重试 + 指数退避 (100ms / 200ms / 300ms)。
// .done 是 sender 的核心幂等保证：一旦写入，重试时 head(.done) 命中直接 skip 不重发。
// 3 次都失败则返回 false，调用方会 throw → msg.retry()，下次 retry 时仍 fetch 一次
// （这就是 < 0.0001% 的"残留翻倍"场景）。
async function writeDoneMarkerWithRetry(env, key) {
  const markerKey = `${key}.done`;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await env.RAW_BUCKET.put(markerKey, '1', {
        httpMetadata: { contentType: 'text/plain' },
      });
      return true;
    } catch (error) {
      if (attempt === 3) {
        log(env, 'warn', `Done marker write failed after ${attempt} attempts: ${key}: ${error.message}`);
        return false;
      }
      await new Promise((resolve) => setTimeout(resolve, attempt * 100));
    }
  }
  return false;
}

// ─── Test surface（仅 vitest/test 用，不参与生产逻辑）────────────────────────
// 暴露内部纯函数给 test/index.test.js 做白盒测试，避免污染默认 export。
export const __test = {
  buildHumanMessage,
  buildBatchSizeNote,
  buildBatchKey,
  buildParseQueueMessage,
  buildPublicStatusResponse,
  buildRunId,
  buildRunInstanceId,
  canReinitializeFromState,
  collectRunArtifactStats,
  compactStateAfterCleanup,
  createEmptyArtifactStats,
  createInitialCleanupState,
  derivePublicStage,
  explainCleanupStatus,
  explainStage,
  explainStatus,
  extractBatchLineCountFromKey,
  extractFileTimeRange,
  getBatchPrefix,
  isPendingCleanupArtifact,
  isPendingRunArtifact,
  isRecordInRunWindow,
  isRunCleaned,
  isTopLevelParentRequest,
  normalizeCleanupState,
  normalizeRunContext,
  parseConfig,
  parseQueueWaitMs,
  parseSendTimeoutMs,
  resolveRunContext,
  toBeijingTime,
  writeBatchAndEnqueue,
  writeDoneMarkerWithRetry,
};

// ─── MD5 (RFC 1321) ─────────────────────────────────────────────────────────
// 用于 buildAuthUrl 计算签名（partner 协议规定）。SubtleCrypto 不支持 MD5，
// 所以这里用纯 JS 实现。每个 sendBatch 调用一次（输入很短，<1ms）。
//
// unescape(encodeURIComponent(...)) 是把 UTF-8 字符串转成字节序列的经典写法
// （即使 unescape 已 deprecated，在 V8/Workers 仍可用，未来可换 TextEncoder）。
function md5(str) {
  const add = (x,y)=>{const l=(x&0xffff)+(y&0xffff);return(((x>>16)+(y>>16)+(l>>16))<<16)|(l&0xffff);};
  const rol = (n,c)=>(n<<c)|(n>>>(32-c));
  const cmn = (q,a,b,x,s,t)=>add(rol(add(add(a,q),add(x,t)),s),b);
  const ff = (a,b,c,d,x,s,t)=>cmn((b&c)|(~b&d),a,b,x,s,t);
  const gg = (a,b,c,d,x,s,t)=>cmn((b&d)|(c&~d),a,b,x,s,t);
  const hh = (a,b,c,d,x,s,t)=>cmn(b^c^d,a,b,x,s,t);
  const ii = (a,b,c,d,x,s,t)=>cmn(c^(b|~d),a,b,x,s,t);
  const utf8 = unescape(encodeURIComponent(str));
  const len = utf8.length;
  const nb = ((len + 8) >>> 6) + 1;
  const blk = new Array(nb * 16).fill(0);
  for (let i = 0; i < len; i++) blk[i >> 2] |= utf8.charCodeAt(i) << (i % 4 * 8);
  blk[len >> 2] |= 0x80 << (len % 4 * 8);
  blk[nb * 16 - 2] = len * 8;
  let a=1732584193,b=-271733879,c=-1732584194,d=271733878;
  for (let i = 0; i < blk.length; i += 16) {
    const [pa,pb,pc,pd] = [a,b,c,d];
    a=ff(a,b,c,d,blk[i],7,-680876936);      d=ff(d,a,b,c,blk[i+1],12,-389564586);
    c=ff(c,d,a,b,blk[i+2],17,606105819);    b=ff(b,c,d,a,blk[i+3],22,-1044525330);
    a=ff(a,b,c,d,blk[i+4],7,-176418897);    d=ff(d,a,b,c,blk[i+5],12,1200080426);
    c=ff(c,d,a,b,blk[i+6],17,-1473231341);  b=ff(b,c,d,a,blk[i+7],22,-45705983);
    a=ff(a,b,c,d,blk[i+8],7,1770035416);    d=ff(d,a,b,c,blk[i+9],12,-1958414417);
    c=ff(c,d,a,b,blk[i+10],17,-42063);      b=ff(b,c,d,a,blk[i+11],22,-1990404162);
    a=ff(a,b,c,d,blk[i+12],7,1804603682);   d=ff(d,a,b,c,blk[i+13],12,-40341101);
    c=ff(c,d,a,b,blk[i+14],17,-1502002290); b=ff(b,c,d,a,blk[i+15],22,1236535329);
    a=gg(a,b,c,d,blk[i+1],5,-165796510);    d=gg(d,a,b,c,blk[i+6],9,-1069501632);
    c=gg(c,d,a,b,blk[i+11],14,643717713);   b=gg(b,c,d,a,blk[i],20,-373897302);
    a=gg(a,b,c,d,blk[i+5],5,-701558691);    d=gg(d,a,b,c,blk[i+10],9,38016083);
    c=gg(c,d,a,b,blk[i+15],14,-660478335);  b=gg(b,c,d,a,blk[i+4],20,-405537848);
    a=gg(a,b,c,d,blk[i+9],5,568446438);     d=gg(d,a,b,c,blk[i+14],9,-1019803690);
    c=gg(c,d,a,b,blk[i+3],14,-187363961);   b=gg(b,c,d,a,blk[i+8],20,1163531501);
    a=gg(a,b,c,d,blk[i+13],5,-1444681467);  d=gg(d,a,b,c,blk[i+2],9,-51403784);
    c=gg(c,d,a,b,blk[i+7],14,1735328473);   b=gg(b,c,d,a,blk[i+12],20,-1926607734);
    a=hh(a,b,c,d,blk[i+5],4,-378558);       d=hh(d,a,b,c,blk[i+8],11,-2022574463);
    c=hh(c,d,a,b,blk[i+11],16,1839030562);  b=hh(b,c,d,a,blk[i+14],23,-35309556);
    a=hh(a,b,c,d,blk[i+1],4,-1530992060);   d=hh(d,a,b,c,blk[i+4],11,1272893353);
    c=hh(c,d,a,b,blk[i+7],16,-155497632);   b=hh(b,c,d,a,blk[i+10],23,-1094730640);
    a=hh(a,b,c,d,blk[i+13],4,681279174);    d=hh(d,a,b,c,blk[i],11,-358537222);
    c=hh(c,d,a,b,blk[i+3],16,-722521979);   b=hh(b,c,d,a,blk[i+6],23,76029189);
    a=hh(a,b,c,d,blk[i+9],4,-640364487);    d=hh(d,a,b,c,blk[i+12],11,-421815835);
    c=hh(c,d,a,b,blk[i+15],16,530742520);   b=hh(b,c,d,a,blk[i+2],23,-995338651);
    a=ii(a,b,c,d,blk[i],6,-198630844);      d=ii(d,a,b,c,blk[i+7],10,1126891415);
    c=ii(c,d,a,b,blk[i+14],15,-1416354905); b=ii(b,c,d,a,blk[i+5],21,-57434055);
    a=ii(a,b,c,d,blk[i+12],6,1700485571);   d=ii(d,a,b,c,blk[i+3],10,-1894986606);
    c=ii(c,d,a,b,blk[i+10],15,-1051523);    b=ii(b,c,d,a,blk[i+1],21,-2054922799);
    a=ii(a,b,c,d,blk[i+8],6,1873313359);    d=ii(d,a,b,c,blk[i+15],10,-30611744);
    c=ii(c,d,a,b,blk[i+6],15,-1560198380);  b=ii(b,c,d,a,blk[i+13],21,1309151649);
    a=ii(a,b,c,d,blk[i+4],6,-145523070);    d=ii(d,a,b,c,blk[i+11],10,-1120210379);
    c=ii(c,d,a,b,blk[i+2],15,718787259);    b=ii(b,c,d,a,blk[i+9],21,-343485551);
    a=add(a,pa);b=add(b,pb);c=add(c,pc);d=add(d,pd);
  }
  return [a,b,c,d].map((n)=>[0,1,2,3].map((j)=>
    ((n>>(j*8+4))&0xf).toString(16)+((n>>(j*8))&0xf).toString(16)
  ).join('')).join('');
}
