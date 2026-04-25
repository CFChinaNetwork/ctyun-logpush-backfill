/**
 * Cloudflare Workers — Logpush Historical Backfill Worker
 *
 * 专用于补传指定时间范围 [BACKFILL_START_TIME, BACKFILL_END_TIME] 的 Logpush 日志。
 * 和生产 worker(ctyun-logpush)完全独立：独立 queues、独立 processed-backfill/ 前缀、
 * 独立 Sender 限速（当前 checked-in 配置：max_concurrency=10 × max_batch_size=1
 *   + 代码级 200ms/invocation 节流 → 理论上限 ≤ 50 batch/s = 50,000 lines/s）
 * 共享同一个 R2 bucket：只读 logs/ 前缀，写入独立的 processed-backfill/ 和 backfill-state/。
 *
 * Architecture:
 *   scheduled(每分钟) → 扫 R2 logs/ → rate-limited 入 parse-queue-backfill
 *                           ↓
 *                      Backfill Parser（逐条过滤父请求）
 *                           ↓
 *                      RunAggregator Durable Object（跨 raw 文件聚合，凑满 batch）
 *                           ↓
 *                      processed-backfill/ → send-queue-backfill
 *                           ↓
 *                      Backfill Sender (code-throttled to ≤ 5 invocations/s per consumer)
 *                           ↓
 *                      接收端服务器（与生产同一个 endpoint，共用同样的 CTYUN_* secrets）
 *
 * Env Secrets : CTYUN_ENDPOINT, CTYUN_PRIVATE_KEY, CTYUN_URI_EDGE
 * Env Vars    : BATCH_SIZE, LOG_LEVEL, LOG_PREFIX, PARSE_QUEUE_NAME, SEND_QUEUE_NAME,
 *               R2_BUCKET_NAME, BACKFILL_START_TIME, BACKFILL_END_TIME,
 *               BACKFILL_RATE, BACKFILL_ENABLED
 *
 * HTTP Endpoints:
 *   GET  /backfill/status — 返回面向客户的简明进度视图
 *   GET  /backfill/status?view=raw — 返回原始 backfill-state/progress.json
 */
'use strict';

const DurableObjectBase = globalThis.DurableObject || class {};

// ─── IATA机场三字码 → 国家两字码（复用自生产 index.js，#45 country 字段）───────
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

// ─── 常量 ──────────────────────────────────────────────────────────────────
const SEP = '\u0001';
const MONTH_ABBR = Object.freeze([
  'Jan','Feb','Mar','Apr','May','Jun',
  'Jul','Aug','Sep','Oct','Nov','Dec',
]);
const VERSION_EDGE = 'cf_vod_v3.0';
const DASHES_9  = Object.freeze(Array(9).fill('-'));
const DASHES_4  = Object.freeze(Array(4).fill('-'));
const DASHES_2  = Object.freeze(Array(2).fill('-'));
const DASHES_16 = Object.freeze(Array(16).fill('-'));
const DASHES_15 = Object.freeze(Array(15).fill('-'));
const DASHES_50 = Object.freeze(Array(50).fill('-'));
const MAX_URL_LEN = 4096;
const MAX_UA_LEN  = 1024;
const MAX_REF_LEN = 2048;

// ⭐ Backfill 专用前缀（和生产的 processed/ 完全隔离）
const BATCH_ROOT_PREFIX     = 'processed-backfill/';
const STATE_KEY             = 'backfill-state/progress.json';
const LEGACY_SEND_STATS_KEY = 'backfill-state/send-stats.json';
const MAX_RANGE_HOURS       = 48;
const WALL_TIME_BUDGET_MS   = 55_000;       // 留 5s 缓冲给 saveState
const CLEANUP_GRACE_MS      = 24 * 60 * 60_000;
const FINALIZE_RECOVERY_STALL_MS = 2 * 60_000;
const MAX_RATE              = 100;
const DEFAULT_RATE          = 5;
const LIST_LIMIT            = 1000;
const MAX_DAY_PREFIXES      = 5;
const AGGREGATOR_CHUNK_LINES = 1000;
const DEFAULT_SEND_TIMEOUT_MS = 300_000;
const MIN_SEND_TIMEOUT_MS     = 1_000;
const LOG_LEVELS            = Object.freeze({ debug: 0, info: 1, warn: 2, error: 3 });

// ⭐ Sender 节流：每次 handleSendQueue invocation 至少花费 MIN_SENDER_INVOCATION_MS
// 配合 max_batch_size=1 (每 invocation 1 条 msg)
//   → 每个 sender consumer 最多约 5 msg/s；总 ceiling 还取决于 wrangler 里的 max_concurrency
// 若 sendBatch 实际耗时已超过此值（如接收端慢），不 sleep，以实际耗时为准（更慢）
// 若接收端非常快（<200ms），sleep 补齐到 200ms，确保不会因 endpoint 抖动而突破上限
// 注：sleep 只占 wall time，不占 CPU time，不增加 Worker 计费
const MIN_SENDER_INVOCATION_MS = 200;

// ─── 主入口 ────────────────────────────────────────────────────────────────
export default {
  // Queue consumer: 同时处理两个 backfill 专用队列
  async queue(batch, env, ctx) {
    if      (batch.queue === env.PARSE_QUEUE_NAME) await handleParseQueue(batch, env);
    else if (batch.queue === env.SEND_QUEUE_NAME)  await handleSendQueue(batch, env);
    else log(env, 'warn', `Unknown queue: ${batch.queue}`);
  },

  // Cron trigger: 每分钟扫 R2 logs/ 并 rate-limited 入队到 parse-queue-backfill
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runBackfillScan(env));
  },

  // HTTP handler: /backfill/status 查询进度
  async fetch(request, env) {
    return handleFetch(request, env);
  },
};

// ─── Parser: R2原始文件 → 流式解析转换 → RunAggregator Durable Object ───────────
// 相比生产 Parser：
//   - 逐条按 [BACKFILL_START_TIME, BACKFILL_END_TIME] 裁切，避免边界文件整段重放
//   - 逐条过滤 Workers 子请求，只保留父请求
//   - 不再按单个 raw file 直接成批；改为喂给 DO 做跨文件聚合
async function handleParseQueue(batch, env) {
  await Promise.allSettled(batch.messages.map((msg) => processFile(msg, env)));
}

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

  try {
    const result = await processSourceFile(key, run, env);
    if (result.missing) {
      msg.ack();
      return;
    }
    msg.ack();
  } catch (err) {
    log(env, 'error', `Failed: ${key}: ${err.message}`);
    msg.retry();
  }
}

async function processSourceFile(key, run, env) {
  log(env, 'info', `Parsing: ${key}`);

  const object = await env.RAW_BUCKET.get(key);
  if (!object) {
    log(env, 'warn', `Not in R2: ${key}`);
    return { missing: true };
  }

  const aggregatorChunkLines = parseAggregatorChunkLines(env);
  const aggregator = getRunAggregatorStub(env, run.id);
  let lines = [], chunkIdx = 0, lineCount = 0, errCount = 0, skipped = 0, invalidTs = 0, skippedWorker = 0, kept = 0;
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
    } catch (e) {
      errCount++;
      log(env, 'warn', `Transform err line ${lineCount}: ${e.message}`);
      return;
    }
    if (lines.length >= aggregatorChunkLines) {
      await sendChunkToAggregator(aggregator, key, chunkIdx++, lines, run);
      lines = [];
    }
  });
  if (lines.length > 0) await sendChunkToAggregator(aggregator, key, chunkIdx++, lines, run);
  await markSourceFileComplete(aggregator, key, run);
  log(env, 'info', `Done: ${key} | lines=${lineCount} kept=${kept} chunks=${chunkIdx} errors=${errCount} skipped=${skipped} invalid_ts=${invalidTs} skipped_worker=${skippedWorker} run=${run.id}`);
  return { missing: false, kept, chunkIdx, lineCount };
}

async function writeBatchAndEnqueue(lines, sourceKey, index, env, run) {
  const batchKey        = buildBatchKey(sourceKey, index, run.id);
  const queuedMarkerKey = `${batchKey}.queued`;

  // 幂等检查：该 batch 已被 Backfill Sender 成功发送过时跳过
  const doneMarker = await env.RAW_BUCKET.head(`${batchKey}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'debug', `Batch already sent (skip): ${batchKey}`);
    return;
  }

  // Parser retry 场景：如果 batch 已经成功写入并入过 send-queue，则不重复入队。
  const queuedMarker = await env.RAW_BUCKET.head(queuedMarkerKey).catch(() => null);
  if (queuedMarker) {
    log(env, 'debug', `Batch already queued (skip duplicate enqueue): ${batchKey}`);
    return;
  }

  const body = lines.join('\n') + '\n';
  try {
    await env.RAW_BUCKET.put(batchKey, body, {
      httpMetadata: { contentType: 'text/plain; charset=utf-8' },
    });
    await env.RAW_BUCKET.put(queuedMarkerKey, '1', {
      httpMetadata: { contentType: 'text/plain' },
    });
    await env.SEND_QUEUE.send({ key: batchKey, queuedAtMs: Date.now(), runId: run.id, lineCount: lines.length });
  } catch (e) {
    // 入队失败，回滚 R2 文件，让 parse-queue-backfill 的 retry 机制干净重试
    await Promise.allSettled([
      env.RAW_BUCKET.delete(batchKey),
      env.RAW_BUCKET.delete(queuedMarkerKey),
    ]);
    throw e;
  }
  log(env, 'debug', `Queued: ${batchKey} (${lines.length} lines)`);
}

// ─── Sender: processed-backfill/ → Gzip → MD5鉴权 → POST → 删除临时文件 ──────
// 核心逻辑与生产 Sender 一致（处理 BATCH_PREFIX 不同的文件）
// 额外增加：invocation-level 节流（见 MIN_SENDER_INVOCATION_MS 常量说明）
async function handleSendQueue(batch, env) {
  const invocationStart = Date.now();

  const results = await Promise.allSettled(
    batch.messages.map(msg => sendBatch(msg, env))
  );
  results.forEach((r, i) => {
    if (r.status === 'fulfilled') batch.messages[i].ack();
    else { log(env, 'warn', `Send failed, retry: ${r.reason}`); batch.messages[i].retry(); }
  });

  // 节流：确保本次 invocation 至少占用 MIN_SENDER_INVOCATION_MS
  // 这保证了单个 sender consumer 的 "invocation/s" 上限；总吞吐还要乘上 wrangler 的 max_concurrency
  const elapsed = Date.now() - invocationStart;
  if (elapsed < MIN_SENDER_INVOCATION_MS) {
    const sleepMs = MIN_SENDER_INVOCATION_MS - elapsed;
    log(env, 'debug', `Sender throttle: slept ${sleepMs}ms after ${elapsed}ms of work (cap ${MIN_SENDER_INVOCATION_MS}ms/invocation)`);
    await new Promise((r) => setTimeout(r, sleepMs));
  }
}

async function sendBatch(msg, env) {
  const { key } = msg.body;
  if (!key) throw new Error(`Invalid message: ${JSON.stringify(msg.body)}`);
  const runId = typeof msg.body?.runId === 'string' && msg.body.runId.trim()
    ? msg.body.runId.trim()
    : extractRunIdFromBatchKey(key);
  const lineCount = parseBatchLineCount(msg.body?.lineCount);
  const queuedMarkerKey = `${key}.queued`;
  const doneMarker = await env.RAW_BUCKET.head(`${key}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'info', `Already sent (skip duplicate): ${key}`);
    return;
  }
  if (runId && await isBatchMarkedSent(env, runId, key)) {
    log(env, 'info', `Already sent via durable marker (skip duplicate): ${key}`);
    return;
  }
  const object = await env.RAW_BUCKET.get(key);
  if (!object) {
    if (runId && (await isRunCleaned(env, runId) || await isBatchMarkedSent(env, runId, key))) {
      log(env, 'info', `Batch already cleaned after completion (skip late duplicate): ${key}`);
      return;
    }
    throw new Error(`Batch missing before successful send: ${key} (no .done marker present)`);
  }
  const uri        = env.CTYUN_URI_EDGE;
  const endpoint   = env.CTYUN_ENDPOINT;
  const privateKey = env.CTYUN_PRIVATE_KEY;
  if (!endpoint || !privateKey || !uri) throw new Error('Missing CTYUN_ENDPOINT, CTYUN_PRIVATE_KEY or CTYUN_URI_EDGE');

  // 流式压缩，避免内存峰值（单 worker 只有 128MB）
  const compressed = await new Response(
    object.body.pipeThrough(new CompressionStream('gzip'))
  ).arrayBuffer();

  const fetchInit = {
    method: 'POST',
    headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Content-Encoding': 'gzip' },
    body: compressed,
  };
  const timeoutMs = parseSendTimeoutMs(env);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  let resp;
  const fetchStartedAt = Date.now();
  try {
    resp = await fetch(buildAuthUrl(endpoint, uri, privateKey), {
      ...fetchInit,
      signal: controller.signal,
    });
  } catch (e) {
    if (controller.signal.aborted) {
      throw new Error(`Send timeout after ${timeoutMs}ms`);
    }
    throw e;
  } finally {
    clearTimeout(timeout);
  }
  const ackMs = Date.now() - fetchStartedAt;
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`HTTP ${resp.status} ${resp.statusText} | ${text.substring(0, 200)}`);
  }
  await resp.body?.cancel().catch(() => {});
  log(env, 'info', `Sent batch lines=${lineCount ?? '?'} bytes=${object.size ?? '?'} (uncompressed) → HTTP ${resp.status} in ${ackMs}ms | ${key}`);
  let durableSent = false;
  if (runId) {
    try {
      await markBatchAsSent(env, runId, key);
      durableSent = true;
    } catch (e) {
      log(env, 'warn', `Durable sent marker write failed: ${key}: ${e.message}`);
    }
  }
  let wroteDone = false;
  try {
    await env.RAW_BUCKET.put(`${key}.done`, '1', {
      httpMetadata: { contentType: 'text/plain' },
    });
    wroteDone = true;
  } catch (e) {
    log(env, 'warn', `Done marker write failed (keeping batch file for investigation): ${key}: ${e.message}`);
  }
  if (!durableSent && !wroteDone) {
    throw new Error(`Successful POST but failed to persist any sent marker for ${key}`);
  }
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
  log(env, 'debug', `Deleted: ${key}`);
}

// ─── Scheduled handler: 扫 R2 logs/ → rate-limited 入 parse-queue-backfill ──
async function runBackfillScan(env) {
  const startedAt = Date.now();
  try {
    const config = parseConfig(env);
    if (!config.valid) {
      log(env, 'error', config.error);
      return;
    }

    if (env.BACKFILL_ENABLED === 'false') {
      log(env, 'info', 'Paused by BACKFILL_ENABLED=false');
      return;
    }

    const state = await loadState(env, config);

    if (state.status === 'cleaned') {
      log(env, 'info', `Backfill cleaned. run_id=${state.run_id}, completed_at=${state.completed_at}, cleaned_at=${state.cleanup?.cleaned_at}. Change BACKFILL_START_TIME/END_TIME and redeploy to run a new window.`);
      return;
    }

    let changed = false;
    if (state.phase === 'enqueue' && Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
      changed = (await runEnqueue(env, state, config, startedAt)) || changed;
    }
    if (state.status === 'done' && Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
      changed = (await syncAggregatorStatus(env, state)) || changed;
      changed = (await maybeRecoverIncompleteFiles(env, state, config, startedAt)) || changed;
      changed = (await maybeAutoCleanup(env, state, startedAt)) || changed;
    }

    if (changed) {
      await saveState(env, state).catch((e) => {
        log(env, 'warn', `Failed to save state (will retry next cron, idempotent): ${e.message}`);
      });
    }
  } catch (e) {
    log(env, 'error', `Backfill cron crashed: ${e.message}\n${e.stack}`);
  }
}

function parseConfig(env) {
  const start = (env.BACKFILL_START_TIME || '').trim();
  const end   = (env.BACKFILL_END_TIME   || '').trim();

  if (!start || !end) {
    return {
      valid: false,
      error: 'BACKFILL_START_TIME and BACKFILL_END_TIME must be set. Format: ISO 8601, e.g. "2026-04-22T14:00:00Z" (UTC) or "2026-04-22T22:00:00+08:00" (Beijing time).'
    };
  }
  const startMs = new Date(start).getTime();
  const endMs   = new Date(end).getTime();
  if (isNaN(startMs) || isNaN(endMs)) {
    return { valid: false, error: `Invalid time format. START="${start}" END="${end}". Expected ISO 8601.` };
  }
  if (startMs >= endMs) {
    return { valid: false, error: `START (${start}) must be earlier than END (${end}).` };
  }
  // 安全校验：END 不能是未来时间
  // backfill 的目的是"补历史缺漏"，END > now 通常是误配置（如误填年份）
  // 独立架构下 backfill 不删任何文件，所以 END 未来也不会破坏数据，但拒绝明显误配是好习惯
  const now = Date.now();
  if (endMs > now) {
    const futureMin = ((endMs - now) / 60000).toFixed(1);
    return {
      valid: false,
      error: `END (${end}) must not be in the future (${futureMin}min ahead of now). Backfill is for historical data only.`
    };
  }
  const spanHours = (endMs - startMs) / 3600000;
  if (spanHours > MAX_RANGE_HOURS) {
    return {
      valid: false,
      error: `Range span ${spanHours.toFixed(1)}h exceeds max ${MAX_RANGE_HOURS}h. Intentional? Edit MAX_RANGE_HOURS in src/index.js.`
    };
  }

  const rateRaw = parseInt(env.BACKFILL_RATE || String(DEFAULT_RATE), 10);
  const rate    = Math.min(MAX_RATE, Math.max(1, isNaN(rateRaw) ? DEFAULT_RATE : rateRaw));

  return {
    valid: true,
    start, end, startMs, endMs, rate,
    windowId: buildRunId(startMs, endMs),
    bucketName: env.R2_BUCKET_NAME || 'cdn-logs-raw',
    logPrefix:  env.LOG_PREFIX     || 'logs/'
  };
}

async function loadState(env, config) {
  const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
  if (obj) {
    try {
      const state = JSON.parse(await obj.text());
      if (state.config?.start === config.start && state.config?.end === config.end) {
        if (!state.run_id) {
          state.run_id = buildRunInstanceId(config.startMs, config.endMs);
        }
        state.cleanup = normalizeCleanupState(state.cleanup);
        state.recovery = normalizeRecoveryState(state.recovery);
        state.aggregate = normalizeAggregateStatus(state.aggregate, state.run_id);
        return state;
      }
      if (!canReinitializeFromState(state)) {
        throw new Error(`Existing backfill run ${state.run_id} is still ${state.status}. Wait until status="cleaned" before changing BACKFILL_START_TIME/BACKFILL_END_TIME.`);
      }
      log(env, 'info', `Config changed (was ${state.config?.start}→${state.config?.end}, now ${config.start}→${config.end}). Re-initializing state.`);
    } catch (e) {
      if (String(e.message || '').includes('Existing backfill run')) throw e;
      log(env, 'warn', `State file corrupted, re-initializing: ${e.message}`);
    }
  }
  const runId = buildRunInstanceId(config.startMs, config.endMs);
  return {
    config:           { start: config.start, end: config.end, rate: config.rate },
    run_id:           runId,
    phase:            'enqueue',
    status:           'running',
    started_at:       new Date().toISOString(),
    enqueue_progress: {},      // { [prefix]: { start_after, done, enqueued } }
    enqueued_count:   0,
    last_cron_at:     null,
    completed_at:     null,
    cleanup:          createInitialCleanupState(),
    recovery:         createInitialRecoveryState(),
    aggregate:        createInitialAggregateStatus(runId),
  };
}

async function saveState(env, state) {
  state.last_cron_at = new Date().toISOString();
  await env.RAW_BUCKET.put(STATE_KEY, JSON.stringify(state, null, 2), {
    httpMetadata: { contentType: 'application/json; charset=utf-8' }
  });
}

// ─── Enqueue Phase: 扫 logs/YYYYMMDD/ 按 rate 入队 ───────────────────────────
async function runEnqueue(env, state, config, startedAt) {
  const prefixes = getR2PrefixesByDay(config.startMs, config.endMs, config.logPrefix);

  for (const p of prefixes) {
    if (!state.enqueue_progress[p]) {
      state.enqueue_progress[p] = { start_after: null, done: false, enqueued: 0 };
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
        limit:      LIST_LIMIT
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
        prog.start_after = key;  // 推进 startAfter，即使过滤掉也不再重扫

        // 防御性过滤（prefix 已经限定 logs/，但保险起见）
        if (key.startsWith(BATCH_ROOT_PREFIX))    continue;
        if (key.startsWith('backfill-state/'))    continue;
        if (key.startsWith('.recover-done-'))     continue;
        if (key.startsWith('processed/'))         continue;  // 生产的 processed/ 前缀

        const range = extractFileTimeRange(key);
        if (!range) continue;

        // 早停优化：R2 list 按字典序返回 key，Logpush 文件名格式 YYYYMMDDTHHMMSSZ_...
        // 字典序 == 时间序，因此一旦扫到 key.startMs > config.endMs，后续所有 key 都会超 END。
        // 立即标记该 prefix 完成，避免继续扫描已知无关的文件。
        // 影响量化：对小范围补传（~百 files）收益在毫秒级；对 1000+ files 场景避免多次 R2 list 分页。
        if (range.startMs > config.endMs) {
          prog.done = true;
          log(env, 'debug', `[Enqueue] Early termination at ${key} (startMs > endMs), prefix=${prefix}`);
          break;  // 跳出 for 循环；外层 while 会因 prog.done=true 在下次 check 退出
        }

        if (range.endMs < config.startMs) continue;

        try {
          // 格式与 R2 Event Notification 一致，handleParseQueue 读 msg.body.object.key
          await env.PARSE_QUEUE.send(buildParseQueueMessage(key, config, state.run_id));
          enqueuedThisCron++;
          prog.enqueued++;
        } catch (e) {
          // throw 让整个 cron 失败，state 不 save，下次从上次保存的 start_after 重试
          log(env, 'error', `Enqueue failed for ${key}: ${e.message}. Cron will retry next minute.`);
          throw e;
        }
      }

      if (!pageExhausted) break;  // 当前页被 rate 截断，保留 start_after，下次续
      if (prog.done) break;       // 早停命中，prefix 已完成

      // R2 list() 是否还有下一页应以 truncated 为准，不能用返回对象数是否达到 limit 来猜。
      // 高流量 bucket 里，一页即使少于 limit，也仍可能有更多对象待翻页。
      if (isR2ListComplete(list)) {
        prog.done = true;
        break;
      }
      // 否则 while 循环继续下一页
    }
  }

  state.enqueued_count += enqueuedThisCron;

  const allDone      = prefixes.every(p => state.enqueue_progress[p].done);
  const prefixesDone = prefixes.filter(p => state.enqueue_progress[p].done).length;
  log(env, 'info', `[Enqueue] +${enqueuedThisCron} files this cron. total=${state.enqueued_count}. prefixes_done=${prefixesDone}/${prefixes.length}`);

  if (allDone) {
    state.phase        = 'done';
    state.status       = 'done';
    state.completed_at = new Date().toISOString();
    const durMin = Math.round((Date.parse(state.completed_at) - Date.parse(state.started_at)) / 60000);
    log(env, 'info', `🎉 Backfill ENQUEUE COMPLETE! enqueued=${state.enqueued_count} files over ${durMin}min of cron activity. Backfill Parser/Sender will continue processing asynchronously at ≤ 5 batch/s (code-capped). Monitor send-queue-backfill backlog for actual delivery completion.`);
  }

  return true;
}

async function maybeAutoCleanup(env, state, startedAt) {
  state.cleanup = normalizeCleanupState(state.cleanup);
  if (state.aggregate?.finalized !== true || (state.aggregate?.pending_batch_count ?? 0) > 0) {
    return false;
  }

  if (state.cleanup.status === 'done') {
    if (state.status !== 'cleaned') {
      state.status = 'cleaned';
      return true;
    }
    return false;
  }

  if (state.cleanup.status === 'ready') {
    const readyAt = Date.parse(state.cleanup.ready_at || '');
    if (Number.isFinite(readyAt) && Date.now() - readyAt < CLEANUP_GRACE_MS) {
      return false;
    }
    state.cleanup.status = 'deleting';
    state.cleanup.delete_start_after = null;
    return true;
  }

  if (state.cleanup.status === 'deleting') {
    return deleteRunArtifacts(env, state, startedAt);
  }

  return inspectRunArtifacts(env, state, startedAt);
}

async function maybeRecoverIncompleteFiles(env, state, config, startedAt) {
  state.recovery = normalizeRecoveryState(state.recovery);
  const aggregate = normalizeAggregateStatus(state.aggregate, state.run_id);
  if (!shouldRecoverIncompleteFiles(state, aggregate)) {
    return false;
  }

  return recoverFirstIncompleteFile(env, state, config, startedAt);
}

async function recoverFirstIncompleteFile(env, state, config, startedAt) {
  const prefixes = getR2PrefixesByDay(config.startMs, config.endMs, config.logPrefix);
  if (prefixes.length === 0) return false;

  let prefixIndex = Number.isInteger(state.recovery.prefix_index) ? state.recovery.prefix_index : 0;
  if (prefixIndex < 0 || prefixIndex >= prefixes.length) {
    prefixIndex = 0;
  }

  let changed = false;
  while (Date.now() - startedAt < WALL_TIME_BUDGET_MS && prefixIndex < prefixes.length) {
    const prefix = prefixes[prefixIndex];
    const page = await env.RAW_BUCKET.list({
      prefix,
      startAfter: state.recovery.scan_start_after || undefined,
      limit: LIST_LIMIT,
    });

    if (page.objects.length === 0) {
      prefixIndex += 1;
      state.recovery.prefix_index = prefixIndex;
      state.recovery.scan_start_after = null;
      changed = true;
      continue;
    }

    for (const obj of page.objects) {
      const key = obj.key;
      state.recovery.last_checked_at = new Date().toISOString();
      state.recovery.prefix_index = prefixIndex;
      state.recovery.scan_start_after = key;
      changed = true;

      if (key.startsWith(BATCH_ROOT_PREFIX) || key.startsWith('backfill-state/') || key.startsWith('.recover-done-') || key.startsWith('processed/')) {
        continue;
      }

      const range = extractFileTimeRange(key);
      if (!range) continue;
      if (range.startMs > config.endMs || range.endMs < config.startMs) continue;

      if (await isSourceFileComplete(env, state.run_id, key)) {
        continue;
      }

      const run = normalizeRunContext({
        id: state.run_id,
        start: config.start,
        end: config.end,
        startMs: config.startMs,
        endMs: config.endMs,
      });
      if (!run.valid) {
        throw new Error(`Invalid run during recovery: ${run.error}`);
      }

      const result = await processSourceFile(key, run, env);
      state.recovery.last_recovered_key = key;
      state.recovery.last_recovered_at = new Date().toISOString();
      if (!result.missing) {
        state.recovery.recovered_files += 1;
        state.aggregate = normalizeAggregateStatus(
          await finalizeAggregatorIfReady(env, state.run_id, state.enqueued_count),
          state.run_id,
        );
      }
      log(env, 'info', `[Recovery] Reprocessed incomplete raw file for run ${state.run_id}: ${key}`);
      return true;
    }

    if (isR2ListComplete(page)) {
      prefixIndex += 1;
      state.recovery.prefix_index = prefixIndex;
      state.recovery.scan_start_after = null;
      continue;
    }
  }

  if (prefixIndex >= prefixes.length) {
    state.recovery.prefix_index = 0;
    state.recovery.scan_start_after = null;
    state.aggregate = normalizeAggregateStatus(
      await reconcileAggregatorCompletedFiles(env, state.run_id, state.enqueued_count, state.enqueued_count),
      state.run_id,
    );
    log(env, 'warn', `[Recovery] Reconciled completed_files drift for run ${state.run_id} after verifying all raw files were already complete.`);
    return true;
  }

  return changed;
}

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
        log(env, 'info', `Auto-cleanup ready for run ${state.run_id}. Waiting ${Math.round(CLEANUP_GRACE_MS / 60000)} minutes before removing temporary artifacts.`);
      }
      return changed;
    }

    for (const obj of page.objects) {
      const pending = await isPendingCleanupArtifact(env, state.run_id, obj.key);
      if (pending) {
        const hadCleanupState = state.cleanup.status !== 'pending' || state.cleanup.ready_at !== null || state.cleanup.inspect_start_after !== null;
        state.cleanup = createInitialCleanupState();
        if (hadCleanupState) {
          log(env, 'debug', `Auto-cleanup waiting: pending artifact still exists for run ${state.run_id}: ${obj.key}`);
        }
        return hadCleanupState;
      }
    }

    state.cleanup.inspect_start_after = page.objects[page.objects.length - 1].key;
    changed = true;

    if (isR2ListComplete(page)) {
      state.cleanup.status = 'ready';
      state.cleanup.ready_at = state.cleanup.ready_at || new Date().toISOString();
      state.cleanup.inspect_start_after = null;
      log(env, 'info', `Auto-cleanup ready for run ${state.run_id}. Waiting ${Math.round(CLEANUP_GRACE_MS / 60000)} minutes before removing temporary artifacts.`);
      return true;
    }

    startAfter = state.cleanup.inspect_start_after || undefined;
  }

  return changed;
}

async function syncAggregatorStatus(env, state) {
  const aggregator = await finalizeAggregatorIfReady(env, state.run_id, state.enqueued_count);
  const normalized = normalizeAggregateStatus(aggregator, state.run_id);
  const changed = JSON.stringify(state.aggregate ?? null) !== JSON.stringify(normalized);
  state.aggregate = normalized;
  return changed;
}

async function deleteRunArtifacts(env, state, startedAt) {
  const prefix = getBatchPrefix(state.run_id);
  let changed = false;
  let startAfter = state.cleanup.delete_start_after || undefined;

  while (Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
    const page = await env.RAW_BUCKET.list({ prefix, startAfter, limit: LIST_LIMIT });

    if (page.objects.length === 0) {
      await env.RAW_BUCKET.delete(LEGACY_SEND_STATS_KEY).catch(() => {});
      Object.assign(state, compactStateAfterCleanup(state));
      log(env, 'info', `Auto-cleanup complete for run ${state.run_id}. Removed temporary backfill artifacts under ${prefix}.`);
      return true;
    }

    const keys = page.objects.map((obj) => obj.key);
    const failedKeys = await deleteR2Keys(env, keys);
    if (failedKeys.length > 0) {
      log(env, 'warn', `Auto-cleanup delete retry needed for ${failedKeys.length} objects in run ${state.run_id}.`);
      return changed;
    }
    state.cleanup.deleted_objects += keys.length;
    state.cleanup.delete_start_after = page.objects[page.objects.length - 1].key;
    changed = true;

    if (isR2ListComplete(page)) {
      await env.RAW_BUCKET.delete(LEGACY_SEND_STATS_KEY).catch(() => {});
      Object.assign(state, compactStateAfterCleanup(state));
      log(env, 'info', `Auto-cleanup complete for run ${state.run_id}. Removed temporary backfill artifacts under ${prefix}.`);
      return true;
    }

    startAfter = state.cleanup.delete_start_after || undefined;
  }

  return changed;
}

async function deleteR2Keys(env, keys) {
  if (keys.length === 0) return;
  const failed = [];
  for (const key of keys) {
    try {
      await env.RAW_BUCKET.delete(key);
      const exists = await env.RAW_BUCKET.head(key).catch(() => null);
      if (exists) failed.push(key);
    } catch {
      failed.push(key);
    }
  }
  return failed;
}

// ─── HTTP fetch handler ─────────────────────────────────────────────────────
async function handleFetch(request, env) {
  const url = new URL(request.url);

  if (url.pathname === '/backfill/status') {
    const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
    if (!obj) {
      return jsonResponse(buildNotStartedStatusResponse(env));
    }
    try {
      const data = JSON.parse(await obj.text());
      if (url.searchParams.get('view') === 'raw') {
        return jsonResponse(data);
      }
      return jsonResponse(buildPublicStatusResponse(data));
    } catch (e) {
      return jsonResponse({ status: 'error', message: `State file corrupted: ${e.message}` }, 500);
    }
  }

  if (url.pathname === '/' || url.pathname === '') {
    return new Response(
      'ctyun-logpush-backfill worker\n' +
      'GET /backfill/status — view backfill progress\n',
      { status: 200, headers: { 'content-type': 'text/plain; charset=utf-8' } }
    );
  }

  return new Response('Not Found', { status: 404 });
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' }
  });
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
    }
  };
}

function buildNotStartedStatusResponse(env) {
  return {
    status: 'not_started',
    stage: 'waiting_to_start',
    message: 'Backfill has not started yet. Wait for the next cron run after enabling the worker.',
    window_start: env.BACKFILL_START_TIME || null,
    window_end: env.BACKFILL_END_TIME || null,
    backfill_enabled: env.BACKFILL_ENABLED !== 'false',
    raw_files_enqueued: 0,
    batches_handed_to_send_queue: 0,
    log_lines_handed_to_send_queue: 0,
    line_count_status: 'exact',
    average_lines_per_batch: null,
    smallest_batch_lines: null,
    largest_batch_lines: null,
    batches_still_buffered_in_worker: 0,
    lines_still_buffered_in_worker: 0,
    started_at: null,
    enqueue_completed_at: null,
    last_updated_at: null,
  };
}

function buildPublicStatusResponse(state) {
  const aggregate = normalizeAggregateStatus(state.aggregate, state.run_id);
  const cleanup = normalizeCleanupState(state.cleanup);
  const stage = derivePublicStatusStage(state, aggregate, cleanup);
  const lineCountStatus = aggregate.line_count_tracking === 'exact'
    ? 'exact'
    : 'not_available_for_legacy_run';

  return {
    status: state.status,
    stage,
    message: buildPublicStatusMessage(stage, lineCountStatus),
    run_id: state.run_id,
    window_start: state.config?.start || null,
    window_end: state.config?.end || null,
    raw_files_enqueued: state.enqueued_count ?? 0,
    batches_handed_to_send_queue: aggregate.emitted_batches,
    log_lines_handed_to_send_queue: lineCountStatus === 'exact' ? aggregate.emitted_lines : null,
    line_count_status: lineCountStatus,
    average_lines_per_batch: aggregate.batch_line_count_avg,
    smallest_batch_lines: aggregate.batch_line_count_min,
    largest_batch_lines: aggregate.batch_line_count_max,
    batches_still_buffered_in_worker: aggregate.pending_batch_count,
    lines_still_buffered_in_worker: aggregate.pending_buffer_lines,
    started_at: state.started_at || null,
    enqueue_completed_at: state.completed_at || null,
    last_updated_at: aggregate.updated_at || state.last_cron_at || state.started_at || null,
  };
}

function derivePublicStatusStage(state, aggregate, cleanup) {
  if (state.status === 'cleaned') return 'completed_and_cleaned';
  if (state.phase === 'enqueue') return 'scanning_raw_logs';
  if (aggregate.finalized !== true) return 'finalizing_batches';
  if ((aggregate.pending_batch_count ?? 0) > 0 || (aggregate.pending_buffer_lines ?? 0) > 0) {
    return 'handing_batches_to_send_queue';
  }
  if (cleanup.status === 'ready' || cleanup.status === 'deleting') {
    return 'cleaning_temporary_files';
  }
  return 'all_batches_handed_to_send_queue';
}

function buildPublicStatusMessage(stage, lineCountStatus) {
  const base = {
    waiting_to_start: 'Backfill has not started yet.',
    scanning_raw_logs: 'Worker is still scanning raw log files in the requested time window.',
    finalizing_batches: 'Raw file scan is complete. Worker is finalizing the last batches.',
    handing_batches_to_send_queue: 'Worker is still writing batches and handing them to send-queue-backfill.',
    all_batches_handed_to_send_queue: 'All batches have been handed to send-queue-backfill. Remaining delivery is now in the sender queue.',
    cleaning_temporary_files: 'Batch handoff is complete. Worker is cleaning temporary backfill artifacts.',
    completed_and_cleaned: 'Backfill is complete and temporary artifacts have been cleaned.',
  }[stage] || 'Backfill status is available.';

  if (lineCountStatus !== 'exact') {
    return `${base} Exact line totals are unavailable for this legacy in-flight run because it started before per-batch line counting was added.`;
  }
  return base;
}

// ─── 流式解析: gzip ndjson → 逐行回调 ─────────────────────────────────────
async function streamParseNdjsonGzip(inputStream, onRecord) {
  const reader  = inputStream.pipeThrough(new DecompressionStream('gzip')).getReader();
  const decoder = new TextDecoder('utf-8');
  let   buffer  = '';
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
        const t = line.trim();
        if (t) await tryParse(t, onRecord);
      }
    }
  } finally { reader.releaseLock(); }
}

async function tryParse(line, onRecord) {
  try { await onRecord(JSON.parse(line)); }
  catch (e) { console.warn(`[WARN] JSON parse failed: ${line.substring(0, 100)}`); }
}

// ─── 格式转换: CF http_requests → CDN partner log format v3.0（145字段）─────
function sf(val, maxLen) {
  if (val == null || val === '') return '-';
  const s = String(val);
  return (maxLen && s.length > maxLen) ? s.substring(0, maxLen) : s;
}
function transformEdge(r) {
  return [
    /* 1  */ VERSION_EDGE,
    /* 2  */ fmtTimeLocal(r.EdgeStartTimestamp),
    /* 3  */ sf(r.RayID),
    /* 4  */ sf(r.EdgeResponseStatus),
    /* 5  */ fmtMsec(r.EdgeStartTimestamp),
    /* 6  */ fmtSec(r.EdgeTimeToFirstByteMs),
    /* 7  */ fmtSec(r.OriginResponseHeaderReceiveDurationMs),
    /* 8  */ fmtSec(r.OriginRequestHeaderSendDurationMs),
    /* 9  */ fmtSec(r.EdgeTimeToFirstByteMs),
    /* 10 */ '-',
    /* 11 */ sf(r.EdgeServerIP),
    /* 12 */ schemeToPort(r.ClientRequestScheme),
    /* 13 */ sf(r.ClientIP),
    /* 14 */ sf(r.ClientSrcPort),
    /* 15 */ sf(r.ClientRequestMethod),
    /* 16 */ sf(r.ClientRequestScheme),
    /* 17 */ sf(r.ClientRequestHost),
    /* 18 */ sf(buildFullUrl(r), MAX_URL_LEN),
    /* 19 */ sf(r.ClientRequestProtocol),
    /* 20 */ sf(r.ClientRequestBytes),
    /* 21 */ responseContentLength(r),
    /* 22 */ sf(r.EdgeResponseBytes),
    /* 23 */ sf(r.EdgeResponseBodyBytes),
    /* 24 */ sf(r.OriginIP),
    /* 25 */ sf(r.OriginResponseStatus),
    /* 26 */ fmtSec(r.OriginResponseDurationMs),
    /* 27 */ mapCache(r.CacheCacheStatus),
    /* 28 */ mapCache(r.CacheCacheStatus),
    /* 29 */ sf(r.OriginIP),
    /* 30 */ sf(r.OriginResponseStatus),
    /* 31 */ '-',
    /* 32 */ '-',
    /* 33 */ sf(r.EdgeResponseContentType),
    /* 34 */ sf(r.ClientRequestReferer, MAX_REF_LEN),
    /* 35 */ sf(r.ClientRequestUserAgent, MAX_UA_LEN),
    /* 36 */ sf(r.ClientIP),
    /* 37 */ '-',
    /* 38 */ '-',
    /* 39 */ '-',
    /* 40 */ sf(r.ClientIP),
    /* 41 */ '-',
    /* 42 */ mapDysta(r.CacheCacheStatus),
    /* 43 */ '-',
    /* 44 */ fmtSec(r.OriginTLSHandshakeDurationMs),
    /* 45 */ coloToCountry(r.EdgeColoCode, r.ClientCountry),
    /* 46-54 */ ...DASHES_9,
    /* 55 */ fmtTimeLocalSimple(r.EdgeStartTimestamp),
    /* 56 */ '-',
    /* 57 */ '-',
    /* 58 */ '-',
    /* 59 */ '-',
    /* 60 */ sf(r.ClientRequestHost),
    /* 61 */ '-',
    /* 62 */ sf(r.ClientSSLProtocol),
    /* 63-64 */ ...DASHES_2,
    /* 65-80 */ ...DASHES_16,
    /* 81-95 */ ...DASHES_15,
    /* 96-145 */ ...DASHES_50,
  ].join(SEP);
}

function responseContentLength(r) {
  if (r.ResponseHeaders && r.ResponseHeaders['content-length']) {
    return sf(r.ResponseHeaders['content-length']);
  }
  return '-';
}
function mapCache(s) {
  if (!s) return '-';
  const l = s.toLowerCase();
  if (['hit','stale','revalidated','updating'].includes(l)) return 'HIT';
  if (['miss','expired','bypass','dynamic','none'].includes(l)) return 'MISS';
  return '-';
}
function mapDysta(s) {
  if (!s) return '-';
  const l = s.toLowerCase();
  return l === 'hit' ? 'static' : l === 'dynamic' ? 'dynamic' : '-';
}

// ─── 鉴权: auth_key={ts}-{rand}-md5({uri}-{ts}-{rand}-{key}) ──────────────
function buildAuthUrl(endpoint, uri, privateKey) {
  const ts   = Math.floor(Date.now() / 1000) + 300;
  const rand = Math.floor(Math.random() * 99999);
  return `${endpoint}${uri}?auth_key=${ts}-${rand}-${md5(`${uri}-${ts}-${rand}-${privateKey}`)}`;
}

// ─── 工具函数 ──────────────────────────────────────────────────────────────
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
function fmtTimeLocal(ts) {
  const ms = parseTimestamp(ts);
  if (ms == null) return '-';
  const d  = new Date(ms + 8 * 3600 * 1000);
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

// ─── 时间范围提取（用于过滤 raw file key）──────────────────────────────────
// 支持两种格式：
//   双时间戳 (Logpush 默认): logs/20260422/20260422T140000Z_20260422T140500Z_abc.log.gz
//   单时间戳 (fallback):     logs/20260422/20260422T140000Z_abc.log.gz
function extractFileTimeRange(key) {
  const m = key.match(/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z[_-](\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/);
  if (m) {
    const s = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4],  +m[5],  +m[6]);
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

function isRecordInRunWindow(recordMs, run) {
  return recordMs >= run.startMs && recordMs <= run.endMs;
}

async function resolveRunContext(run, env) {
  const normalized = normalizeRunContext(run);
  if (normalized.valid) return normalized;

  const config = parseConfig(env);
  if (!config.valid) {
    return {
      valid: false,
      error: normalized.error || config.error,
    };
  }

  const stateRunId = await loadExistingStateRunId(env, config);
  if (!stateRunId) {
    return {
      valid: false,
      error: normalized.error || 'Missing run metadata and no active state.run_id to fall back to.',
    };
  }

  return {
    valid: true,
    id:      stateRunId,
    start:   config.start,
    end:     config.end,
    startMs: config.startMs,
    endMs:   config.endMs,
  };
}

function normalizeRunContext(run) {
  if (!run || typeof run !== 'object') {
    return { valid: false, error: 'Missing run metadata in queue message.' };
  }

  const id = typeof run.id === 'string' ? run.id.trim() : '';
  const startMs = Number(run.startMs);
  const endMs   = Number(run.endMs);
  if (!id || !Number.isFinite(startMs) || !Number.isFinite(endMs) || startMs >= endMs) {
    return { valid: false, error: `Malformed run metadata: ${JSON.stringify(run)}` };
  }

  return {
    valid: true,
    id,
    start: typeof run.start === 'string' ? run.start : new Date(startMs).toISOString(),
    end:   typeof run.end === 'string'   ? run.end   : new Date(endMs).toISOString(),
    startMs,
    endMs,
  };
}

function buildRunId(startMs, endMs) {
  return `${fmtCompactUtc(startMs)}_${fmtCompactUtc(endMs)}`;
}

function buildRunInstanceId(startMs, endMs, createdAtMs = Date.now()) {
  return `${buildRunId(startMs, endMs)}_${fmtCompactUtc(createdAtMs)}`;
}

function fmtCompactUtc(ms) {
  const d = new Date(ms);
  const yyyy = d.getUTCFullYear();
  const mm   = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd   = String(d.getUTCDate()).padStart(2, '0');
  const hh   = String(d.getUTCHours()).padStart(2, '0');
  const mi   = String(d.getUTCMinutes()).padStart(2, '0');
  const ss   = String(d.getUTCSeconds()).padStart(2, '0');
  return `${yyyy}${mm}${dd}T${hh}${mi}${ss}Z`;
}

function buildBatchKey(sourceKey, index, runId) {
  const safeKey = sourceKey.replace(/[^a-zA-Z0-9_-]/g, '_');
  return `${getBatchPrefix(runId)}${safeKey}-${index}.txt`;
}

function getBatchPrefix(runId) {
  return `${BATCH_ROOT_PREFIX}${runId}/`;
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

function parsePositiveInt(raw, fallback, min) {
  const parsed = parseInt(raw ?? '', 10);
  if (isNaN(parsed) || parsed < min) return fallback;
  return parsed;
}

function parseAggregatorChunkLines(env) {
  return parsePositiveInt(env.BATCH_SIZE, AGGREGATOR_CHUNK_LINES, 1);
}

function isTopLevelParentRequest(record) {
  return String(record?.ParentRayID ?? '') === '00' && record?.WorkerSubrequest !== true && record?.WorkerSubrequest !== 'true';
}

function sanitizeStorageKey(key) {
  return String(key || '').replace(/[^a-zA-Z0-9_-]/g, '_');
}

function buildAggregateChunkId(sourceKey, index) {
  return `${sanitizeStorageKey(sourceKey)}-${index}`;
}

function buildFileMarkerKey(sourceKey) {
  return `file:${sanitizeStorageKey(sourceKey)}`;
}

function buildFinalBatchKey(runId, index) {
  return `${getBatchPrefix(runId)}batch-${String(index).padStart(8, '0')}.txt`;
}

function buildPendingBatchId(index) {
  return String(index).padStart(8, '0');
}

function createInitialAggregateStatus(runId) {
  return {
    run_id: runId,
    processed_chunks: 0,
    completed_files: 0,
    expected_files: null,
    emitted_batches: 0,
    emitted_lines: 0,
    line_count_tracking: 'exact',
    batch_line_count_avg: null,
    batch_line_count_min: null,
    batch_line_count_max: null,
    last_batch_line_count: null,
    next_batch_seq: 0,
    pending_batch_ids: [],
    pending_batch_count: 0,
    pending_buffer_lines: 0,
    finalized: false,
    finalized_at: null,
    last_chunk_id: null,
    last_source_key: null,
    last_batch_key: null,
    updated_at: null,
  };
}

function normalizeAggregateStatus(status, runId) {
  const normalized = {
    ...createInitialAggregateStatus(runId),
    ...(status && typeof status === 'object' ? status : {}),
    run_id: runId,
  };
  normalized.pending_batch_ids = Array.isArray(normalized.pending_batch_ids) ? normalized.pending_batch_ids : [];
  normalized.pending_batch_count = normalized.pending_batch_ids.length;

  const hasTrackedLineTotals = Boolean(
    status
    && typeof status === 'object'
    && Object.prototype.hasOwnProperty.call(status, 'emitted_lines')
  );
  if (!hasTrackedLineTotals && normalized.emitted_batches > 0) {
    normalized.emitted_lines = null;
    normalized.line_count_tracking = 'unavailable_legacy_run';
    normalized.batch_line_count_avg = null;
    normalized.batch_line_count_min = null;
    normalized.batch_line_count_max = null;
    normalized.last_batch_line_count = null;
  } else {
    normalized.line_count_tracking = normalized.line_count_tracking || 'exact';
  }
  return normalized;
}

function applyBatchTelemetry(state, lineCount) {
  if (state.line_count_tracking !== 'exact') return;

  const parsedLineCount = parseBatchLineCount(lineCount);
  if (parsedLineCount === null) return;

  state.emitted_batches += 1;
  state.emitted_lines += parsedLineCount;
  state.batch_line_count_avg = roundMetric(state.emitted_lines / state.emitted_batches, 2);
  state.batch_line_count_min = state.batch_line_count_min === null
    ? parsedLineCount
    : Math.min(state.batch_line_count_min, parsedLineCount);
  state.batch_line_count_max = state.batch_line_count_max === null
    ? parsedLineCount
    : Math.max(state.batch_line_count_max, parsedLineCount);
  state.last_batch_line_count = parsedLineCount;
}

function roundMetric(value, digits) {
  if (!Number.isFinite(value)) return null;
  return Number(value.toFixed(digits));
}

function getRunAggregatorStub(env, runId) {
  const id = env.RUN_AGGREGATOR.idFromName(runId);
  return env.RUN_AGGREGATOR.get(id);
}

async function sendChunkToAggregator(stub, sourceKey, chunkIndex, lines, run) {
  return callAggregator(stub, '/ingest', {
    runId: run.id,
    sourceKey,
    chunkId: buildAggregateChunkId(sourceKey, chunkIndex),
    lines,
  });
}

async function markSourceFileComplete(stub, sourceKey, run) {
  return callAggregator(stub, '/complete-file', {
    runId: run.id,
    sourceKey,
  });
}

async function isSourceFileComplete(env, runId, sourceKey) {
  if (!runId || !sourceKey) return false;
  const result = await callAggregator(getRunAggregatorStub(env, runId), '/is-file-complete', {
    runId,
    sourceKey,
  });
  return result?.complete === true;
}

async function reconcileAggregatorCompletedFiles(env, runId, completedFiles, expectedFiles) {
  return callAggregator(getRunAggregatorStub(env, runId), '/reconcile-completed-files', {
    runId,
    completedFiles,
    expectedFiles,
  });
}

async function finalizeAggregatorIfReady(env, runId, expectedFiles) {
  return callAggregator(getRunAggregatorStub(env, runId), '/finalize', {
    runId,
    expectedFiles,
  });
}

async function markBatchAsSent(env, runId, batchKey) {
  return callAggregator(getRunAggregatorStub(env, runId), '/mark-sent', {
    runId,
    batchKey,
  });
}

async function isBatchMarkedSent(env, runId, batchKey) {
  if (!runId) return false;
  const result = await callAggregator(getRunAggregatorStub(env, runId), '/is-sent', {
    runId,
    batchKey,
  });
  return result?.sent === true;
}

async function callAggregator(stub, path, payload) {
  const response = await stub.fetch(`https://run-aggregator${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json; charset=utf-8' },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Aggregator ${path} failed: HTTP ${response.status} ${text.substring(0, 200)}`);
  }

  return response.json();
}

async function writeFinalBatchAndEnqueue(batchKey, lines, env, runId) {
  const doneMarker = await env.RAW_BUCKET.head(`${batchKey}.done`).catch(() => null);
  if (doneMarker) return { duplicate: true };
  if (runId && await isBatchMarkedSent(env, runId, batchKey)) return { duplicate: true };

  const body = lines.join('\n') + '\n';
  await env.RAW_BUCKET.put(batchKey, body, {
    httpMetadata: { contentType: 'text/plain; charset=utf-8' },
  });
  try {
    await env.SEND_QUEUE.send({ key: batchKey, queuedAtMs: Date.now(), runId, lineCount: lines.length });
  } catch (error) {
    await env.RAW_BUCKET.delete(batchKey).catch(() => {});
    throw error;
  }
  return { duplicate: false };
}

async function loadPendingBatch(storage, pendingId) {
  return storage.get(`pending-batch:${pendingId}`);
}

async function deletePendingBatch(storage, pendingId) {
  await storage.delete(`pending-batch:${pendingId}`);
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

function createInitialRecoveryState() {
  return {
    prefix_index: 0,
    scan_start_after: null,
    last_checked_at: null,
    last_recovered_at: null,
    last_recovered_key: null,
    recovered_files: 0,
  };
}

function normalizeRecoveryState(recovery) {
  return {
    ...createInitialRecoveryState(),
    ...(recovery && typeof recovery === 'object' ? recovery : {}),
  };
}

function shouldRecoverIncompleteFiles(state, aggregate, nowMs = Date.now()) {
  const expectedFiles = Number.isFinite(aggregate?.expected_files)
    ? aggregate.expected_files
    : (state?.enqueued_count ?? 0);
  if (state?.status !== 'done' || aggregate?.finalized === true || expectedFiles <= 0) {
    return false;
  }
  if ((aggregate?.completed_files ?? 0) >= expectedFiles) {
    return false;
  }

  const lastProgressMs = maxDateMs(
    aggregate?.updated_at,
    state?.completed_at,
    state?.recovery?.last_recovered_at,
  );
  if (lastProgressMs === null) {
    return true;
  }
  return nowMs - lastProgressMs >= FINALIZE_RECOVERY_STALL_MS;
}

function maxDateMs(...values) {
  let max = null;
  for (const value of values) {
    const ms = Date.parse(value || '');
    if (!Number.isFinite(ms)) continue;
    max = max === null ? ms : Math.max(max, ms);
  }
  return max;
}

function isPendingRunArtifact(key) {
  return key.endsWith('.txt') || key.endsWith('.queued');
}

function canReinitializeFromState(state) {
  return state?.status === 'cleaned';
}

async function isPendingCleanupArtifact(env, runId, key) {
  if (key.endsWith('.queued')) return true;
  if (!key.endsWith('.txt')) return false;

  const doneMarker = await env.RAW_BUCKET.head(`${key}.done`).catch(() => null);
  if (doneMarker) return false;
  if (runId && await isBatchMarkedSent(env, runId, key)) return false;
  return true;
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
    aggregate: {
      ...(state.aggregate || createInitialAggregateStatus(state.run_id)),
      pending_batch_ids: [],
      pending_batch_count: 0,
      pending_buffer_lines: 0,
      finalized: true,
      finalized_at: state.aggregate?.finalized_at || cleanedAt,
      updated_at: cleanedAt,
    },
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

async function loadExistingStateRunId(env, config) {
  const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
  if (!obj) return null;

  try {
    const state = JSON.parse(await obj.text());
    if (state.config?.start !== config.start || state.config?.end !== config.end) {
      return null;
    }
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

// ─── 根据时间范围生成日期 prefix 列表（限定 R2 list 扫描范围）──────────────
function getR2PrefixesByDay(startMs, endMs, basePrefix) {
  const prefixes = [];
  const d = new Date(startMs);
  d.setUTCHours(0, 0, 0, 0);
  const endDay = new Date(endMs);
  endDay.setUTCHours(0, 0, 0, 0);
  let iter = 0;
  while (d.getTime() <= endDay.getTime() && iter++ < MAX_DAY_PREFIXES) {
    const yyyy = d.getUTCFullYear();
    const mm   = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd   = String(d.getUTCDate()).padStart(2, '0');
    prefixes.push(`${basePrefix}${yyyy}${mm}${dd}/`);
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return prefixes;
}

function log(env, level, msg) {
  if ((LOG_LEVELS[level] ?? 1) >= (LOG_LEVELS[env?.LOG_LEVEL] ?? 1)) {
    const fn = level === 'error' ? console.error : level === 'warn' ? console.warn : console.log;
    fn(`[BACKFILL][${level.toUpperCase()}] ${new Date().toISOString()} ${msg}`);
  }
}

export class RunAggregator extends DurableObjectBase {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === 'GET' && url.pathname === '/status') {
      return jsonResponse(await this.getPublicState());
    }

    const body = await request.json().catch(() => ({}));
    switch (url.pathname) {
      case '/ingest':
        return jsonResponse(await this.ingest(body));
      case '/complete-file':
        return jsonResponse(await this.completeFile(body));
      case '/is-file-complete':
        return jsonResponse(await this.isFileComplete(body));
      case '/reconcile-completed-files':
        return jsonResponse(await this.reconcileCompletedFiles(body));
      case '/finalize':
        return jsonResponse(await this.finalize(body));
      case '/mark-sent':
        return jsonResponse(await this.markSent(body));
      case '/is-sent':
        return jsonResponse(await this.isSent(body));
      default:
        return new Response('Not Found', { status: 404 });
    }
  }

  async ingest(body) {
    const runId = String(body?.runId || '').trim();
    const chunkId = String(body?.chunkId || '').trim();
    const sourceKey = String(body?.sourceKey || '').trim();
    const lines = Array.isArray(body?.lines) ? body.lines.filter((line) => typeof line === 'string' && line.length > 0) : [];

    if (!runId || !chunkId || !sourceKey) {
      throw new Error('Aggregator ingest requires runId, chunkId and sourceKey.');
    }

    const chunkMarkerKey = `chunk:${chunkId}`;
    let state;
    let alreadyProcessed = false;

    await this.withStorageTxn(async (txn) => {
      state = normalizeAggregateStatus(await txn.get('state'), runId);
      if (await txn.get(chunkMarkerKey)) {
        alreadyProcessed = true;
        return;
      }

      let buffer = await this.loadBufferFrom(txn);
      buffer.push(...lines);

      while (buffer.length >= this.getBatchSize()) {
        const batchLines = buffer.splice(0, this.getBatchSize());
        const seq = state.next_batch_seq++;
        const pendingId = buildPendingBatchId(seq);
        const batchKey = buildFinalBatchKey(runId, seq);
        await txn.put(`pending-batch:${pendingId}`, { id: pendingId, batchKey, lines: batchLines });
        state.pending_batch_ids.push(pendingId);
      }

      state.processed_chunks += 1;
      state.pending_batch_count = state.pending_batch_ids.length;
      state.pending_buffer_lines = buffer.length;
      state.last_chunk_id = chunkId;
      state.last_source_key = sourceKey;
      state.updated_at = new Date().toISOString();

      await this.saveBufferTo(txn, buffer);
      await txn.put(chunkMarkerKey, '1');
      await txn.put('state', state);
    });

    if (alreadyProcessed) {
      state = await this.flushPendingBatches(state);
      return this.toPublicState(state);
    }

    state = await this.flushPendingBatches(state);
    return this.toPublicState(state);
  }

  async completeFile(body) {
    const runId = String(body?.runId || '').trim();
    const sourceKey = String(body?.sourceKey || '').trim();
    if (!runId || !sourceKey) {
      throw new Error('Aggregator complete-file requires runId and sourceKey.');
    }

    const fileMarkerKey = buildFileMarkerKey(sourceKey);
    let state;
    await this.withStorageTxn(async (txn) => {
      state = normalizeAggregateStatus(await txn.get('state'), runId);
      if (await txn.get(fileMarkerKey)) {
        return;
      }
      await txn.put(fileMarkerKey, '1');
      state.completed_files += 1;
      state.last_source_key = sourceKey;
      state.updated_at = new Date().toISOString();
      await txn.put('state', state);
    });
    state = await this.flushPendingBatches(state);
    return this.toPublicState(state);
  }

  async isFileComplete(body) {
    const runId = String(body?.runId || '').trim();
    const sourceKey = String(body?.sourceKey || '').trim();
    if (!runId || !sourceKey) {
      throw new Error('Aggregator is-file-complete requires runId and sourceKey.');
    }

    return {
      run_id: runId,
      sourceKey,
      complete: Boolean(await this.ctx.storage.get(buildFileMarkerKey(sourceKey))),
    };
  }

  async reconcileCompletedFiles(body) {
    const runId = String(body?.runId || '').trim();
    const completedFiles = Number(body?.completedFiles);
    const expectedFiles = Number(body?.expectedFiles);
    if (!runId) {
      throw new Error('Aggregator reconcile-completed-files requires runId.');
    }

    let state;
    await this.withStorageTxn(async (txn) => {
      state = normalizeAggregateStatus(await txn.get('state'), runId);
      if (Number.isFinite(completedFiles) && completedFiles >= 0) {
        state.completed_files = Math.max(state.completed_files, completedFiles);
      }
      if (Number.isFinite(expectedFiles) && expectedFiles >= 0) {
        state.expected_files = Math.max(state.expected_files ?? 0, expectedFiles);
      }

      if (!state.finalized && state.expected_files !== null && state.completed_files >= state.expected_files) {
        let buffer = await this.loadBufferFrom(txn);
        if (buffer.length > 0) {
          const seq = state.next_batch_seq++;
          const pendingId = buildPendingBatchId(seq);
          const batchKey = buildFinalBatchKey(runId, seq);
          await txn.put(`pending-batch:${pendingId}`, { id: pendingId, batchKey, lines: buffer });
          state.pending_batch_ids.push(pendingId);
          buffer = [];
        }
        await this.saveBufferTo(txn, buffer);
        state.pending_batch_count = state.pending_batch_ids.length;
        state.pending_buffer_lines = 0;
        state.finalized = true;
        state.finalized_at = new Date().toISOString();
        state.updated_at = state.finalized_at;
      }

      await txn.put('state', state);
    });

    state = await this.flushPendingBatches(state);
    return this.toPublicState(state);
  }

  async finalize(body) {
    const runId = String(body?.runId || '').trim();
    const expectedFiles = Number(body?.expectedFiles);
    if (!runId) {
      throw new Error('Aggregator finalize requires runId.');
    }

    let state;
    await this.withStorageTxn(async (txn) => {
      state = normalizeAggregateStatus(await txn.get('state'), runId);
      if (Number.isFinite(expectedFiles) && expectedFiles >= 0) {
        state.expected_files = Math.max(state.expected_files ?? 0, expectedFiles);
      }

      if (!state.finalized && state.expected_files !== null && state.completed_files >= state.expected_files) {
        let buffer = await this.loadBufferFrom(txn);
        if (buffer.length > 0) {
          const seq = state.next_batch_seq++;
          const pendingId = buildPendingBatchId(seq);
          const batchKey = buildFinalBatchKey(runId, seq);
          await txn.put(`pending-batch:${pendingId}`, { id: pendingId, batchKey, lines: buffer });
          state.pending_batch_ids.push(pendingId);
          buffer = [];
        }
        await this.saveBufferTo(txn, buffer);
        state.pending_batch_count = state.pending_batch_ids.length;
        state.pending_buffer_lines = 0;
        state.finalized = true;
        state.finalized_at = new Date().toISOString();
        state.updated_at = state.finalized_at;
      }

      await txn.put('state', state);
    });

    state = await this.flushPendingBatches(state);
    return this.toPublicState(state);
  }

  async markSent(body) {
    const runId = String(body?.runId || '').trim();
    const batchKey = String(body?.batchKey || '').trim();
    if (!runId || !batchKey) {
      throw new Error('Aggregator mark-sent requires runId and batchKey.');
    }

    await this.ctx.storage.put(`sent:${batchKey}`, '1');
    return { run_id: runId, batchKey, sent: true };
  }

  async isSent(body) {
    const runId = String(body?.runId || '').trim();
    const batchKey = String(body?.batchKey || '').trim();
    if (!runId || !batchKey) {
      throw new Error('Aggregator is-sent requires runId and batchKey.');
    }

    return {
      run_id: runId,
      batchKey,
      sent: Boolean(await this.ctx.storage.get(`sent:${batchKey}`)),
    };
  }

  async loadState(runId) {
    const state = await this.ctx.storage.get('state');
    return normalizeAggregateStatus(state, runId);
  }

  async saveState(state) {
    await this.ctx.storage.put('state', state);
  }

  async withStorageTxn(fn) {
    if (typeof this.ctx.storage.transaction === 'function') {
      return this.ctx.storage.transaction(fn);
    }
    return fn(this.ctx.storage);
  }

  async loadBuffer() {
    const buffer = await this.ctx.storage.get('buffer');
    return Array.isArray(buffer) ? buffer : [];
  }

  async loadBufferFrom(storage) {
    const buffer = await storage.get('buffer');
    return Array.isArray(buffer) ? buffer : [];
  }

  async saveBuffer(buffer) {
    if (buffer.length === 0) {
      await this.ctx.storage.delete('buffer');
      return;
    }
    await this.ctx.storage.put('buffer', buffer);
  }

  async saveBufferTo(storage, buffer) {
    if (buffer.length === 0) {
      if (typeof storage.delete === 'function') {
        await storage.delete('buffer');
      }
      return;
    }
    await storage.put('buffer', buffer);
  }

  getBatchSize() {
    return parsePositiveInt(this.env.BATCH_SIZE, AGGREGATOR_CHUNK_LINES, 1);
  }

  async flushPendingBatches(state) {
    const currentIds = Array.isArray(state.pending_batch_ids) ? [...state.pending_batch_ids] : [];
    if (currentIds.length === 0) {
      state.pending_batch_count = 0;
      return state;
    }

    const remainingIds = [];
    let changed = false;

    for (let i = 0; i < currentIds.length; i++) {
      const pendingId = currentIds[i];
      const pending = await loadPendingBatch(this.ctx.storage, pendingId);
      if (!pending) {
        changed = true;
        continue;
      }

      try {
        const { duplicate } = await writeFinalBatchAndEnqueue(pending.batchKey, pending.lines, this.env, state.run_id);
        await deletePendingBatch(this.ctx.storage, pendingId);
        if (!duplicate) {
          applyBatchTelemetry(state, pending.lineCount ?? pending.lines?.length);
        }
        state.last_batch_key = pending.batchKey;
        changed = true;
      } catch (error) {
        log(this.env, 'warn', `Aggregator pending batch flush failed for ${pending.batchKey}: ${error.message}`);
        remainingIds.push(pendingId, ...currentIds.slice(i + 1));
        break;
      }
    }

    if (remainingIds.length === 0 && currentIds.length > 0 && !changed) {
      state.pending_batch_ids = currentIds;
      state.pending_batch_count = currentIds.length;
      return state;
    }

    if (remainingIds.length === 0 && changed && currentIds.length === state.pending_batch_ids.length) {
      state.pending_batch_ids = [];
    } else if (remainingIds.length > 0 || changed) {
      state.pending_batch_ids = remainingIds;
    }

    state.pending_batch_count = state.pending_batch_ids.length;
    if (changed || remainingIds.length !== currentIds.length) {
      state.updated_at = new Date().toISOString();
      await this.saveState(state);
    }
    return state;
  }

  async getPublicState(runId) {
    return this.toPublicState(await this.loadState(runId));
  }

  toPublicState(state) {
    return {
      run_id: state.run_id,
      processed_chunks: state.processed_chunks,
      completed_files: state.completed_files,
      expected_files: state.expected_files,
      emitted_batches: state.emitted_batches,
      emitted_lines: state.emitted_lines,
      line_count_tracking: state.line_count_tracking,
      batch_line_count_avg: state.batch_line_count_avg,
      batch_line_count_min: state.batch_line_count_min,
      batch_line_count_max: state.batch_line_count_max,
      last_batch_line_count: state.last_batch_line_count,
      pending_batch_count: state.pending_batch_count,
      pending_buffer_lines: state.pending_buffer_lines,
      finalized: state.finalized,
      finalized_at: state.finalized_at,
      updated_at: state.updated_at,
    };
  }
}

export const __test = {
  applyBatchTelemetry,
  buildFileMarkerKey,
  buildParseQueueMessage,
  buildNotStartedStatusResponse,
  buildPublicStatusMessage,
  buildPublicStatusResponse,
  createInitialCleanupState,
  createInitialRecoveryState,
  createInitialAggregateStatus,
  derivePublicStatusStage,
  buildBatchKey,
  buildRunId,
  buildRunInstanceId,
  buildAggregateChunkId,
  buildFinalBatchKey,
  isTopLevelParentRequest,
  canReinitializeFromState,
  compactStateAfterCleanup,
  extractRunIdFromBatchKey,
  extractFileTimeRange,
  getBatchPrefix,
  normalizeAggregateStatus,
  isRecordInRunWindow,
  isR2ListComplete,
  isRunCleaned,
  isPendingCleanupArtifact,
  isPendingRunArtifact,
  normalizeCleanupState,
  normalizeRecoveryState,
  parseAggregatorChunkLines,
  normalizeRunContext,
  parseConfig,
  parseSendTimeoutMs,
  shouldRecoverIncompleteFiles,
  resolveRunContext,
  deleteR2Keys,
  loadExistingStateRunId,
  writeBatchAndEnqueue,
};

// ─── MD5 (RFC 1321, Workers SubtleCrypto不支持MD5) ─────────────────────────
function md5(str) {
  const add = (x,y)=>{const l=(x&0xffff)+(y&0xffff);return(((x>>16)+(y>>16)+(l>>16))<<16)|(l&0xffff);};
  const rol  = (n,c)=>(n<<c)|(n>>>(32-c));
  const cmn  = (q,a,b,x,s,t)=>add(rol(add(add(a,q),add(x,t)),s),b);
  const ff   = (a,b,c,d,x,s,t)=>cmn((b&c)|(~b&d),a,b,x,s,t);
  const gg   = (a,b,c,d,x,s,t)=>cmn((b&d)|(c&~d),a,b,x,s,t);
  const hh   = (a,b,c,d,x,s,t)=>cmn(b^c^d,a,b,x,s,t);
  const ii   = (a,b,c,d,x,s,t)=>cmn(c^(b|~d),a,b,x,s,t);
  const utf8 = unescape(encodeURIComponent(str));
  const len  = utf8.length;
  const nb   = ((len+8)>>>6)+1;
  const blk  = new Array(nb*16).fill(0);
  for(let i=0;i<len;i++) blk[i>>2]|=utf8.charCodeAt(i)<<(i%4*8);
  blk[len>>2]|=0x80<<(len%4*8);
  blk[nb*16-2]=len*8;
  let a=1732584193,b=-271733879,c=-1732584194,d=271733878;
  for(let i=0;i<blk.length;i+=16){
    const[pa,pb,pc,pd]=[a,b,c,d];
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
  return[a,b,c,d].map(n=>[0,1,2,3].map(j=>
    ((n>>(j*8+4))&0xf).toString(16)+((n>>(j*8))&0xf).toString(16)
  ).join('')).join('');
}
