/**
 * Cloudflare Workers — Logpush Historical Backfill Worker
 *
 * 专用于补传指定时间范围 [BACKFILL_START_TIME, BACKFILL_END_TIME] 的 Logpush 日志。
 * 和生产 worker(ctyun-logpush)完全独立：独立 queues、独立 processed-backfill/ 前缀、
 * 独立 Sender 限速（max_concurrency=1 × max_batch_size=1 + 代码级 200ms/invocation 节流
 *   → 严格 ≤ 5 batch/s = 5,000 lines/s，不受接收端 RTT 波动影响）
 * 共享同一个 R2 bucket：只读 logs/ 前缀，写入独立的 processed-backfill/ 和 backfill-state/。
 *
 * Architecture:
 *   scheduled(每分钟) → 扫 R2 logs/ → rate-limited 入 parse-queue-backfill
 *                           ↓
 *                      Backfill Parser → processed-backfill/ → send-queue-backfill
 *                           ↓
 *                      Backfill Sender (max_concurrency=1, max_batch_size=1,
 *                                       code-throttled to ≤ 5 invocations/s)
 *                           ↓
 *                      接收端服务器（与生产同一个 endpoint，共用同样的 CTYUN_* secrets）
 *
 * Env Secrets : CTYUN_ENDPOINT, CTYUN_PRIVATE_KEY, CTYUN_URI_EDGE
 * Env Vars    : BATCH_SIZE, LOG_LEVEL, LOG_PREFIX, PARSE_QUEUE_NAME, SEND_QUEUE_NAME,
 *               R2_BUCKET_NAME, BACKFILL_START_TIME, BACKFILL_END_TIME,
 *               BACKFILL_RATE, BACKFILL_ENABLED
 *
 * HTTP Endpoints:
 *   GET /backfill/status — 返回 R2 里的 backfill-state/progress.json
 */
'use strict';

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
const SEND_STATS_KEY        = 'backfill-state/send-stats.json';
const MAX_RANGE_HOURS       = 48;
const WALL_TIME_BUDGET_MS   = 55_000;       // 留 5s 缓冲给 saveState
const CLEANUP_GRACE_MS      = 24 * 60 * 60_000;
const MAX_RATE              = 100;
const DEFAULT_RATE          = 5;
const LIST_LIMIT            = 1000;
const MAX_DAY_PREFIXES      = 5;
const DEFAULT_SEND_TIMEOUT_MS = 300_000;
const MIN_SEND_TIMEOUT_MS     = 1_000;
const LOG_LEVELS            = Object.freeze({ debug: 0, info: 1, warn: 2, error: 3 });

// ⭐ Sender 节流：每次 handleSendQueue invocation 至少花费 MIN_SENDER_INVOCATION_MS
// 配合 max_concurrency=1 (无并行 invocation) + max_batch_size=1 (每 invocation 1 条 msg)
//   → 严格保证 ≤ 5 invocations/s = 5 msg/s = 5,000 lines/s 的发送上限
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

// ─── Parser: R2原始文件 → 流式解析转换 → R2 processed-backfill/ → send-queue-backfill ──
// 相比生产 Parser：
//   - 逐条按 [BACKFILL_START_TIME, BACKFILL_END_TIME] 裁切，避免边界文件整段重放
//   - 写入 processed-backfill/<run-id>/（不同 backfill run 互不污染）
async function handleParseQueue(batch, env) {
  for (const msg of batch.messages) {
    await processFile(msg, env);
  }
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

  log(env, 'info', `Parsing: ${key}`);
  try {
    const object = await env.RAW_BUCKET.get(key);
    if (!object) { log(env, 'warn', `Not in R2: ${key}`); msg.ack(); return; }
    const batchSize = parseInt(env.BATCH_SIZE || '1000', 10);
    let lines = [], batchIdx = 0, lineCount = 0, errCount = 0, skipped = 0, invalidTs = 0;
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
      try {
        lines.push(transformEdge(record));
      } catch (e) {
        errCount++;
        log(env, 'warn', `Transform err line ${lineCount}: ${e.message}`);
        return;
      }
      if (lines.length >= batchSize) {
        await writeBatchAndEnqueue(lines, key, batchIdx++, env, run);
        lines = [];
      }
    });
    if (lines.length > 0) await writeBatchAndEnqueue(lines, key, batchIdx++, env, run);
    log(env, 'info', `Done: ${key} | lines=${lineCount} batches=${batchIdx} errors=${errCount} skipped=${skipped} invalid_ts=${invalidTs} run=${run.id}`);
    msg.ack();
  } catch (err) {
    log(env, 'error', `Failed: ${key}: ${err.message}`);
    msg.retry();
  }
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
    await env.SEND_QUEUE.send({ key: batchKey, queuedAtMs: Date.now(), runId: run.id });
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
  // 这保证了 "invocation/s" 的代码级上限，配合 max_concurrency=1 严格限制吞吐
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
      await recordSendOutcome(env, {
        runId,
        key,
        outcome: 'timeout',
        ackMs: timeoutMs,
        queueWaitMs,
        error: `Send timeout after ${timeoutMs}ms`,
      });
      throw new Error(`Send timeout after ${timeoutMs}ms`);
    }
    await recordSendOutcome(env, {
      runId,
      key,
      outcome: 'other_error',
      ackMs: Date.now() - fetchStartedAt,
      queueWaitMs,
      error: e.message,
    });
    throw e;
  } finally {
    clearTimeout(timeout);
  }
  const ackMs = Date.now() - fetchStartedAt;
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    await recordSendOutcome(env, {
      runId,
      key,
      outcome: 'http_error',
      ackMs,
      queueWaitMs,
      error: `HTTP ${resp.status} ${resp.statusText} | ${text.substring(0, 200)}`,
    });
    throw new Error(`HTTP ${resp.status} ${resp.statusText} | ${text.substring(0, 200)}`);
  }
  await resp.body?.cancel().catch(() => {});
  await recordSendOutcome(env, {
    runId,
    key,
    outcome: 'success',
    ackMs,
    queueWaitMs,
    bytes: object.size ?? null,
  });
  log(env, 'info', `Sent ${object.size ?? '?'} bytes (uncompressed) → HTTP ${resp.status} | ack_ms=${ackMs}${queueWaitMs === null ? '' : ` queue_wait_ms=${queueWaitMs}`} | ${key}`);
  let wroteDone = false;
  await env.RAW_BUCKET.put(`${key}.done`, '1', {
    httpMetadata: { contentType: 'text/plain' },
  }).then(() => {
    wroteDone = true;
  }).catch((e) => {
    log(env, 'warn', `Done marker write failed (keeping batch file for investigation): ${key}: ${e.message}`);
  });
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
    log(env, 'debug', `Deleted: ${key}`);
  }
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
        return state;
      }
      log(env, 'info', `Config changed (was ${state.config?.start}→${state.config?.end}, now ${config.start}→${config.end}). Re-initializing state.`);
    } catch (e) {
      log(env, 'warn', `State file corrupted, re-initializing: ${e.message}`);
    }
  }
  return {
    config:           { start: config.start, end: config.end, rate: config.rate },
    run_id:           buildRunInstanceId(config.startMs, config.endMs),
    phase:            'enqueue',
    status:           'running',
    started_at:       new Date().toISOString(),
    enqueue_progress: {},      // { [prefix]: { start_after, done, enqueued } }
    enqueued_count:   0,
    last_cron_at:     null,
    completed_at:     null,
    cleanup:          createInitialCleanupState(),
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
          await env.PARSE_QUEUE.send({
            bucket: config.bucketName,
            object: { key },
            run: {
              id:      state.run_id,
              start:   config.start,
              end:     config.end,
              startMs: config.startMs,
              endMs:   config.endMs,
            }
          });
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

      if (list.objects.length < LIST_LIMIT) {
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
      if (isPendingRunArtifact(obj.key)) {
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

    if (page.objects.length < LIST_LIMIT) {
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

async function deleteRunArtifacts(env, state, startedAt) {
  const prefix = getBatchPrefix(state.run_id);
  let changed = false;
  let startAfter = state.cleanup.delete_start_after || undefined;

  while (Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
    const page = await env.RAW_BUCKET.list({ prefix, startAfter, limit: LIST_LIMIT });

    if (page.objects.length === 0) {
      await env.RAW_BUCKET.delete(SEND_STATS_KEY).catch(() => {});
      Object.assign(state, compactStateAfterCleanup(state));
      log(env, 'info', `Auto-cleanup complete for run ${state.run_id}. Removed temporary backfill artifacts under ${prefix}.`);
      return true;
    }

    const keys = page.objects.map((obj) => obj.key);
    await deleteR2Keys(env, keys);
    state.cleanup.deleted_objects += keys.length;
    state.cleanup.delete_start_after = page.objects[page.objects.length - 1].key;
    changed = true;

    if (page.objects.length < LIST_LIMIT) {
      await env.RAW_BUCKET.delete(SEND_STATS_KEY).catch(() => {});
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

  try {
    await env.RAW_BUCKET.delete(keys);
  } catch {
    await Promise.allSettled(keys.map((key) => env.RAW_BUCKET.delete(key)));
  }
}

// ─── HTTP fetch handler ─────────────────────────────────────────────────────
async function handleFetch(request, env) {
  const url = new URL(request.url);

  if (url.pathname === '/backfill/status') {
    const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
    if (!obj) {
      return jsonResponse({
        status: 'not_started',
        message: 'No backfill state found yet. Ensure BACKFILL_START_TIME and BACKFILL_END_TIME are set in wrangler-backfill.toml and wait for the next Cron trigger (within 1 minute).',
        config_hint: {
          BACKFILL_START_TIME: env.BACKFILL_START_TIME || '(unset)',
          BACKFILL_END_TIME:   env.BACKFILL_END_TIME   || '(unset)',
          BACKFILL_RATE:       env.BACKFILL_RATE       || String(DEFAULT_RATE),
          BACKFILL_ENABLED:    env.BACKFILL_ENABLED    || '(default: true)',
        }
      });
    }
    try {
      const data = JSON.parse(await obj.text());
      data.send_stats = await readSendStats(env, data.run_id);
      return jsonResponse(data);
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

function parsePositiveInt(raw, fallback, min) {
  const parsed = parseInt(raw ?? '', 10);
  if (isNaN(parsed) || parsed < min) return fallback;
  return parsed;
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

function parseQueueWaitMs(queuedAtMs) {
  const queuedAt = Number(queuedAtMs);
  if (!Number.isFinite(queuedAt) || queuedAt <= 0) return null;
  return Math.max(0, Date.now() - queuedAt);
}

async function readSendStats(env, runId) {
  if (!runId) return null;

  const obj = await env.RAW_BUCKET.get(SEND_STATS_KEY).catch(() => null);
  if (!obj) return null;

  try {
    const stats = JSON.parse(await obj.text());
    return stats.run_id === runId ? stats : null;
  } catch {
    return null;
  }
}

async function recordSendOutcome(env, outcome) {
  if (!outcome.runId) return;

  try {
    const current = (await readSendStats(env, outcome.runId)) || createSendStats(outcome.runId);
    const next = updateSendStats(current, outcome);
    await env.RAW_BUCKET.put(SEND_STATS_KEY, JSON.stringify(next, null, 2), {
      httpMetadata: { contentType: 'application/json; charset=utf-8' }
    });
  } catch (e) {
    log(env, 'warn', `Failed to record send stats for ${outcome.key}: ${e.message}`);
  }
}

function createSendStats(runId) {
  return {
    run_id: runId,
    success_count: 0,
    timeout_count: 0,
    http_error_count: 0,
    other_error_count: 0,
    ack_count: 0,
    ack_ms_sum: 0,
    ack_ms_avg: null,
    ack_ms_min: null,
    ack_ms_max: null,
    queue_wait_count: 0,
    queue_wait_ms_sum: 0,
    queue_wait_ms_avg: null,
    queue_wait_ms_min: null,
    queue_wait_ms_max: null,
    last_ack_ms: null,
    last_queue_wait_ms: null,
    last_outcome: null,
    last_error: null,
    last_key: null,
    last_bytes: null,
    updated_at: null,
  };
}

function updateSendStats(stats, outcome) {
  const next = { ...stats };
  next.updated_at = new Date().toISOString();
  next.last_outcome = outcome.outcome;
  next.last_error = outcome.error ?? null;
  next.last_key = outcome.key;
  next.last_bytes = outcome.bytes ?? null;

  if (outcome.outcome === 'success') next.success_count += 1;
  else if (outcome.outcome === 'timeout') next.timeout_count += 1;
  else if (outcome.outcome === 'http_error') next.http_error_count += 1;
  else next.other_error_count += 1;

  if (Number.isFinite(outcome.ackMs)) {
    next.ack_count += 1;
    next.ack_ms_sum += outcome.ackMs;
    next.ack_ms_avg = Math.round(next.ack_ms_sum / next.ack_count);
    next.ack_ms_min = next.ack_ms_min === null ? outcome.ackMs : Math.min(next.ack_ms_min, outcome.ackMs);
    next.ack_ms_max = next.ack_ms_max === null ? outcome.ackMs : Math.max(next.ack_ms_max, outcome.ackMs);
    next.last_ack_ms = outcome.ackMs;
  }

  if (Number.isFinite(outcome.queueWaitMs)) {
    next.queue_wait_count += 1;
    next.queue_wait_ms_sum += outcome.queueWaitMs;
    next.queue_wait_ms_avg = Math.round(next.queue_wait_ms_sum / next.queue_wait_count);
    next.queue_wait_ms_min = next.queue_wait_ms_min === null ? outcome.queueWaitMs : Math.min(next.queue_wait_ms_min, outcome.queueWaitMs);
    next.queue_wait_ms_max = next.queue_wait_ms_max === null ? outcome.queueWaitMs : Math.max(next.queue_wait_ms_max, outcome.queueWaitMs);
    next.last_queue_wait_ms = outcome.queueWaitMs;
  }

  return next;
}

export const __test = {
  createSendStats,
  createInitialCleanupState,
  buildBatchKey,
  buildRunId,
  buildRunInstanceId,
  compactStateAfterCleanup,
  extractRunIdFromBatchKey,
  extractFileTimeRange,
  getBatchPrefix,
  isRecordInRunWindow,
  isRunCleaned,
  isPendingRunArtifact,
  normalizeCleanupState,
  parseQueueWaitMs,
  normalizeRunContext,
  parseConfig,
  parseSendTimeoutMs,
  resolveRunContext,
  loadExistingStateRunId,
  updateSendStats,
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
