/**
 * Cloudflare Workers — Logpush Historical Backfill Worker
 *
 * 专用于补传指定时间范围 [BACKFILL_START_TIME, BACKFILL_END_TIME] 的 Logpush 日志。
 * 和生产 worker(ctyun-logpush)完全独立：独立 queues、独立 processed-backfill/ 前缀、
 * 独立 Sender 限速（max_concurrency=1 × max_batch_size=1 ≈ 5 batch/s 稳定匀速）。
 * 共享同一个 R2 bucket：只读 logs/ 前缀，写入独立的 processed-backfill/ 和 backfill-state/。
 *
 * Architecture:
 *   scheduled(每分钟) → 扫 R2 logs/ → rate-limited 入 parse-queue-backfill
 *                           ↓
 *                      Backfill Parser → processed-backfill/ → send-queue-backfill
 *                           ↓
 *                      Backfill Sender (max_concurrency=1, ~5 batch/s 匀速)
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
const BATCH_PREFIX          = 'processed-backfill/';
const STATE_KEY             = 'backfill-state/progress.json';
const MAX_RANGE_HOURS       = 48;
const WALL_TIME_BUDGET_MS   = 55_000;       // 留 5s 缓冲给 saveState
const MAX_RATE              = 100;
const DEFAULT_RATE          = 5;
const LIST_LIMIT            = 1000;
const MAX_DAY_PREFIXES      = 5;
const LOG_LEVELS            = Object.freeze({ debug: 0, info: 1, warn: 2, error: 3 });

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
//   - 无 PUSH_START_TIME 过滤（backfill 不需要）
//   - 写入 BATCH_PREFIX = 'processed-backfill/'（独立前缀）
async function handleParseQueue(batch, env) {
  await Promise.allSettled(batch.messages.map(msg => processFile(msg, env)));
}

async function processFile(msg, env) {
  const key = msg.body?.object?.key;
  if (!key) {
    log(env, 'warn', `No object.key: ${JSON.stringify(msg.body)}`);
    msg.ack();
    return;
  }

  log(env, 'info', `Parsing: ${key}`);
  try {
    const object = await env.RAW_BUCKET.get(key);
    if (!object) { log(env, 'warn', `Not in R2: ${key}`); msg.ack(); return; }
    const batchSize = parseInt(env.BATCH_SIZE || '1000', 10);
    let lines = [], batchIdx = 0, lineCount = 0, errCount = 0;
    await streamParseNdjsonGzip(object.body, async (record) => {
      lineCount++;
      try {
        lines.push(transformEdge(record));
      } catch (e) {
        errCount++;
        log(env, 'warn', `Transform err line ${lineCount}: ${e.message}`);
        return;
      }
      if (lines.length >= batchSize) {
        await writeBatchAndEnqueue(lines, key, batchIdx++, env);
        lines = [];
      }
    });
    if (lines.length > 0) await writeBatchAndEnqueue(lines, key, batchIdx++, env);
    log(env, 'info', `Done: ${key} | lines=${lineCount} batches=${batchIdx} errors=${errCount}`);
    msg.ack();
  } catch (err) {
    log(env, 'error', `Failed: ${key}: ${err.message}`);
    msg.retry();
  }
}

async function writeBatchAndEnqueue(lines, sourceKey, index, env) {
  const safeKey  = sourceKey.replace(/[^a-zA-Z0-9_-]/g, '_');
  const batchKey = `${BATCH_PREFIX}${safeKey}-${index}.txt`;

  // 幂等检查：该 batch 已被 Backfill Sender 成功发送过时跳过
  const doneMarker = await env.RAW_BUCKET.head(`${batchKey}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'debug', `Batch already sent (skip): ${batchKey}`);
    return;
  }
  const body = lines.join('\n') + '\n';
  await env.RAW_BUCKET.put(batchKey, body, {
    httpMetadata: { contentType: 'text/plain; charset=utf-8' },
  });
  try {
    await env.SEND_QUEUE.send({ key: batchKey });
  } catch (e) {
    // 入队失败，回滚 R2 文件，让 parse-queue-backfill 的 retry 机制干净重试
    await env.RAW_BUCKET.delete(batchKey).catch(() => {});
    throw e;
  }
  log(env, 'debug', `Queued: ${batchKey} (${lines.length} lines)`);
}

// ─── Sender: processed-backfill/ → Gzip → MD5鉴权 → POST → 删除临时文件 ──────
// 与生产 Sender 逻辑完全一致，只是处理 BATCH_PREFIX 不同的文件
async function handleSendQueue(batch, env) {
  const results = await Promise.allSettled(
    batch.messages.map(msg => sendBatch(msg, env))
  );
  results.forEach((r, i) => {
    if (r.status === 'fulfilled') batch.messages[i].ack();
    else { log(env, 'warn', `Send failed, retry: ${r.reason}`); batch.messages[i].retry(); }
  });
}

async function sendBatch(msg, env) {
  const { key } = msg.body;
  if (!key) throw new Error(`Invalid message: ${JSON.stringify(msg.body)}`);
  const doneMarker = await env.RAW_BUCKET.head(`${key}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'info', `Already sent (skip duplicate): ${key}`);
    return;
  }
  const object = await env.RAW_BUCKET.get(key);
  if (!object) { log(env, 'warn', `Batch not found (may be sent or rolled back): ${key}`); return; }
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
  const resp = await fetch(buildAuthUrl(endpoint, uri, privateKey), fetchInit);
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`HTTP ${resp.status} ${resp.statusText} | ${text.substring(0, 200)}`);
  }
  await resp.body?.cancel().catch(() => {});
  log(env, 'info', `Sent ${object.size ?? '?'} bytes (uncompressed) → HTTP ${resp.status} | ${key}`);
  await env.RAW_BUCKET.put(`${key}.done`, '1', {
    httpMetadata: { contentType: 'text/plain' },
  }).catch((e) => {
    log(env, 'warn', `Done marker write failed (may cause duplicate on retry): ${key}: ${e.message}`);
  });
  await env.RAW_BUCKET.delete(key).catch((e) => {
    log(env, 'warn', `Delete failed (will be cleaned by lifecycle): ${key}: ${e.message}`);
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

    if (state.status === 'done') {
      log(env, 'info', `Backfill done. enqueued=${state.enqueued_count}, completed_at=${state.completed_at}. To re-run: change BACKFILL_START_TIME/END_TIME and redeploy, OR delete R2 object '${STATE_KEY}'.`);
      return;
    }

    let changed = false;
    if (state.phase === 'enqueue' && Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
      changed = (await runEnqueue(env, state, config, startedAt)) || changed;
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
        return state;
      }
      log(env, 'info', `Config changed (was ${state.config?.start}→${state.config?.end}, now ${config.start}→${config.end}). Re-initializing state.`);
    } catch (e) {
      log(env, 'warn', `State file corrupted, re-initializing: ${e.message}`);
    }
  }
  return {
    config:           { start: config.start, end: config.end, rate: config.rate },
    phase:            'enqueue',
    status:           'running',
    started_at:       new Date().toISOString(),
    enqueue_progress: {},      // { [prefix]: { start_after, done, enqueued } }
    enqueued_count:   0,
    last_cron_at:     null,
    completed_at:     null
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
        if (key.startsWith(BATCH_PREFIX))         continue;
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
            object: { key }
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
    log(env, 'info', `🎉 Backfill ENQUEUE COMPLETE! enqueued=${state.enqueued_count} files over ${durMin}min of cron activity. Backfill Parser/Sender will continue processing asynchronously at ~5 batch/s. Monitor send-queue-backfill backlog for actual delivery completion.`);
  }

  return true;
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
    /* 58 */ '2cee6ba6ff8247a385902ddf5686df0c',
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
