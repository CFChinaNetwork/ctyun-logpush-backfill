'use strict';

/**
 * Lightweight backfill pipeline:
 *   scheduled -> parse-queue-backfill -> parser -> processed-backfill/<run-id>/
 *   -> send-queue-backfill -> sender -> customer endpoint
 *
 * This keeps the customer-facing requirements only:
 * - precise [BACKFILL_START_TIME, BACKFILL_END_TIME] replay
 * - drop Worker subrequests
 * - transform to partner format
 * - authenticated POST with configurable sender ceiling
 *
 * It intentionally removes the hot-path Durable Object aggregation layer and the
 * related finalize/recovery logic. One raw file may therefore end with one
 * underfilled batch, which is operationally cheaper than coordinating every
 * chunk through a single Durable Object.
 */

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

const BATCH_ROOT_PREFIX       = 'processed-backfill/';
const STATE_KEY               = 'backfill-state/progress.json';
const MAX_RANGE_HOURS         = 24 * 7;
const WALL_TIME_BUDGET_MS     = 55_000;
const CLEANUP_GRACE_MS        = 24 * 60 * 60_000;
const MAX_RATE                = 100;
const DEFAULT_RATE            = 5;
const LIST_LIMIT              = 1000;
const MAX_DAY_PREFIXES        = 10;
const DEFAULT_SEND_TIMEOUT_MS = 300_000;
const MIN_SEND_TIMEOUT_MS     = 1_000;
const DEFAULT_BATCH_SIZE      = 1000;
const LOG_LEVELS             = Object.freeze({ debug: 0, info: 1, warn: 2, error: 3 });

// One invocation sends at most one POST and lasts at least 200ms.
// lines/s ceiling = max_concurrency * max_batch_size * (1000 / 200) * BATCH_SIZE
const MIN_SENDER_INVOCATION_MS = 200;

export default {
  async queue(batch, env) {
    if (batch.queue === env.PARSE_QUEUE_NAME) await handleParseQueue(batch, env);
    else if (batch.queue === env.SEND_QUEUE_NAME) await handleSendQueue(batch, env);
    else log(env, 'warn', `Unknown queue: ${batch.queue}`);
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runBackfillScan(env));
  },

  async fetch(request, env) {
    return handleFetch(request, env);
  },
};

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

async function writeBatchAndEnqueue(lines, sourceKey, index, env, run) {
  const batchKey = buildBatchKey(sourceKey, index, run.id, lines.length);
  const queuedMarkerKey = `${batchKey}.queued`;

  const doneMarker = await env.RAW_BUCKET.head(`${batchKey}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'debug', `Batch already sent (skip): ${batchKey}`);
    return;
  }

  const queuedMarker = await env.RAW_BUCKET.head(queuedMarkerKey).catch(() => null);
  if (queuedMarker) {
    log(env, 'debug', `Batch already queued (skip duplicate enqueue): ${batchKey}`);
    return;
  }

  const body = `${lines.join('\n')}\n`;
  try {
    await env.RAW_BUCKET.put(batchKey, body, {
      httpMetadata: { contentType: 'text/plain; charset=utf-8' },
    });
    await env.RAW_BUCKET.put(queuedMarkerKey, '1', {
      httpMetadata: { contentType: 'text/plain' },
    });
    await env.SEND_QUEUE.send({
      key: batchKey,
      queuedAtMs: Date.now(),
      runId: run.id,
      lineCount: lines.length,
    });
  } catch (error) {
    await Promise.allSettled([
      env.RAW_BUCKET.delete(batchKey),
      env.RAW_BUCKET.delete(queuedMarkerKey),
    ]);
    throw error;
  }

  log(env, 'debug', `Queued: ${batchKey} (${lines.length} lines)`);
}

async function handleSendQueue(batch, env) {
  const invocationStart = Date.now();
  const results = await Promise.allSettled(batch.messages.map((msg) => sendBatch(msg, env)));

  results.forEach((result, index) => {
    if (result.status === 'fulfilled') batch.messages[index].ack();
    else {
      log(env, 'warn', `Send failed, retry: ${result.reason}`);
      batch.messages[index].retry();
    }
  });

  const elapsed = Date.now() - invocationStart;
  if (elapsed < MIN_SENDER_INVOCATION_MS) {
    await new Promise((resolve) => setTimeout(resolve, MIN_SENDER_INVOCATION_MS - elapsed));
  }
}

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
    if (Number.isFinite(readyAt) && Date.now() - readyAt < CLEANUP_GRACE_MS) return false;
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

async function deleteR2Keys(env, keys) {
  if (keys.length === 0) return;
  try {
    await env.RAW_BUCKET.delete(keys);
  } catch {
    await Promise.allSettled(keys.map((key) => env.RAW_BUCKET.delete(key)));
  }
}

async function handleFetch(request, env) {
  const url = new URL(request.url);

  if (url.pathname === '/backfill/status') {
    const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
    if (!obj) {
      return jsonResponse({
        status: 'not_started',
        message: 'No backfill state found yet. Ensure BACKFILL_START_TIME and BACKFILL_END_TIME are set and wait for the next cron trigger.',
        config_hint: {
          BACKFILL_START_TIME: env.BACKFILL_START_TIME || '(unset)',
          BACKFILL_END_TIME: env.BACKFILL_END_TIME || '(unset)',
          BACKFILL_RATE: env.BACKFILL_RATE || String(DEFAULT_RATE),
          BACKFILL_ENABLED: env.BACKFILL_ENABLED || '(default: false)',
        },
      });
    }

    try {
      const data = JSON.parse(await obj.text());
      const artifactStats = await collectRunArtifactStats(env, data.run_id, parsePositiveInt(env.BATCH_SIZE, DEFAULT_BATCH_SIZE, 1));
      if (url.searchParams.get('view') === 'raw') {
        return jsonResponse({ ...data, artifact_stats: artifactStats });
      }
      return jsonResponse(buildPublicStatusResponse(data, artifactStats, env));
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

function sf(val, maxLen) {
  if (val == null || val === '') return '-';
  const s = String(val);
  return maxLen && s.length > maxLen ? s.substring(0, maxLen) : s;
}

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

function buildAuthUrl(endpoint, uri, privateKey) {
  const ts = Math.floor(Date.now() / 1000) + 300;
  const rand = Math.floor(Math.random() * 99999);
  return `${endpoint}${uri}?auth_key=${ts}-${rand}-${md5(`${uri}-${ts}-${rand}-${privateKey}`)}`;
}

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

function isRecordInRunWindow(recordMs, run) {
  return recordMs >= run.startMs && recordMs <= run.endMs;
}

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

function buildPublicStatusResponse(state, artifactStats, env) {
  const stage = derivePublicStage(state, artifactStats);
  const checkedAt = new Date().toISOString();
  const lastUpdatedAt = state.updated_at || state.last_cron_at || state.completed_at || state.started_at || null;
  return {
    status: state.status,
    stage,
    message: buildHumanMessage(state, artifactStats, stage),
    run_id: state.run_id,
    window_start: state.config?.start || null,
    window_end: state.config?.end || null,
    started_at: state.started_at || null,
    started_at_beijing: toBeijingTime(state.started_at),
    enqueue_completed_at: state.completed_at || null,
    enqueue_completed_at_beijing: toBeijingTime(state.completed_at),
    last_updated_at: lastUpdatedAt,
    last_updated_at_beijing: toBeijingTime(lastUpdatedAt),
    status_checked_at: checkedAt,
    status_checked_at_beijing: toBeijingTime(checkedAt),
    raw_files_matched: state.enqueued_count ?? 0,
    raw_files_per_minute_limit: state.config?.rate ?? null,
    batches_sent: artifactStats.sent_batches,
    batches_total: artifactStats.total_batches,
    batches_pending: artifactStats.pending_batches,
    log_lines_sent: artifactStats.sent_lines,
    log_lines_total: artifactStats.total_lines,
    log_lines_pending: artifactStats.pending_lines,
    batch_size_max_configured: artifactStats.configured_batch_size,
    batch_lines_avg: artifactStats.avg_batch_lines,
    batch_lines_min: artifactStats.min_batch_lines,
    batch_lines_max: artifactStats.max_batch_lines,
    batch_size_note: `单个 batch 最多 ${artifactStats.configured_batch_size} 条；多数 batch 会接近上限，最后一个尾 batch 可能更小。`,
    cleanup: state.cleanup,
    rerun_hint: '如果同一时间窗需要重新跑，请先删除 R2 里的 backfill-state/progress.json，再保持 BACKFILL_ENABLED=true 重新部署或等待下一次 cron。旧 run 的 processed-backfill/<run-id>/ 不会影响新 run。',
    ui_time_note: 'R2 Dashboard 里 progress.json 的 Date Created 不是这次 run 是否最新的判断依据。请以 started_at、last_updated_at 和 status_checked_at 为准。',
  };
}

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

export const __test = {
  buildHumanMessage,
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
