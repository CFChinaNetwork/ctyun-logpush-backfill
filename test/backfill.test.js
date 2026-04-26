import test from 'node:test';
import assert from 'node:assert/strict';

import { __test } from '../src/index.js';

function createFakeBucket() {
  const objects = new Map();
  return {
    objects,
    async head(key) {
      return objects.has(key) ? { key } : null;
    },
    async get(key) {
      if (!objects.has(key)) return null;
      const value = objects.get(key);
      return {
        async text() {
          return typeof value === 'string' ? value : JSON.stringify(value);
        },
      };
    },
    async put(key, value) {
      objects.set(key, value);
    },
    async list({ prefix = '', startAfter, limit = 1000 } = {}) {
      const sorted = [...objects.keys()].filter((key) => key.startsWith(prefix)).sort();
      const filtered = startAfter ? sorted.filter((key) => key > startAfter) : sorted;
      const page = filtered.slice(0, limit).map((key) => ({ key }));
      return {
        objects: page,
        truncated: filtered.length > limit,
      };
    },
    async delete(key) {
      if (Array.isArray(key)) {
        key.forEach((item) => objects.delete(item));
        return;
      }
      objects.delete(key);
    },
  };
}

test('record window clipping keeps only records inside [start, end]', () => {
  const run = { startMs: 1000, endMs: 2000 };

  assert.equal(__test.isRecordInRunWindow(999, run), false);
  assert.equal(__test.isRecordInRunWindow(1000, run), true);
  assert.equal(__test.isRecordInRunWindow(1500, run), true);
  assert.equal(__test.isRecordInRunWindow(2000, run), true);
  assert.equal(__test.isRecordInRunWindow(2001, run), false);
});

test('isTopLevelParentRequest keeps only parent worker requests', () => {
  assert.equal(__test.isTopLevelParentRequest({ ParentRayID: '00', WorkerSubrequest: false }), true);
  assert.equal(__test.isTopLevelParentRequest({ ParentRayID: '00', WorkerSubrequest: 'true' }), false);
  assert.equal(__test.isTopLevelParentRequest({ ParentRayID: 'abc', WorkerSubrequest: false }), false);
  assert.equal(__test.isTopLevelParentRequest({ ParentRayID: null, WorkerSubrequest: false }), false);
});

test('parseConfig builds canonical run ids and rejects over-7-day windows', () => {
  const config = __test.parseConfig({
    BACKFILL_START_TIME: '2026-04-22T22:00:00+08:00',
    BACKFILL_END_TIME: '2026-04-22T22:30:00+08:00',
    BACKFILL_RATE: '100',
    R2_BUCKET_NAME: 'cdn-logs-raw',
    LOG_PREFIX: 'logs/',
  });

  assert.equal(config.valid, true);
  assert.equal(config.windowId, '20260422T140000Z_20260422T143000Z');
  assert.equal(
    __test.buildRunInstanceId(config.startMs, config.endMs, Date.UTC(2026, 3, 24, 1, 2, 3)),
    '20260422T140000Z_20260422T143000Z_20260424T010203Z'
  );
  assert.equal(__test.parseSendTimeoutMs({ SEND_TIMEOUT_MS: '300000' }), 300000);
  assert.equal(__test.parseSendTimeoutMs({ SEND_TIMEOUT_MS: '1' }), 300000);

  const tooWide = __test.parseConfig({
    BACKFILL_START_TIME: '2026-04-01T00:00:00Z',
    BACKFILL_END_TIME: '2026-04-09T00:00:00Z',
  });
  assert.equal(tooWide.valid, false);
  assert.match(tooWide.error, /exceeds max 168h/);
});

test('buildParseQueueMessage preserves run metadata for parser retries', () => {
  const config = __test.parseConfig({
    BACKFILL_START_TIME: '2026-04-25T10:40:00+08:00',
    BACKFILL_END_TIME: '2026-04-25T11:00:00+08:00',
    BACKFILL_RATE: '100',
    R2_BUCKET_NAME: 'cdn-logs-raw',
    LOG_PREFIX: 'logs/',
  });

  const message = __test.buildParseQueueMessage('logs/20260425/foo.log.gz', config, 'run-1');

  assert.deepEqual(message, {
    bucket: 'cdn-logs-raw',
    object: { key: 'logs/20260425/foo.log.gz' },
    run: {
      id: 'run-1',
      start: '2026-04-25T10:40:00+08:00',
      end: '2026-04-25T11:00:00+08:00',
      startMs: config.startMs,
      endMs: config.endMs,
    },
  });
});

test('buildBatchKey stays unique for different source keys that sanitize similarly', () => {
  const runId = '20260422T140000Z_20260422T143000Z_20260424T010203Z';
  const a = __test.buildBatchKey('logs/20260422/a:b.log.gz', 0, runId, 1000);
  const b = __test.buildBatchKey('logs/20260422/a/b.log.gz', 0, runId, 237);

  assert.notEqual(a, b);
  assert.match(a, /processed-backfill\/.+-[a-f0-9]{32}-0-n1000\.txt$/);
  assert.match(b, /processed-backfill\/.+-[a-f0-9]{32}-0-n237\.txt$/);
  assert.equal(__test.extractBatchLineCountFromKey(a), 1000);
  assert.equal(__test.extractBatchLineCountFromKey(`${b}.done`), 237);
});

test('writeBatchAndEnqueue skips duplicate queue fanout while a batch is already queued', async () => {
  const RAW_BUCKET = createFakeBucket();
  const sent = [];
  const env = {
    RAW_BUCKET,
    SEND_QUEUE: {
      async send(body) {
        sent.push(body);
      },
    },
  };
  const run = { id: '20260422T140000Z_20260422T143000Z' };

  await __test.writeBatchAndEnqueue(['line-1'], 'logs/20260422/file.log.gz', 0, env, run);
  await __test.writeBatchAndEnqueue(['line-1'], 'logs/20260422/file.log.gz', 0, env, run);

  const batchKey = __test.buildBatchKey('logs/20260422/file.log.gz', 0, run.id, 1);
  assert.equal(sent.length, 1);
  assert.equal(sent[0].key, batchKey);
  assert.equal(sent[0].runId, run.id);
  assert.equal(sent[0].lineCount, 1);
  assert.equal(typeof sent[0].queuedAtMs, 'number');
  assert.equal(RAW_BUCKET.objects.has(batchKey), true);
  assert.equal(RAW_BUCKET.objects.has(`${batchKey}.queued`), true);
});

test('writeBatchAndEnqueue skips batches already marked done', async () => {
  const RAW_BUCKET = createFakeBucket();
  const sent = [];
  const env = {
    RAW_BUCKET,
    SEND_QUEUE: {
      async send(body) {
        sent.push(body);
      },
    },
  };
  const run = { id: '20260422T140000Z_20260422T143000Z' };
  const batchKey = __test.buildBatchKey('logs/20260422/file.log.gz', 1, run.id, 1);
  RAW_BUCKET.objects.set(`${batchKey}.done`, '1');

  await __test.writeBatchAndEnqueue(['line-2'], 'logs/20260422/file.log.gz', 1, env, run);

  assert.equal(sent.length, 0);
  assert.equal(RAW_BUCKET.objects.has(batchKey), false);
});

test('resolveRunContext falls back to persisted state.run_id for legacy queue messages', async () => {
  const RAW_BUCKET = createFakeBucket();
  RAW_BUCKET.objects.set('backfill-state/progress.json', JSON.stringify({
    config: {
      start: '2026-04-22T14:00:00Z',
      end: '2026-04-22T14:30:00Z',
    },
    run_id: '20260422T140000Z_20260422T143000Z_20260424T010203Z',
  }));

  const run = await __test.resolveRunContext(null, {
    RAW_BUCKET,
    BACKFILL_START_TIME: '2026-04-22T14:00:00Z',
    BACKFILL_END_TIME: '2026-04-22T14:30:00Z',
    BACKFILL_RATE: '100',
    R2_BUCKET_NAME: 'cdn-logs-raw',
    LOG_PREFIX: 'logs/',
  });

  assert.equal(run.valid, true);
  assert.equal(run.id, '20260422T140000Z_20260422T143000Z_20260424T010203Z');
});

test('cleanup helpers treat sent .txt and .queued artifacts as non-pending', async () => {
  const RAW_BUCKET = createFakeBucket();
  RAW_BUCKET.objects.set('processed-backfill/run-1/batch-00000001-n1000.txt.done', '1');

  const pendingTxt = await __test.isPendingCleanupArtifact({ RAW_BUCKET }, 'processed-backfill/run-1/batch-00000001-n1000.txt');
  const pendingQueued = await __test.isPendingCleanupArtifact({ RAW_BUCKET }, 'processed-backfill/run-1/batch-00000001-n1000.txt.queued');
  const pendingOther = await __test.isPendingCleanupArtifact({ RAW_BUCKET }, 'processed-backfill/run-1/batch-00000002-n237.txt');

  assert.equal(pendingTxt, false);
  assert.equal(pendingQueued, false);
  assert.equal(pendingOther, true);
});

test('writeDoneMarkerWithRetry retries transient failures', async () => {
  let attempts = 0;
  const RAW_BUCKET = {
    async put() {
      attempts++;
      if (attempts < 3) throw new Error('transient');
    },
  };

  const ok = await __test.writeDoneMarkerWithRetry({ RAW_BUCKET, LOG_LEVEL: 'error' }, 'processed-backfill/run-1/batch-1.txt');
  assert.equal(ok, true);
  assert.equal(attempts, 3);
});

test('queue wait math and cleanup state helpers stay stable', async () => {
  const wait = __test.parseQueueWaitMs(Date.now() - 5000);
  assert.equal(wait >= 4900, true);
  assert.equal(__test.parseQueueWaitMs('nope'), null);

  const cleanup = __test.normalizeCleanupState(null);
  assert.equal(cleanup.status, 'pending');
  assert.equal(__test.canReinitializeFromState({ status: 'cleaned' }), true);
  assert.equal(__test.canReinitializeFromState({ status: 'running' }), false);

  const compacted = __test.compactStateAfterCleanup({
    config: { start: 'a', end: 'b', rate: 100 },
    run_id: 'run-1',
    started_at: '2026-04-24T03:00:00.000Z',
    completed_at: '2026-04-24T04:00:00.000Z',
    enqueued_count: 42,
    last_cron_at: '2026-04-24T04:01:00.000Z',
    cleanup: { ready_at: '2026-04-24T04:02:00.000Z', deleted_objects: 10 },
  });
  assert.equal(compacted.status, 'cleaned');
  assert.equal(compacted.cleanup.deleted_objects, 10);

  const RAW_BUCKET = createFakeBucket();
  RAW_BUCKET.objects.set('backfill-state/progress.json', JSON.stringify({ run_id: 'run-1', status: 'cleaned' }));
  assert.equal(await __test.isRunCleaned({ RAW_BUCKET }, 'run-1'), true);
  assert.equal(__test.isPendingRunArtifact('processed-backfill/run-1/batch-0001.txt'), true);
  assert.equal(__test.isPendingRunArtifact('processed-backfill/run-1/batch-0001.txt.queued'), true);
});

test('collectRunArtifactStats summarizes batch and line totals from current run artifacts', async () => {
  const RAW_BUCKET = createFakeBucket();
  RAW_BUCKET.objects.set('processed-backfill/run-1/a-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-0-n1000.txt.done', '1');
  RAW_BUCKET.objects.set('processed-backfill/run-1/b-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1-n500.txt', 'body');
  RAW_BUCKET.objects.set('processed-backfill/run-1/b-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1-n500.txt.queued', '1');
  RAW_BUCKET.objects.set('processed-backfill/run-1/c-cccccccccccccccccccccccccccccccc-2-n300.txt.done', '1');

  const stats = await __test.collectRunArtifactStats({ RAW_BUCKET }, 'run-1', 1000);

  assert.deepEqual(stats, {
    total_batches: 3,
    sent_batches: 2,
    pending_batches: 1,
    total_lines: 1800,
    sent_lines: 1300,
    pending_lines: 500,
    min_batch_lines: 300,
    max_batch_lines: 1000,
    avg_batch_lines: 600,
    configured_batch_size: 1000,
  });
});

test('buildPublicStatusResponse returns a human-friendly reconciliation view', () => {
  const response = __test.buildPublicStatusResponse({
    status: 'done',
    phase: 'done',
    run_id: 'run-1',
    config: {
      start: '2026-04-25T10:40:00+08:00',
      end: '2026-04-25T11:00:00+08:00',
      rate: 100,
    },
    started_at: '2026-04-26T02:30:38.951Z',
    updated_at: '2026-04-26T02:30:43.484Z',
    completed_at: '2026-04-26T02:30:42.416Z',
    enqueued_count: 32,
    cleanup: { status: 'ready', ready_at: '2026-04-26T02:30:43.484Z' },
  }, {
    total_batches: 3,
    sent_batches: 3,
    pending_batches: 0,
    total_lines: 2129,
    sent_lines: 2129,
    pending_lines: 0,
    min_batch_lines: 129,
    max_batch_lines: 1000,
    avg_batch_lines: 709.67,
    configured_batch_size: 1000,
  }, { BATCH_SIZE: '1000' });

  assert.equal(response.stage_code, 'sent_waiting_cleanup');
  assert.equal(response.batches_sent, 3);
  assert.equal(response.log_lines_sent, 2129);
  assert.equal(response.batch_lines_max, 1000);
  assert.match(response.summary, /已经发送完成/);
  assert.match(response.rerun_same_window_how, /progress\.json/);
  assert.match(response.r2_console_note, /Date Created/);
  assert.match(response.batch_size_note, /不是固定 1000 条/);
  assert.equal(response.fully_completed, false);
  assert.equal(response.delivery_completed, true);
});
