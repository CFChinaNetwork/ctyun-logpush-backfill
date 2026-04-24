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
    async delete(key) {
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

test('parseConfig builds a canonical window id from the configured window', () => {
  const config = __test.parseConfig({
    BACKFILL_START_TIME: '2026-04-22T22:00:00+08:00',
    BACKFILL_END_TIME: '2026-04-22T22:30:00+08:00',
    BACKFILL_RATE: '5',
    R2_BUCKET_NAME: 'cdn-logs-raw',
    LOG_PREFIX: 'logs/',
  });

  assert.equal(config.valid, true);
  assert.equal(config.windowId, '20260422T140000Z_20260422T143000Z');
  assert.equal(
    __test.buildRunInstanceId(config.startMs, config.endMs, Date.UTC(2026, 3, 24, 1, 2, 3)),
    '20260422T140000Z_20260422T143000Z_20260424T010203Z'
  );
  assert.equal(__test.parseSendTimeoutMs({ SEND_TIMEOUT_MS: '180000' }), 180000);
  assert.equal(__test.parseSendTimeoutMs({ SEND_TIMEOUT_MS: '1' }), 180000);
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

  const batchKey = __test.buildBatchKey('logs/20260422/file.log.gz', 0, run.id);
  assert.equal(sent.length, 1);
  assert.equal(sent[0].key, batchKey);
  assert.equal(sent[0].runId, run.id);
  assert.equal(typeof sent[0].queuedAtMs, 'number');
  assert.equal(RAW_BUCKET.objects.has(batchKey), true);
  assert.equal(RAW_BUCKET.objects.has(`${batchKey}.queued`), true);
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
    BACKFILL_RATE: '5',
    R2_BUCKET_NAME: 'cdn-logs-raw',
    LOG_PREFIX: 'logs/',
  });

  assert.equal(run.valid, true);
  assert.equal(run.id, '20260422T140000Z_20260422T143000Z_20260424T010203Z');
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
  const batchKey = __test.buildBatchKey('logs/20260422/file.log.gz', 1, run.id);
  RAW_BUCKET.objects.set(`${batchKey}.done`, '1');

  await __test.writeBatchAndEnqueue(['line-2'], 'logs/20260422/file.log.gz', 1, env, run);

  assert.equal(sent.length, 0);
  assert.equal(RAW_BUCKET.objects.has(batchKey), false);
});

test('updateSendStats aggregates ack and queue wait metrics', () => {
  const initial = __test.createSendStats('run-1');
  const next = __test.updateSendStats(initial, {
    runId: 'run-1',
    key: 'processed-backfill/run-1/file-0.txt',
    outcome: 'success',
    ackMs: 1200,
    queueWaitMs: 4500,
    bytes: 100,
  });

  assert.equal(next.success_count, 1);
  assert.equal(next.ack_ms_avg, 1200);
  assert.equal(next.ack_ms_max, 1200);
  assert.equal(next.queue_wait_ms_avg, 4500);
  assert.equal(next.last_key, 'processed-backfill/run-1/file-0.txt');
  assert.equal(next.last_bytes, 100);
});
