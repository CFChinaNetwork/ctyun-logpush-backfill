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
      if (Array.isArray(key)) {
        key.forEach((item) => objects.delete(item));
        return;
      }
      objects.delete(key);
    },
  };
}

function createFakeAggregatorEnv() {
  const sent = new Set();
  return {
    sent,
    RUN_AGGREGATOR: {
      idFromName(name) {
        return name;
      },
      get() {
        return {
          async fetch(url, init) {
            const path = new URL(url).pathname;
            const body = JSON.parse(init.body);
            if (path === '/is-sent') {
              return Response.json({ sent: sent.has(body.batchKey) });
            }
            if (path === '/mark-sent') {
              sent.add(body.batchKey);
              return Response.json({ sent: true });
            }
            return Response.json({ ok: true });
          },
        };
      },
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
  assert.equal(__test.parseSendTimeoutMs({ SEND_TIMEOUT_MS: '300000' }), 300000);
  assert.equal(__test.parseSendTimeoutMs({ SEND_TIMEOUT_MS: '1' }), 300000);
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

test('cleanup helpers normalize state and mark pending artifacts correctly', () => {
  const cleanup = __test.normalizeCleanupState(null);
  const aggregate = __test.normalizeAggregateStatus({ pending_batch_ids: ['00000001', '00000002'] }, 'run-1');

  assert.equal(cleanup.status, 'pending');
  assert.equal(cleanup.ready_at, null);
  assert.equal(aggregate.pending_batch_count, 2);
  assert.equal(__test.isPendingRunArtifact('processed-backfill/run-1/file-0.txt'), true);
  assert.equal(__test.isPendingRunArtifact('processed-backfill/run-1/file-0.txt.queued'), true);
  assert.equal(__test.isPendingRunArtifact('processed-backfill/run-1/file-0.txt.done'), false);
  assert.equal(__test.canReinitializeFromState({ status: 'cleaned' }), true);
  assert.equal(__test.canReinitializeFromState({ status: 'done' }), false);
});

test('compactStateAfterCleanup keeps only minimal cleaned marker state', () => {
  const compacted = __test.compactStateAfterCleanup({
    config: { start: '2026-04-24T03:15:00Z', end: '2026-04-24T03:30:00Z', rate: 5 },
    run_id: 'run-1',
    phase: 'done',
    status: 'done',
    started_at: '2026-04-24T04:33:31.353Z',
    completed_at: '2026-04-24T04:39:31.905Z',
    enqueued_count: 30,
    last_cron_at: '2026-04-24T04:39:31.905Z',
    cleanup: {
      status: 'deleting',
      ready_at: '2026-04-24T04:45:00.000Z',
      deleted_objects: 27,
    },
    aggregate: {
      emitted_batches: 11,
      pending_batch_ids: ['00000001'],
      pending_batch_count: 1,
      finalized: true,
      finalized_at: '2026-04-24T04:40:00.000Z',
    },
  });

  assert.equal(compacted.status, 'cleaned');
  assert.equal(compacted.cleanup.status, 'done');
  assert.equal(compacted.cleanup.deleted_objects, 27);
  assert.equal(compacted.enqueued_count, 30);
  assert.equal(compacted.aggregate.emitted_batches, 11);
  assert.equal(compacted.aggregate.pending_batch_count, 0);
  assert.equal('enqueue_progress' in compacted, false);
});

test('isRunCleaned recognizes cleaned completion markers', async () => {
  const RAW_BUCKET = createFakeBucket();
  RAW_BUCKET.objects.set('backfill-state/progress.json', JSON.stringify({
    run_id: 'run-1',
    status: 'cleaned',
  }));

  assert.equal(await __test.isRunCleaned({ RAW_BUCKET }, 'run-1'), true);
  assert.equal(await __test.isRunCleaned({ RAW_BUCKET }, 'run-2'), false);
});

test('isR2ListComplete follows truncated instead of object count', () => {
  assert.equal(__test.isR2ListComplete({ objects: new Array(68), truncated: true }), false);
  assert.equal(__test.isR2ListComplete({ objects: new Array(68), truncated: false }), true);
  assert.equal(__test.isR2ListComplete({ objects: new Array(1000) }), true);
});

test('isPendingCleanupArtifact ignores sent .txt artifacts', async () => {
  const RAW_BUCKET = createFakeBucket();
  const env = { RAW_BUCKET, ...createFakeAggregatorEnv() };
  RAW_BUCKET.objects.set('processed-backfill/run-1/batch-00000001.txt.done', '1');

  assert.equal(await __test.isPendingCleanupArtifact(env, 'run-1', 'processed-backfill/run-1/batch-00000001.txt'), false);
  assert.equal(await __test.isPendingCleanupArtifact(env, 'run-1', 'processed-backfill/run-1/batch-00000001.txt.queued'), true);
});

test('deleteR2Keys reports failed deletions', async () => {
  const RAW_BUCKET = createFakeBucket();
  RAW_BUCKET.objects.set('a', '1');
  RAW_BUCKET.objects.set('b', '1');
  RAW_BUCKET.delete = async (key) => {
    if (key === 'b') throw new Error('boom');
    RAW_BUCKET.objects.delete(key);
  };

  const failed = await __test.deleteR2Keys({ RAW_BUCKET }, ['a', 'b']);

  assert.deepEqual(failed, ['b']);
  assert.equal(RAW_BUCKET.objects.has('a'), false);
  assert.equal(RAW_BUCKET.objects.has('b'), true);
});
