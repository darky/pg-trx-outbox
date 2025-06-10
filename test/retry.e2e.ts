import { afterEach, beforeEach, test } from 'node:test'
import { PgTrxOutbox } from '../src/index.ts'
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { Client } from 'pg'
import assert from 'node:assert'
import { setTimeout } from 'timers/promises'

let pgDocker: StartedPostgreSqlContainer
let pg: Client
let pgTrxOutbox: PgTrxOutbox

beforeEach(async () => {
  pgDocker = await new PostgreSqlContainer('postgres:17')
    .withReuse()
    .withCommand(['-c', 'fsync=off', '-c', 'wal_level=logical'])
    .start()

  pg = new Client({
    host: pgDocker.getHost(),
    port: pgDocker.getPort(),
    user: pgDocker.getUsername(),
    password: pgDocker.getPassword(),
    database: pgDocker.getDatabase(),
    application_name: 'pg_trx_outbox_admin',
  })
  await pg.connect()
  await pg.query('DROP TABLE IF EXISTS pg_trx_outbox')
  await pg.query(`
    CREATE TABLE pg_trx_outbox (
      id bigserial NOT NULL,
      processed bool NOT NULL DEFAULT false,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now(),
      since_at timestamptz NULL,
      topic text NOT NULL,
      "key" text NULL,
      value jsonb NULL,
      "partition" int2 NULL,
      "timestamp" int8 NULL,
      headers jsonb NULL,
      response jsonb NULL,
      error text NULL,
      meta jsonb NULL,
      context_id double precision NOT NULL DEFAULT random(),
      attempts smallint NOT NULL DEFAULT 0,
      is_event boolean NOT NULL DEFAULT false,
      error_approved boolean NOT NULL DEFAULT false,
      CONSTRAINT pg_trx_outbox_pk PRIMARY KEY (id)
    );
  `)
})

afterEach(async () => {
  await pgTrxOutbox.stop()
  await pg.end()
})

test('on retry error not processed', async () => {
  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [{ reason: new Error('test err'), status: 'rejected' }]
      },
    },
    pgOptions: {
      host: pgDocker.getHost(),
      port: pgDocker.getPort(),
      user: pgDocker.getUsername(),
      password: pgDocker.getPassword(),
      database: pgDocker.getDatabase(),
    },
    outboxOptions: {
      retryError(err) {
        assert.strictEqual(err.message, 'test err')
        return true
      },
      pollInterval: 300,
    },
  })
  await pgTrxOutbox.start()
  await pg.query(`
    INSERT INTO pg_trx_outbox
      (topic, "key", value)
      VALUES ('pg.trx.outbox', 'testKey', '{"test": true}');
    `)
  await setTimeout(1000)

  const processedRow: {
    processed: boolean
    created_at: Date
    updated_at: Date
    response: unknown
    error: string
  } = await pg.query(`select * from pg_trx_outbox`).then(resp => resp.rows[0])
  assert.strictEqual(processedRow.processed, false)
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true)
  assert.strictEqual(processedRow.response, null)
  assert.match(processedRow.error, /Error: test err/)
})

test('on not retry error is processed', async () => {
  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [{ reason: new Error('test err'), status: 'rejected' }]
      },
    },
    pgOptions: {
      host: pgDocker.getHost(),
      port: pgDocker.getPort(),
      user: pgDocker.getUsername(),
      password: pgDocker.getPassword(),
      database: pgDocker.getDatabase(),
    },
    outboxOptions: {
      retryError(err) {
        assert.strictEqual(err.message, 'test err')
        return false
      },
      pollInterval: 300,
    },
  })
  await pgTrxOutbox.start()
  await pg.query(`
    INSERT INTO pg_trx_outbox
      (topic, "key", value)
      VALUES ('pg.trx.outbox', 'testKey', '{"test": true}');
    `)
  await setTimeout(1000)

  const processedRow: {
    processed: boolean
    created_at: Date
    updated_at: Date
    response: unknown
    error: string
  } = await pg.query(`select * from pg_trx_outbox`).then(resp => resp.rows[0])
  assert.strictEqual(processedRow.processed, true)
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true)
  assert.strictEqual(processedRow.response, null)
  assert.match(processedRow.error, /Error: test err/)
})

test('preserve error after retry', async () => {
  const resps = [
    { reason: new Error('test err'), status: 'rejected' } as PromiseRejectedResult,
    { status: 'fulfilled' as const, value: { ok: true } } as PromiseFulfilledResult<{ ok: true }>,
  ]
  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [resps.shift()!]
      },
    },
    pgOptions: {
      host: pgDocker.getHost(),
      port: pgDocker.getPort(),
      user: pgDocker.getUsername(),
      password: pgDocker.getPassword(),
      database: pgDocker.getDatabase(),
    },
    outboxOptions: {
      retryError() {
        return true
      },
      retryDelay: 0,
      pollInterval: 300,
    },
  })
  await pgTrxOutbox.start()
  await pg.query(`
    INSERT INTO pg_trx_outbox
      (topic, "key", value)
      VALUES ('pg.trx.outbox', 'testKey', '{"test": true}');
    `)
  await setTimeout(1000)

  const processedRow: {
    processed: boolean
    created_at: Date
    updated_at: Date
    response: { ok: true }
    error: string
    attempts: number
  } = await pg.query(`select * from pg_trx_outbox`).then(resp => resp.rows[0])
  assert.strictEqual(processedRow.processed, true)
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true)
  assert.strictEqual(processedRow.response.ok, true)
  assert.strictEqual(processedRow.attempts, 1)
  assert.match(processedRow.error, /Error: test err/)
})

test('retry max attempts exceeded', async () => {
  const resps = [
    { reason: new Error('test err'), status: 'rejected' } as PromiseRejectedResult,
    { reason: new Error('test err'), status: 'rejected' } as PromiseRejectedResult,
    { reason: new Error('test err'), status: 'rejected' } as PromiseRejectedResult,
    { status: 'fulfilled' as const, value: { ok: true } } as PromiseFulfilledResult<{ ok: true }>,
  ]
  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [resps.shift()!]
      },
    },
    pgOptions: {
      host: pgDocker.getHost(),
      port: pgDocker.getPort(),
      user: pgDocker.getUsername(),
      password: pgDocker.getPassword(),
      database: pgDocker.getDatabase(),
    },
    outboxOptions: {
      retryError() {
        return true
      },
      retryDelay: 0,
      retryMaxAttempts: 2,
      pollInterval: 300,
    },
  })
  await pgTrxOutbox.start()
  await pg.query(`
    INSERT INTO pg_trx_outbox
      (topic, "key", value)
      VALUES ('pg.trx.outbox', 'testKey', '{"test": true}');
    `)
  await setTimeout(1000)

  const processedRow: {
    processed: boolean
    created_at: Date
    updated_at: Date
    response: null
    error: string
    attempts: number
  } = await pg.query(`select * from pg_trx_outbox`).then(resp => resp.rows[0])
  assert.strictEqual(processedRow.processed, true)
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true)
  assert.strictEqual(processedRow.response, null)
  assert.strictEqual(processedRow.attempts, 2)
  assert.match(processedRow.error, /Error: test err/)
})

test('retry delay', async () => {
  const resps = [
    { reason: new Error('test err'), status: 'rejected' } as PromiseRejectedResult,
    { status: 'fulfilled' as const, value: { ok: true } } as PromiseFulfilledResult<{ ok: true }>,
  ]
  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [resps.shift()!]
      },
    },
    pgOptions: {
      host: pgDocker.getHost(),
      port: pgDocker.getPort(),
      user: pgDocker.getUsername(),
      password: pgDocker.getPassword(),
      database: pgDocker.getDatabase(),
    },
    outboxOptions: {
      retryError() {
        return true
      },
      retryDelay: 60,
      pollInterval: 300,
    },
  })
  await pgTrxOutbox.start()
  await pg.query(`
    INSERT INTO pg_trx_outbox
      (topic, "key", value)
      VALUES ('pg.trx.outbox', 'testKey', '{"test": true}');
    `)
  await setTimeout(1000)

  const processedRow: {
    processed: boolean
    created_at: Date
    updated_at: Date
    since_at: Date
    response: null
    error: string
    attempts: number
  } = await pg.query(`select * from pg_trx_outbox`).then(resp => resp.rows[0])
  assert.strictEqual(processedRow.processed, false)
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true)
  assert.strictEqual(processedRow.response, null)
  assert.strictEqual(processedRow.attempts, 1)
  assert.strictEqual(+processedRow.since_at - Date.now() > 59 * 1000, true)
  assert.strictEqual(+processedRow.since_at - Date.now() < 60 * 1000, true)
  assert.match(processedRow.error, /Error: test err/)
})
