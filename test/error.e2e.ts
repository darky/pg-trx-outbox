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

test('sending error', async () => {
  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        throw new Error('test')
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
    error_approved: boolean
  } = await pg.query(`select * from pg_trx_outbox`).then(resp => resp.rows[0])
  assert.strictEqual(processedRow.processed, true)
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true)
  assert.strictEqual(processedRow.response, null)
  assert.strictEqual(processedRow.error_approved, false)
  assert.match(processedRow.error, /Error: test/)
})

test('sending object error', async () => {
  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        throw { error: 'error' }
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
    error_approved: boolean
  } = await pg.query(`select * from pg_trx_outbox`).then(resp => resp.rows[0])
  assert.strictEqual(processedRow.processed, true)
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true)
  assert.strictEqual(processedRow.response, null)
  assert.strictEqual(processedRow.error, "{ error: 'error' }")
  assert.strictEqual(processedRow.error_approved, false)
})

test('onError callback', async () => {
  let err!: Error
  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return []
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
      pollInterval: 500,
      onError(e) {
        err = e
      },
    },
  })
  await pgTrxOutbox.start()
  await pg.query(`
    DROP TABLE pg_trx_outbox;
  `)
  await setTimeout(1000)

  assert.match(err.message, /relation "pg_trx_outbox" does not exist/)
})

test('approved error', async () => {
  class ApprovedError extends Error {
    isApproved = true
  }

  pgTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [{ status: 'rejected', reason: new ApprovedError('test') }]
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
    error_approved: boolean
  } = await pg.query(`select * from pg_trx_outbox`).then(resp => resp.rows[0])
  assert.strictEqual(processedRow.processed, true)
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true)
  assert.strictEqual(processedRow.response, null)
  assert.strictEqual(processedRow.error_approved, true)
  assert.match(processedRow.error, /Error: test/)
})
