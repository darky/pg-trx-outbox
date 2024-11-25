import { afterEach, beforeEach, test } from 'node:test'
import { PgTrxOutbox } from '../src/index.ts'
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { Client } from 'pg'
import assert from 'node:assert'
import { OutboxMessage } from '../src/types.ts'
import { setTimeout } from 'timers/promises'

let pgDocker: StartedPostgreSqlContainer
let pg: Client
let pgKafkaTrxOutbox: PgTrxOutbox

beforeEach(async () => {
  pgDocker = await new PostgreSqlContainer()
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
      CONSTRAINT pg_trx_outbox_pk PRIMARY KEY (id)
    );
  `)
})

afterEach(async () => {
  await pgKafkaTrxOutbox.stop()
  await pg.end()
})

test('waitResponse success', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
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
      pollInterval: 300,
    },
  })
  await pgKafkaTrxOutbox.start()
  const [{ id } = { id: '' }] = await pg
    .query<{ id: string }>(
      `
        INSERT INTO pg_trx_outbox (topic, "key", value, processed, response)
        VALUES ('pg.kafka.trx.outbox', 'testKey', '{"test": true}', true, '{"test": true}')
        RETURNING id;
      `
    )
    .then(resp => resp.rows)

  const resp = await pgKafkaTrxOutbox.waitResponse<{ test: true }>(id)

  assert.strictEqual(resp.test, true)
})

test('waitResponse error', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
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
      pollInterval: 300,
    },
  })
  await pgKafkaTrxOutbox.start()
  const [{ id } = { id: '' }] = await pg
    .query<{ id: string }>(
      `
        INSERT INTO pg_trx_outbox (topic, "key", value, processed, error)
        VALUES ('pg.kafka.trx.outbox', 'testKey', '{"test": true}', true, 'test')
        RETURNING id;
      `
    )
    .then(resp => resp.rows)

  assert.rejects(() => pgKafkaTrxOutbox.waitResponse(id), new Error('test'))
})

test('Adapter.send should satisfy Promise.allSettled', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send(messages) {
        return Promise.allSettled(
          messages.map(async m => ((m.value as { success: true }).success ? { ok: true } : Promise.reject('err')))
        )
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
  await pgKafkaTrxOutbox.start()
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('pg.kafka.trx.outbox', 'testKey', '{"success": true}'),
        ('pg.kafka.trx.outbox', 'testKey', '{"error": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)
  assert.strictEqual((resp[0]?.response as { ok: true }).ok, true)
  assert.strictEqual(resp[0]?.error, null)
  assert.strictEqual(resp[1]?.response, null)
  assert.strictEqual(resp[1]?.error, 'err')
})

test('should work with string response (JSONB error)', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send(messages) {
        return Promise.allSettled(messages.map(() => 'string'))
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
  await pgKafkaTrxOutbox.start()
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('pg.kafka.trx.outbox', 'testKey', '{"success": true}'),
        ('pg.kafka.trx.outbox', 'testKey', '{"error": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)
  assert.strictEqual((resp[0]?.response as { r: string }).r, 'string')
  assert.strictEqual(resp[0]?.error, null)
  assert.strictEqual((resp[1]?.response as { r: string }).r, 'string')
  assert.strictEqual(resp[1]?.error, null)
})

test('should work with array response (JSONB error)', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [
          { value: [1], status: 'fulfilled' },
          { value: [1], status: 'fulfilled' },
        ]
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
  await pgKafkaTrxOutbox.start()
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('pg.kafka.trx.outbox', 'testKey', '{"success": true}'),
        ('pg.kafka.trx.outbox', 'testKey', '{"error": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)
  assert.deepEqual((resp[0]?.response as { r: [1] }).r, [1])
  assert.strictEqual(resp[0]?.error, null)
  assert.deepEqual((resp[1]?.response as { r: [1] }).r, [1])
  assert.strictEqual(resp[1]?.error, null)
})
