import { afterEach, beforeEach, test } from 'node:test'
import { PgTrxOutbox } from '../src'
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { Client } from 'pg'
import assert from 'node:assert'
import { OutboxMessage } from '../src/types'
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
      CONSTRAINT pg_trx_outbox_pk PRIMARY KEY (id, key)
    ) PARTITION BY HASH (key);
  `)
  await pg.query(`CREATE TABLE pg_trx_outbox_0 PARTITION OF pg_trx_outbox FOR VALUES WITH (MODULUS 3, REMAINDER 0);`)
  await pg.query(`CREATE TABLE pg_trx_outbox_1 PARTITION OF pg_trx_outbox FOR VALUES WITH (MODULUS 3, REMAINDER 1);`)
  await pg.query(`CREATE TABLE pg_trx_outbox_2 PARTITION OF pg_trx_outbox FOR VALUES WITH (MODULUS 3, REMAINDER 2);`)
})

afterEach(async () => {
  await pgKafkaTrxOutbox.stop()
  await pg.end()
})

test('partition handling success', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [{ status: 'fulfilled', value: { ok: true } }]
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
      partition: 0,
    },
  })
  await pgKafkaTrxOutbox.start()
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('pg.kafka.trx.outbox', 'testKey', '{"test": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox_0').then(r => r.rows)
  assert.deepEqual(resp[0]?.response, { ok: true })
})

test('waitResponse on partition success', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async onHandled() {},
      async send() {
        return [{ status: 'fulfilled', value: { ok: true } }]
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
      partition: 0,
    },
  })
  await pgKafkaTrxOutbox.start()
  const id = await pg
    .query<{ id: string }>(
      `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('pg.kafka.trx.outbox', 'testKey', '{"test": true}')
      RETURNING id
    `
    )
    .then(r => (r.rows[0] ?? { id: '' }).id)

  const resp = await pgKafkaTrxOutbox.waitResponse<{ ok: true }>(id, 'testKey')
  assert.deepEqual(resp, { ok: true })
})
