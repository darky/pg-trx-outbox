import { afterEach, beforeEach, test } from 'node:test'
import { PgTrxOutbox } from '../src'
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { Client } from 'pg'
import assert from 'node:assert'
import { OutboxMessage } from '../src/types'
import { setTimeout } from 'timers/promises'
import { SerialAdapter } from '../src/adapters/abstract/serial'

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
      CONSTRAINT pg_trx_outbox_pk PRIMARY KEY (id)
    );
  `)
})

afterEach(async () => {
  await pgKafkaTrxOutbox.stop()
  await pg.end()
})

test('meta response works', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: {
      async start() {},
      async stop() {},
      async send() {
        return [{ meta: { metaWorks: true }, value: {}, status: 'fulfilled' }]
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
      VALUES
        ('pg.kafka.trx.outbox', 'testKey', '{"ok": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)

  assert.deepEqual(resp[0]?.meta, { metaWorks: true })
})

test('built-in meta works', async () => {
  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      async handleMessage() {
        return { value: { ok: true }, meta: { response: true } }
      }
    })(),
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
      VALUES
        ('pg.kafka.trx.outbox', 'testKey', '{"ok": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)
  const meta = resp[0]?.meta as {
    response: true
    pgTrxOutbox: { time: number; libuv: { max: number; min: number; stddev: number; mean: number } }
  }

  assert.strictEqual(meta.response, true)
  assert.strictEqual(typeof meta.pgTrxOutbox.time, 'number')
  assert.strictEqual(typeof meta.pgTrxOutbox.libuv.max, 'number')
  assert.strictEqual(typeof meta.pgTrxOutbox.libuv.min, 'number')
  assert.strictEqual(typeof meta.pgTrxOutbox.libuv.stddev === 'number' || meta.pgTrxOutbox.libuv.stddev === null, true)
  assert.strictEqual(typeof meta.pgTrxOutbox.libuv.mean === 'number' || meta.pgTrxOutbox.libuv.mean === null, true)
})
