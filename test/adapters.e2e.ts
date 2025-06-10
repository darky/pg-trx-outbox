import { afterEach, beforeEach, test } from 'node:test'
import { PgTrxOutbox } from '../src/index.ts'
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { Client } from 'pg'
import assert from 'node:assert'
import { OutboxMessage } from '../src/types.ts'
import { setTimeout } from 'timers/promises'
import { SerialAdapter } from '../src/adapters/abstract/serial.ts'
import { ParallelAdapter } from '../src/adapters/abstract/parallel.ts'

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

test('SerialAdapter works', async () => {
  const handledMessages: OutboxMessage[] = []

  pgTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      override async onHandled(messages: readonly OutboxMessage[]): Promise<void> {
        messages.forEach(m => handledMessages.push(m))
      }
      async handleMessage(message: OutboxMessage) {
        if ((message.value as { ok: true }).ok) {
          return { value: { success: true } }
        }
        throw new Error('err')
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
  await pgTrxOutbox.start()
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES
        ('pg.trx.outbox', 'testKey', '{"ok": true}'),
        ('pg.trx.outbox', 'testKey', '{"err": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)

  assert.deepEqual(resp[0]?.response, { success: true })
  assert.strictEqual(resp[0]?.error, null)

  assert.strictEqual(resp[1]?.response, null)
  assert.match(resp[1]?.error ?? '', /err/)

  assert.deepStrictEqual(resp[0].value, handledMessages![0]?.value)
  assert.deepStrictEqual(resp[1].value, handledMessages![1]?.value)
})

test('ParallelAdapter works', async () => {
  const handledMessages: OutboxMessage[] = []

  pgTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends ParallelAdapter {
      async start() {}
      async stop() {}
      override async onHandled(messages: readonly OutboxMessage[]): Promise<void> {
        messages.forEach(m => handledMessages.push(m))
      }
      async handleMessage(message: OutboxMessage) {
        if ((message.value as { ok: true }).ok) {
          return { value: { success: true } }
        }
        throw new Error('err')
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
  await pgTrxOutbox.start()
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES
        ('pg.trx.outbox', 'testKey', '{"ok": true}'),
        ('pg.trx.outbox', 'testKey', '{"err": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)

  assert.deepEqual(resp[0]?.response, { success: true })
  assert.strictEqual(resp[0]?.error, null)

  assert.strictEqual(resp[1]?.response, null)
  assert.match(resp[1]?.error ?? '', /err/)

  assert.deepStrictEqual(resp[0].value, handledMessages![0]?.value)
  assert.deepStrictEqual(resp[1].value, handledMessages![1]?.value)
})

test('GroupedAdapter works', async () => {
  const handledMessages: OutboxMessage[] = []

  pgTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends ParallelAdapter {
      async start() {}
      async stop() {}
      override async onHandled(messages: readonly OutboxMessage[]): Promise<void> {
        messages.forEach(m => handledMessages.push(m))
      }
      async handleMessage(message: OutboxMessage) {
        if ((message.value as { ok: true }).ok) {
          return { value: { success: true } }
        }
        throw new Error('err')
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
  await pgTrxOutbox.start()
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES
        ('pg.trx.outbox', 'testKey', '{"ok": true}'),
        ('pg.trx.outbox', 'testKey', '{"err": true}')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)

  assert.deepEqual(resp[0]?.response, { success: true })
  assert.strictEqual(resp[0]?.error, null)

  assert.strictEqual(resp[1]?.response, null)
  assert.match(resp[1]?.error ?? '', /err/)

  assert.deepStrictEqual(resp[0].value, handledMessages![0]?.value)
  assert.deepStrictEqual(resp[1].value, handledMessages![1]?.value)
})
