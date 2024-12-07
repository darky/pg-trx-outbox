import { afterEach, beforeEach, test } from 'node:test'
import { PgTrxOutbox } from '../src/index.ts'
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { Client } from 'pg'
import assert from 'node:assert'
import { OutboxMessage } from '../src/types.ts'
import { SerialAdapter } from '../src/adapters/abstract/serial.ts'
import { setTimeout } from 'node:timers/promises'

let pgDocker: StartedPostgreSqlContainer
let pg: Client
let pgKafkaTrxOutbox: PgTrxOutbox
let pgKafkaTrxOutbox2: PgTrxOutbox

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
      is_event boolean NOT NULL DEFAULT false,
      CONSTRAINT pg_trx_outbox_pk PRIMARY KEY (id)
    );
  `)
})

afterEach(async () => {
  await pgKafkaTrxOutbox.stop()
  pgKafkaTrxOutbox2! && (await pgKafkaTrxOutbox2!.stop())
  await pg.end()
})

test('ES init sync works', async () => {
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value, is_event)
      VALUES
        ('pg.kafka.trx.outbox', 'testKey', '{"test": true}', true),
        ('pg.kafka.trx.outbox', 'testKey', '{"test2": true}', true)
    `
  )

  const messages = [] as OutboxMessage[]

  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      override async onHandled(): Promise<void> {}
      async handleMessage(message: OutboxMessage) {
        messages.push(message)
        return { value: { success: true } }
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
      pollInterval: 10000,
    },
  })
  await pgKafkaTrxOutbox.start()

  assert.deepStrictEqual(
    messages.map(m => m.value),
    [{ test: true }, { test2: true }]
  )
})

test('ES init sync should ignore commands', async () => {
  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value, is_event)
      VALUES
        ('pg.kafka.trx.outbox', 'testKey', '{"test": true}', false),
        ('pg.kafka.trx.outbox', 'testKey', '{"test2": true}', false)
    `
  )

  const messages = [] as OutboxMessage[]

  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      override async onHandled(): Promise<void> {}
      async handleMessage(message: OutboxMessage) {
        messages.push(message)
        return { value: { success: true } }
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
      pollInterval: 10000,
    },
  })
  await pgKafkaTrxOutbox.start()

  assert.deepStrictEqual(
    messages.map(m => m.value),
    []
  )
})

test('should fetch events with commands', async () => {
  const messages = [] as OutboxMessage[]

  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      override async onHandled(): Promise<void> {}
      async handleMessage(message: OutboxMessage) {
        messages.push(message)
        return { value: { success: true } }
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
      INSERT INTO pg_trx_outbox (topic, "key", value, is_event)
      VALUES
        ('pg.kafka.trx.outbox', 'testKey', '{"test": true}', true),
        ('pg.kafka.trx.outbox', 'testKey', '{"test2": true}', false),
        ('pg.kafka.trx.outbox', 'testKey', '{"test3": true}', true)
    `
  )

  await setTimeout(1000)

  assert.deepStrictEqual(
    messages.map(m => m.value),
    [{ test: true }, { test2: true }, { test3: true }]
  )
})

test('events should be reconsumed by another consumer', async () => {
  const messages = [] as string[]

  pgKafkaTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      override async onHandled(): Promise<void> {}
      async handleMessage(message: OutboxMessage) {
        messages.push(Object.keys(message.value ?? {})![0] ?? '')
        return { value: { success: true } }
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

  pgKafkaTrxOutbox2 = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      override async onHandled(): Promise<void> {}
      async handleMessage(message: OutboxMessage) {
        messages.push(Object.keys(message.value ?? {})![0] ?? '')
        return { value: { success: true } }
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
  await pgKafkaTrxOutbox2.start()

  await pg.query(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value, is_event)
      VALUES
        ('pg.kafka.trx.outbox', 'testKey', '{"test": true}', true),
        ('pg.kafka.trx.outbox', 'testKey', '{"test2": true}', false),
        ('pg.kafka.trx.outbox', 'testKey', '{"test3": true}', true)
    `
  )

  await setTimeout(1000)

  assert.deepStrictEqual(messages.toSorted(), ['test', 'test', 'test2', 'test3', 'test3'])
})
