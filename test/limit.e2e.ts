import { afterEach, beforeEach, test } from 'node:test'
import { Client } from 'pg'
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { PgTrxOutbox } from '../src/index.ts'
import { setTimeout } from 'timers/promises'
import assert from 'assert'
import { SerialAdapter } from '../src/adapters/abstract/serial.ts'
import type { OutboxMessage } from '../src/types.ts'

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
    CREATE TABLE IF NOT EXISTS pg_trx_outbox (
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

test('limit', async () => {
  let i = 0
  let message: OutboxMessage
  pgTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      override async onHandled(): Promise<void> {}
      async handleMessage(m: OutboxMessage) {
        i++
        message = m
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
      limit: 1,
    },
  })
  await pg.query(`
    INSERT INTO pg_trx_outbox
      (topic, "key", value)
      VALUES ('pg.trx.outbox', 'testKey', '{"test": true, "n": 1}'),
        ('pg.trx.outbox', 'testKey', '{"test": true, "n": 2}');
    `)
  await pgTrxOutbox.start()
  await setTimeout(350)

  const processedRow: {
    processed: boolean
    created_at: Date
    updated_at: Date
  }[] = await pg.query(`select * from pg_trx_outbox order by id`).then(resp => resp.rows)
  assert.strictEqual(processedRow[0]?.processed, true)
  assert.strictEqual(processedRow[0]?.updated_at > processedRow[0]?.created_at, true)

  assert.strictEqual(processedRow[1]?.processed, false)
  assert.strictEqual(processedRow[1]?.updated_at.toISOString(), processedRow[1]?.created_at.toISOString())

  assert.strictEqual(i, 1)
  assert.deepStrictEqual(message!.value, { n: 1, test: true })
})
