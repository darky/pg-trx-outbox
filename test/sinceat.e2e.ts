import { afterEach, beforeEach, test } from 'node:test'
import { PgTrxOutbox } from '../src/index.ts'
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql'
import { Client } from 'pg'
import assert from 'node:assert'
import { OutboxMessage } from '../src/types.ts'
import { setTimeout } from 'timers/promises'
import { SerialAdapter } from '../src/adapters/abstract/serial.ts'

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

test('basic since_at check', async () => {
  pgTrxOutbox = new PgTrxOutbox({
    adapter: new (class extends SerialAdapter {
      async start() {}
      async stop() {}
      async onHandled() {}
      async handleMessage() {
        return { value: { ok: true } }
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
      INSERT INTO pg_trx_outbox (topic, "key", value, since_at)
      VALUES
        ('pg.trx.outbox', 'testKey', '{"ok": true}', null),
        ('pg.trx.outbox', 'testKey', '{"ok": true}', now() - interval '1 minute'),
        ('pg.trx.outbox', 'testKey', '{"ok": true}', now() + interval '1 minute')
    `
  )
  await setTimeout(1000)

  const resp = await pg.query<OutboxMessage>('select * from pg_trx_outbox order by id').then(r => r.rows)

  assert.strictEqual(resp[0]?.processed, true)
  assert.strictEqual(resp[1]?.processed, true)
  assert.strictEqual(resp[2]?.processed, false)
})
