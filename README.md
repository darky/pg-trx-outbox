# pg-trx-outbox

![photo_2023-04-22_03-38-43](https://user-images.githubusercontent.com/1832800/234091651-2a496563-6016-45fa-96f6-0b875899fe7e.jpg)

Transactional outbox of Postgres for Node.js<br/>
Primarly created for Kafka, but can be used with any destination<br/>
More info about Transactional Outbox pattern: https://microservices.io/patterns/data/transactional-outbox.html

## DB structure

```sql
CREATE TABLE IF NOT EXISTS pg_trx_outbox (
  id bigserial NOT NULL,
  processed bool NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  topic text NOT NULL,
  "key" text NULL,
  value jsonb NULL,
  "partition" int2 NULL,
  "timestamp" int8 NULL,
  headers jsonb NULL,
  response jsonb NULL,
  error text NULL,
  CONSTRAINT pg_trx_outbox_pk PRIMARY KEY (id)
);

CREATE INDEX pg_trx_outbox_not_processed_idx
  ON pg_trx_outbox (processed, id)
  WHERE (processed = false);
```

## Modes

### Short polling mode

Messages polled from PostgreSQL using `FOR UPDATE NOWAIT` with `COMMIT` order. This batch of messaged produced to destination. Then messages marked as `processed`. This mode used by default.

#### Kafka Example (short polling)

##### Deps

`npm install --save kafkajs`

##### Code

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'
import { Kafka } from 'pg-trx-outbox/dist/src/adapters/kafka'

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new Kafka({
    kafkaOptions: {/* [2] */},
    producerOptions: {
      /* [3] */
      acks: -1 // [4],
      timeout: 30000 // [4]
    },
  }),
  outboxOptions: {
    mode: 'short-polling',
    pollInterval: 5000, // how often to poll PostgreSQL for new messages, default 5000 milliseconds
    limit: 50, // how much messages in batch, default 50
    onError(err) {/**/} // callback for catching uncaught error
  }
});

await pgTrxOutbox.start();

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://kafka.js.org/docs/configuration
- [3] https://kafka.js.org/docs/producing#options
- [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options

### Notify mode

For reducing latency PostgreSQL `LISTEN/NOTIFY` can be used. When message inserted in DB, `NOTIFY` called and all listeners try to fetch new messages.
**Short polling** mode also used here, because `LISTEN/NOTIFY` not robust mechanism and notifications can be lost.

#### Kafka Example (notify)

##### Deps

`npm install --save kafkajs pg-listen`

##### SQL migration

```sql
CREATE OR REPLACE FUNCTION pg_trx_outbox() RETURNS trigger AS $trigger$
  BEGIN
    PERFORM pg_notify('pg_trx_outbox', '{}');
    RETURN NEW;
  END;
$trigger$ LANGUAGE plpgsql;

CREATE TRIGGER pg_trx_outbox AFTER INSERT ON pg_trx_outbox
EXECUTE PROCEDURE pg_trx_outbox();
```

##### Code

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'
import { Kafka } from 'pg-trx-outbox/dist/src/adapters/kafka'

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new Kafka({
    kafkaOptions: {/* [2] */},
    producerOptions: {
      /* [3] */
      acks: -1 // [4],
      timeout: 30000 // [4]
    },
  }),
  outboxOptions: {
    mode: 'notify',
    pollInterval: 5000, // how often to poll PostgreSQL for new messages, default 5000 milliseconds
    limit: 50, // how much messages in batch, default 50
    onError(err) {/**/} // callback for catching uncaught error
  }
});

await pgTrxOutbox.start();

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://kafka.js.org/docs/configuration
- [3] https://kafka.js.org/docs/producing#options
- [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options

### Logical replication mode

In this mode messages replicated via PostgreSQL logical replication.

#### Kafka Example (logical)

Firstly, need to setup `wal_level=logical` in PostgreSQL config and restart it.

##### Deps

`npm install --save kafkajs pg-logical-replication dataloader p-queue`

##### SQL migration

```sql
SELECT pg_create_logical_replication_slot(
  'pg_trx_outbox',
  'pgoutput'
);

CREATE PUBLICATION pg_trx_outbox FOR TABLE pg_trx_outbox WITH (publish = 'insert');
```

##### Code

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'
import { Kafka } from 'pg-trx-outbox/dist/src/adapters/kafka'

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new Kafka({
    kafkaOptions: {/* [2] */},
    producerOptions: {
      /* [3] */
      acks: -1 // [4],
      timeout: 30000 // [4]
    },
  }),
  outboxOptions: {
    mode: 'logical',
    logicalBatchInterval: 100, // how often to process new messages in 'logical' mode, default 100 milliseconds
    onError(err) {/**/} // callback for catching uncaught error
  }
});

await pgTrxOutbox.start();

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://kafka.js.org/docs/configuration
- [3] https://kafka.js.org/docs/producing#options
- [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options

## Create custom adapter for destination

Some adapters for Transactional Outbox destination are built-in.<br/>
You can find it here: https://github.com/darky/pg-trx-outbox/tree/master/src/adapters <br/>
But sometimes you want to create your own

#### Custom adapter example

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'
import { Adapter } from 'pg-trx-outbox/dist/src/types'

class MyOwnAdapter implements Adapter {
  async start() {
    // some init logic here, establish connection and so on
  },
  
  async stop() {
    // some graceful shutdown logic here, close connection and so on
  },
  
  async send(messages) {
    // messages handling here
    // need return responses array for each message in appropriate order
    
    // parallel example
    return Promise.allSettled(messages.map(async m => { /* message handler with response */ }))
    
    // serial example
    const resp = [];
    for (const message of messages) {
      resp.push(await pReflect(/* [3] some message handler */))
    }
    return resp;
  },
}

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnAdapter(),
  outboxOptions: {
    /* [2] */
  }
});

await pgTrxOutbox.start();

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L32
- [3] https://github.com/sindresorhus/p-reflect

## Wait response of specific message

It's possible to wait response of specific message handling<br/>
Short polling algorithm will be used for this purpose now (maybe in future `notify` and `logical` will be implemented too)

#### Wait response example

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnOrBuiltInAdapter(),
  outboxOptions: {
    respondInterval: 100 // how often to process responses of messages, default 100 milliseconds
    /* [2] */
  }
});

await pgTrxOutbox.start();

const [{ id } = { id: '' }] = await pg
  .query<{ id: string }>(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('some.topic', 'someKey', '{"someValue": true}')
      RETURNING id;
    `
  )
  .then(resp => resp.rows)

try {
  const waitResp = await pgKafkaTrxOutbox.waitResponse<{someValue: boolean}>(id)
} catch(e) {
  // if error will be happens on message handling
}

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L32

## Partitioning

Over time, performance of one table `pg_trx_outbox` can be not enough.
Also, handling messages throughput can be not enough too.
In this cases native PostgreSQL partitioning can help and **pg-trx-outbox** supports it.
Implementation inspired by Kafka key partitioning mechanism.

#### Partitioning example

##### SQL migration

```sql
CREATE TABLE pg_trx_outbox (
  id bigserial NOT NULL,
  processed bool NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  topic text NOT NULL,
  "key" text NULL,
  value jsonb NULL,
  "partition" int2 NULL,
  "timestamp" int8 NULL,
  headers jsonb NULL,
  response jsonb NULL,
  error text NULL,
  CONSTRAINT pg_trx_outbox_pk PRIMARY KEY (id, key)
) PARTITION BY HASH (key);

CREATE TABLE pg_trx_outbox_0 PARTITION OF pg_trx_outbox FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE pg_trx_outbox_1 PARTITION OF pg_trx_outbox FOR VALUES WITH (MODULUS 3, REMAINDER 1);
CREATE TABLE pg_trx_outbox_2 PARTITION OF pg_trx_outbox FOR VALUES WITH (MODULUS 3, REMAINDER 2);
```

##### Code

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'

const pgTrxOutbox0 = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnOrBuiltInAdapter(),
  outboxOptions: {
    partition: 0 // this istance of `PgTrxOutbox` will handle only `0` partition
    /* [2] */
  }
});
const pgTrxOutbox1 = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnOrBuiltInAdapter(),
  outboxOptions: {
    partition: 1 // this instance of `PgTrxOutbox` will handle only `1` partition
    /* [2] */
  }
});

await pgTrxOutbox0.start();
await pgTrxOutbox1.start();

const [{ id } = { id: '' }] = await pg
  .query<{ id: string }>(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('some.topic', 'someKey', '{"someValue": true}')
      RETURNING id;
    `
  )
  .then(resp => resp.rows)

try {
  // waitResponse can handle response from any partition
  const waitResp = await pgKafkaTrxOutbox0.waitResponse<{someValue: boolean}>(id)
} catch(e) {
  // if error will be happens on message handling
}

// on shutdown

await pgTrxOutbox0.stop();
await pgTrxOutbox1.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L32
