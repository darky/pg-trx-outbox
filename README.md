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
  since_at timestamptz,
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
    // need return Promise.allSettled compatible array for each message in appropriate order
    
    return [
      // example of successful message handling
      { value: {someResponse: true}, status: 'fulfilled' },
      // example of exception while message handling
      { reason: 'some error text', status: 'rejected' },
      // Optional meta property can be returned
      // which will be saved in appropriate DB column
      { value: {someResponse: true}, status: 'fulfilled', meta: {someMeta: true} }
    ]
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

#### Built-in adapters for message handling order

##### SerialAdapter example

`SerialAdapter` can be used for handling messages of batch serially, step by step

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'
import { SerialAdapter } from 'pg-trx-outbox/dist/src/adapters/abstract/serial'

class MyOwnAdapter extends SerialAdapter {
  async start() {
    // some init logic here, establish connection and so on
  },
  
  async stop() {
    // some graceful shutdown logic here, close connection and so on
  },
  
  async handleMessage(message) {
    // serial messages handling here, step by step
    
    // example of successful message handling
    return { value: {someResponse: true}, status: 'fulfilled' },
    // example of exception while message handling
    return { reason: 'some error text', status: 'rejected' },
    // Optional meta property can be returned
    // which will be saved in appropriate DB column
    return { value: {someResponse: true}, status: 'fulfilled', meta: {someMeta: true} }
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

##### ParallelAdapter example

`ParallelAdapter` can be used for handling messages in parallel independently of each other

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'
import { ParallelAdapter } from 'pg-trx-outbox/dist/src/adapters/abstract/parallel'

class MyOwnAdapter extends ParallelAdapter {
  async start() {
    // some init logic here, establish connection and so on
  },
  
  async stop() {
    // some graceful shutdown logic here, close connection and so on
  },
  
  async handleMessage(message) {
    // parallel messages handling here
    
    // example of successful message handling
    return { value: {someResponse: true}, status: 'fulfilled' },
    // example of exception while message handling
    return { reason: 'some error text', status: 'rejected' },
    // Optional meta property can be returned
    // which will be saved in appropriate DB column
    return { value: {someResponse: true}, status: 'fulfilled', meta: {someMeta: true} }
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


##### GroupedAdapter example

`GroupedAdapter` is combination of `SerialAdapter` and `ParallelAdapter`
Messages with same `key` will be handled serially, step by step. 
But messages with different `key` will be handled in parallel

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'
import { GroupedAdapter } from 'pg-trx-outbox/dist/src/adapters/abstract/grouped'

class MyOwnAdapter extends GroupedAdapter {
  async start() {
    // some init logic here, establish connection and so on
  },
  
  async stop() {
    // some graceful shutdown logic here, close connection and so on
  },
  
  async handleMessage(message) {
    // grouped by `key` message handling here
    
    // example of successful message handling
    return { value: {someResponse: true}, status: 'fulfilled' },
    // example of exception while message handling
    return { reason: 'some error text', status: 'rejected' },
    // Optional meta property can be returned
    // which will be saved in appropriate DB column
    return { value: {someResponse: true}, status: 'fulfilled', meta: {someMeta: true} }
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
  const waitResp = await pgTrxOutbox.waitResponse<{someValue: boolean}>(id)
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
  since_at timestamptz,
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
  // key passing will prune unnecessary partitions
  const waitResp = await pgTrxOutbox0.waitResponse<{someValue: boolean}>(id, 'someKey')
} catch(e) {
  // if error will be happens on message handling
}

// on shutdown

await pgTrxOutbox0.stop();
await pgTrxOutbox1.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L32

## Built-in messages meta

In *Custom adapter* section you already see, that `meta` field can be returned in message handling
and implicitly will be saved in DB in appropriate column. Also **pg-trx-outbox** provide some built-in `meta`,
which will be persisted with using of `SerialAdapter`, `ParallelAdapter` or `GroupedAdapter`:

```js
{
  pgTrxOutbox: {
    time: 1.343434, // time of message handling execution, high-precision count of milliseconds
    libuv: { /* [1] */ } // contains libuv metrics for detecting CPU stucks
  }
}
```

- [1] https://nodejs.org/dist/latest-v20.x/docs/api/perf_hooks.html#class-histogram

## Context id

Very useful to link chain of messages handling with through `contextId`<br/>
More info about pattern: https://microservices.io/patterns/observability/distributed-tracing.html <br/>
Using `SerialAdapter`, `ParallelAdapter` or `GroupedAdapter` very easy to extract `contextId` of current handled message and pass it futher:

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'
import { SerialAdapter } from 'pg-trx-outbox/dist/src/adapters/abstract/serial'

class MyOwnAdapter extends SerialAdapter {
  async start() {
    // some init logic here, establish connection and so on
  },
  
  async stop() {
    // some graceful shutdown logic here, close connection and so on
  },
  
  async handleMessage(message) {
    // `contextId` can be extracted in any place of async stack and passed to futher
    // AsyncLocalStorage will used for this purpose [3]
    // `pg` is hypothetical object of your code, which means PostgreSQL client
    await pg.query(`
      insert into pg_trx_outbox (topic, "key", value, context_id)
      values ('some.topic', 'key', '{"someValue": true}', $1)
    `, [pgTrxOutbox.contextId()])
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
- [3] https://nodejs.org/dist/latest-v20.x/docs/api/async_context.html#class-asynclocalstorage

## Filter messages handling by topic

Messages handling can be filtered by topic. 
It's useful, when you have multiple microservices with one shared DB 
and each microservice should handle specific bunch of messages filtered by topic.

##### More optimized index for not processed messages

```sql
CREATE INDEX pg_trx_outbox_not_processed_idx
  ON pg_trx_outbox (processed, topic, id)
  WHERE (processed = false);
```

##### Topics filter example

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnOrBuiltInAdapter(),
  outboxOptions: {
    topicFilter: ['for.handle'] // Array of topics, which should be handled only
    /* [2] */
  }
});

await pgTrxOutbox.start();

await pg
  .query<{ id: string }>(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('for.handle', 'someKey', '{"someValue": true}'), -- this message will be handled
        ('not.for.handle', 'someKey', '{"someValue": true}') -- this message not will be handled
    `
  )

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L32

## Scheduling of messages

Messages can be delayed and handled at some time in future

##### Messages scheduling example

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnOrBuiltInAdapter(),
  outboxOptions: {
    /* [2] */
  }
});

await pgTrxOutbox.start();

await pg
  .query<{ id: string }>(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value, since_at)
      VALUES ('for.handle', 'someKey', '{"someValue": true}', now() + interval '1 hour'), -- this message will be handled since 1 hour
        ('not.for.handle', 'someKey', '{"someValue": true}', null), -- this message not will be handled ASAP
        ('not.for.handle', 'someKey', '{"someValue": true}', now() - interval '1 hour'), -- this message not will be handled ASAP
    `
  )

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L32

## Retrying of errors

Messages can be retried via predicate

##### Retrying of errors example

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnOrBuiltInAdapter(),
  outboxOptions: {
    retryError(err) {
      // return true for retrying
    }
    /* [2] */
  }
});

await pgTrxOutbox.start();

await pg
  .query<{ id: string }>(
    `
      INSERT INTO pg_trx_outbox (topic, "key", value)
      VALUES ('for.handle', 'someKey', '{"someValue": true}')
    `
  )

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L32
