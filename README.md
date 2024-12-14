# pg-trx-outbox

![photo_2023-04-22_03-38-43](https://user-images.githubusercontent.com/1832800/234091651-2a496563-6016-45fa-96f6-0b875899fe7e.jpg)

Transactional outbox of Postgres for Node.js with little Event Sourcing<br/>
More info about Transactional Outbox pattern: https://microservices.io/patterns/data/transactional-outbox.html<br/>
More info about Event Sourcing pattern: https://microservices.io/patterns/data/event-sourcing.html

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
  attempts smallint NOT NULL DEFAULT 0,
  is_event bool NOT NULL DEFAULT false,
  CONSTRAINT pg_trx_outbox_pk PRIMARY KEY (id)
);

CREATE INDEX pg_trx_outbox_not_processed_idx
  ON pg_trx_outbox (processed, id)
  WHERE (processed = false);
```

## Types of DB entities

### Command

* Command it's row when `is_event = false`
* Command handled by one consumer only once (at least once in theory), Postgres SQL `for update` used for row locking
* Usually used for third-party integration
* May propagate events or another commands in same transaction

### Event

* Event it's row when `is_event = true`
* Same event wlll be handled by all consumers
* Usually used for DB denormalization, cache creation, response providing, etc

## Modes

### Short polling mode

Messages polled from PostgreSQL using `FOR UPDATE` with `COMMIT` order. This batch of messaged produced to destination. Then messages marked as `processed`. This mode used by default.

#### Short polling example

##### Code

```ts
import { PgTrxOutbox } from 'pg-trx-outbox'

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnAdapter(), // about adapters see below
  outboxOptions: {
    mode: 'short-polling',
    pollInterval: 5000, // how often to poll PostgreSQL for new messages, default 5000 milliseconds
    limit: 50, // how much messages in batch, default 50
    onError(err) {/**/} // callback for catching uncaught error
  },
  eventSourcingOptions: {/* [2] */}
});

await pgTrxOutbox.start();

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L85

### Notify mode

For reducing latency PostgreSQL `LISTEN/NOTIFY` can be used. When message inserted in DB, `NOTIFY` called and all listeners try to fetch new messages.
**Short polling** mode also used here, because `LISTEN/NOTIFY` not robust mechanism and notifications can be lost.

#### Notify example

##### Deps

`npm install --save pg-listen`

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

const pgTrxOutbox = new PgTrxOutbox({
  pgOptions: {/* [1] */},
  adapter: new MyOwnAdapter(), // about adapters see below
  outboxOptions: {
    mode: 'notify',
    pollInterval: 5000, // how often to poll PostgreSQL for new messages, default 5000 milliseconds
    limit: 50, // how much messages in batch, default 50
    onError(err) {/**/} // callback for catching uncaught error
  },
  eventSourcingOptions: {/* [2] */}
});

await pgTrxOutbox.start();

// on shutdown

await pgTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/pool
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L85

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

  async onHandled(messages) {
    // some optional logic here for handled messages in batch
  }
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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42

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

  async onHandled(messages) {
    // some optional logic here for handled messages in batch
  }
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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42

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

  async onHandled(messages) {
    // some optional logic here for handled messages in batch
  }
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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42


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

  async onHandled(messages) {
    // some optional logic here for handled messages in batch
  }
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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42

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
  attempts smallint NOT NULL DEFAULT 0,
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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42

## Built-in messages meta

In *Custom adapter* section you already see, that `meta` field can be returned in message handling
and implicitly will be saved in DB in appropriate column. Also **pg-trx-outbox** provide some built-in `meta`,
which will be persisted with using of `SerialAdapter`, `ParallelAdapter` or `GroupedAdapter`:

```js
{
  pgTrxOutbox: {
    time: 1.343434, // time of message handling execution, high-precision count of milliseconds
    libuv: { /* [1] */ }, // contains libuv metrics for detecting CPU stucks
    beforeMemory: { /* [2] */ }, // memory usage before event
    afterMemory: { /* [2] */ }, // memory usage after event
    uptime: 100, // [3] uptime of process
    cpuUsage: { /* [4] */ } // CPU usage per event
  }
}
```

- [1] https://nodejs.org/dist/latest-v20.x/docs/api/perf_hooks.html#class-histogram
- [2] https://nodejs.org/dist/latest-v20.x/docs/api/process.html#processmemoryusage
- [3] https://nodejs.org/dist/latest-v20.x/docs/api/process.html#processuptime
- [4] https://nodejs.org/dist/latest-v20.x/docs/api/process.html#processcpuusagepreviousvalue

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

  async onHandled(messages) {
    // some optional logic here for handled messages in batch
  }
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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42
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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42

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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42

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
    },
    retryDelay: 5, // delay of retry in seconds, default is 5
    retryMaxAttempts: 5 // max attempts for retry, default is 5
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
- [2] https://github.com/darky/pg-trx-outbox/blob/master/src/types.ts#L42
