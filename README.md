# pg-kafka-trx-outbox

![photo_2023-04-22_03-38-43](https://user-images.githubusercontent.com/1832800/234091651-2a496563-6016-45fa-96f6-0b875899fe7e.jpg)

Transactional outbox from Postgres to Kafka for Node.js<br/>
More info: https://microservices.io/patterns/data/transactional-outbox.html

## DB structure

```sql
CREATE TABLE IF NOT EXISTS pg_kafka_trx_outbox (
  id bigserial NOT NULL,
  processed bool NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  topic text NOT NULL,
  "key" text NULL,
  value text NULL,
  "partition" int2 NULL,
  "timestamp" int8 NULL,
  headers jsonb NULL,
  CONSTRAINT pg_kafka_trx_outbox_pk PRIMARY KEY (id)
);

CREATE INDEX pg_kafka_trx_outbox_not_processed_idx
  ON pg_kafka_trx_outbox (processed, id)
  WHERE (processed = false);
```

## Short polling mode

Messages polled from PostgreSQL using `FOR UPDATE NOWAIT` with `COMMIT` order. This batch of messaged produced to Kafka. Then messages marked as `processed`. This mode used by default.

### Example (short polling)

```ts
import { PgKafkaTrxOutbox } from 'pg-kafka-trx-outbox'

const pgKafkaTrxOutbox = new PgKafkaTrxOutbox({
  pgOptions: {/* [1] */},
  kafkaOptions: {/* [2] */},
  producerOptions: {
    /* [3] */
    acks: -1 // [4],
    timeout: 30000 // [4]
  },,
  outboxOptions: {
    mode: 'short-polling',
    pollInterval: 5000, // how often to poll PostgreSQL for new messages, default 5000 milliseconds
    limit: 50, // how much messages in batch, default 50
    onError(err) {/**/} // callback for catching uncaught error
  }
});

await pgKafkaTrxOutbox.start();

// on shutdown

await pgKafkaTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/client#new-client
- [2] https://kafka.js.org/docs/configuration
- [3] https://kafka.js.org/docs/producing#options
- [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options

## Notify mode

For reducing latency PostgreSQL `LISTEN/NOTIFY` can be used. When message inserted in DB, `NOTIFY` called and all listeners try to fetch new messages.
**Short polling** mode also used here, because `LISTEN/NOTIFY` not robust mechanism and notifications can be lost.

### Example (notify)

```sql
CREATE OR REPLACE FUNCTION pg_kafka_trx_outbox() RETURNS trigger AS $trigger$
  BEGIN
    PERFORM pg_notify('pg_kafka_trx_outbox', '{}');
    RETURN NEW;
  END;
$trigger$ LANGUAGE plpgsql;

CREATE TRIGGER pg_kafka_trx_outbox AFTER INSERT ON pg_kafka_trx_outbox
EXECUTE PROCEDURE pg_kafka_trx_outbox();
```

```ts
import { PgKafkaTrxOutbox } from 'pg-kafka-trx-outbox'

const pgKafkaTrxOutbox = new PgKafkaTrxOutbox({
  pgOptions: {/* [1] */},
  kafkaOptions: {/* [2] */},
  producerOptions: {
    /* [3] */
    acks: -1 // [4],
    timeout: 30000 // [4]
  },,
  outboxOptions: {
    mode: 'notify',
    pollInterval: 5000, // how often to poll PostgreSQL for new messages, default 5000 milliseconds
    limit: 50, // how much messages in batch, default 50
    onError(err) {/**/} // callback for catching uncaught error
  }
});

await pgKafkaTrxOutbox.start();

// on shutdown

await pgKafkaTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/client#new-client
- [2] https://kafka.js.org/docs/configuration
- [3] https://kafka.js.org/docs/producing#options
- [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options

## Logical replication mode

In this mode messages replicated via PostgreSQL logical replication.

### Example (logical)

Firstly, need to setup `wal_level=logical` in PostgreSQL config and restart it.

```sql
SELECT pg_create_logical_replication_slot(
  'pg_kafka_trx_outbox',
  'pgoutput'
);

CREATE PUBLICATION pg_kafka_trx_outbox FOR TABLE pg_kafka_trx_outbox WITH (publish = 'insert');
```

```ts
import { PgKafkaTrxOutbox } from 'pg-kafka-trx-outbox'

const pgKafkaTrxOutbox = new PgKafkaTrxOutbox({
  pgOptions: {/* [1] */},
  kafkaOptions: {/* [2] */},
  producerOptions: {
    /* [3] */
    acks: -1 // [4],
    timeout: 30000 // [4]
  },,
  outboxOptions: {
    mode: 'logical',
    onError(err) {/**/} // callback for catching uncaught error
  }
});

await pgKafkaTrxOutbox.start();

// on shutdown

await pgKafkaTrxOutbox.stop();
```

- [1] https://node-postgres.com/apis/client#new-client
- [2] https://kafka.js.org/docs/configuration
- [3] https://kafka.js.org/docs/producing#options
- [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options
