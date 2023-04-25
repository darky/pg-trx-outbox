# pg-kafka-trx-outbox

![photo_2023-04-22_03-38-43](https://user-images.githubusercontent.com/1832800/234091651-2a496563-6016-45fa-96f6-0b875899fe7e.jpg)

Transactional outbox from Postgres to Kafka <br/>
More info: https://microservices.io/patterns/data/transactional-outbox.html

## Algo

Messages polled from PostgreSQL using `FOR UPDATE NOWAIT` with `COMMIT` order. This batch of messaged produced to Kafka. Then messages marked as `processed`.

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

## Example (short polling)

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
    pollInterval: 5000, // how often to poll PostgreSQL for new messages, default 5000 milliseconds
    limit: 50, // how much messages in batch, default 50
  }
});

await pgKafkaTrxOutbox.connect();
pgKafkaTrxOutbox.start();

// on shutdown

await pgKafkaTrxOutbox.disconnect();
```

- [1] https://node-postgres.com/apis/client#new-client
- [2] https://kafka.js.org/docs/configuration
- [3] https://kafka.js.org/docs/producing#options
- [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options

## Example (pub/sub)

For reducing latency PostgreSQL LISTEN/NOTIFY can be used

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
    notify: true // Use PostgreSQL LISTEN/NOTIFY for reducing poll latency
  }
});

await pgKafkaTrxOutbox.connect();
pgKafkaTrxOutbox.start();

// on shutdown

await pgKafkaTrxOutbox.disconnect();
```

- [1] https://node-postgres.com/apis/client#new-client
- [2] https://kafka.js.org/docs/configuration
- [3] https://kafka.js.org/docs/producing#options
- [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options
