# pg-kafka-trx-outbox

Transactional outbox from Postgres to Kafka <br/>
More info: https://microservices.io/patterns/data/transactional-outbox.html

## Algo

Messages polled from PostgreSQL using `FOR UPDATE NOWAIT` with `COMMIT` order. This batch of messaged produced to Kafka. Then messages marked as `produced`. 

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
  pgOptions: {/**/}, // [1]
  kafkaOptions: {/**/}, // [2],
  producerOptions: {/**/}, // [3],
  outboxOptions: {
    pollInterval: 5000, // how often to poll PostgreSQL for new messages, default 5000 milliseconds
    acks: -1 // [4],
    timeout: 30000 // [4]
  }
});

await pgKafkaTrxOutbox.connect();
pgKafkaTrxOutbox.start();

// on shutdown

await pgKafkaTrxOutbox.disconnect();
```

* [1] https://node-postgres.com/apis/client#new-client
* [2] https://kafka.js.org/docs/configuration
* [3] https://kafka.js.org/docs/producing#options
* [4] https://kafka.js.org/docs/producing#producing-messages acks, timeout options
