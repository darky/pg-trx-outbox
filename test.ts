import { Admin, Consumer, EachMessagePayload, Kafka } from "kafkajs";
import { afterEach, beforeEach, test } from "node:test";
import { Client } from "pg";
import {
  KafkaContainer,
  PostgreSqlContainer,
  StartedKafkaContainer,
  StartedPostgreSqlContainer,
} from "testcontainers";
import { PgKafkaTrxOutbox } from "./index";
import { setTimeout } from "timers/promises";
import assert from "assert";

let kafkaDocker: StartedKafkaContainer;
let pgDocker: StartedPostgreSqlContainer;
let pg: Client;
let kafkaAdmin: Admin;
let kafkaConsumer: Consumer;
let pgKafkaTrxOutbox: PgKafkaTrxOutbox;
let messages: EachMessagePayload[] = [];

beforeEach(async () => {
  kafkaDocker = await new KafkaContainer()
    .withExposedPorts(9093)
    .withReuse()
    .start();
  pgDocker = await new PostgreSqlContainer()
    .withReuse()
    .withCommand(["-c", "fsync=off"])
    .start();

  pg = new Client({
    host: pgDocker.getHost(),
    port: pgDocker.getPort(),
    user: pgDocker.getUsername(),
    password: pgDocker.getPassword(),
    database: pgDocker.getDatabase(),
    application_name: "pg_kafka_trx_outbox_admin",
  });
  await pg.connect();
  await pg.query(`
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
  `);
  await pg.query("truncate pg_kafka_trx_outbox");

  const kafka = new Kafka({
    clientId: "pg_kafka_trx_outbox_admin",
    brokers: [`${kafkaDocker.getHost()}:${kafkaDocker.getMappedPort(9093)}`],
  });
  kafkaAdmin = kafka.admin();
  await kafkaAdmin.connect();
  try {
    await kafkaAdmin.deleteTopics({ topics: ["pg.kafka.trx.outbox"] });
  } catch (e) {}
  await kafkaAdmin.createTopics({ topics: [{ topic: "pg.kafka.trx.outbox" }] });

  kafkaConsumer = kafka.consumer({ groupId: "test" });
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: "pg.kafka.trx.outbox" });
  kafkaConsumer.run({
    eachMessage: async (payload) => (messages.push(payload), void 0),
  });

  pgKafkaTrxOutbox = new PgKafkaTrxOutbox({
    kafkaOptions: {
      brokers: [`${kafkaDocker.getHost()}:${kafkaDocker.getMappedPort(9093)}`],
    },
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
  });
  await pgKafkaTrxOutbox.connect();
});

afterEach(async () => {
  await pgKafkaTrxOutbox.disconnect();
  await pg.end();
  await kafkaAdmin.disconnect();
  await kafkaConsumer.disconnect();
  messages = [];
});

test("basic polling", async () => {
  await pg.query(`
    INSERT INTO pg_kafka_trx_outbox
      (topic, "key", value)
      VALUES ('pg.kafka.trx.outbox', 'testKey', '{"test": true}');
    `);
  pgKafkaTrxOutbox.start();
  await setTimeout(1000);

  const processedRow: {
    processed: boolean;
    created_at: Date;
    updated_at: Date;
  } = await pg
    .query(`select * from pg_kafka_trx_outbox`)
    .then((resp) => resp.rows[0]);
  assert.strictEqual(processedRow.processed, true);
  assert.strictEqual(processedRow.updated_at > processedRow.created_at, true);

  assert.strictEqual(messages.length, 1);
  assert.strictEqual(messages[0]?.topic, "pg.kafka.trx.outbox");
  assert.strictEqual(messages[0]?.partition, 0);
  assert.strictEqual(messages[0]?.message.key?.toString(), "testKey");
  assert.strictEqual(messages[0]?.message.value?.toString(), '{"test": true}');
  assert.strictEqual(messages[0]?.message.offset, "0");
  assert.strictEqual(
    Date.now() - Number(messages[0]?.message.timestamp) < 1000,
    true
  );
});
