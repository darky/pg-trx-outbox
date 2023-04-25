import { IHeaders, Kafka, KafkaConfig, Producer, ProducerConfig } from 'kafkajs'
import { Client, ClientConfig } from 'pg'
import createSubscriber, { Subscriber } from 'pg-listen'

type OutboxMessage = {
  id: string
  processed: false
  created_at: Date
  updated_at: Date
  topic: string
  key: string | null
  value: string | null
  partition: number | null
  timestamp: string
  headers: IHeaders | null
}

export class PgKafkaTrxOutbox {
  private producer: Producer
  private kafka: Kafka
  private pg: Client
  private pollIntervalId?: NodeJS.Timer
  private notifier?: Subscriber
  private processing = false
  private repeat = false

  constructor(
    private readonly options: {
      pgOptions: ClientConfig
      kafkaOptions: KafkaConfig
      producerOptions?: ProducerConfig & {
        acks?: -1 | 0 | 1
        timeout?: number
      }
      outboxOptions?: {
        pollInterval?: number
        limit?: number
        notify?: boolean
      }
    }
  ) {
    this.kafka = new Kafka({
      clientId: 'pg_kafka_trx_outbox',
      ...options.kafkaOptions,
    })
    this.producer = this.kafka.producer(options.producerOptions)
    this.pg = new Client({
      application_name: 'pg_kafka_trx_outbox',
      ...options.pgOptions,
    })
    if (options.outboxOptions?.notify) {
      this.notifier = createSubscriber({
        application_name: 'pg_kafka_trx_outbox_pubsub',
        ...options.pgOptions,
      })
    }
  }

  async connect() {
    await this.producer.connect()
    await this.pg.connect()
    if (this.notifier) {
      await this.notifier.connect()
      await this.notifier.listenTo('pg_kafka_trx_outbox')
    }
  }

  start() {
    this.pollIntervalId = setInterval(
      () => this.processing || this.transferMessages(),
      this.options.outboxOptions?.pollInterval ?? 5000
    )
    if (this.notifier) {
      this.notifier.notifications.on('pg_kafka_trx_outbox', () => {
        this.pollIntervalId?.refresh()
        this.processing ? (this.repeat = true) : this.transferMessages()
      })
    }
  }

  async disconnect() {
    clearInterval(this.pollIntervalId)
    if (this.notifier) {
      await this.notifier.close()
    }
    await this.producer.disconnect()
    await this.pg.end()
  }

  private async transferMessages() {
    this.processing = true
    this.repeat = false
    try {
      await this.pg.query('begin')
      const messages = await this.fetchPgMessages()
      const topicMessages = this.makeBatchForKafka(messages)
      await this.producer.sendBatch({
        topicMessages,
        acks: this.options.producerOptions?.acks ?? -1,
        timeout: this.options.producerOptions?.timeout ?? 30000,
      })
      await this.updateToProcessed(messages.map(r => r.id))
      await this.pg.query('commit')
    } catch (e) {
      await this.pg.query('rollback')
      if ((e as { code: string }).code !== '55P03') {
        throw e
      }
    } finally {
      this.processing = false
      if (this.repeat) {
        this.transferMessages()
      }
    }
  }

  private async fetchPgMessages() {
    return await this.pg
      .query<OutboxMessage>(
        `
          select * from pg_kafka_trx_outbox
          where processed = false
          order by id
          limit $1
          for update nowait
        `,
        [this.options.outboxOptions?.limit ?? 50]
      )
      .then(resp => resp.rows)
  }

  private async updateToProcessed(ids: string[]) {
    await this.pg.query(
      `
        update pg_kafka_trx_outbox
        set processed = true, updated_at = now()
        where id = any($1)
      `,
      [ids]
    )
  }

  private makeBatchForKafka(messages: OutboxMessage[]) {
    const grouped = new Map<string, OutboxMessage[]>()
    messages.forEach(m => grouped.set(m.topic, (grouped.get(m.topic) ?? []).concat(m)))
    return Array.from(grouped.entries()).map(([topic, rows]) => ({
      topic,
      messages: rows.map(r => ({
        key: r.key,
        value: r.value,
        partititon: r.partition,
        timestamp: r.timestamp,
        headers: r.headers ?? {},
      })),
    }))
  }
}
