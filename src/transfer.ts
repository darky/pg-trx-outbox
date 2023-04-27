import { Kafka, Producer } from 'kafkajs'
import type { Options, OutboxMessage, StartStop } from './types'
import { Client } from 'pg'

export class Transfer implements StartStop {
  private producer: Producer
  private kafka: Kafka
  private pg: Client

  constructor(private readonly options: Options) {
    this.kafka = new Kafka({
      clientId: 'pg_kafka_trx_outbox',
      ...options.kafkaOptions,
    })
    this.producer = this.kafka.producer(options.producerOptions)
    this.pg = new Client({
      application_name: 'pg_kafka_trx_outbox',
      ...options.pgOptions,
    })
    this.pg.on('error', err => this.options.outboxOptions?.onError?.(err))
  }

  async start() {
    await this.producer.connect()
    await this.pg.connect()
  }

  async stop() {
    await this.producer.disconnect()
    await this.pg.end()
  }

  async transferMessages(passedMessages: readonly OutboxMessage[] = []) {
    try {
      await this.pg.query('begin')
      const messages = passedMessages.length ? passedMessages : await this.fetchPgMessages()
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

  private makeBatchForKafka(messages: readonly OutboxMessage[]) {
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
