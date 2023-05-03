import { Producer, Kafka as KafkaJS, KafkaConfig, ProducerConfig } from 'kafkajs'
import type { OutboxMessage, Send, StartStop } from '../types'

export class Kafka implements StartStop, Send {
  private producer: Producer
  private kafka: KafkaJS

  constructor(
    private options: {
      kafkaOptions: KafkaConfig
      producerOptions?: ProducerConfig & {
        acks?: -1 | 0 | 1
        timeout?: number
      }
    }
  ) {
    this.kafka = new KafkaJS({
      clientId: 'pg_trx_outbox',
      ...options.kafkaOptions,
    })
    this.producer = this.kafka.producer(options.producerOptions)
  }

  async start() {
    await this.producer.connect()
  }

  async stop() {
    await this.producer.disconnect()
  }

  async send(messages: readonly OutboxMessage[]) {
    await this.producer.sendBatch({
      topicMessages: this.makeBatchForKafka(messages),
      acks: this.options.producerOptions?.acks ?? -1,
      timeout: this.options.producerOptions?.timeout ?? 30000,
    })
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
