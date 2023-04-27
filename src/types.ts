import type { IHeaders, KafkaConfig, ProducerConfig } from 'kafkajs'
import type { ClientConfig } from 'pg'

export type OutboxMessage = {
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

export interface StartStop {
  start(): Promise<void>
  stop(): Promise<void>
}

export type Options = {
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
    onError?: (err: Error) => unknown
  }
}
