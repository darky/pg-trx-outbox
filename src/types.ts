import type { PoolConfig } from 'pg'

export type OutboxMessage = {
  id: string
  processed: false
  created_at: Date
  updated_at: Date
  topic: string
  key: string | null
  value: unknown
  partition: number | null
  timestamp: string
  response: unknown
  error: string | null
  context_id: number
  meta: object | null
  headers: Record<string, string> | null
  attempts: number
  since_at: Date | null
}

export interface StartStop {
  start(): Promise<void>
  stop(): Promise<void>
}

export interface Send {
  send(
    messages: readonly OutboxMessage[]
  ): Promise<((PromiseFulfilledResult<unknown> | PromiseRejectedResult) & { meta?: object })[]>
}

export interface OnHandled {
  onHandled(messages: readonly OutboxMessage[]): Promise<void>
}

export type Adapter = StartStop & Send & OnHandled

export type Options = {
  pgOptions: PoolConfig
  adapter: Adapter
  outboxOptions?: {
    /**
     * how often to poll PostgreSQL for new messages, default 5000 milliseconds
     */
    pollInterval?: number
    /**
     * how much messages send for processing, default 50
     */
    limit?: number
    mode?: 'short-polling' | 'notify'
    /**
     * callback for handling uncaught error
     */
    onError?: (err: Error) => unknown
    /**
     * For scaling by partitions, pass partition number 0..n
     */
    partition?: number
    /**
     * Array of topics, which should be handled only
     */
    topicFilter?: string[]
    /**
     * predicate for error retrying
     */
    retryError?: (err: Error) => boolean
    /**
     * retrying delay in seconds, default 5 seconds
     */
    retryDelay?: number
    /**
     * max attempts for retry, default 5
     */
    retryMaxAttempts?: number
  }
  eventSourcingOptions?: {
    /**
     * how much messages send for processing on init sync, default 100
     */
    initSyncBatchSize?: number
  }
}
