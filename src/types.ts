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
  headers: Record<string, string> | null
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

export type Adapter = StartStop & Send

export type Options = {
  pgOptions: PoolConfig
  adapter: Adapter
  outboxOptions?: {
    /**
     * how often to poll PostgreSQL for new messages, default 5000 milliseconds
     */
    pollInterval?: number
    /**
     * how often to process new messages in 'logical' mode, default 100 milliseconds
     */
    logicalBatchInterval?: number
    /**
     * how often to process responses of messages, default 100 milliseconds
     */
    respondInterval?: number
    /**
     * how much messages send for processing, default 50
     */
    limit?: number
    mode?: 'short-polling' | 'notify' | 'logical'
    /**
     * callback for handling uncaught error
     */
    onError?: (err: Error) => unknown
    /**
     * For scaling by partitions, pass partition number 0..n
     */
    partition?: number
  }
}
