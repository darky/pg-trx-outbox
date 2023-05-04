import type { PoolConfig } from 'pg'

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
  response: unknown
  error: unknown
  headers: Record<string, string> | null
}

export interface StartStop {
  start(): Promise<void>
  stop(): Promise<void>
}

export interface Send {
  send(messages: readonly OutboxMessage[]): Promise<unknown[]>
}

export type Adapter = StartStop & Send

export type Options = {
  pgOptions: PoolConfig
  adapter: Adapter
  outboxOptions?: {
    pollInterval?: number
    limit?: number
    mode?: 'short-polling' | 'notify' | 'logical'
    onError?: (err: Error) => unknown
  }
}
