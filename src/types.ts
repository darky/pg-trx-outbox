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
  headers: Record<string, string> | null
}

export interface StartStop {
  start(): Promise<void>
  stop(): Promise<void>
}

export interface Send {
  send(messages: readonly OutboxMessage[]): Promise<void>
}

export type Options = {
  pgOptions: ClientConfig
  adapter: StartStop & Send
  outboxOptions?: {
    pollInterval?: number
    limit?: number
    mode?: 'short-polling' | 'notify' | 'logical'
    onError?: (err: Error) => unknown
  }
}
