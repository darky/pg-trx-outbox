import type { Adapter, OutboxMessage } from '../../types'

export abstract class ParallelAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract handleMessage(message: OutboxMessage): Promise<unknown>

  async send(messages: readonly OutboxMessage[]) {
    return Promise.allSettled(
      messages.map(async m => {
        return this.handleMessage(m)
      })
    )
  }
}
