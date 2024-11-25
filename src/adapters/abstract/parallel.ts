import type { Adapter, OutboxMessage } from '../../types.ts'
import { BaseAdapter } from './base.ts'

export abstract class ParallelAdapter extends BaseAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract onHandled(messages: readonly OutboxMessage[]): Promise<void>

  async send(messages: readonly OutboxMessage[]) {
    return Promise.all(
      messages.map(async msg => {
        return await this.baseHandleMessage(msg)
      })
    )
  }
}
