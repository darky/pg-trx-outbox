import type { Adapter, OutboxMessage } from '../../types'
import { BaseAdapter } from './base'

export abstract class ParallelAdapter extends BaseAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  async send(messages: readonly OutboxMessage[]) {
    return Promise.all(
      messages.map(async msg => {
        return await this.baseHandleMessage(msg)
      })
    )
  }
}
