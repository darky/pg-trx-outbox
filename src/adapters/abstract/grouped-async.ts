import PQueue from 'p-queue'
import type { Adapter, OutboxMessage } from '../../types.ts'
import { BaseAdapter } from './base.ts'

export abstract class GroupedAsyncAdapter extends BaseAdapter implements Adapter {
  private queues = new Map<OutboxMessage['key'], PQueue>()

  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract onHandled(messages: readonly OutboxMessage[]): Promise<void>

  async send(messages: readonly OutboxMessage[]) {
    const resp = []
    for (const message of messages) {
      if (!this.queues.has(message.key)) {
        this.queues.set(
          message.key,
          new PQueue({ concurrency: 1 }).once('idle', () => this.queues.delete(message.key))
        )
      }
      resp.push(this.queues.get(message.key)!.add(() => this.baseHandleMessage(message)))
    }
    return await Promise.all(resp as ReturnType<typeof this.baseHandleMessage>[])
  }
}
