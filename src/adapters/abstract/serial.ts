import type { Adapter, OutboxMessage } from '../../types.ts'
import { BaseAdapter } from './base.ts'

export abstract class SerialAdapter extends BaseAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract onHandled(messages: readonly OutboxMessage[]): Promise<void>

  async send(messages: readonly OutboxMessage[]) {
    const resp: Awaited<ReturnType<typeof this.baseHandleMessage>>[] = []
    for (const msg of messages) {
      resp.push(await this.baseHandleMessage(msg))
    }
    return resp
  }
}
