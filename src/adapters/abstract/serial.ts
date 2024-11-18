import type { Adapter, OutboxMessage } from '../../types'
import { BaseAdapter } from './base'

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
