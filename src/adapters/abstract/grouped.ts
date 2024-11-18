import type { Adapter, OutboxMessage } from '../../types'
import { BaseAdapter } from './base'

export abstract class GroupedAdapter extends BaseAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract onHandled(messages: readonly OutboxMessage[]): Promise<void>

  async send(messages: readonly OutboxMessage[]) {
    const respMap = new Map<OutboxMessage['id'], Awaited<ReturnType<typeof this.baseHandleMessage>>>()
    const grouped = new Map<OutboxMessage['key'], OutboxMessage[]>()
    messages.forEach(msg => grouped.set(msg.key, (grouped.get(msg.key) ?? []).concat(msg)))
    await Promise.all(
      Array.from(grouped.values()).map(async msgs => {
        for (const msg of msgs) {
          const resp = await this.baseHandleMessage(msg)
          respMap.set(msg.id, resp)
        }
      })
    )
    return messages.map(msg => respMap.get(msg.id)!)
  }
}
