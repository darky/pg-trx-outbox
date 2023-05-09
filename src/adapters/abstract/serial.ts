import type { Adapter, OutboxMessage } from '../../types'

export abstract class SerialAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract handleMessage(message: OutboxMessage): Promise<unknown>

  async send(messages: readonly OutboxMessage[]) {
    const resp: (PromiseFulfilledResult<unknown> | PromiseRejectedResult)[] = []
    for (const msg of messages) {
      try {
        const value = await this.handleMessage(msg)
        resp.push({ value, status: 'fulfilled' })
      } catch (reason) {
        resp.push({ reason, status: 'rejected' })
      }
    }
    return resp
  }
}
