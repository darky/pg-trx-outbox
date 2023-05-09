import { diInit, diSet } from 'ts-fp-di'
import type { Adapter, OutboxMessage } from '../../types'

export abstract class ParallelAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract handleMessage(message: OutboxMessage): Promise<unknown>

  async send(messages: readonly OutboxMessage[]) {
    return Promise.allSettled(
      messages.map(async msg => {
        return await diInit(async () => {
          diSet('pg_trx_outbox_context_id', msg.context_id)
          return await this.handleMessage(msg)
        })
      })
    )
  }
}
