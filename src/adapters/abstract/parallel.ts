import { diInit, diSet } from 'ts-fp-di'
import type { Adapter, OutboxMessage } from '../../types'
import { monitorEventLoopDelay } from 'perf_hooks'

export abstract class ParallelAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract handleMessage(message: OutboxMessage): Promise<{ value: unknown; meta?: object }>

  async send(messages: readonly OutboxMessage[]) {
    return Promise.all(
      messages.map(async msg => {
        return await diInit(async () => {
          diSet('pg_trx_outbox_context_id', msg.context_id)
          let respItem: Awaited<ReturnType<Adapter['send']>>[0]
          const hist = monitorEventLoopDelay()
          hist.enable()
          const before = performance.now()
          try {
            const { value, meta } = await this.handleMessage(msg)
            respItem = { value, status: 'fulfilled', ...(meta ? { meta } : {}) }
          } catch (reason) {
            respItem = { reason, status: 'rejected' }
          }
          const time = performance.now() - before
          hist.disable()
          const { max, min, mean, stddev } = hist
          return { ...respItem, meta: { time, hist: { max, min, mean, stddev }, ...respItem.meta } }
        })
      })
    )
  }
}