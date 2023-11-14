import { diInit, diSet } from 'ts-fp-di'
import type { Adapter, OutboxMessage } from '../../types'
import { monitorEventLoopDelay } from 'perf_hooks'

export abstract class BaseAdapter {
  async baseHandleMessage(message: OutboxMessage) {
    return await diInit(async () => {
      diSet('pg_trx_outbox_context_id', message.context_id)
      let respItem: Awaited<ReturnType<Adapter['send']>>[0]
      const hist = monitorEventLoopDelay()
      hist.enable()
      const now = performance.now()
      const beforeMemory = process.memoryUsage()
      try {
        const { value, meta } = await this.handleMessage(message)
        respItem = { value, status: 'fulfilled', ...(meta ? { meta } : {}) }
      } catch (reason) {
        respItem = { reason, status: 'rejected' }
      }
      const afterMemory = process.memoryUsage()
      const time = performance.now() - now
      hist.disable()
      const { max, min, mean, stddev } = hist
      const pgTrxOutbox = {
        time,
        libuv: { max, min, mean, stddev },
        beforeMemory,
        afterMemory,
        uptime: process.uptime(),
      }
      return { ...respItem, meta: { pgTrxOutbox, ...respItem.meta } }
    })
  }

  abstract handleMessage(message: OutboxMessage): Promise<{ value: unknown; meta?: object }>
}
