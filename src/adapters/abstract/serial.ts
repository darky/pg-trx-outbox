import { diInit, diSet } from 'ts-fp-di'
import type { Adapter, OutboxMessage } from '../../types'
import { monitorEventLoopDelay } from 'perf_hooks'

export abstract class SerialAdapter implements Adapter {
  abstract start(): Promise<void>

  abstract stop(): Promise<void>

  abstract handleMessage(message: OutboxMessage): Promise<{ value: unknown; meta?: object }>

  async send(messages: readonly OutboxMessage[]) {
    type RespItemType = Awaited<ReturnType<Adapter['send']>>[0]
    const resp: RespItemType[] = []
    for (const msg of messages) {
      await diInit(async () => {
        diSet('pg_trx_outbox_context_id', msg.context_id)
        let respItem: RespItemType
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
        resp.push({ ...respItem, meta: { time, hist: { max, min, mean, stddev }, ...respItem.meta } })
      })
    }
    return resp
  }
}
