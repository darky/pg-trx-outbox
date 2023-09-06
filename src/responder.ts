import { action, createMachine, interpret, invoke, state, transition } from 'robot3'
import type { Pg } from './pg'
import type { Options, OutboxMessage, StartStop } from './types'

export class Responder implements StartStop {
  private fsm = interpret(
    createMachine('wait', {
      wait: state(transition('respond', 'processing')),
      processing: invoke(
        () => this.respond(),
        transition('done', 'wait'),
        transition(
          'error',
          'wait',
          action((_, { error }: { error: Error }) => this.options.outboxOptions?.onError?.(error))
        )
      ),
    }),
    () => {}
  )
  private timer?: NodeJS.Timeout
  private waitResponsesMap = new Map<
    string,
    { resolve: (value: unknown) => void; reject: (reason?: unknown) => void; key?: string | void }
  >()

  constructor(private options: Options, private pg: Pg) {}

  async start() {
    this.timer = setInterval(() => {
      this.fsm.send('respond')
    }, this.options.outboxOptions?.respondInterval ?? 100)
  }

  async stop() {
    clearInterval(this.timer)
  }

  async waitResponse(id: string, key?: string) {
    return new Promise((resolve, reject) => {
      this.waitResponsesMap.set(id, { resolve, reject, key })
    })
  }

  private async respond() {
    if (!this.waitResponsesMap.size) {
      return
    }

    const keys = Array.from(this.waitResponsesMap.values())
      .filter(x => !!x.key)
      .map(x => x.key as string)

    const processed = await this.pg
      .query<Pick<OutboxMessage, 'id' | 'error' | 'response'>>(
        `
          select id, response, error
          from pg_trx_outbox
          where id = any($1) and processed ${keys.length ? 'and key = any($2)' : ''}
        `,
        [Array.from(this.waitResponsesMap.keys()), ...(keys.length ? [keys] : [])]
      )
      .then(resp => resp.rows)

    processed.forEach(message => {
      message.error
        ? this.waitResponsesMap.get(message.id)?.reject(message.error)
        : this.waitResponsesMap.get(message.id)?.resolve(message.response)
      this.waitResponsesMap.delete(message.id)
    })
  }
}
