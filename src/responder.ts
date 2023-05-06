import type { Pg } from './pg'
import type { Options, OutboxMessage, StartStop } from './types'

export class Responder implements StartStop {
  private timer?: NodeJS.Timer
  private waitResponsesMap = new Map<
    string,
    { resolve: (value: unknown) => void; reject: (reason?: unknown) => void }
  >()

  constructor(private options: Options, private pg: Pg) {}

  async start() {
    this.timer = setInterval(() => {
      this.respond()
    }, this.options.outboxOptions?.respondInterval ?? 100)
  }

  async stop() {
    clearInterval(this.timer)
  }

  async waitResponse(id: string) {
    return new Promise((resolve, reject) => {
      this.waitResponsesMap.set(id, { resolve, reject })
    })
  }

  private async respond() {
    if (!this.waitResponsesMap.size) {
      return
    }

    const processed = await this.pg
      .query<Pick<OutboxMessage, 'id' | 'error' | 'response'>>(
        `
          select id, response, error
          from pg_trx_outbox
          where id = any($1) and processed
        `,
        [Array.from(this.waitResponsesMap.keys())]
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
