import { Client } from 'pg'
import type { Options, StartStop } from './types'

export class Pg implements StartStop {
  private pg: Client

  constructor(options: Options) {
    this.pg = new Client({
      application_name: 'pg_trx_outbox',
      ...options.pgOptions,
    })
    this.pg.on('error', err => options.outboxOptions?.onError?.(err))
  }

  getClient() {
    return this.pg
  }

  async start() {
    await this.pg.connect()
  }

  async stop() {
    await this.pg.end()
  }
}
