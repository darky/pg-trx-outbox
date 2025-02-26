import { Pool } from 'pg'
import type { Options, StartStop } from './types.ts'
import { randomUUID } from 'node:crypto'

export class Pg implements StartStop {
  private pool: Pool

  constructor(options: Options) {
    this.pool = new Pool({
      application_name: `pg_trx_outbox_${randomUUID().substring(0, 4).toLowerCase()}`,
      ...options.pgOptions,
    })
    this.pool.on('error', err => options.outboxOptions?.onError?.(err))
  }

  getClient() {
    return this.pool.connect()
  }

  async start() {
    await this.pool.query('select 1')
  }

  async stop() {
    await this.pool.end()
  }
}
