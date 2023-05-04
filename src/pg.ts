import { Pool, QueryResultRow } from 'pg'
import type { Options, StartStop } from './types'

export class Pg implements StartStop {
  private pool: Pool

  constructor(options: Options) {
    this.pool = new Pool({
      application_name: 'pg_trx_outbox',
      ...options.pgOptions,
    })
    this.pool.on('error', err => options.outboxOptions?.onError?.(err))
  }

  query<T extends QueryResultRow>(sql: string, values: unknown[]) {
    return this.pool.query<T>(sql, values)
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
