import type { Kafka } from './kafka'
import type { Options, OutboxMessage, StartStop } from './types'
import { Client } from 'pg'

export class Transfer implements StartStop {
  private pg: Client

  constructor(private readonly options: Options, private readonly kafka: Kafka) {
    this.pg = new Client({
      application_name: 'pg_kafka_trx_outbox',
      ...options.pgOptions,
    })
    this.pg.on('error', err => this.options.outboxOptions?.onError?.(err))
  }

  async start() {
    await this.pg.connect()
  }

  async stop() {
    await this.pg.end()
  }

  async transferMessages(passedMessages: readonly OutboxMessage[] = []) {
    try {
      await this.pg.query('begin')
      const messages = passedMessages.length ? passedMessages : await this.fetchPgMessages()
      await this.kafka.send(messages)
      await this.updateToProcessed(messages.map(r => r.id))
      await this.pg.query('commit')
    } catch (e) {
      await this.pg.query('rollback')
      if ((e as { code: string }).code !== '55P03') {
        throw e
      }
    }
  }

  private async fetchPgMessages() {
    return await this.pg
      .query<OutboxMessage>(
        `
          select * from pg_kafka_trx_outbox
          where processed = false
          order by id
          limit $1
          for update nowait
        `,
        [this.options.outboxOptions?.limit ?? 50]
      )
      .then(resp => resp.rows)
  }

  private async updateToProcessed(ids: string[]) {
    await this.pg.query(
      `
        update pg_kafka_trx_outbox
        set processed = true, updated_at = now()
        where id = any($1)
      `,
      [ids]
    )
  }
}
