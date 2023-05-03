import { Pg } from './pg'
import type { Options, OutboxMessage, Send, StartStop } from './types'

export class Transfer {
  constructor(private readonly options: Options, private readonly pg: Pg, private readonly adapter: StartStop & Send) {}

  async transferMessages(passedMessages: readonly OutboxMessage[] = []) {
    try {
      await this.pg.getClient().query('begin')
      const messages = passedMessages.length ? passedMessages : await this.fetchPgMessages()
      if (messages.length) {
        const responses = await this.adapter.send(messages)
        await this.updateToProcessed(
          messages.map(r => r.id),
          responses
        )
      }
      await this.pg.getClient().query('commit')
    } catch (e) {
      await this.pg.getClient().query('rollback')
      if ((e as { code: string }).code !== '55P03') {
        throw e
      }
    }
  }

  private async fetchPgMessages() {
    return await this.pg
      .getClient()
      .query<OutboxMessage>(
        `
          select * from pg_trx_outbox
          where processed = false
          order by id
          limit $1
          for update nowait
        `,
        [this.options.outboxOptions?.limit ?? 50]
      )
      .then(resp => resp.rows)
  }

  private async updateToProcessed(ids: string[], responses: unknown[]) {
    await this.pg.getClient().query(
      `
        with info as (select * from unnest($1::bigint[], $2::jsonb[]) x(id, resp))
        update pg_trx_outbox p
        set
          processed = true,
          updated_at = now(),
          response = (select resp from info where info.id = p.id limit 1)
        where p.id = any($1)
      `,
      [ids, responses]
    )
  }
}
