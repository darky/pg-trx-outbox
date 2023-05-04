import { Pg } from './pg'
import type { Adapter, Options, OutboxMessage } from './types'

export class Transfer {
  constructor(private readonly options: Options, private readonly pg: Pg, private readonly adapter: Adapter) {}

  async transferMessages(passedMessages: readonly OutboxMessage[] = []) {
    let messages: readonly OutboxMessage[] = []
    try {
      await this.pg.getClient().query('begin')
      messages = passedMessages.length ? passedMessages : await this.fetchPgMessages()
      if (messages.length) {
        const responses = await this.adapter.send(messages)
        await this.updateToProcessed(
          messages.map(r => r.id),
          responses,
          messages.map(() => null)
        )
      }
    } catch (e) {
      if ((e as { code: string }).code !== '55P03') {
        if (messages.length) {
          await this.updateToProcessed(
            messages.map(r => r.id),
            messages.map(() => null),
            messages.map(() => (e as Error).stack ?? (e as Error).message ?? e)
          )
        }
        throw e
      }
    } finally {
      await this.pg.getClient().query('commit')
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

  private async updateToProcessed(ids: string[], responses: unknown[], errors: (string | null)[]) {
    await this.pg.getClient().query(
      `
        with info as (select * from unnest($1::bigint[], $2::jsonb[], $3::text[]) x(id, resp, err))
        update pg_trx_outbox p
        set
          processed = true,
          updated_at = now(),
          response = (select resp from info where info.id = p.id limit 1),
          error = (select err from info where info.id = p.id limit 1)
        where p.id = any($1)
      `,
      [ids, responses, errors]
    )
  }
}
