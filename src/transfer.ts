import type { PoolClient } from 'pg'
import { Pg } from './pg'
import type { Adapter, Options, OutboxMessage } from './types'

export class Transfer {
  constructor(private readonly options: Options, private readonly pg: Pg, private readonly adapter: Adapter) {}

  async transferMessages(passedMessages: readonly OutboxMessage[] = []) {
    let messages: readonly OutboxMessage[] = []
    const client = await this.pg.getClient()
    try {
      await client.query('begin')
      messages = passedMessages.length ? passedMessages : await this.fetchPgMessages(client)
      if (messages.length) {
        const responses = await this.adapter.send(messages)
        await this.updateToProcessed(
          client,
          messages.map(r => r.id),
          responses.map(r => (r.status === 'fulfilled' ? r.value : null)),
          responses.map(r =>
            r.status === 'rejected' ? (r.reason as Error).stack ?? (r.reason as Error).message ?? r.reason : null
          )
        )
      }
    } catch (e) {
      if ((e as { code: string }).code !== '55P03') {
        if (messages.length) {
          await this.updateToProcessed(
            client,
            messages.map(r => r.id),
            messages.map(() => null),
            messages.map(() => (e as Error).stack ?? (e as Error).message ?? e)
          )
        }
        throw e
      }
    } finally {
      await client.query('commit')
      client.release()
    }
  }

  private async fetchPgMessages(client: PoolClient) {
    return await client
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

  private async updateToProcessed(client: PoolClient, ids: string[], responses: unknown[], errors: (string | null)[]) {
    await client.query(
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
