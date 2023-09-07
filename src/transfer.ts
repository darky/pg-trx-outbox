import type { PoolClient } from 'pg'
import { Pg } from './pg'
import type { Adapter, Options, OutboxMessage } from './types'
import thr from 'throw'

export class Transfer {
  constructor(private readonly options: Options, private readonly pg: Pg, private readonly adapter: Adapter) {}

  async transferMessages(passedMessages: readonly OutboxMessage[] = []) {
    let messages: readonly OutboxMessage[] = []
    const client = await this.pg.getClient()
    try {
      await client.query('begin')
      messages = passedMessages.length ? passedMessages : await this.fetchPgMessages(client)
      if (messages.length) {
        const results = await this.adapter.send(messages)
        const ids = []
        const responses = []
        const errors = []
        const metas = []
        const processed = []
        const attempts = []
        const sinceAt = []
        for (const [i, r] of results.entries()) {
          const message = messages[i] ?? thr(new Error('Message not exists for response'))
          ids.push(message.id)
          metas.push(r.meta ?? null)
          responses.push(
            r.status === 'fulfilled'
              ? typeof r.value === 'string' || Array.isArray(r.value)
                ? { r: r.value }
                : r.value
              : null
          )
          errors.push(
            r.status === 'rejected'
              ? (r.reason as Error).stack ?? (r.reason as Error).message ?? r.reason
              : message.error
          )
          const needRetry =
            r.status === 'rejected' &&
            this.options.outboxOptions?.retryError?.(r.reason) &&
            message.attempts < (this.options.outboxOptions?.retryMaxAttempts ?? 5)
          processed.push(!needRetry)
          attempts.push(message.attempts + (needRetry ? 1 : 0))
          sinceAt.push(
            needRetry ? new Date(Date.now() + (this.options.outboxOptions?.retryDelay ?? 5) * 1000) : message.since_at
          )
        }
        await this.updateToProcessed(client, ids, responses, errors, metas, processed, attempts, sinceAt)
      }
    } catch (e) {
      if ((e as { code: string }).code !== '55P03') {
        if (messages.length) {
          await this.updateToProcessed(
            client,
            messages.map(r => r.id),
            messages.map(() => null),
            messages.map(() => (e as Error).stack ?? (e as Error).message ?? e),
            messages.map(() => null),
            messages.map(() => true),
            messages.map(m => m.attempts),
            messages.map(m => m.since_at)
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
          select * from pg_trx_outbox${
            this.options.outboxOptions?.partition == null ? '' : `_${this.options.outboxOptions?.partition}`
          }
          where processed = false and (since_at is null or now() > since_at) ${
            this.options.outboxOptions?.topicFilter?.length ? 'and topic = any($2)' : ''
          }
          order by id
          limit $1
          for update nowait
        `,
        [
          this.options.outboxOptions?.limit ?? 50,
          ...(this.options.outboxOptions?.topicFilter?.length ? [this.options.outboxOptions?.topicFilter] : []),
        ]
      )
      .then(resp => resp.rows)
  }

  private async updateToProcessed(
    client: PoolClient,
    ids: string[],
    responses: unknown[],
    errors: (string | null)[],
    meta: (object | null)[],
    done: boolean[],
    attempts: number[],
    sinceAt: (Date | null)[]
  ) {
    await client.query(
      `
        with info as (
          select *
          from unnest($1::bigint[], $2::jsonb[], $3::text[], $4::jsonb[], $5::boolean[], $6::smallint[], $7::timestamptz[])
          x(id, resp, err, meta, processed, attempts, since_at))
        update pg_trx_outbox${
          this.options.outboxOptions?.partition == null ? '' : `_${this.options.outboxOptions?.partition}`
        } p
        set
          processed = (select processed from info where info.id = p.id limit 1),
          updated_at = now(),
          response = (select resp from info where info.id = p.id limit 1),
          error = (select err from info where info.id = p.id limit 1),
          meta = (select meta from info where info.id = p.id limit 1),
          attempts = (select attempts from info where info.id = p.id limit 1),
          since_at = (select since_at from info where info.id = p.id limit 1)
        where p.id = any($1)
      `,
      [ids, responses, errors, meta, done, attempts, sinceAt]
    )
  }
}
