import type { PoolClient } from 'pg'
import { Pg } from './pg.ts'
import type { Adapter, Options, OutboxMessage } from './types.ts'
import thr from 'throw'
import type { Es } from './es.ts'

export class Transfer {
  constructor(
    private readonly options: Options,
    private readonly pg: Pg,
    private readonly adapter: Adapter,
    private readonly es: Es
  ) {}

  async transferMessages() {
    let messages: readonly OutboxMessage[] = []
    const client = await this.pg.getClient()
    try {
      await client.query('begin')
      messages = await this.fetchPgMessages(client)
      if (messages.length) {
        const results = await this.adapter.send(messages)
        const ids = []
        const responses = []
        const errors = []
        const metas = []
        const processed = []
        const attempts = []
        const sinceAt = []
        for (const [i, message] of messages.entries()) {
          if (message.is_event) {
            continue
          }
          const resp = results[i] ?? thr(new Error('Result not exists for message'))
          ids.push(message.id)
          metas.push(resp.meta ?? null)
          responses.push(
            resp.status === 'fulfilled'
              ? typeof resp.value === 'string' || Array.isArray(resp.value)
                ? { r: resp.value }
                : resp.value
              : null
          )
          errors.push(
            resp.status === 'rejected'
              ? (resp.reason as Error).stack ?? (resp.reason as Error).message ?? resp.reason
              : message.error
          )
          const needRetry =
            resp.status === 'rejected' &&
            this.options.outboxOptions?.retryError?.(resp.reason) &&
            message.attempts < (this.options.outboxOptions?.retryMaxAttempts ?? 5)
          processed.push(!needRetry)
          attempts.push(message.attempts + (needRetry ? 1 : 0))
          sinceAt.push(
            needRetry ? new Date(Date.now() + (this.options.outboxOptions?.retryDelay ?? 5) * 1000) : message.since_at
          )
        }
        await this.updateToProcessed(client, ids, responses, errors, metas, processed, attempts, sinceAt)
        this.es.setLastEventId(messages.at(-1)?.id ?? '0')
      }
    } catch (e) {
      const messagesCommands = messages.filter(m => !m.is_event)
      if (messagesCommands.length) {
        await this.updateToProcessed(
          client,
          messagesCommands.map(r => r.id),
          messagesCommands.map(() => null),
          messagesCommands.map(() => (e as Error).stack ?? (e as Error).message ?? e),
          messagesCommands.map(() => null),
          messagesCommands.map(() => true),
          messagesCommands.map(m => m.attempts),
          messagesCommands.map(m => m.since_at)
        )
      }
      throw e
    } finally {
      await client.query('commit')
      client.release()
    }
    await this.adapter.onHandled(messages)
  }

  private async fetchPgMessages(client: PoolClient) {
    return await Promise.all([
      client
        .query<OutboxMessage>(
          `
            select
              id,
              topic,
              key,
              value,
              context_id,
              error,
              attempts,
              since_at,
              is_event
            from pg_trx_outbox${
              this.options.outboxOptions?.partition == null ? '' : `_${this.options.outboxOptions?.partition}`
            }
            where not is_event and processed = false and (since_at is null or now() > since_at) ${
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
        .catch(e => {
          console.log(111111, e)
          if ((e as { code: string }).code === '55P03') {
            return []
          }
          throw e
        }),
      client
        .query<OutboxMessage>(
          `
            select
              id,
              topic,
              key,
              value,
              context_id,
              error,
              attempts,
              since_at,
              is_event
            from pg_trx_outbox${
              this.options.outboxOptions?.partition == null ? '' : `_${this.options.outboxOptions?.partition}`
            }
            where is_event and id > $2 ${this.options.outboxOptions?.topicFilter?.length ? 'and topic = any($3)' : ''}
            order by id
            limit $1
          `,
          [
            this.options.outboxOptions?.limit ?? 50,
            this.es.getLastEventId(),
            ...(this.options.outboxOptions?.topicFilter?.length ? [this.options.outboxOptions?.topicFilter] : []),
          ]
        )
        .then(resp => resp.rows),
    ]).then(rows => rows.flat().toSorted((m1, m2) => Number(m1.id) - Number(m2.id)))
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
