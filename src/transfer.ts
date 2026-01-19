import type { PoolClient } from 'pg'
import { Pg } from './pg.ts'
import type { Adapter, Options, OutboxMessage, StartStop } from './types.ts'
import thr from 'throw'
import type { Es } from './es.ts'
import { match, P } from 'ts-pattern'
import { inspect } from 'node:util'
import debug from 'debug'
import { appName } from './app-name.ts'
import type PQueue from 'p-queue'

export class Transfer implements StartStop {
  private logger = debug(`pg-trx-outbox:${appName}`)
  private queue!: PQueue

  private readonly options: Options
  private readonly pg: Pg
  private readonly adapter: Adapter
  private readonly es: Es

  constructor(options: Options, pg: Pg, adapter: Adapter, es: Es) {
    this.logger.log = console.log.bind(console)

    this.options = options
    this.pg = pg
    this.adapter = adapter
    this.es = es

    import('p-queue').then(
      ({ default: PQueue }) =>
        (this.queue = new PQueue({ concurrency: options.outboxOptions?.concurrency ? Infinity : 1 }))
    )
  }

  async start() {}

  async stop() {
    this.queue.clear()
    await this.queue.onIdle()
  }

  async transferMessages() {
    await this.queue.add(async () => this.doTransferMessages('event'), { priority: 1 })
    await this.queue.add(async () => this.doTransferMessages('command'))
  }

  private async doTransferMessages(mode: 'command' | 'event') {
    let messages: readonly OutboxMessage[] = []
    const client = await this.pg.getClient()
    try {
      await client.query('begin')
      messages = await this.fetchPgMessages(client, mode)
      if (messages.length) {
        const results = await this.adapter.send(messages)
        const ids = []
        const responses = []
        const errors = []
        const metas = []
        const processed = []
        const attempts = []
        const sinceAt = []
        const errorApproved = []
        for (const [i, resp] of results.entries()) {
          const message = messages[i] ?? thr(new Error('Message not exists for result'))
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
            (resp.error ? this.normalizeError(resp.error) : null) ??
              (resp.status === 'rejected' ? this.normalizeError(resp.reason) : message.error)
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
          errorApproved.push(
            (resp.error as { isApproved: boolean })?.isApproved ||
              (resp.status === 'rejected' && resp.reason.isApproved)
          )
        }
        await this.updateToProcessed(client, ids, responses, errors, metas, processed, attempts, sinceAt, errorApproved)
        if (mode === 'event') {
          this.es.setLastEventId(messages.at(-1)?.id ?? '0')
        }
      }
    } catch (e) {
      if ((e as { code: string }).code !== '55P03') {
        if (messages.length) {
          await this.updateToProcessed(
            client,
            messages.map(r => r.id),
            messages.map(() => null),
            messages.map(() => this.normalizeError(e)),
            messages.map(() => null),
            messages.map(() => true),
            messages.map(m => m.attempts),
            messages.map(m => m.since_at),
            messages.map(() => false)
          )
        }
        throw e
      }
    } finally {
      await client.query('commit')
      client.release()
    }
    await this.adapter.onHandled(messages)
  }

  private normalizeError(error: unknown) {
    return match(error)
      .with({ stack: P.string }, ({ stack }) => stack)
      .with(P.string, err => err)
      .otherwise(err => inspect(err))
  }

  private async fetchPgMessages(client: PoolClient, mode: 'command' | 'event') {
    const limit = this.options.outboxOptions?.limit ?? 50
    const lastEventId = this.es.getLastEventId()

    this.logger('fetching of messages, limit %d, from event id %d', limit, lastEventId)

    const resp = await client.query<OutboxMessage>(
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
        where
          ${
            mode === 'command'
              ? `(is_event = false and processed = false and (since_at is null or now() > since_at))
                  ${this.options.outboxOptions?.topicFilter?.length ? 'and topic = any($2)' : ''}`
              : `(is_event = true and id > $2)
                  ${this.options.outboxOptions?.topicFilter?.length ? 'and topic = any($3)' : ''}`
          }
        order by id
        limit $1
        ${mode === 'command' ? `for update ${this.options.outboxOptions?.concurrency ? 'skip locked' : ''}` : ''}
      `,
      [
        limit,
        ...(mode === 'event' ? [lastEventId] : []),
        ...(this.options.outboxOptions?.topicFilter?.length ? [this.options.outboxOptions?.topicFilter] : []),
      ]
    )

    this.logger(
      'received messages with ids %o',
      resp.rows.map(r => r.id)
    )

    return resp.rows
  }

  private async updateToProcessed(
    client: PoolClient,
    ids: string[],
    responses: unknown[],
    errors: (string | null)[],
    meta: (object | null)[],
    done: boolean[],
    attempts: number[],
    sinceAt: (Date | null)[],
    errorApproved: boolean[]
  ) {
    await client.query(
      `
        with info as (
          select *
          from unnest($1::bigint[], $2::jsonb[], $3::text[], $4::jsonb[], $5::boolean[], $6::smallint[], $7::timestamptz[], $8::boolean[])
          x(id, resp, err, meta, processed, attempts, since_at, error_approved))
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
          since_at = (select since_at from info where info.id = p.id limit 1),
          error_approved = (select error_approved from info where info.id = p.id limit 1)
        where p.id = any($1)
      `,
      [ids, responses, errors, meta, done, attempts, sinceAt, errorApproved]
    )
  }
}
