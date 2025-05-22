import type { Adapter, Options, StartStop } from './types.ts'
import { Notifier } from './notifier.ts'
import { Poller } from './poller.ts'
import { Transfer } from './transfer.ts'
import { FSM } from './fsm.ts'
import { P, match } from 'ts-pattern'
import { Pg } from './pg.ts'
import { diDep, diExists, diHas } from 'ts-fp-di'
import { Es } from './es.ts'

export class PgTrxOutbox implements StartStop {
  private pg: Pg
  private transfer: Transfer
  private adapter: Adapter
  private poller?: Poller
  private notifier?: Notifier
  private es: Es
  private fsm: FSM

  constructor(options: Options) {
    const opts: Options = {
      ...options,
      outboxOptions: {
        onError(err: Error) {
          console.error(`Error happens on pg-trx-outbox: ${err.stack ?? err.message ?? err}`)
        },
        ...options.outboxOptions,
      },
    }
    this.adapter = opts.adapter
    this.pg = new Pg(opts)
    this.es = new Es(this.pg, this.adapter, opts)
    this.transfer = new Transfer(opts, this.pg, this.adapter, this.es)
    this.fsm = new FSM(opts, this.transfer)
    match(opts.outboxOptions?.mode)
      .with(P.union('short-polling', void 0), () => (this.poller = new Poller(opts, this.fsm)))
      .with('notify', () => {
        this.poller = new Poller(opts, this.fsm)
        this.notifier = new Notifier(opts, this.fsm)
      })
      .exhaustive()
  }

  async start() {
    await this.transfer.start()
    await this.adapter.start()
    await this.pg.start()
    await this.es.start()
    await this.poller?.start()
    await this.notifier?.start()
  }

  async stop() {
    await this.es.stop()
    await this.notifier?.stop()
    await this.poller?.stop()
    await this.transfer.stop()
    await this.pg.stop()
    await this.adapter.stop()
  }

  contextId() {
    return diExists() && diHas('pg_trx_outbox_context_id') ? diDep<number>('pg_trx_outbox_context_id') : null
  }

  getLastEventId() {
    return this.es.getLastEventId()
  }

  fetchEvents() {
    this.fsm.send('manual')
  }
}
