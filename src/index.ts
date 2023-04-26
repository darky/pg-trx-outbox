import type { Options, OutboxProvider } from './types'
import { Notifier } from './notifier'
import { Poller } from './poller'
import { Transfer } from './transfer'
import { FSM } from './fsm'

export class PgKafkaTrxOutbox implements OutboxProvider {
  private transfer: Transfer
  private fsm: FSM
  private poller: Poller
  private notifier: Notifier | void

  constructor(private readonly options: Options) {
    this.transfer = new Transfer(this.options)
    this.fsm = new FSM(this.options, this.transfer)
    this.poller = new Poller(this.options, this.fsm.fsm)
    this.notifier = this.options.outboxOptions?.notify ? new Notifier(this.options, this.fsm.fsm) : void 0
  }

  async connect() {
    await this.transfer.connect()
    await this.poller.connect()
    await this.notifier?.connect()
  }

  start() {
    this.transfer.start()
    this.poller.start()
    this.notifier?.start()
  }

  async disconnect() {
    await this.notifier?.disconnect()
    await this.poller.disconnect()
    await this.transfer.disconnect()
  }
}
