import type { Options, StartStop } from './types'
import { Notifier } from './notifier'
import { Poller } from './poller'
import { Transfer } from './transfer'
import { FSM } from './fsm'

export class PgKafkaTrxOutbox implements StartStop {
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

  async start() {
    await this.transfer.start()
    await this.poller.start()
    await this.notifier?.start()
  }

  async stop() {
    await this.notifier?.stop()
    await this.poller.stop()
    await this.transfer.stop()
  }
}
