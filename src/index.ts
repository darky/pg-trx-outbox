import type { Options, StartStop } from './types'
import { Notifier } from './notifier'
import { Poller } from './poller'
import { Transfer } from './transfer'
import { FSM } from './fsm'

export class PgKafkaTrxOutbox implements StartStop {
  private transfer: Transfer
  private poller: Poller
  private notifier: Notifier | void

  constructor(options: Options) {
    this.transfer = new Transfer(options)
    const fsm = new FSM(options, this.transfer)
    this.poller = new Poller(options, fsm)
    this.notifier = options.outboxOptions?.mode === 'notify' ? new Notifier(options, fsm) : void 0
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
