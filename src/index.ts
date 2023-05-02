import type { Options, StartStop } from './types'
import { Notifier } from './notifier'
import { Poller } from './poller'
import { Transfer } from './transfer'
import { FSM } from './fsm'
import { Logical } from './logical'
import { P, match } from 'ts-pattern'
import { Kafka } from './kafka'

export class PgKafkaTrxOutbox implements StartStop {
  private transfer: Transfer
  private kafka: Kafka
  private poller?: Poller
  private notifier?: Notifier
  private logical?: Logical

  constructor(options: Options) {
    this.kafka = new Kafka(options)
    this.transfer = new Transfer(options, this.kafka)
    const fsm = new FSM(options, this.transfer)
    match(options.outboxOptions?.mode)
      .with(P.union('short-polling', void 0), () => (this.poller = new Poller(options, fsm)))
      .with('notify', () => {
        this.poller = new Poller(options, fsm)
        this.notifier = new Notifier(options, fsm)
      })
      .with('logical', () => (this.logical = new Logical(options, this.transfer)))
      .exhaustive()
  }

  async start() {
    await this.kafka.start()
    await this.transfer.start()
    await this.poller?.start()
    await this.notifier?.start()
    await this.logical?.start()
  }

  async stop() {
    await this.logical?.stop()
    await this.notifier?.stop()
    await this.poller?.stop()
    await this.transfer.stop()
    await this.kafka.stop()
  }
}
