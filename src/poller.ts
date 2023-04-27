import type { Machine, Service } from 'robot3'
import type { Options, StartStop } from './types'

export class Poller implements StartStop {
  private pollTimer?: NodeJS.Timer

  constructor(private options: Options, private fsm: Service<Machine<any, any, any>>) {}

  async start() {
    this.pollTimer = setInterval(() => this.fsm.send('poll'), this.options.outboxOptions?.pollInterval ?? 5000)
  }

  async stop() {
    clearInterval(this.pollTimer)
  }
}
