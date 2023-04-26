import type { Machine, Service } from 'robot3'
import type { Options, OutboxProvider } from './types'

export class Poller implements OutboxProvider {
  private pollTimer?: NodeJS.Timer

  constructor(private options: Options, private fsm: Service<Machine<any, any, any>>) {}

  async connect() {}

  start() {
    this.pollTimer = setInterval(() => this.fsm.send('poll'), this.options.outboxOptions?.pollInterval ?? 5000)
  }

  async disconnect() {
    clearInterval(this.pollTimer)
  }
}
