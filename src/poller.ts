import type { Options, StartStop } from './types.ts'
import type { FSM } from './fsm.ts'

export class Poller implements StartStop {
  private pollTimer?: NodeJS.Timeout

  constructor(private options: Options, private fsm: FSM) {}

  async start() {
    this.pollTimer = setInterval(() => this.fsm.send('poll'), this.options.outboxOptions?.pollInterval ?? 5000)
  }

  async stop() {
    clearInterval(this.pollTimer)
  }
}
