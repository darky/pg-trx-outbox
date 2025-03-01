import type { Options, StartStop } from './types.ts'
import type { FSM } from './fsm.ts'

export class Poller implements StartStop {
  private pollTimer?: NodeJS.Timeout
  private options: Options
  private fsm: FSM

  constructor(options: Options, fsm: FSM) {
    this.options = options
    this.fsm = fsm
  }

  async start() {
    this.pollTimer = setInterval(() => this.fsm.send('poll'), this.options.outboxOptions?.pollInterval ?? 5000)
  }

  async stop() {
    clearInterval(this.pollTimer)
  }
}
