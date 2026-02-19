import type { Options, StartStop } from './types.ts'
import { Transfer } from './transfer.ts'

export class Poller implements StartStop {
  private pollTimer?: NodeJS.Timeout
  private options: Options
  private transfer: Transfer

  constructor(options: Options, transfer: Transfer) {
    this.options = options
    this.transfer = transfer
  }

  async start() {
    this.pollTimer = setInterval(
      () => this.transfer.transferMessages().catch(err => this.options.outboxOptions?.onError?.(err)),
      this.options.outboxOptions?.pollInterval ?? 5000
    )
  }

  async stop() {
    clearInterval(this.pollTimer)
  }
}
