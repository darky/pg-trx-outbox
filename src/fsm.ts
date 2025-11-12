import { action, createMachine, interpret, invoke, state, transition } from 'robot3'
import type { Transfer } from './transfer.ts'
import type { Options } from './types.ts'

type Transition = 'poll' | 'notify' | 'manual'

export class FSM {
  private options: Options
  private transfer: Transfer

  constructor(options: Options, transfer: Transfer) {
    this.options = options
    this.transfer = transfer
  }

  private fsm = interpret(
    createMachine('wait', {
      wait: state(
        transition('poll' as Transition, 'processing'),
        transition('notify', 'processing'),
        transition('manual', 'processing')
      ),
      processing: invoke(
        () => this.transfer.transferMessages(),
        transition('done', 'wait'),
        transition(
          'error',
          'wait',
          action((_, { error }: { error: Error }) => this.options.outboxOptions?.onError?.(error))
        )
      ),
    }),
    () => {}
  )

  send(event: Transition) {
    return this.fsm.send(event)
  }
}
