import { action, createMachine, interpret, invoke, state, transition } from 'robot3'
import type { Transfer } from './transfer.ts'
import type { Options } from './types.ts'

export class FSM {
  constructor(private options: Options, private transfer: Transfer) {}

  private fsm = interpret(
    createMachine('wait', {
      wait: state(
        transition('more', 'processing'),
        transition('poll', 'processing'),
        transition('notify', 'processing')
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

  send(event: string) {
    return this.fsm.send(event)
  }
}
