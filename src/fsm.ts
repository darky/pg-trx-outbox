import { action, createMachine, interpret, invoke, state, transition } from 'robot3'
import type { Transfer } from './transfer.ts'
import type { Options } from './types.ts'

export class FSM {
  constructor(private options: Options, private transfer: Transfer) {}

  private fsm = interpret(
    createMachine('wait', {
      wait: state(
        transition('poll', 'processing'),
        transition('notify', 'processing'),
        transition('repeat', 'processing')
      ),
      processing: invoke(
        () => this.transfer.transferMessages(),
        transition('done', 'wait'),
        transition(
          'error',
          'wait',
          action((_, { error }: { error: Error }) => this.options.outboxOptions?.onError?.(error))
        ),
        transition('notify', 'waitRepeatProcessing')
      ),
      waitRepeatProcessing: state(
        transition(
          'done',
          'wait',
          action(() => process.nextTick(() => this.fsm.send('repeat')))
        ),
        transition(
          'error',
          'wait',
          action((_, { error }: { error: Error }) => this.options.outboxOptions?.onError?.(error)),
          action(() => process.nextTick(() => this.fsm.send('repeat')))
        )
      ),
    }),
    () => {}
  )

  send(event: string) {
    return this.fsm.send(event)
  }
}
