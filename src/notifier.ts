import type { Subscriber } from 'pg-listen'
import type { Options, StartStop } from './types'
import type { FSM } from './fsm'

export class Notifier implements StartStop {
  private notifier!: Subscriber

  constructor(options: Options, private fsm: FSM) {
    import('pg-listen').then(({ default: createSubscriber }) => {
      this.notifier = createSubscriber({
        application_name: 'pg_trx_outbox_pubsub',
        ...options.pgOptions,
      })
      this.notifier.events.on('error', err => options.outboxOptions?.onError?.(err))
    })
  }

  async start() {
    await this.notifier.connect()
    await this.notifier.listenTo('pg_trx_outbox')
    this.notifier.notifications.on('pg_trx_outbox', () => this.fsm.send('notify'))
  }

  async stop() {
    await this.notifier.close()
  }
}
