import createSubscriber, { Subscriber } from 'pg-listen'
import type { Options, OutboxProvider } from './types'
import type { Machine, Service } from 'robot3'

export class Notifier implements OutboxProvider {
  private notifier: Subscriber

  constructor(options: Options, private fsm: Service<Machine<any, any, any>>) {
    this.notifier = createSubscriber({
      application_name: 'pg_kafka_trx_outbox_pubsub',
      ...options.pgOptions,
    })
    this.notifier.events.on('error', err => options.outboxOptions?.onError?.(err))
  }

  async connect() {
    await this.notifier.connect()
    await this.notifier.listenTo('pg_kafka_trx_outbox')
  }

  start() {
    this.notifier.notifications.on('pg_kafka_trx_outbox', () => this.fsm.send('notify'))
  }

  async disconnect() {
    await this.notifier.close()
  }
}
