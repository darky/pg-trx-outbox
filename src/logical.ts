import type { LogicalReplicationService } from 'pg-logical-replication'
import type { Options, OutboxMessage, StartStop } from './types'
import type { Transfer } from './transfer'

export class Logical implements StartStop {
  private logical!: LogicalReplicationService

  constructor(options: Options, transfer: Transfer) {
    import('pg-logical-replication').then(({ LogicalReplicationService }) => {
      this.logical = new LogicalReplicationService({
        ...options.pgOptions,
        application_name: 'pg_kafka_trx_outbox_logical',
      })
      this.logical.on('error', err => options.outboxOptions?.onError?.(err))
      this.logical.on('data', (_, message: OutboxMessage) => transfer.queueMessageForTransfer(message))
    })
  }

  async start() {
    await import('pg-logical-replication').then(async ({ Wal2JsonPlugin }) =>
      this.logical.subscribe(new Wal2JsonPlugin(), 'pg_kafka_trx_outbox')
    )
  }

  async stop() {
    await this.logical.stop()
  }
}
