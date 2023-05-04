import type { LogicalReplicationService } from 'pg-logical-replication'
import type { Options, OutboxMessage, StartStop } from './types'
import type { Transfer } from './transfer'
import type { MessageInsert } from 'pg-logical-replication/dist/output-plugins/pgoutput/pgoutput.types'
import type DataLoader from 'dataloader'
import type PQueue from 'p-queue'

export class Logical implements StartStop {
  private logical!: LogicalReplicationService
  private dataLoader?: DataLoader<OutboxMessage, OutboxMessage>
  private queue?: PQueue

  constructor(private options: Options, private transfer: Transfer) {
    import('pg-logical-replication').then(({ LogicalReplicationService }) => {
      this.logical = new LogicalReplicationService({
        ...this.options.pgOptions,
        application_name: 'pg_trx_outbox_logical',
      })
      this.logical.on('error', err => this.options.outboxOptions?.onError?.(err))
      this.logical.on('data', (_, message: MessageInsert & { new: OutboxMessage }) => {
        message.tag === 'insert' && this.queueMessageForTransfer(message.new)
      })
    })
  }

  async start() {
    await import('pg-logical-replication').then(async ({ PgoutputPlugin }) => {
      this.logical.subscribe(
        new PgoutputPlugin({ protoVersion: 1, publicationNames: ['pg_trx_outbox'] }),
        'pg_trx_outbox'
      )
    })
  }

  async stop() {
    await this.logical.stop()
  }

  private async queueMessageForTransfer(message: OutboxMessage) {
    await this.initQueue()
    await this.initDataLoader()
    this.dataLoader!.load(message)
  }

  private async initQueue() {
    if (!this.queue) {
      const dynamicImport = new Function('specifier', 'return import(specifier)')
      const exp = (await dynamicImport('p-queue')) as typeof import('p-queue')
      const PQueueClass = exp.default
      this.queue = new PQueueClass({ concurrency: 1 })
    }
  }

  private async initDataLoader() {
    if (!this.dataLoader) {
      await import('dataloader').then(({ default: DataLoader }) => {
        this.dataLoader = new DataLoader(
          async messages => {
            this.queue!.add(() => this.transfer.transferMessages(messages))
            return messages
          },
          {
            cache: false,
            batchScheduleFn: cb => {
              setTimeout(cb, this.options.outboxOptions?.logicalBatchInterval ?? 100)
            },
            name: 'pg_trx_outbox',
          }
        )
      })
    }
  }
}
