import type { Pg } from './pg.ts'
import type { Adapter, Options, OutboxMessage, StartStop } from './types.ts'
import Cursor from 'pg-cursor'

export class Es implements StartStop {
  private lastEventId = '0'

  constructor(private readonly pg: Pg, private readonly adapter: Adapter, private readonly options: Options) {}

  async start() {
    await this.initSync()
  }

  async stop() {}

  setLastEventId(index: OutboxMessage['id']) {
    this.lastEventId = index
  }

  getLastEventId() {
    return this.lastEventId
  }

  private async initSync() {
    const client = await this.pg.getClient()
    const cursor = client.query(
      new Cursor(
        `
          select * from pg_trx_outbox
          where is_event and id > $1
          order by id
        `,
        [this.getLastEventId()]
      )
    )
    while (true) {
      const messages: OutboxMessage[] = await cursor.read(this.options.eventSourcingOptions?.initSyncBatchSize ?? 100)
      if (!messages.length) {
        break
      }
      await this.adapter.send(messages)
      this.setLastEventId(messages.at(-1)!.id)
    }
    await cursor.close()
    client.release()
  }
}
