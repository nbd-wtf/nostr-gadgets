import { Filter } from '@nostr/tools/filter'
import { NostrEvent } from '@nostr/tools/pure'
import { DatabaseError } from '../store'
import { utf8Decoder } from '@nostr/tools/utils'

export class RedEventStore {
  private dbName: string
  private requests: Record<string, any> = {}
  private worker: Worker | undefined
  private serial = 1

  /**
   * creates a new event store instance.
   * @param dbName - name of the indexedDB database (default: '@nostr/gadgets/events')
   */
  constructor(dbName: string = '@nostr/gadgets/events') {
    this.dbName = dbName
    this.worker = new Worker('./dist/worker.js', { type: 'module' })

    this.worker!.addEventListener('message', (ev: MessageEvent) => {
      const { resolve, reject } = this.requests[ev.data.id]
      if (ev.data.success) resolve(ev.data.result)
      else reject('worker: ' + ev.data.error)
    })
  }

  private async call<D, R>(method: string, data: D): Promise<R> {
    const id = this.serial++
    return new Promise((resolve, reject) => {
      this.requests[id] = { resolve, reject }
      this.worker!.postMessage({ id, method: method, data: data })
    })
  }

  async init(): Promise<void> {
    return this.call('init', this.dbName)
  }

  /**
   * closes the database. you probably do not need this.
   */
  async close(): Promise<void> {
    if (this.worker) {
      this.call('close', null)
      this.worker = undefined
    }
  }

  private saveBatch:
    | null
    | [ids: string[], events: NostrEvent[], followedBys: (string[] | undefined)[], tasks: SaveTask[]]
  /**
   * saves (or replaces) a nostr event to the store with automatic batching for performance.
   * (if you want the batching to work you can't `await` it immediately upon calling it)
   *
   * @param event - the nostr event to save
   * @param seenOn - optional array of relay URLs where this event was seen
   * @param followedBy - optional array of pubkeys that are following this event
   * @returns boolean - true if the event was new, false if it was already saved
   * @throws {DatabaseError} if event values are out of bounds or storage fails
   */
  async saveEvent(
    event: NostrEvent,
    { seenOn, followedBy }: { seenOn?: string[]; followedBy?: string[] } = {},
  ): Promise<void> {
    if (!this.worker) await this.init()

    // sanity checking
    if (event.created_at > 0xffffffff || event.kind > 0xffff) {
      throw new DatabaseError('event with values out of expected boundaries')
    }

    // store relays on "seen_on" (so it's saved on database when calling JSON.stringify)
    if (seenOn) {
      ;(event as unknown as { seen_on: string[] }).seen_on = seenOn
    }

    // start batching logic
    let batch = this.saveBatch

    if (!batch) {
      batch = [[], [], [], []]
      this.saveBatch = batch

      // once we know we have a fresh batch, we schedule this batch to run
      const events = batch[1]
      const followedBys = batch[2]
      const tasks = batch[3]
      queueMicrotask(() => {
        // as soon as we start processing this batch, we need to null it
        // to ensure that any new requests will be added to a new batch
        this.saveBatch = null

        this.saveEventsBatch(events, followedBys).then(() => {
          for (let i = 0; i < tasks.length; i++) {
            tasks[i].resolve()
          }
        })
      })
    }

    batch = batch!

    // return existing task if it exists
    let idx = batch[0].indexOf(event.id)
    if (idx !== -1) return batch[3][idx].p

    // add a new one
    idx = batch[0].push(event.id) - 1
    let task = (batch[3][idx] = {} as SaveTask)
    batch[1][idx] = event
    batch[2][idx] = followedBy

    task.p = new Promise<void>(function (resolve, reject) {
      task.resolve = resolve
      task.reject = reject
    })

    return task.p
  }

  private saveEventsBatch(events: NostrEvent[], followedBys: (string[] | undefined)[]): Promise<void> {
    return this.call('saveEvents', { events, followedBys })
  }

  /**
   * deletes events from the store by their ID.
   * removes the events and all associated indexes.
   *
   * @param ids - hex-encoded event IDs to delete
   * @param followedBy - optional array of pubkeys that are following this event
   * @returns the number of events actually deleted, ignoring those that we couldn't find
   * @throws {DatabaseError} if deletion fails
   */
  async deleteEvents(ids: string[]): Promise<number> {
    if (!this.worker) await this.init()
    return this.call('deleteEvents', { ids })
  }

  /**
   * queries events using a nostr filter, any filters supported (except "search").
   * the actual limit of the query will be the minimum between the filter "limit" if it exists
   * and the maxLimit param.
   *
   * @param filter - nostr filter specification
   * @param maxLimit - maximum number of events to return (default: 500)
   * @returns events matching the filter criteria
   */
  async queryEvents(filter: Filter & { followedBy?: string }): Promise<NostrEvent[]> {
    if (!this.worker) await this.init()
    const events = await this.call('queryEvents', filter) as Uint8Array[]
    return events.map(b => JSON.parse(utf8Decoder.decode(b)))
  }
}

type SaveTask = {
  p: Promise<void>
  resolve(): void
  reject(e: Error): void
}
