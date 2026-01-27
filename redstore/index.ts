import { sha256 } from '@noble/hashes/sha256'
import { Filter } from '@nostr/tools/filter'
import { NostrEvent } from '@nostr/tools/pure'
import { hexToBytes, utf8Decoder, utf8Encoder } from '@nostr/tools/utils'

export class DatabaseError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'DatabaseError'
  }
}

export class RedEventStore {
  #initialized: undefined | Promise<boolean>
  #fullyInitialized: boolean = false
  private worker: Worker
  private name: string
  private requests: Record<string, any> = {}
  private serial = 1

  /**
   * creates a new event store instance.
   * @param dbName - name of the indexedDB database (default: '@nostr/gadgets/events')
   */
  constructor(dbName: string = '@gadgets-redstore') {
    this.name = dbName
    this.worker = new Worker(new URL('./redstore-worker.js', import.meta.url), {
      type: 'module',
    })

    this.worker!.addEventListener('message', (ev: MessageEvent) => {
      const [id, success, result] = ev.data
      const { resolve, reject } = this.requests[id]
      if (success) resolve(result)
      else reject('worker: ' + result)
      delete this.requests[id]
    })
  }

  async call(method: string, data: any): Promise<any> {
    const id = this.serial++

    const resp = await new Promise((resolve, reject) => {
      this.requests[id] = { resolve, reject }
      this.worker!.postMessage([id, method, data])
    })
    return resp
  }

  /**
   * initializes the database.
   * @returns boolean - true if we're the main database operators, false if we're just forwarding calls to others.
   */
  async init(): Promise<boolean> {
    if (this.#initialized) return this.#initialized
    this.#initialized = this.call('init', { fileName: this.name })
    this.#initialized.then(() => {
      this.#fullyInitialized = true
    })
    return this.#initialized
  }

  /**
   * closes the database.
   */
  async close(): Promise<void> {
    if (this.#initialized) {
      this.call('close', {})
      this.#fullyInitialized = false
      this.#initialized = undefined
    }
  }

  private saveBatch:
    | null
    | [ids: string[], lastAttempts: number[], followedBys: string[][], rawEvents: Uint8Array[], tasks: SaveTask[]] =
    null
  /**
   * saves (or replaces) a nostr event to the store with automatic batching for performance.
   * (if you want the batching to work you can't `await` it immediately upon calling it)
   *
   * @param event - the nostr event to save
   * @param seenOn - optional array of relay URLs where this event was seen
   * @param lastAttempt - optional timestamp, used by the replaceable fetchers
   * @param followedBy - optional array of pubkeys that are following this event
   * @returns boolean - true if the event was new, false if it was already saved
   * @throws {DatabaseError} if event values are out of bounds or storage fails
   */
  async saveEvent(
    event: NostrEvent,
    { seenOn, lastAttempt, followedBy }: { seenOn?: string[]; lastAttempt?: number; followedBy?: string[] } = {},
  ): Promise<boolean> {
    if (!this.#fullyInitialized) await this.init()

    // sanity checking
    if (event.created_at > 0xffffffff || event.kind > 0xffff) {
      throw new DatabaseError('event with values out of expected boundaries')
    }

    // start batching logic
    let batch = this.saveBatch

    if (!batch) {
      batch = [[], [], [], [], []]
      this.saveBatch = batch

      // once we know we have a fresh batch, we schedule this batch to run
      queueMicrotask(() => {
        const batch = this.saveBatch
        if (!batch) return

        const lastAttempts = batch[1]
        const followedBys = batch[2]
        const rawEvents = batch[3]
        const tasks = batch[4]

        // as soon as we start processing this batch, we need to null it
        // to ensure that any new requests will be added to a new batch
        this.saveBatch = null

        this.call('saveEvents', { lastAttempts, followedBys, rawEvents }).then(results => {
          for (let i = 0; i < tasks.length; i++) {
            tasks[i].resolve((results as boolean[])[i])
          }
        })
      })
    }

    batch = batch!

    // return existing task if it exists
    let idx = batch[0].indexOf(event.id)
    if (idx !== -1) return batch[4][idx].p

    // add a new one
    let extra = ''
    if (seenOn) {
      // store relays on "seen_on" only in the JSON
      extra += `,"seen_on":${JSON.stringify(seenOn)}`
    }

    idx = batch[0].push(event.id) - 1
    let task = (batch[4][idx] = {} as SaveTask)
    batch[1][idx] = lastAttempt || 0
    batch[2][idx] = followedBy || []
    batch[3][idx] = utf8Encoder.encode(
      `{"pubkey":"${event.pubkey}","id":"${event.id}","kind":${event.kind},"created_at":${event.created_at},"sig":"${event.sig}","tags":${JSON.stringify(event.tags)},"content":${JSON.stringify(event.content)}${extra}}`,
    )

    task.p = new Promise<boolean>(function (resolve, reject) {
      task.resolve = resolve
      task.reject = reject
    })

    return task.p
  }

  /**
   * deletes events from the store by their ID.
   * removes the events and all associated indexes.
   *
   * @param ids - hex-encoded event IDs to delete
   * @returns the ids of the deleted events, ignoring those that we couldn't find
   * @throws {DatabaseError} if deletion fails
   */
  async deleteEvents(ids: string[]): Promise<string[]> {
    if (!this.#fullyInitialized) await this.init()
    return this.call('deleteEvents', [{ ids }])
  }

  async deleteEventsFilters(filters: Filter[]): Promise<string[]> {
    if (!this.#fullyInitialized) await this.init()
    return this.call('deleteEvents', filters)
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
  async queryEvents(filter: Filter & { followedBy?: string }, maxLimit: number = 2500): Promise<NostrEvent[]> {
    if (!this.#fullyInitialized) await this.init()
    filter.limit = filter.limit ? Math.min(filter.limit, maxLimit) : maxLimit
    return ((await this.call('queryEvents', filter)) as Uint8Array[]).map(b => {
      const event = JSON.parse(utf8Decoder.decode(b))
      event[isLocalSymbol] = true
      event[seenOnSymbol] = event.seen_on
      delete event.seen_on
      return event
    })
  }

  async loadReplaceables(
    specs: [kind: number, pubkey: string, dtag?: string][],
  ): Promise<[last_attempt: number | undefined, event: NostrEvent | undefined][]> {
    if (!this.#fullyInitialized) await this.init()

    const binQuery = new Uint8Array(18 * specs.length)
    for (let i = 0; i < specs.length; i++) {
      const offset = i * 18
      // big-endian u16
      binQuery[offset] = (specs[i][0] >> 8) & 0xff
      binQuery[offset + 1] = specs[i][0] & 0xff
      binQuery.set(hexToBytes(specs[i][1].slice(48, 64)), offset + 2)
      if (specs[i][2]) {
        const hash = sha256(specs[i][2]!)
        binQuery.set(hash.slice(0, 8), offset + 10)
      }
    }

    return ((await this.call('loadReplaceables', binQuery)) as [number | undefined, Uint8Array | undefined][]).map(
      b => {
        if (!b[1]) return b

        const event = JSON.parse(utf8Decoder.decode(b[1]))
        event[isLocalSymbol] = true
        event[seenOnSymbol] = event.seen_on
        delete event.seen_on
        return [b[0], event]
      },
    )
  }

  /**
   * removes followedBy indexes for all events of a pubkey followed by another pubkey.
   *
   * @param follower - the pubkey that is unfollowing
   * @param followed - the pubkey being unfollowed
   */
  async markFollow(follower: string, followed: string): Promise<void> {
    if (!this.#fullyInitialized) await this.init()
    await this.call('markFollow', { follower, followed })
  }

  /**
   * removes followedBy indexes for all events of a pubkey followed by another pubkey.
   *
   * @param follower - the pubkey that is unfollowing
   * @param followed - the pubkey being unfollowed
   */
  async markUnfollow(follower: string, followed: string): Promise<void> {
    if (!this.#fullyInitialized) await this.init()
    await this.call('markUnfollow', { follower, followed })
  }

  /**
   * cleans followedBy indexes for events followed by a specific pubkey, except for some.
   *
   * @param followedBy - the pubkey whose followed events to clean
   * @param except - list of authors whose indexes should not be deleted.
   */
  async cleanFollowed(followedBy: string, except: string[]): Promise<void> {
    if (!this.#fullyInitialized) await this.init()
    await this.call('cleanFollowed', { followedBy, except })
  }

  /**
   * only for use by the outbox module.
   */
  async getOutboxBounds(): Promise<{ [pubkey: string]: [number, number] }> {
    if (!this.#fullyInitialized) await this.init()
    const response = await this.call('getOutboxBounds', {})
    return JSON.parse(utf8Decoder.decode(response))
  }

  /**
   * only for use by the outbox module.
   */
  async setOutboxBound(pubkey: string, bound: [start: number, end: number]): Promise<void> {
    if (!this.#fullyInitialized) await this.init()
    await this.call('setOutboxBound', { pubkey, bound })
  }

  /**
   * deletes a database file from OPFS.
   *
   * @param dbName - name of the database to delete
   * @returns boolean - true if deletion was successful
   */
  static async delete(dbName: string): Promise<void> {
    const dir = await navigator.storage.getDirectory()
    await dir.removeEntry(dbName)
  }

  /**
   * lists all existing database files in OPFS with their stats.
   *
   * @returns array of database info objects with name, size, and lastModified
   */
  static async list(): Promise<{ name: string; size: number; lastModified: number }[]> {
    const result: { name: string; size: number; lastModified: number }[] = []
    const dir = await navigator.storage.getDirectory()
    await walkDir(dir)
    return result

    async function walkDir(dir: FileSystemDirectoryHandle, base: string = '') {
      for await (let [name, entry] of dir.entries()) {
        if (entry.kind === 'file') {
          const file = await (entry as FileSystemFileHandle).getFile()
          result.push({ name: base + name, size: file.size, lastModified: file.lastModified })
        } else {
          walkDir(entry as FileSystemDirectoryHandle, name + '/')
        }
      }
    }
  }
}

type SaveTask = {
  p: Promise<boolean>
  resolve(saved: boolean): void
  reject(e: Error): void
}

// special properties we sneak into the event objects
const isLocalSymbol = Symbol('this event is stored locally')
const seenOnSymbol = Symbol('relays where this event was seen before stored')

export function isLocal(event: NostrEvent): boolean {
  return (event as any)[isLocalSymbol] || false
}

export function seenOn(event: NostrEvent): string[] {
  return (event as any)[seenOnSymbol] || []
}
