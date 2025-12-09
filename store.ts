import { Filter } from '@nostr/tools/filter'
import { NostrEvent } from '@nostr/tools/pure'
import { hexToBytes } from '@nostr/tools/utils'
import { isReplaceableKind } from '@nostr/tools/kinds'

type Query = {
  startingPoint: Uint8Array
  endingPoint: Uint8Array
  lastFetched?: number // idx serial

  exhausted: boolean
  results: QueryResult[]
}

type QueryResult = {
  ts: number
  serial: number
}

type SaveTask = {
  p: Promise<boolean>
  resolve(isSaved: boolean): void
  reject(e: Error): void
}

export class DatabaseError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'DatabaseError'
  }
}

// constants matching the Go implementation
const INDEX_CREATED_AT_PREFIX = 1
const INDEX_KIND_PREFIX = 2
const INDEX_PUBKEY_PREFIX = 3
const INDEX_PUBKEY_KIND_PREFIX = 4
const INDEX_TAG_PREFIX = 5
const INDEX_TAG32_PREFIX = 6
const INDEX_TAG_ADDR_PREFIX = 7
const INDEX_FOLLOWED_PREFIX = 8

/**
 * indexeddb store for events with optimized indexes that are small in size and fast in speed.
 */
export class IDBEventStore {
  private dbName: string
  _db: IDBDatabase | undefined

  /**
   * creates a new event store instance.
   * @param dbName - name of the indexedDB database (default: '@nostr/gadgets/events')
   */
  constructor(dbName: string = '@nostr/gadgets/events') {
    this.dbName = dbName
  }

  /**
   * initializes the database connection and creates object stores if needed.
   * automatically called by other methods if not already initialized, so you can ignore it.
   */
  async init(): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 2)

      request.onerror = () => {
        reject(new DatabaseError(`failed to open database: ${request.error?.message}`))
      }

      request.onsuccess = () => {
        this._db = request.result
        resolve()
      }

      request.onupgradeneeded = change => {
        const db = request.result

        // create object stores
        if (!change.oldVersion) {
          db.createObjectStore('events', { autoIncrement: true })
          db.createObjectStore('ids')
          db.createObjectStore('indexes')
        }

        if (change.oldVersion <= 1) {
          // this is used by OutboxManager, but it can be ignored otherwise
          db.createObjectStore('syncing')
        }
      }
    })
  }

  /**
   * closes the database. you probably do not need this.
   */
  async close(): Promise<void> {
    if (this._db) {
      this._db.close()
      this._db = undefined
    }
  }

  private saveBatch:
    | null
    | [ids: string[], events: NostrEvent[], followedBys: (string[] | undefined)[], tasks: SaveTask[]]
  /**
   * saves a nostr event to the store with automatic batching for performance.
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
  ): Promise<boolean> {
    if (!this._db) await this.init()

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

        const transaction = this._db!.transaction(['events', 'ids', 'indexes'], 'readwrite', {
          durability: 'relaxed',
        })

        const promises = this.saveEventsBatch(transaction, events, followedBys)
        for (let i = 0; i < promises.length; i++) {
          promises[i].catch(tasks[i].reject).then(isSaved => {
            if (typeof isSaved !== 'undefined') tasks[i].resolve(isSaved)
          })
        }
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

    task.p = new Promise<boolean>(function (resolve, reject) {
      task.resolve = resolve
      task.reject = reject
    })

    return task.p
  }

  private saveEventsBatch(
    transaction: IDBTransaction,
    events: NostrEvent[],
    followedBys: (string[] | undefined)[],
  ): Promise<boolean>[] {
    const idStore = transaction.objectStore('ids')
    const promises = new Array<Promise<boolean>>(events.length)

    for (let i = 0; i < events.length; i++) {
      const event = events[i]
      const followedBy = followedBys[i]

      promises[i] = new Promise<boolean>((resolve, reject) => {
        // check for duplicates
        const idKey = new Uint8Array(8)
        putHexAsBytes(idKey, 0, event.id, 8)

        const checkRequest = idStore.getKey(idKey.buffer)
        checkRequest.onsuccess = () => {
          if (checkRequest.result && checkRequest.result) {
            resolve(false)
            return
          }

          // save the event
          this.saveEventInternal(transaction, event, followedBy)
            .then(() => {
              resolve(true)
              transaction.commit()
            })
            .catch(reject)
        }

        checkRequest.onerror = () => {
          reject(new DatabaseError(`failed to check for duplicate: ${checkRequest.error?.message}`))
        }
      })
    }

    return promises
  }

  private async saveEventInternal(
    transaction: IDBTransaction,
    event: NostrEvent,
    followedBy?: string[],
  ): Promise<void> {
    const eventStore = transaction.objectStore('events')
    const idStore = transaction.objectStore('ids')
    const indexStore = transaction.objectStore('indexes')

    return new Promise((resolve, reject) => {
      const saveEventRequest = eventStore.put(JSON.stringify(event))

      saveEventRequest.onsuccess = () => {
        const serial = saveEventRequest.result as number
        const indexPromises: Promise<void>[] = []

        // create the id index
        const idKey = new Uint8Array(8)
        putHexAsBytes(idKey, 0, event.id, 8)
        const promise = new Promise<void>((resolve, reject) => {
          const idRequest = idStore.add(serial, idKey)
          idRequest.onsuccess = () => resolve()
          idRequest.onerror = () => reject(new DatabaseError(`Failed to create index: ${idRequest.error?.message}`))
        })
        indexPromises.push(promise)

        // create all the other indexes
        for (const indexKey of getIndexKeysForEvent(event, serial, followedBy)) {
          const p = new Promise<void>((resolve, reject) => {
            const indexRequest = indexStore.put(null, indexKey.buffer as ArrayBuffer)
            indexRequest.onsuccess = () => resolve()
            indexRequest.onerror = () =>
              reject(new DatabaseError(`Failed to create index: ${indexRequest.error?.message}`))
          })
          indexPromises.push(p)
        }

        Promise.all(indexPromises)
          .then(() => resolve())
          .catch(reject)
      }

      saveEventRequest.onerror = () => {
        reject(new DatabaseError(`Failed to save event: ${saveEventRequest.error?.message}`))
      }
    })
  }

  /**
   * deletes an event from the store by its ID.
   * removes the event and all associated indexes.
   *
   * @param id - hex-encoded event ID to delete
   * @param followedBy - optional array of pubkeys that are following this event
   * @returns true if event was found and deleted, false if not found
   * @throws {DatabaseError} if deletion fails
   */
  async deleteEvent(id: string, followedBy?: string[]): Promise<boolean> {
    if (!this._db) await this.init()

    const transaction = this._db!.transaction(['events', 'ids', 'indexes'], 'readwrite')

    return new Promise((resolve, reject) => {
      this.deleteEventInternal(transaction, id, followedBy).then(resolve).catch(reject)
    })
  }

  private async deleteEventInternal(transaction: IDBTransaction, id: string, followedBy?: string[]): Promise<boolean> {
    const eventStore = transaction.objectStore('events')
    const idStore = transaction.objectStore('ids')
    const indexStore = transaction.objectStore('indexes')

    // find the event by ID index
    const idKey = new Uint8Array(8)
    putHexAsBytes(idKey, 0, id, 8)

    return new Promise((resolve, reject) => {
      const idReq = idStore.get(idKey.buffer)

      idReq.onsuccess = () => {
        const serial = idReq.result as number | undefined
        if (serial === undefined) {
          resolve(false) // event not found
          return
        }

        // get the event to calculate its indexes
        const getEventRequest = eventStore.get(serial)

        getEventRequest.onsuccess = () => {
          const eventData = getEventRequest.result
          if (!eventData) {
            resolve(false)
            return
          }

          const event: NostrEvent = JSON.parse(eventData)

          // delete all indexes for this event
          const deletePromises: Promise<void>[] = []
          for (const indexKey of getIndexKeysForEvent(event, serial, followedBy || [])) {
            const promise = new Promise<void>((resolveDelete, rejectDelete) => {
              const deleteRequest = indexStore.delete(indexKey as IDBValidKey)
              deleteRequest.onsuccess = () => resolveDelete()
              deleteRequest.onerror = () =>
                rejectDelete(new DatabaseError(`Failed to delete index: ${deleteRequest.error?.message}`))
            })
            deletePromises.push(promise)
          }

          // delete the raw event
          const deleteEventPromise = new Promise<void>((resolveDelete, rejectDelete) => {
            const deleteRequest = eventStore.delete(serial)
            deleteRequest.onsuccess = () => resolveDelete()
            deleteRequest.onerror = () =>
              rejectDelete(new DatabaseError(`Failed to delete event: ${deleteRequest.error?.message}`))
          })

          deletePromises.push(deleteEventPromise)

          Promise.all(deletePromises)
            .then(() => resolve(true))
            .catch(reject)
        }

        getEventRequest.onerror = () => {
          reject(new DatabaseError(`failed to get event for deletion: ${getEventRequest.error?.message}`))
        }
      }

      idReq.onerror = () => {
        reject(new DatabaseError(`failed to find event for deletion: ${idReq.error?.message}`))
      }
    })
  }

  private async getSerial(transaction: IDBTransaction, id: string): Promise<number | undefined> {
    const idKey = new Uint8Array(8)
    putHexAsBytes(idKey, 0, id, 8)

    const idStore = transaction.objectStore('ids')

    return new Promise(resolve => {
      const req = idStore.get(idKey.buffer)
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => resolve(undefined)
    })
  }

  /**
   * replaces an existing event with a new one, handling replaceable/addressable event logic.
   * i.e., matching same kind/author(/d-tag).
   * only stores the new event if it's newer than existing one.
   *
   * @param event - the replacement event
   * @param followedBy - optional array of pubkeys that are following this event
   * @returns boolean - true if the event was new, false if it was already saved
   * @throws {DatabaseError} if event values are out of bounds or storage fails
   */
  async replaceEvent(
    event: NostrEvent,
    { seenOn, followedBy }: { seenOn?: string[]; followedBy?: string[] } = {},
  ): Promise<boolean> {
    if (!this._db) await this.init()

    // sanity checking
    if (event.created_at > 0xffffffff || event.kind > 0xffff) {
      throw new DatabaseError('event with values out of expected boundaries')
    }

    // store relays on "seen_on" (so it's saved on database when calling JSON.stringify)
    if (seenOn) {
      ;(event as unknown as { seen_on: string[] }).seen_on = seenOn
    }

    const transaction = this._db!.transaction(['events', 'ids', 'indexes'], 'readwrite', { durability: 'relaxed' })

    const filter: Filter = {
      limit: 1,
      kinds: [event.kind],
      authors: [event.pubkey],
    }

    if (isAddressable(event.kind)) {
      filter['#d'] = [getDTag(event.tags)]
    }

    let shouldStore = true
    const deletePromises: Promise<boolean>[] = []

    // query for existing events
    for await (let previous of this.queryInternal(transaction, filter, 10)) {
      if (isOlder(previous, event)) {
        deletePromises.push(this.deleteEventInternal(transaction, previous.id, followedBy))
      } else {
        shouldStore = false
      }
    }

    await Promise.all(deletePromises)
      .then(() => {
        if (shouldStore) {
          return this.saveEventInternal(transaction, event, followedBy)
        }
      })
      .then(() => transaction.commit())

    return shouldStore
  }

  /**
   * retrieves events by their IDs.
   * this is equivalent to passing a {ids: [...]} filter to queryEvents(), but slightly faster/simpler.
   *
   * @param ids - array of hex-encoded event IDs to fetch
   * @returns array of found events (may be shorter than input if some IDs not found)
   */
  async getByIds(ids: string[]): Promise<NostrEvent[]> {
    if (!this._db) await this.init()

    const transaction = this._db!.transaction(['events', 'ids'], 'readonly')
    return this.getByIdsInternal(transaction, ids)
  }

  private async getByIdsInternal(transaction: IDBTransaction, ids: string[]): Promise<NostrEvent[]> {
    const idStore = transaction.objectStore('ids')
    const eventStore = transaction.objectStore('events')

    // for ids we do a special logic
    const idEventPromises: Promise<NostrEvent | null>[] = []
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i]

      idEventPromises.push(
        new Promise(resolve => {
          const idKey = new Uint8Array(8)
          putHexAsBytes(idKey, 0, id, 8)

          const idReq = idStore.get(idKey.buffer)
          idReq.onsuccess = () => {
            const serial = idReq.result as number | undefined
            if (serial === undefined) {
              resolve(null) // event not found
              return
            }

            const getEventRequest = eventStore.get(serial)

            getEventRequest.onsuccess = () => {
              const eventData = getEventRequest.result
              if (!eventData) {
                resolve(null)
              }

              const event = JSON.parse(eventData)

              // if we see a property "seen_on", convert that to something that can't be jsonified by accident later
              if ('seen_on' in event) {
                event[seenOnSymbol] = event.seen_on
                delete event.seen_on
              }

              // add another special property to denote that this event was loaded from the store
              event[isLocalSymbol] = true

              resolve(event as NostrEvent)
            }

            getEventRequest.onerror = () => {
              console.error(`failed to get event: ${getEventRequest.error?.message}`)
              resolve(null)
            }
          }
        }),
      )
    }
    const idEventResults = await Promise.all(idEventPromises)
    return idEventResults.filter(evt => !!evt)
  }

  /**
   * queries events using a nostr filter, any filters supported (except "search").
   * the actual limit of the query will be the minimum between the filter "limit" if it exists
   * and the maxLimit param.
   *
   * @param filter - nostr filter specification
   * @param maxLimit - maximum number of events to return (default: 500)
   * @yields events matching the filter criteria
   */
  async *queryEvents(filter: Filter & { followedBy?: string }, maxLimit: number = 500): AsyncGenerator<NostrEvent> {
    if (!this._db) await this.init()

    if (filter.search) {
      return // search not supported
    }

    const theoreticalLimit = getTheoreticalLimit(filter)
    if (theoreticalLimit === 0) {
      return
    }
    const limit = Math.min(maxLimit, filter.limit || maxLimit, theoreticalLimit)

    // if there are ids we do a special query
    if (filter.ids) {
      const transaction = this._db!.transaction(['events', 'ids'], 'readonly')
      yield* await this.getByIdsInternal(transaction, filter.ids)
      return
    }

    // otherwise we do a normal query
    const transaction = this._db!.transaction(['events', 'indexes'], 'readonly')
    yield* this.queryInternal(transaction, filter, limit)
  }

  private async *queryInternal(
    transaction: IDBTransaction,
    filter: Filter & { followedBy?: string },
    limit: number,
  ): AsyncGenerator<NostrEvent> {
    const indexStore = transaction.objectStore('indexes')
    const eventStore = transaction.objectStore('events')

    const { queries, extraTagFilter, extraAuthorFilter, extraKindFilter } = prepareQueries(filter)

    const batchSize = Math.min(1_000, batchSizePerNumberOfQueries(limit, queries.length))

    // initial pull from all queries
    await Promise.all(
      queries.map(async query => {
        const hasMore = await this.pull(indexStore, query, batchSize, query.results)
        if (!hasMore) {
          // exhaust
          query.exhausted = true
        }
      }),
    )

    // main iteration loop
    let emittedTotal = 0
    const numberOfIteratorsToPullOnEachRound = Math.max(1, Math.ceil(queries.length / 2))
    const tempResults: QueryResult[] = new Array(batchSize * 2) // [timestamp, serial][]
    const eventPromises = new Array(batchSize * 2)
    const pulls = new Array(numberOfIteratorsToPullOnEachRound)
    const toDelete = new Array(numberOfIteratorsToPullOnEachRound)

    while (queries.length > 0) {
      tempResults.length = 0
      eventPromises.length = 0
      pulls.length = 0
      toDelete.length = 0

      // find threshold: highest timestamp among the oldest of each query
      queries.sort((a, b) => (b.results[b.results.length - 1]?.ts || 0) - (a.results[a.results.length - 1]?.ts || 0))
      if (queries[0].results.length === 0) return

      const threshold: number = queries[0].results[queries[0].results.length - 1].ts

      // collect all events >= threshold from ALL iterators
      for (let q = 0; q < queries.length; q++) {
        const query = queries[q]
        let hasSpliced = false
        let cc = 0

        for (let i = 0; i < query.results.length; i++) {
          const result = query.results[i]

          if (result.ts >= threshold) {
            tempResults.push(result)
            cc++
          } else {
            // reached an item that isn't >= threshold, so stop here and remove the previous elements from the array
            query.results.splice(0, i)
            hasSpliced = true
            break
          }
        }

        // if we collected everything we never reached the splice call above,
        if (!hasSpliced) {
          //  so clear all the results array here
          query.results.length = 0

          if (query.exhausted) {
            // swap-delete
            queries[q] = queries[queries.length - 1]
            queries.length = queries.length - 1
            q--
          }
        }
      }

      // sort temp results (this ensures our results are emitted in the correct order)
      tempResults.sort((a, b) => b.ts - a.ts)

      // load temp results from database in individual queries to the eventstore
      eventPromises.length = tempResults.length
      for (let i = 0; i < tempResults.length; i++) {
        const serial = tempResults[i].serial

        eventPromises[i] = new Promise<NostrEvent | null>(resolve => {
          const getEventRequest = eventStore.get(serial)

          getEventRequest.onsuccess = () => {
            const eventData = getEventRequest.result
            if (!eventData) {
              console.error('tried to get event with serial', serial, 'but it did not exist')
              resolve(null)
              return
            }

            const event = JSON.parse(eventData)

            // if we see a property "seen_on", convert that to something that can't be jsonified by accident later
            if ('seen_on' in event) {
              event[seenOnSymbol] = event.seen_on
              delete event.seen_on
            }

            // add another special property to denote that this event was loaded from the store
            event[isLocalSymbol] = true

            resolve(event as NostrEvent)
          }

          getEventRequest.onerror = () => {
            console.error(`failed to get event: ${getEventRequest.error?.message}`)
            resolve(null)
          }
        })
      }

      // when they're all loaded filter them by any extraTagFilters and emit them
      let events = await Promise.all(eventPromises)
      for (let i = 0; i < events.length; i++) {
        const evt = events[i]

        // apply extra filtering
        if (
          !evt ||
          !filterMatchesTags(extraTagFilter, evt) ||
          (extraAuthorFilter && !extraAuthorFilter.includes(evt.pubkey)) ||
          (extraKindFilter && !extraKindFilter.includes(evt.kind))
        ) {
          continue
        }

        yield evt
        emittedTotal++
        if (emittedTotal >= limit) {
          return
        }
      }

      // proceed by pulling more data from the query that had the threshold timestamp then repeating the process
      pulls.length = numberOfIteratorsToPullOnEachRound
      for (let q = 0; q < numberOfIteratorsToPullOnEachRound && q < queries.length; q++) {
        const query = queries[q]

        if (query.exhausted) {
          // skip this, do not pull
          if (query.results.length === 0) {
            // also if it's empty remove it (swap-remove)
            queries[q] = queries[queries.length - 1]
            if (queries.length - 1 < numberOfIteratorsToPullOnEachRound) {
              // in this case we can just pull from this query that we just swapped
              q--
            }
            queries.length = queries.length - 1
          }
          continue
        }

        pulls[q] = this.pull(indexStore, query, batchSize, query.results).then(hasMore => {
          if (!hasMore) {
            // exhaust
            query.exhausted = true

            if (query.results.length === 0) {
              // also if it's empty remove it
              toDelete.push(q)
            }
          }
        })
      }
      await Promise.all(pulls)

      // before ending this loop iteration clean up queries we just fetched from and got no results
      for (let t = toDelete.length - 1; t >= 0; t--) {
        queries[toDelete[t]] = queries[queries.length - 1]
        queries.length--
      }
    }
  }

  private async pull(
    indexStore: IDBObjectStore,
    query: Query,
    batchSize: number,
    resultsInto: QueryResult[],
  ): Promise<boolean> {
    let last: ArrayBuffer | null = null

    return new Promise(resolve => {
      const range = IDBKeyRange.bound(query.startingPoint.buffer, query.endingPoint.buffer, true, true)
      let skipUntilLastFetched = !!query.lastFetched // this will always be true except in the first query
      // we will always have at least one repeated result because we'll include the timestamp of the last fetched

      const keysReq = indexStore.getAllKeys(range, batchSize)
      keysReq.onsuccess = async () => {
        for (let i = 0; i < keysReq.result.length; i++) {
          const indexKey = keysReq.result[i] as ArrayBuffer

          // extract timestamp from index key
          const tsBytes = indexKey.slice(indexKey.byteLength - 8, indexKey.byteLength - 4)
          const ts = timestampFromInvertedBytes(tsBytes)
          last = tsBytes

          // extract idx/serial from index key
          const serial = new DataView(indexKey).getUint32(indexKey.byteLength - 4)

          // if we have previously fetched anything we can't emit the same events again
          // so skip until we get our last fetched event idx
          if (skipUntilLastFetched) {
            if (serial === query.lastFetched) {
              skipUntilLastFetched = false
              // won't skip anymore now
            }
            continue
          }

          // this will be used in the next query, for skipping repeated results
          query.lastFetched = serial

          // collect the results
          resultsInto.push({ ts, serial })
        }

        // update startingPoint if we are going to do this query again
        if (last) {
          query.startingPoint.set(new Uint8Array(last), query.startingPoint.length - 4 - 4)
        }

        resolve(
          // return true if we fetched the exact number that was requested, which means this is not exhausted
          keysReq.result.length === batchSize,
        )
      }
    })
  }

  /**
   * marks all events of a pubkey as followed by another pubkey.
   * adds followedBy indexes for existing events if not already present.
   *
   * @param follower - the pubkey that is following
   * @param followed - the pubkey being followed
   */
  async markFollow(follower: string, followed: string): Promise<void> {
    if (!this._db) await this.init()

    const transaction = this._db!.transaction(['events', 'ids', 'indexes'], 'readwrite')
    const filter = { authors: [followed] }

    const ops: Promise<void>[] = []
    for await (const event of this.queryInternal(transaction, filter, Number.MAX_SAFE_INTEGER)) {
      const serial = await this.getSerial(transaction, event.id)
      if (serial === undefined) continue

      const idx = new Uint8Array(4)
      idx[0] = (serial >> 24) & 0xff
      idx[1] = (serial >> 16) & 0xff
      idx[2] = (serial >> 8) & 0xff
      idx[3] = serial & 0xff

      const tsBytes = invertedTimestampBytes(event.created_at)

      const key = new Uint8Array(1 + 8 + 4 + 4)
      key[0] = INDEX_FOLLOWED_PREFIX
      putHexAsBytes(key, 1, follower, 8)
      key.set(tsBytes, 1 + 8)
      key.set(idx, 1 + 8 + 4)

      const indexStore = transaction.objectStore('indexes')
      const req = indexStore.put(null, key.buffer)

      ops.push(
        new Promise(resolve => {
          req.onsuccess = () => resolve()
          req.onerror = () => resolve()
        }),
      )
    }

    await Promise.all(ops)

    transaction.commit()
  }

  /**
   * removes followedBy indexes for all events of a pubkey followed by another pubkey.
   *
   * @param follower - the pubkey that is unfollowing
   * @param followed - the pubkey being unfollowed
   */
  async markUnfollow(follower: string, followed: string): Promise<void> {
    if (!this._db) await this.init()

    const transaction = this._db!.transaction(['events', 'ids', 'indexes'], 'readwrite')
    const filter = { authors: [followed], followedBy: follower }

    const keysToDelete: Uint8Array[] = []

    for await (const event of this.queryInternal(transaction, filter, Number.MAX_SAFE_INTEGER)) {
      const serial = await this.getSerial(transaction, event.id)
      if (serial === undefined) continue

      const idx = new Uint8Array(4)
      idx[0] = (serial >> 24) & 0xff
      idx[1] = (serial >> 16) & 0xff
      idx[2] = (serial >> 8) & 0xff
      idx[3] = serial & 0xff

      const tsBytes = invertedTimestampBytes(event.created_at)

      const key = new Uint8Array(1 + 8 + 4 + 4)
      key[0] = INDEX_FOLLOWED_PREFIX
      putHexAsBytes(key, 1, follower, 8)
      key.set(tsBytes, 1 + 8)
      key.set(idx, 1 + 8 + 4)

      keysToDelete.push(key)
    }

    const indexStore = transaction.objectStore('indexes')

    await Promise.all(
      keysToDelete.map(key => {
        const req = indexStore.delete(key.buffer as IDBValidKey)

        return new Promise<void>((resolve, reject) => {
          req.onsuccess = () => resolve()
          req.onerror = () =>
            reject(new Error(`failed to delete followedBy index ${key} for ${follower} => ${followed}`))
        })
      }),
    )

    transaction.commit()
  }

  /**
   * cleans followedBy indexes for events followed by a specific pubkey.
   * fetches all events followed by the given pubkey, calls the predicate function on each,
   * and deletes the followedBy index if the predicate returns false or if the pointed event doesn't exist.
   *
   * @param followedBy - the pubkey whose followed events to clean
   * @param shouldKeep - function that takes an event and returns false if the followed index should be deleted
   */
  async cleanFollowed(followedBy: string, shouldKeep: (event: NostrEvent) => boolean): Promise<void> {
    if (!this._db) await this.init()

    const transaction = this._db!.transaction(['events', 'indexes'], 'readwrite')
    const indexStore = transaction.objectStore('indexes')
    const eventStore = transaction.objectStore('events')

    const startingPoint = new Uint8Array(1 + 8 + 4 + 4)
    startingPoint[0] = INDEX_FOLLOWED_PREFIX
    putHexAsBytes(startingPoint, 1, followedBy, 8)
    startingPoint.fill(0x00, 1 + 8)

    const endingPoint = startingPoint.slice()
    endingPoint.fill(0xff, 1 + 8)

    const range = IDBKeyRange.bound(startingPoint.buffer, endingPoint.buffer)
    const keysReq = indexStore.getAllKeys(range)

    const ops: Promise<void>[] = []

    await new Promise<void>((topResolve, topReject) => {
      keysReq.onsuccess = async () => {
        const eventPromises: Promise<NostrEvent | null>[] = []
        const keyBuffers: ArrayBuffer[] = []

        for (const keyBuffer of keysReq.result as ArrayBuffer[]) {
          keyBuffers.push(keyBuffer)
          const serial = new DataView(keyBuffer).getUint32(keyBuffer.byteLength - 4)

          eventPromises.push(
            new Promise(resolve => {
              const getEventRequest = eventStore.get(serial)
              getEventRequest.onsuccess = () => {
                const eventData = getEventRequest.result
                if (!eventData) {
                  resolve(null)
                  return
                }
                const event = JSON.parse(eventData) as NostrEvent
                resolve(event)
              }
              getEventRequest.onerror = () => resolve(null)
            }),
          )
        }

        const events = await Promise.all(eventPromises)
        for (let i = 0; i < events.length; i++) {
          const event = events[i]
          if (event && shouldKeep(event)) continue

          const req = indexStore.delete(keyBuffers[i])
          ops.push(
            new Promise((resolve, reject) => {
              req.onsuccess = () => resolve()
              req.onerror = () => reject(new Error(`failed to delete followedBy entry ${keyBuffers[i]} for ${event}`))
            }),
          )
        }

        try {
          await Promise.all(ops)
          transaction.commit()
          topResolve()
        } catch (err) {
          topReject(err)
          transaction.abort()
          return
        }
      }

      keysReq.onerror = () => topReject(new Error('failed to fetch keys'))
    })
  }
}

function getTagIndexPrefix(tagLetter: string, tagValue: string): [Uint8Array, number] {
  let key: Uint8Array
  let offset: number

  try {
    // assume it's an addressable format, if it isn't we will error
    const { kind, pk, d } = getAddrTagElements(tagValue)
    // store value in the new special "a" tag index
    key = new Uint8Array(1 + 1 + 2 + 8 + d.length + 4 + 4)
    key[0] = INDEX_TAG_ADDR_PREFIX

    // write tag name
    key[1] = tagLetter.charCodeAt(0) % 256

    // write kind as big-endian uint16
    key[2] = (kind >> 8) & 0xff
    key[3] = kind & 0xff

    // copy first 8 bytes of pubkey
    putHexAsBytes(key, 1 + 1 + 2, pk, 8)

    // copy d tag value
    const encoder = new TextEncoder()
    const dBytes = encoder.encode(d)
    key.set(dBytes, 1 + 1 + 2 + 8)

    offset = 1 + 1 + 2 + 8 + d.length
    return [key, offset]
  } catch {
    try {
      // store value as bytes (if it's not valid hex it will error)
      key = new Uint8Array(1 + 1 + 8 + 4 + 4)
      key[0] = INDEX_TAG32_PREFIX
      key[1] = tagLetter.charCodeAt(0) % 256
      putHexAsBytes(key, 1 + 1, tagValue, 8)
      offset = 1 + 1 + 8
      return [key, offset]
    } catch {
      // store whatever as utf-8
      const encoder = new TextEncoder()
      const valueBytes = encoder.encode(tagValue)
      key = new Uint8Array(1 + 1 + valueBytes.length + 4 + 4)
      key[0] = INDEX_TAG_PREFIX
      key[1] = tagLetter.charCodeAt(0) % 256
      key.set(valueBytes, 1 + 1)
      offset = 1 + 1 + valueBytes.length
      return [key, offset]
    }
  }
}

function* getIndexKeysForEvent(
  event: NostrEvent,
  serialOrIdx: number | Uint8Array,
  followedBy?: string[],
): Generator<Uint8Array> {
  let idx: Uint8Array
  if (typeof serialOrIdx === 'object') {
    idx = serialOrIdx
  } else {
    idx = new Uint8Array(4)
    idx[0] = (serialOrIdx >> 24) & 0xff
    idx[1] = (serialOrIdx >> 16) & 0xff
    idx[2] = (serialOrIdx >> 8) & 0xff
    idx[3] = serialOrIdx & 0xff
  }

  // this is so the events are ordered from newer to older
  const tsBytes = invertedTimestampBytes(event.created_at)

  // by date only
  {
    const key = new Uint8Array(1 + 4 + 4)
    key[0] = INDEX_CREATED_AT_PREFIX
    key.set(tsBytes, 1)
    key.set(idx, 1 + 4)
    yield key
  }

  // by kind + date
  {
    const key = new Uint8Array(1 + 2 + 4 + 4)
    key[0] = INDEX_KIND_PREFIX
    key[1] = (event.kind >> 8) & 0xff
    key[2] = event.kind & 0xff
    key.set(tsBytes, 1 + 2)
    key.set(idx, 1 + 2 + 4)
    yield key
  }

  // by pubkey + date
  {
    const key = new Uint8Array(1 + 8 + 4 + 4)
    key[0] = INDEX_PUBKEY_PREFIX
    putHexAsBytes(key, 1, event.pubkey, 8)
    key.set(tsBytes, 1 + 8)
    key.set(idx, 1 + 8 + 4)
    yield key
  }

  // by pubkey + kind + date
  {
    const key = new Uint8Array(1 + 8 + 2 + 4 + 4)
    key[0] = INDEX_PUBKEY_KIND_PREFIX
    putHexAsBytes(key, 1, event.pubkey, 8)
    key[9] = (event.kind >> 8) & 0xff
    key[10] = event.kind & 0xff
    key.set(tsBytes, 1 + 8 + 2)
    key.set(idx, 1 + 8 + 2 + 4)
    yield key
  }

  // by tag value + date
  const seenTagValues = new Set<string>()
  for (const tag of event.tags) {
    if (tag[0]!.length !== 1 || !tag[1] || tag[1].length > 100) {
      continue
    }
    if (seenTagValues.has(tag[1])) {
      continue // skip duplicates
    }
    seenTagValues.add(tag[1])

    const [key, offset] = getTagIndexPrefix(tag[0], tag[1])
    key.set(tsBytes, offset)
    key.set(idx, offset + 4)

    yield key
  }

  // by followed + date
  if (followedBy) {
    for (const follower of followedBy) {
      const key = new Uint8Array(1 + 8 + 4 + 4)
      key[0] = INDEX_FOLLOWED_PREFIX
      putHexAsBytes(key, 1, follower, 8)
      key.set(tsBytes, 1 + 8)
      key.set(idx, 1 + 8 + 4)
      yield key
    }
  }
}

function getAddrTagElements(tagValue: string): { kind: number; pk: string; d: string } {
  const parts = tagValue.split(':')
  if (parts.length <= 3) {
    const kind = parseInt(parts[0]!, 10)
    const pk = parts[1]!
    const d = parts.slice(2).join(':')
    if (!isNaN(kind) && pk.length === 64) {
      return { kind, pk, d }
    }
  }
  throw 'invalid addressable tag'
}

function filterMatchesTags(tagFilter: [string, string[]][], event: NostrEvent): boolean {
  for (const [tagName, values] of tagFilter) {
    if (values && values.length > 0) {
      const hasMatchingTag = event.tags.some(tag => tag.length >= 2 && tag[0] === tagName && values.includes(tag[1]!))
      if (!hasMatchingTag) {
        return false
      }
    }
  }
  return true
}

function isOlder(a: NostrEvent, b: NostrEvent): boolean {
  return a.created_at < b.created_at
}

function getTheoreticalLimit(filter: Filter): number {
  if (filter.ids) return filter.ids.length

  if (filter.until && filter.since && filter.until < filter.since) return 0

  if (filter.authors !== undefined && filter.kinds !== undefined) {
    const allAreReplaceable = filter.kinds.every(isReplaceableKind)
    if (allAreReplaceable) {
      return filter.authors.length * filter.kinds.length
    }

    if (filter['#d']?.length) {
      const allAreAddressable = filter.kinds.every(isAddressable)
      if (allAreAddressable) {
        return filter.authors.length * filter.kinds.length * filter['#d'].length
      }
    }
  }

  return Number.MAX_SAFE_INTEGER
}

function isAddressable(kind: number): boolean {
  return (kind >= 30000 && kind < 40000) || kind === 0 || kind === 3
}

function getDTag(tags: string[][]): string {
  const dTag = tags.find(tag => tag.length >= 2 && tag[0] === 'd')
  return dTag?.[1] || ''
}

function prepareQueries(filter: Filter & { followedBy?: string }): {
  queries: Query[]
  extraTagFilter: [tagLetter: string, tagValues: string[]][]
  extraAuthorFilter: string[] | null
  extraKindFilter: number[] | null
} {
  const queries: Query[] = []
  const extraTagFilter: [tagLetter: string, tagValues: string[]][] = []
  let extraAuthorFilter: string[] | null = null
  let extraKindFilter: number[] | null = null
  const timestampStartingPoint = invertedTimestampBytes(filter.until || 0xffffffff)
  const timestampEndingPoint = invertedTimestampBytes(filter.since || 0)

  // handle high-priority tag filters (these trump everything else)
  const highPriority = ['q', 'e', 'E', 'i', 'I', 'a', 'A', 'g', 'r']
  {
    let bestPrio = 100
    let bestIndex = -1
    for (let tagName in filter) {
      if (tagName[0] !== '#' || tagName.length !== 2) continue

      // add everything as tag filters (this will be used by other queries afterwards even if we don't go with this)
      extraTagFilter.push([tagName[1], filter[tagName]])

      let prio = highPriority.indexOf(tagName[1])
      if (prio >= 0 && prio < bestPrio) {
        bestPrio = prio
        bestIndex = extraTagFilter.length - 1
      }
    }

    if (bestIndex >= 0) {
      let [tagLetter, tagValues] = extraTagFilter[bestIndex]
      for (const value of tagValues) {
        const [startingPoint, offset] = getTagIndexPrefix(tagLetter, value)
        startingPoint.set(timestampStartingPoint, offset)
        startingPoint.fill(0x00, offset + 4)

        const endingPoint = startingPoint.slice()
        endingPoint.set(timestampEndingPoint, offset)
        endingPoint.fill(0xff, offset + 4)

        queries.push({
          startingPoint,
          endingPoint,
          exhausted: false,
          results: [],
        })
      }

      // swap-delete the best one from the list of extras
      extraTagFilter[bestIndex] = extraTagFilter[extraTagFilter.length - 1]
      extraTagFilter.pop()

      // if authors and kinds were specified we have to filter for those afterwards
      extraAuthorFilter = filter.authors || null
      extraKindFilter = filter.kinds || null

      // (this means we had tags in the query so we can exit now with the queries we just gathered)
      return { queries, extraTagFilter, extraAuthorFilter, extraKindFilter }
    }
  }

  if (filter.followedBy) {
    const startingPoint = new Uint8Array(1 + 8 + 4 + 4)
    startingPoint[0] = INDEX_FOLLOWED_PREFIX
    putHexAsBytes(startingPoint, 1, filter.followedBy, 8)
    startingPoint.set(timestampStartingPoint, 1 + 8)
    startingPoint.fill(0x00, 1 + 8 + 4)

    const endingPoint = startingPoint.slice()
    endingPoint.set(timestampEndingPoint, 1 + 8)
    endingPoint.fill(0xff, 1 + 8 + 4)

    queries.push({
      startingPoint,
      endingPoint,
      exhausted: false,
      results: [],
    })

    // if authors and kinds were specified we have to filter for those afterwards
    extraAuthorFilter = filter.authors || null
    extraKindFilter = filter.kinds || null

    return { queries, extraTagFilter, extraAuthorFilter, extraKindFilter }
  }

  if (filter.authors && filter.authors.length > 0) {
    // handle combined author + kind filter
    if (filter.kinds && filter.kinds.length > 0) {
      // only use the pubkey-kind index if we're requesting few kinds
      if (filter.kinds.length < 8) {
        for (let author of filter.authors) {
          const authorBytes = hexToBytes(author.substring(0, 16))
          for (const kind of filter.kinds) {
            const startingPoint = new Uint8Array(1 + 8 + 2 + 4 + 4)
            startingPoint[0] = INDEX_PUBKEY_KIND_PREFIX
            startingPoint.set(authorBytes, 1)
            startingPoint[9] = (kind >> 8) & 0xff
            startingPoint[10] = kind & 0xff
            startingPoint.set(timestampStartingPoint, 1 + 8 + 2)
            startingPoint.fill(0x00, 1 + 8 + 2 + 4)

            const endingPoint = startingPoint.slice()
            endingPoint.set(timestampEndingPoint, 1 + 8 + 2)
            endingPoint.fill(0xff, 1 + 8 + 2 + 4)

            queries.push({
              startingPoint,
              endingPoint,
              exhausted: false,
              results: [],
            })
          }
        }

        return { queries, extraTagFilter, extraAuthorFilter, extraKindFilter }
      } else {
        // when requesting too many kinds it's better to just get everything from the pubkey
        // then filter later
        extraKindFilter = filter.kinds
      }
    }

    // use just the author kind
    for (const author of filter.authors) {
      const startingPoint = new Uint8Array(1 + 8 + 4 + 4)
      startingPoint[0] = INDEX_PUBKEY_PREFIX
      putHexAsBytes(startingPoint, 1, author, 8)
      startingPoint.set(timestampStartingPoint, 1 + 8)
      startingPoint.fill(0x00, 1 + 8 + 4)

      const endingPoint = startingPoint.slice()
      endingPoint.set(timestampEndingPoint, 1 + 8)
      endingPoint.fill(0xff, 1 + 8 + 4)

      queries.push({
        startingPoint,
        endingPoint,
        exhausted: false,
        results: [],
      })
    }

    return { queries, extraTagFilter, extraAuthorFilter, extraKindFilter }
  }

  // handle kind filter
  if (filter.kinds && filter.kinds.length > 0) {
    for (const kind of filter.kinds) {
      const startingPoint = new Uint8Array(1 + 2 + 4 + 4)
      startingPoint[0] = INDEX_KIND_PREFIX
      startingPoint[1] = (kind >> 8) & 0xff
      startingPoint[2] = kind & 0xff
      startingPoint.set(timestampStartingPoint, 1 + 2)
      startingPoint.fill(0x00, 1 + 2 + 4)

      const endingPoint = startingPoint.slice()
      endingPoint.set(timestampEndingPoint, 1 + 2)
      endingPoint.fill(0xff, 1 + 2 + 4)

      queries.push({
        startingPoint,
        endingPoint,
        exhausted: false,
        results: [],
      })
    }

    return { queries, extraTagFilter, extraAuthorFilter, extraKindFilter }
  }

  // handle low-priority tag filters (these are worse than kind, authors etc)
  {
    for (let i = 0; i < extraTagFilter.length; i++) {
      // naïvely, the first tag we find will be the index
      let [tagLetter, tagValues] = extraTagFilter[i]
      for (let value of tagValues) {
        const [startingPoint, offset] = getTagIndexPrefix(tagLetter, value)
        startingPoint.set(timestampStartingPoint, offset)
        startingPoint.fill(0x00, offset + 4)

        const endingPoint = startingPoint.slice()
        endingPoint.set(timestampEndingPoint, offset)
        endingPoint.fill(0xff, offset + 4)

        queries.push({
          startingPoint,
          endingPoint,
          exhausted: false,
          results: [],
        })
      }

      // remove main index from list of extra tags (swap-delete)
      extraTagFilter[i] = extraTagFilter[extraTagFilter.length - 1]
      extraTagFilter.pop()

      // and we're done, we only needed one
      return { queries, extraTagFilter, extraAuthorFilter, extraKindFilter }
    }
  }

  // fallback: query by created_at only
  const startingPoint = new Uint8Array(1 + 4 + 4)
  startingPoint[0] = INDEX_CREATED_AT_PREFIX
  startingPoint.set(timestampStartingPoint, 1)
  startingPoint.fill(0x00, 1 + 4)

  const endingPoint = startingPoint.slice()
  endingPoint.set(timestampEndingPoint, 1)
  endingPoint.fill(0xff, 1 + 4)

  queries.push({
    startingPoint,
    endingPoint,
    exhausted: false,
    results: [],
  })

  return { queries, extraTagFilter, extraAuthorFilter, extraKindFilter }
}

function batchSizePerNumberOfQueries(totalFilterLimit: number, numberOfQueries: number): number {
  if (totalFilterLimit <= 10) return totalFilterLimit
  if (numberOfQueries <= 2) return totalFilterLimit

  return Math.ceil(Math.pow(totalFilterLimit, 0.8) / Math.pow(numberOfQueries, 0.11))
}

function putHexAsBytes(target: Uint8Array, offset: number, hex: string, bytesToWrite: number) {
  for (let i = 0; i < bytesToWrite; i++) {
    const j = i * 2
    const byte = Number.parseInt(hex.substring(j, j + 2), 16)
    target[offset + i] = byte
  }
}

function invertedTimestampBytes(created_at: number) {
  const invertedTimestamp = 0xffffffff - created_at
  const tsBytes = new Uint8Array(4)
  tsBytes[0] = (invertedTimestamp >> 24) & 0xff
  tsBytes[1] = (invertedTimestamp >> 16) & 0xff
  tsBytes[2] = (invertedTimestamp >> 8) & 0xff
  tsBytes[3] = invertedTimestamp & 0xff
  return tsBytes
}

function timestampFromInvertedBytes(tsBytes: ArrayBuffer): number {
  const invertedTimestamp = new DataView(tsBytes).getUint32(0)
  const created_at = 0xffffffff - invertedTimestamp
  return created_at
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
