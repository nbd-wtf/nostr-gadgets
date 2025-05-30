import { Filter } from '@nostr/tools/filter'
import { NostrEvent } from '@nostr/tools/pure'
import { bytesToHex, hexToBytes } from '@nostr/tools/utils'
import { isReplaceableKind } from '@nostr/tools/kinds'

type IterEvent = {
  event: NostrEvent
  q: number // query index
}

type Query = {
  startingPoint: Uint8Array
  endingPoint: Uint8Array
  lastFetched?: number // idx serial
}

type Task = {
  p: Promise<void>
  resolve(): void
  reject(e: Error): void
}

export class DuplicateEventError extends Error {
  constructor(event: NostrEvent, prefix: Uint8Array, match: ArrayBuffer) {
    super(
      `Event ${JSON.stringify(event)} already exists at key 0x${bytesToHex(new Uint8Array(match))} (searched 0x${bytesToHex(prefix)})`,
    )
    this.name = 'DuplicateEventError'
  }
}

export class DatabaseError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'DatabaseError'
  }
}

// Constants matching the Go implementation
const INDEX_CREATED_AT_PREFIX = 1
const INDEX_KIND_PREFIX = 2
const INDEX_PUBKEY_PREFIX = 3
const INDEX_PUBKEY_KIND_PREFIX = 4
const INDEX_TAG_PREFIX = 5
const INDEX_TAG32_PREFIX = 6
const INDEX_TAG_ADDR_PREFIX = 7

export class IDBEventStore {
  private dbName: string
  private db: IDBDatabase | undefined

  constructor(dbName: string = '@nostr/gadgets/events') {
    this.dbName = dbName
  }

  async init(): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 1)

      request.onerror = () => {
        reject(new DatabaseError(`failed to open database: ${request.error?.message}`))
      }

      request.onsuccess = () => {
        this.db = request.result
        resolve()
      }

      request.onupgradeneeded = () => {
        const db = request.result

        // create object stores
        db.createObjectStore('events', { autoIncrement: true })
        db.createObjectStore('ids')
        db.createObjectStore('indexes')
      }
    })
  }

  async close(): Promise<void> {
    if (this.db) {
      this.db.close()
      this.db = undefined
    }
  }

  private saveBatch: null | [ids: string[], events: NostrEvent[], tasks: Task[]]
  async saveEvent(event: NostrEvent): Promise<void> {
    if (!this.db) await this.init()

    // sanity checking
    if (event.created_at > 0xffffffff || event.kind > 0xffff) {
      throw new DatabaseError('event with values out of expected boundaries')
    }

    // start batching logic
    let batch = this.saveBatch

    if (!batch) {
      batch = [[], [], []]
      this.saveBatch = batch

      // once we know we have a fresh batch, we schedule this batch to run
      const events = batch[1]
      const tasks = batch[2]
      queueMicrotask(() => {
        // as soon as we start processing this batch, we need to null it
        // to ensure that any new requests will be added to a new batch
        this.saveBatch = null

        const transaction = this.db!.transaction(['events', 'ids', 'indexes'], 'readwrite', {
          durability: 'relaxed',
        })

        const promises = this.saveEventsBatch(transaction, events)
        for (let i = 0; i < promises.length; i++) {
          promises[i].catch(tasks[i].reject).then(tasks[i].resolve)
        }
      })
    }

    batch = batch!

    // return existing task if it exists
    let idx = batch[0].indexOf(event.id)
    if (idx !== -1) return batch[2][idx].p

    // add a new one
    idx = batch[0].push(event.id) - 1
    let task = (batch[2][idx] = {} as Task)
    batch[1][idx] = event

    task.p = new Promise<void>(function (resolve, reject) {
      task.resolve = resolve
      task.reject = reject
    })
  }

  private saveEventsBatch(transaction: IDBTransaction, events: NostrEvent[]): Promise<void>[] {
    const idStore = transaction.objectStore('ids')
    const promises = new Array<Promise<void>>(events.length)

    for (let i = 0; i < events.length; i++) {
      const event = events[i]

      promises[i] = new Promise<void>((resolve, reject) => {
        // check for duplicates
        const idKey = new Uint8Array(8)
        putHexAsBytes(idKey, 0, event.id, 8)

        const checkRequest = idStore.getKey(idKey.buffer)
        checkRequest.onsuccess = () => {
          if (checkRequest.result && checkRequest.result) {
            reject(new DuplicateEventError(event, idKey, checkRequest.result as ArrayBuffer))
            return
          }

          // save the event
          this.saveEventInternal(transaction, event)
            .then(() => {
              resolve()
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

  private async saveEventInternal(transaction: IDBTransaction, event: NostrEvent): Promise<void> {
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
        for (const indexKey of getIndexKeysForEvent(event, serial)) {
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

  async deleteEvent(id: string): Promise<boolean> {
    if (!this.db) await this.init()

    const transaction = this.db!.transaction(['events', 'indexes'], 'readwrite')

    return new Promise((resolve, reject) => {
      this.deleteEventInternal(transaction, id).then(resolve).catch(reject)
    })
  }

  private async deleteEventInternal(transaction: IDBTransaction, id: string): Promise<boolean> {
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
          const event = getEventRequest.result as NostrEvent | undefined
          if (!event) {
            resolve(false)
            return
          }

          // delete all indexes for this event
          const deletePromises: Promise<void>[] = []
          for (const indexKey of getIndexKeysForEvent(event, serial)) {
            const promise = new Promise<void>((resolveDelete, rejectDelete) => {
              const deleteRequest = indexStore.delete(indexKey)
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

  async replaceEvent(event: NostrEvent): Promise<void> {
    if (!this.db) await this.init()

    // Sanity checking
    if (event.created_at > 0xffffffff || event.kind > 0xffff) {
      throw new DatabaseError('event with values out of expected boundaries')
    }

    const transaction = this.db!.transaction(['events', 'indexes'], 'readwrite', { durability: 'relaxed' })

    return new Promise((resolve, reject) => {
      const filter: Filter = {
        limit: 1,
        kinds: [event.kind],
        authors: [event.pubkey],
      }

      if (isAddressable(event.kind)) {
        filter['#d'] = [getDTag(event.tags)]
      }

      // query for existing events
      this.queryInternal(transaction, filter, 10)
        .then(results => {
          let shouldStore = true
          const deletePromises: Promise<boolean>[] = []

          for (let i = 0; i < results.length; i++) {
            const previous = results[i]
            if (isOlder(previous.event, event)) {
              deletePromises.push(this.deleteEventInternal(transaction, previous.event.id))
            } else {
              shouldStore = false
            }
          }

          Promise.all(deletePromises)
            .then(() => {
              if (shouldStore) {
                return this.saveEventInternal(transaction, event)
              }
            })
            .then(() => {
              transaction.commit()
              resolve()
            })
            .catch(reject)
        })
        .catch(reject)
    })
  }

  async getByIds(ids: string[]): Promise<NostrEvent[]> {
    if (!this.db) await this.init()

    const transaction = this.db!.transaction(['events', 'ids'], 'readonly')
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
              if (eventData) {
                const event: NostrEvent = JSON.parse(eventData)
                resolve(event)
              }
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

  async *queryEvents(filter: Filter, maxLimit: number = 500): AsyncGenerator<NostrEvent> {
    if (!this.db) await this.init()

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
      const transaction = this.db!.transaction(['events', 'ids'], 'readonly')
      yield* await this.getByIdsInternal(transaction, filter.ids)
      return
    }

    // otherwise we do a normal query
    const transaction = this.db!.transaction(['events', 'indexes'], 'readonly')
    const results = await this.queryInternal(transaction, filter, limit)
    for (const result of results) {
      yield result.event
    }
  }

  private async queryInternal(transaction: IDBTransaction, filter: Filter, limit: number): Promise<IterEvent[]> {
    const indexStore = transaction.objectStore('indexes')
    const eventStore = transaction.objectStore('events')

    const { queries, extraTagFilter } = prepareQueries(filter)
    if (queries.length === 0) {
      return []
    }

    // two-phase query implementation based on nostrlib
    const exhausted: boolean[] = new Array(queries.length).fill(false)
    const results: IterEvent[][] = new Array(queries.length)
    const pulledPerQuery: number[] = new Array(queries.length).fill(0)

    for (let q = 0; q < queries.length; q++) {
      // initialize results arrays
      results[q] = []
    }

    // track oldest event across all iterators
    let oldest: IterEvent | null = null

    let sndPhase = false // second phase flag
    let secondBatch: IterEvent[][] = []
    let sndPhaseParticipants: number[] = []

    // alternating result arrays for second phase
    let sndPhaseResultsA: IterEvent[] = []
    let sndPhaseResultsB: IterEvent[] = []
    let sndPhaseResultsToggle = false
    let sndPhaseHasResultsPending = false

    let remainingUnexhausted = queries.length
    let batchSizePerQuery = batchSizePerNumberOfQueries(limit, remainingUnexhausted)
    let firstPhaseTotalPulled = 0

    function exhaust(q: number) {
      exhausted[q] = true
      remainingUnexhausted--
      if (q === oldest?.q) {
        oldest = null
      }
    }

    let firstPhaseResults: IterEvent[] = []

    // main iteration loop
    for (let c = 0; ; c++) {
      batchSizePerQuery = batchSizePerNumberOfQueries(limit, remainingUnexhausted)

      // process each query in batches
      for (let q = 0; q < queries.length; q++) {
        if (exhausted[q]) {
          continue
        }
        if (oldest?.q === q && remainingUnexhausted > 1) {
          continue
        }

        const query = queries[q]!
        const [hasMore, queryResults] = await this.executeQueryBatch(
          indexStore,
          eventStore,
          q,
          query,
          extraTagFilter,
          batchSizePerQuery,
          filter.since,
        )

        pulledPerQuery[q] += queryResults.length

        for (let i = 0; i < queryResults.length; i++) {
          const ievt = queryResults[i]

          if (sndPhase) {
            // second phase logic - dynamic threshold adjustment
            if (oldest === null) {
              // branch when we don't have the oldest event
              results[q]!.push(ievt)
              sndPhaseHasResultsPending = true
            } else {
              const nextThreshold = firstPhaseResults[firstPhaseResults.length - 2]
              if (nextThreshold && nextThreshold.event.created_at > oldest.event.created_at) {
                // one of the stored events is the actual next threshold
                firstPhaseResults = firstPhaseResults.slice(0, -1)
                oldest = null
                results[q].push(ievt)
                sndPhaseHasResultsPending = true
              } else if (nextThreshold && nextThreshold.event.created_at < ievt.event.created_at) {
                // the next last event is the next threshold
                firstPhaseResults = firstPhaseResults.slice(0, -1)
                results[q]!.push(ievt)
                sndPhaseHasResultsPending = true
                if (oldest === null || ievt.event.created_at < oldest?.event.created_at) {
                  oldest = ievt
                }
              } else {
                // we are the next threshold
                firstPhaseResults[firstPhaseResults.length - 1] = ievt
              }
            }
          } else {
            results[q]!.push(ievt)
            firstPhaseTotalPulled++

            // update oldest event
            if (oldest === null || ievt.event.created_at < oldest.event.created_at) {
              oldest = ievt
            }
          }
        }

        if (pulledPerQuery[q]! >= limit) {
          exhaust(q)
          continue
        }

        if (!hasMore) {
          exhaust(q)
          continue
        }
      }

      // second phase aggregation
      if (sndPhase && sndPhaseHasResultsPending && (oldest === null || remainingUnexhausted === 0)) {
        secondBatch = []
        for (let s = 0; s < sndPhaseParticipants.length; s++) {
          const q = sndPhaseParticipants[s]!

          if (results[q]!.length > 0) {
            secondBatch.push(results[q]!)
          }

          if (exhausted[q]) {
            sndPhaseParticipants = swapDelete(sndPhaseParticipants, s)
            s--
          }
        }

        // alternate between A and B result arrays
        if (sndPhaseResultsToggle) {
          secondBatch.push(sndPhaseResultsB)
          sndPhaseResultsA = mergeSortMultiple(secondBatch, limit)
          oldest = sndPhaseResultsA[sndPhaseResultsA.length - 1]!
        } else {
          secondBatch.push(sndPhaseResultsA)
          sndPhaseResultsB = mergeSortMultiple(secondBatch, limit)
          oldest = sndPhaseResultsB[sndPhaseResultsB.length - 1]!
        }
        sndPhaseResultsToggle = !sndPhaseResultsToggle

        // reset results arrays
        for (const q of sndPhaseParticipants) {
          results[q] = []
        }
        sndPhaseHasResultsPending = false
      } else if (!sndPhase && firstPhaseTotalPulled >= limit && remainingUnexhausted > 0) {
        // transition to second phase
        oldest = null

        // combine and sort first phase results
        const allResults = [...results]
        firstPhaseResults = mergeSortMultiple(allResults, limit)
        oldest = firstPhaseResults[limit - 1]!

        // exhaust iterators that have passed the cutting point
        for (let q = 0; q < queries.length; q++) {
          if (exhausted[q]) {
            continue
          }

          if (
            results[q]!.length > 0 &&
            results[q]![results[q]!.length - 1].event.created_at < oldest.event.created_at
          ) {
            exhausted[q] = true
            remainingUnexhausted--
            continue
          }

          // Clear results and add to second phase participants
          results[q] = []
          sndPhaseParticipants.push(q)
        }

        // Initialize second phase result arrays
        sndPhaseResultsA = []
        sndPhaseResultsB = []
        sndPhase = true
      }

      if (remainingUnexhausted === 0) {
        break
      }
    }

    // final result combination
    let combinedResults: IterEvent[]

    if (sndPhase) {
      // combine first phase and second phase results
      const sndPhaseResults = sndPhaseResultsToggle ? sndPhaseResultsB : sndPhaseResultsA
      const allResults = [firstPhaseResults, sndPhaseResults]
      combinedResults = mergeSortMultiple(allResults, limit)
    } else {
      combinedResults = mergeSortMultiple(results, limit)
    }

    return combinedResults
  }

  private async executeQueryBatch(
    indexStore: IDBObjectStore,
    eventStore: IDBObjectStore,
    queryIndex: number,
    query: Query,
    extraTagFilter: [string, string[]][],
    batchSize: number,
    since: undefined | number,
  ): Promise<[hasMore: boolean, iterEvents: IterEvent[]]> {
    const results: IterEvent[] = []

    return new Promise(resolve => {
      const range = IDBKeyRange.bound(query.startingPoint.buffer, query.endingPoint.buffer, true, true)
      let skipUntilLastFetched = !!query.lastFetched

      const keysReq = indexStore.getAllKeys(range, batchSize)
      keysReq.onsuccess = async () => {
        const eventPromises: Promise<[serial: number, event: NostrEvent] | null>[] = []
        for (let key of keysReq.result) {
          let indexKey = key as ArrayBuffer
          // extract idx from index key
          const idx = new Uint8Array(indexKey.slice(indexKey.byteLength - 4))
          // get the actual event
          const serial = idx[3] | (idx[2] << 8) | (idx[1] << 16) | (idx[0] << 24)

          // if we have previously fetched anything we can't emit the same events again
          // so skip until we get our last fetched event idx
          if (skipUntilLastFetched) {
            if (serial === query.lastFetched) {
              skipUntilLastFetched = false
              // won't skip anymore now
            }
            continue
          }

          eventPromises.push(
            new Promise<[number, NostrEvent] | null>(resolve => {
              const getEventRequest = eventStore.get(serial)
              getEventRequest.onsuccess = () => {
                const eventData = getEventRequest.result
                if (eventData) {
                  const event: NostrEvent = JSON.parse(eventData)

                  // apply extra filtering
                  if (!filterMatchesTags(extraTagFilter, event)) {
                    resolve(null)
                    return
                  }

                  resolve([serial, event])
                }
              }

              getEventRequest.onerror = () => {
                console.error(`failed to get event: ${getEventRequest.error?.message}`)
                resolve(null)
              }
            }),
          )
        }

        for (let res of await Promise.all(eventPromises)) {
          if (!res) continue
          results.push({
            event: res[1],
            q: queryIndex,
          })
          query.lastFetched = res[0]
        }

        let hasMore = false
        // update startingPoint if we are going to do this query again
        if (results.length === batchSize) {
          const last = results[results.length - 1]
          if (!since || last.event.created_at !== since) {
            const timestampStartingPoint = invertedTimestampBytes(last.event.created_at)
            query.startingPoint.set(timestampStartingPoint, query.startingPoint.length - 4 - 4)
            hasMore = true
          }
        }

        resolve([hasMore, results])
      }
    })
  }
}

function getTagIndexPrefix(tagValue: string): [Uint8Array, number] {
  let key: Uint8Array
  let offset: number

  try {
    // assume it's an addressable format, if it isn't we will error
    const { kind, pk, d } = getAddrTagElements(tagValue)
    // store value in the new special "a" tag index
    key = new Uint8Array(1 + 2 + 8 + d.length + 4 + 4)
    key[0] = INDEX_TAG_ADDR_PREFIX

    // write kind as big-endian uint16
    key[1] = (kind >> 8) & 0xff
    key[2] = kind & 0xff

    // copy first 8 bytes of pubkey
    putHexAsBytes(key, 1 + 2, pk, 8)

    // copy d tag value
    const encoder = new TextEncoder()
    const dBytes = encoder.encode(d)
    key.set(dBytes, 1 + 2 + 8)

    offset = 1 + 2 + 8 + d.length
    return [key, offset]
  } catch {
    try {
      // store value as bytes (if it's not valid hex it will error)
      key = new Uint8Array(1 + 8 + 4 + 4)
      key[0] = INDEX_TAG32_PREFIX
      putHexAsBytes(key, 1, tagValue, 8)
      offset = 1 + 8
      return [key, offset]
    } catch {
      // store whatever as utf-8
      const encoder = new TextEncoder()
      const valueBytes = encoder.encode(tagValue)
      key = new Uint8Array(1 + valueBytes.length + 4 + 4)
      key[0] = INDEX_TAG_PREFIX
      key.set(valueBytes, 1)
      offset = 1 + valueBytes.length
      return [key, offset]
    }
  }
}

function* getIndexKeysForEvent(event: NostrEvent, serialOrIdx: number | Uint8Array): Generator<Uint8Array> {
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
    if (tag.length < 2 || tag[0]!.length !== 1 || !tag[1] || tag[1].length === 0 || tag[1].length > 100) {
      continue
    }
    if (seenTagValues.has(tag[1])) {
      continue // skip duplicates
    }
    seenTagValues.add(tag[1])

    const [key, offset] = getTagIndexPrefix(tag[1])
    key.set(tsBytes, offset)
    key.set(idx, offset + 4)

    yield key
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

function prepareQueries(filter: Filter): {
  queries: Query[]
  extraTagFilter: [string, string[]][]
} {
  const queries: Query[] = []
  const extraTagFilter: [string, string[]][] = []
  const timestampStartingPoint = invertedTimestampBytes(filter.until || 0xffffffff)
  const timestampEndingPoint = invertedTimestampBytes(filter.since || 0)

  if (filter.authors && filter.authors.length > 0) {
    // handle combined author + kind filter
    if (filter.kinds && filter.kinds.length > 0) {
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
          })
        }
      }

      for (let tagName in filter) {
        if (tagName[0] === '#' && tagName.length === 2) {
          extraTagFilter.push([tagName[1], filter[tagName]])
        }
      }

      return { queries, extraTagFilter }
    }

    // handle just author filter
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
      })
    }

    for (let tagName in filter) {
      if (tagName[0] === '#' && tagName.length === 2) {
        extraTagFilter.push([tagName[1], filter[tagName]])
      }
    }

    return { queries, extraTagFilter }
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
      })
    }

    for (let tagName in filter) {
      if (tagName[0] === '#' && tagName.length === 2) {
        extraTagFilter.push([tagName[1], filter[tagName]])
      }
    }

    return { queries, extraTagFilter }
  }

  // handle tag filters
  let haveFirstTag = false
  for (let tagName in filter) {
    if (tagName[0] !== '#' || tagName.length !== 2) continue

    if (!haveFirstTag) {
      // naïvely, the first tag we find will be the index
      for (const value of filter[tagName]) {
        const [startingPoint, offset] = getTagIndexPrefix(value)
        startingPoint.set(timestampStartingPoint, offset)
        startingPoint.fill(0x00, offset + 4)

        const endingPoint = startingPoint.slice()
        endingPoint.set(timestampEndingPoint, offset)
        endingPoint.fill(0xff, offset + 4)

        queries.push({
          startingPoint,
          endingPoint,
        })
      }
      haveFirstTag = true
    } else {
      // after that we'll just use them as filters
      extraTagFilter.push([tagName[1], filter[tagName]])
    }
  }

  if (queries.length) {
    // (this means we had tags in the query so we can exit now with the results we just gathered)
    return { queries, extraTagFilter }
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
  })

  return { queries, extraTagFilter }
}

function batchSizePerNumberOfQueries(totalFilterLimit: number, numberOfQueries: number): number {
  if (numberOfQueries === 1 || totalFilterLimit * numberOfQueries < 50) {
    return totalFilterLimit
  }

  return Math.ceil(Math.pow(totalFilterLimit, 0.8) / Math.pow(numberOfQueries, 0.71))
}

function swapDelete<A>(arr: A[], i: number): A[] {
  arr[i] = arr[arr.length - 1]!
  return arr.slice(0, -1)
}

function compareIterEvent(a: IterEvent, b: IterEvent): number {
  if (a.event.id === '') {
    if (b.event.id === '') {
      return 0
    } else {
      return -1
    }
  } else if (b.event.id === '') {
    return 1
  }

  if (a.event.created_at === b.event.created_at) {
    return a.event.id.localeCompare(b.event.id)
  }
  return a.event.created_at - b.event.created_at
}

function mergeSortMultiple(batches: IterEvent[][], limit: number): IterEvent[] {
  // Clear empty batches
  const nonEmptyBatches = batches.filter(batch => batch.length > 0)

  if (nonEmptyBatches.length === 0) {
    return []
  }

  if (nonEmptyBatches.length === 1) {
    return nonEmptyBatches[0]!.slice(0, limit)
  }

  // Simple merge sort implementation
  let result: IterEvent[] = []
  const indices = new Array(nonEmptyBatches.length).fill(0)

  while (result.length < limit) {
    let minIndex = -1
    let minEvent: IterEvent | null = null

    // Find the minimum event across all batches
    for (let i = 0; i < nonEmptyBatches.length; i++) {
      if (indices[i]! < nonEmptyBatches[i]!.length) {
        const event = nonEmptyBatches[i]![indices[i]!]
        if (minEvent === null || compareIterEvent(event, minEvent) > 0) {
          // reverse order for newest first
          minEvent = event
          minIndex = i
        }
      }
    }

    if (minIndex === -1) {
      break // All batches exhausted
    }

    result.push(minEvent!)
    indices[minIndex]!++
  }

  return result
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
