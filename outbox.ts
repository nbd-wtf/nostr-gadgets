import { getSemaphore } from '@henrygd/semaphore'
import { Filter, NostrEvent, SimplePool } from '@nostr/tools'
import { normalizeURL } from '@nostr/tools/utils'

import { loadRelayList } from './lists.ts'
import { DuplicateEventError, IDBEventStore } from './store.ts'
import { pool } from './global.ts'

export class SyncRaceError extends Error {
  constructor(pubkey: string) {
    super(`this outbox manager is syncing the pubkey ${pubkey} already.`)
    this.name = 'SyncRaceError'
  }
}

/**
 * OutboxManager handles the pool, store, and thresholds for outbox feeds.
 * Use it to create OutboxFeed instances.
 */
export class OutboxManager {
  readonly store: IDBEventStore
  readonly kinds: number[]
  private thresholds: { [pubkey: string]: [oldest: number, newest: number] }
  private thresholdsLocalStorageKey: string
  private pool: SimplePool
  private label: string
  private currentlySyncing = new Set<string>()

  constructor(
    kinds: number[],
    opts?: {
      store?: IDBEventStore
      thresholdsLocalStorageKey?: string
      pool?: SimplePool
      label?: string
    },
  ) {
    this.kinds = kinds
    this.store = opts?.store || new IDBEventStore()
    this.thresholdsLocalStorageKey = opts?.thresholdsLocalStorageKey || 'thresholds'
    this.thresholds = JSON.parse(localStorage.getItem(this.thresholdsLocalStorageKey) || '{}')
    this.pool = opts?.pool || pool
    this.label = opts?.label || 'outbox'
  }

  private saveThresholds() {
    localStorage.setItem(this.thresholdsLocalStorageKey, JSON.stringify(this.thresholds))
  }

  private guardSyncing(authors: string[]) {
    for (let i = 0; i < authors.length; i++) {
      let author = authors[i]
      if (this.currentlySyncing.has(author)) throw new SyncRaceError(author)
    }
  }

  private markSyncing(authors: string[]) {
    for (let i = 0; i < authors.length; i++) {
      this.currentlySyncing.add(authors[i])
    }
  }

  private releaseSyncing(authors: string[]) {
    for (let i = 0; i < authors.length; i++) {
      this.currentlySyncing.delete(authors[i])
    }
  }

  async sync(authors: string[], signal?: AbortSignal) {
    this.guardSyncing(authors)
    this.markSyncing(authors)

    // sync up each of the pubkeys to present
    console.log('starting catch up sync')
    const now = Math.round(Date.now() / 1000)
    const promises: Promise<void>[] = []
    for (let i = 0; i < authors.length; i++) {
      let pubkey = authors[i]
      let bound = this.thresholds[pubkey]
      let newest = bound ? bound[1] : undefined

      if (newest && newest > now - 60 * 60 * 2) {
        // if this person was caught up to 2 hours ago there is no need to repeat this
        // (we'll make up for these missing events in the ongoing live subscription)
        console.log(`${i + 1}/${authors.length} skip`, newest, '>', now - 60 * 60 * 2)
        continue
      }

      const sem = getSemaphore('outbox-sync', 15) // do it only 15 pubkeys at a time because of relay limits
      promises.push(
        sem.acquire().then(async () => {
          if (signal?.aborted) {
            sem.release()
            this.releaseSyncing(authors)
            return
          }

          let relays = (await loadRelayList(pubkey)).items
            .filter(r => r.write)
            .slice(0, 4)
            .map(r => r.url)

          if (signal?.aborted) {
            sem.release()
            this.releaseSyncing(authors)
            return
          }

          let events: NostrEvent[]
          try {
            events = await Promise.race([
              this.pool.querySync(
                relays,
                { kinds: this.kinds, authors: [pubkey], since: newest, limit: 200 },
                { label: `catchup-${pubkey.substring(0, 6)}` },
              ),
              new Promise<NostrEvent[]>((_, rej) => setTimeout(rej, 5000)),
            ])
          } catch (err) {
            console.warn('failed to query events for', pubkey, 'at', relays)
            events = []
          }

          if (signal?.aborted) {
            sem.release()
            this.releaseSyncing(authors)
            return
          }

          console.debug(
            `${i + 1}/${authors.length} catching up with`,
            pubkey,
            relays,
            { kinds: this.kinds, authors: [pubkey], since: newest },
            `got ${events.length} events`,
            events,
          )

          for (let event of events) {
            try {
              await this.store.saveEvent(event)
            } catch (err) {
              if (err instanceof DuplicateEventError) {
                console.warn('tried to save duplicate:', event)
              } else {
                throw err
              }
            }
          }

          // update stored bound thresholds for this person since they're caught up to now
          if (bound) {
            bound[1] = now
          } else if (events.length) {
            // didn't have anything before, but now we have all of these
            bound = [events[events.length - 1].created_at, now]
          } else {
            // no bound, no events
            bound = [now - 1, now]
          }
          console.debug('new bound for', pubkey, bound)
          this.thresholds[pubkey] = bound

          sem.release()
        }),
      )
    }

    await Promise.all(promises)

    // now we've caught up with the current moment for everybody
    this.saveThresholds()
    console.debug(`all caught up`)
    this.releaseSyncing(authors)
  }

  async live(authors: string[], onUpdate: () => void, signal: AbortSignal) {
    this.guardSyncing(authors)

    const declaration = await outboxFilterRelayBatch(authors, {
      kinds: this.kinds,
      since: Math.round(Date.now() / 1000) - 60 * 60 * 2, // since 2 hours ago
    })

    const closer = this.pool.subscribeMap(declaration, {
      label: `live-${this.label}`,
      async onevent(event) {
        try {
          await this.store.saveEvent(event)
          this.thresholds[event.pubkey][1] = Math.round(Date.now() / 1000)
          onUpdate()
        } catch (err) {
          if (err instanceof DuplicateEventError) {
            console.warn('tried to save duplicate from ongoing:', event)
          } else {
            throw err
          }
        }
      },
    })

    signal.onabort = () => {
      closer.close()
    }
  }

  async before(authors: string[], ts: number, signal?: AbortSignal) {
    this.guardSyncing(authors)
    this.markSyncing(authors)

    // from all our authors check which ones need a new page fetch
    for (let pubkey of authors) {
      const sem = getSemaphore('outbox-sync', 15) // do it only 15 pubkeys at a time
      await sem.acquire().then(async () => {
        if (signal?.aborted) {
          sem.release()
          return
        }

        let bound = this.thresholds[pubkey]
        if (!bound) {
          // this should never happen because we set the bounds for everybody
          // (on the first fetch if they don't have one)
          console.error('pagination on pubkey without a bound', pubkey)
          sem.release()
          return
        }

        let oldest = bound ? bound[0] : undefined

        // if we already have events for this person that are older don't try to fetch anything
        if (oldest && oldest < ts) {
          sem.release()
          return
        }

        let relays = (await loadRelayList(pubkey)).items
          .filter(r => r.write)
          .slice(0, 4)
          .map(r => r.url)

        if (signal?.aborted) {
          sem.release()
          return
        }

        const events = await this.pool.querySync(
          relays,
          { kinds: this.kinds, authors: [pubkey], until: oldest, limit: 200 },
          { label: `page-${pubkey.substring(0, 6)}` },
        )

        console.debug(
          'paginating to the past',
          pubkey,
          relays,
          { kinds: this.kinds, authors: [pubkey], until: oldest },
          events,
        )

        for (let event of events) {
          try {
            await this.store.saveEvent(event)
          } catch (err) {
            if (err instanceof DuplicateEventError) {
              console.warn('tried to save duplicate:', event)
            } else {
              throw err
            }
          }
        }

        // update oldest bound threshold
        if (events.length) {
          // didn't have anything before, but now we have all of these
          bound[0] = events[events.length - 1].created_at
        }
        console.debug('updated bound for', pubkey, bound)
        this.thresholds[pubkey] = bound

        sem.release()
      })
    }

    this.saveThresholds()
    console.debug('paginated back')
    this.releaseSyncing(authors)
  }
}

/**
 * Takes a list of public keys and a filter (assumed to not contain an `authors` field).
 *
 * Returns a list of maps that can be passed to @nostr/tools/pool's `subscribeMap()`.
 *
 * It tries to select 2, 3 or 4 (depending on how many pubkeys you're giving -- more
 * pubkeys means less relays are used) outbox relays from each of these pubkeys, based
 * solely on their kind:10002 relay list, and query those. from all hinted outbox relays
 * it will pick the most popular -- considering the other pubkeys -- so the number of
 * relay connections is minimized.
 */
export async function outboxFilterRelayBatch(
  pubkeys: string[],
  ...baseFilters: Filter[]
): Promise<{ url: string; filter: Filter }[]> {
  const declaration: { url: string; filter: Filter }[] = []

  type Count = { count: number }
  const relaysByCount: { [url: string]: Count } = {}
  const relaysByPubKey: { [pubkey: string]: { [url: string]: Count } } = {}
  const numberOfRelaysPerUser = pubkeys.length < 100 ? 4 : pubkeys.length < 800 ? 3 : pubkeys.length < 1200 ? 2 : 1

  // get the most popular relays among the list of followed people
  await Promise.all(
    pubkeys.map(async pubkey => {
      const rl = await loadRelayList(pubkey)
      relaysByPubKey[pubkey] = {}

      let w = 0
      for (let i = 0; i < rl.items.length; i++) {
        if (rl.items[i].write) {
          try {
            const url = normalizeURL(rl.items[i].url)
            const count = relaysByCount[url] || { count: 0 }
            relaysByCount[url] = count
            relaysByPubKey[pubkey][url] = count
            count.count++
            w++
          } catch (_err) {
            /***/
          }
        }

        if (w >= 7) break
      }
    }),
  )

  // choose from the most popular first for each user
  for (let i = 0; i < pubkeys.length; i++) {
    const pubkey = pubkeys[i]
    const list: [string, number][] = Object.entries(relaysByPubKey[pubkey]).map(([url, count]) => [url, count.count])
    list.sort((a, b) => b[1] - a[1])

    // we'll get a number of relays per user that will be bigger if we're following less people,
    // smaller otherwise
    const top = list.slice(0, numberOfRelaysPerUser)

    for (let r = 0; r < top.length; r++) {
      const url = top[r][0]
      let found = false
      for (let i = 0; i < declaration.length; i++) {
        const decl = declaration[i]
        if (decl.url === url) {
          // if this relay is found that means it already has all the filters
          // so we just add the pubkey to all of them
          found = true
          decl.filter.authors!.push(pubkey)
        }
      }

      // otherwise we add all the filters to this relay
      if (!found) {
        for (let f = 0; f < baseFilters.length; f++) {
          declaration.push({
            url,
            filter: { ...baseFilters[f], authors: [pubkey] },
          })
        }
      }
    }
  }

  return declaration
}
