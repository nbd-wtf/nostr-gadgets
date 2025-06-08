import { normalizeURL } from '@nostr/tools/utils'
import { getSemaphore } from '@henrygd/semaphore'
import { SubCloser } from '@nostr/tools/abstract-pool'
import { Filter, matchFilter, NostrEvent, SimplePool } from '@nostr/tools'

import { loadRelayList } from './lists.ts'
import { DuplicateEventError, IDBEventStore } from './store.ts'
import { pool } from './global.ts'

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

/**
 * OutboxStoredFeed is an opinionated feed manager.
 *
 * It saves stuff to a browser and gets things to sync in the long-term.
 */
export class OutboxStoredFeed {
  readonly store: IDBEventStore
  private thresholds: { [pubkey: string]: [oldest: number, newest: number] }
  private thresholdsLocalStorageKey: string
  readonly allEvents: NostrEvent[] = []
  readonly authors: string[]
  readonly kinds: number[]
  private pool: SimplePool
  private baseFilter: Filter
  private label: string
  private paginator: {
    // number of events shown: it only grows as we paginate back in history or new live events are received
    eventsShown: number

    // called anytime new events are received or we paginate back
    onEvents: (_: NostrEvent[]) => void

    // to prevent stuff from happening out of order
    canPaginate: boolean
  }
  #closed: boolean = false
  #subCloser: SubCloser

  constructor(props: {
    thresholdsLocalStorageKey?: string
    authors: string[]
    kinds: number[]
    pool?: SimplePool
    baseFilter?: Filter
    label?: string
  }) {
    this.store = new IDBEventStore()
    this.thresholdsLocalStorageKey = props.thresholdsLocalStorageKey || 'thresholds'
    this.thresholds = JSON.parse(localStorage.getItem(this.thresholdsLocalStorageKey) || '{}')
    this.authors = props.authors
    this.kinds = props.kinds
    this.pool = props.pool || pool
    this.baseFilter = props.baseFilter || {}
    this.label = props.label || 'outbox'
  }

  close() {
    this.#closed = true
    this.#subCloser?.close?.()
  }

  canPaginate(): boolean {
    return this.paginator.canPaginate
  }

  saveThresholds() {
    localStorage.setItem(this.thresholdsLocalStorageKey, JSON.stringify(this.thresholds))
  }

  async run(initialPageSize: number, onEvents: (evts: NostrEvent[]) => void) {
    this.paginator = {
      eventsShown: initialPageSize,
      onEvents,
      canPaginate: false,
    }

    // display what we have stored immediately
    for await (let evt of this.store.queryEvents({ kinds: this.kinds, authors: this.authors, limit: 100 })) {
      if (this.#closed) return
      this.allEvents.push(evt)
    }

    if (this.#closed) return
    console.debug('stored events:', this.authors, this.allEvents)
    if (this.allEvents.length !== 0) {
      this.flush()
    } else {
      // if there is nothing in the database we do a preliminary query
      // just to show something before we start the "sync" process below
      console.debug('doing preliminary fetch before the full sync process')
      await new Promise(async resolve => {
        let preliminaryEvents: NostrEvent[] = []
        let preliminarySub = this.pool.subscribeMap(
          await outboxFilterRelayBatch(this.authors, {
            kinds: this.kinds,
            limit: initialPageSize,
            ...this.baseFilter,
          }),
          {
            label: `preliminary-${this.label}`,
            async onevent(event) {
              preliminaryEvents.push(event)

              try {
                await this.store.saveEvent(event)
              } catch (err) {
                if (err instanceof DuplicateEventError) {
                  console.warn('tried to save duplicate from ongoing:', event)
                } else {
                  throw err
                }
              }
            },
            oneose() {
              preliminarySub.close()
              if (this.#closed) return
              preliminaryEvents.sort((a, b) => b.created_at - a.created_at)
              this.paginator.onEvents(preliminaryEvents.slice(0, initialPageSize))
            },
            onclose: resolve,
          },
        )
      })
    }

    // sync up each of the pubkeys to present
    console.log('starting catch up sync')
    let addedNewEventsOnSync = false
    const now = Math.round(Date.now() / 1000)
    const promises: Promise<void>[] = []
    for (let i = 0; i < this.authors.length; i++) {
      let pubkey = this.authors[i]
      let bound = this.thresholds[pubkey]
      let newest = bound ? bound[1] : undefined

      if (newest && newest > now - 60 * 60 * 2) {
        // if this person was caught up to 2 hours ago there is no need to repeat this
        // (we'll make up for these missing events in the ongoing live subscription)
        console.log(`${i + 1}/${this.authors.length} skip`, newest, '>', now - 60 * 60 * 2)
        continue
      }

      const sem = getSemaphore('outbox-sync', 15) // do it only 15 pubkeys at a time
      promises.push(
        sem.acquire().then(async () => {
          if (this.#closed) {
            sem.release()
            return
          }

          let relays = (await loadRelayList(pubkey)).items
            .filter(r => r.write)
            .slice(0, 4)
            .map(r => r.url)

          if (this.#closed) {
            sem.release()
            return
          }

          let events: NostrEvent[]
          try {
            events = await Promise.race([
              pool.querySync(
                relays,
                { kinds: [1222, 1244], authors: [pubkey], since: newest, limit: 200 },
                { label: `catchup-${pubkey.substring(0, 6)}` },
              ),
              new Promise<NostrEvent[]>((_, rej) => setTimeout(rej, 5000)),
            ])
          } catch (err) {
            console.warn('failed to query events for', pubkey, 'at', relays)
            events = []
          }

          if (this.#closed) {
            sem.release()
            return
          }

          console.debug(
            `${i + 1}/${this.authors.length} catching up with`,
            pubkey,
            relays,
            { kinds: [1222, 1244], authors: [pubkey], since: newest },
            `got ${events.length} events`,
            events,
          )

          for (let event of events) {
            try {
              await this.store.saveEvent(event)

              // saved, now we know this was a new event, we can add it to our list of events to be displayed
              if (!this.baseFilter || matchFilter(this.baseFilter, event)) {
                this.allEvents.push(event)
                this.paginator.eventsShown++
                addedNewEventsOnSync = true
              }
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
    if (this.#closed) return

    console.debug(`all caught up`)
    this.paginator.canPaginate = true

    if (addedNewEventsOnSync) {
      this.allEvents.sort((a, b) => b.created_at - a.created_at)
      this.flush()
    }

    // finally open this ongoing subscription
    const declaration = await outboxFilterRelayBatch(this.authors, {
      kinds: this.kinds,
      since: now - 60 * 60 * 2,
    })

    this.#subCloser = this.pool.subscribeMap(declaration, {
      label: `feed-${this.label}`,
      onevent: async event => {
        if (!this.baseFilter || matchFilter(this.baseFilter, event)) {
          this.allEvents.unshift(event)
          this.flush()
        }

        try {
          await this.store.saveEvent(event)

          this.thresholds[event.pubkey][1] = Math.round(Date.now() / 1000)
        } catch (err) {
          if (err instanceof DuplicateEventError) {
            console.warn('tried to save duplicate from ongoing:', event)
          } else {
            throw err
          }
        }
      },
    })
  }

  showMore(n: number) {
    const hasNew = this.allEvents.length > this.paginator.eventsShown
    if (hasNew) {
      this.paginator.eventsShown += n
      this.flush()
    } else {
      this.paginator.canPaginate = false

      if (this.allEvents.length) {
        this.maybeFetchBackwardsUntil(this.allEvents[this.allEvents.length - 1].created_at).then(() => {
          if (this.#closed) return

          this.paginator.eventsShown += n
          this.flush()
          this.paginator.canPaginate = true
        })
      }
    }
  }

  flush() {
    this.paginator.onEvents(this.allEvents.slice(0, Math.min(this.allEvents.length, this.paginator.eventsShown)))
  }

  async maybeFetchBackwardsUntil(ts: number) {
    // from all our authors check which ones need a new page fetch
    for (let pubkey of this.authors) {
      const sem = getSemaphore('outbox-sync', 15) // do it only 15 pubkeys at a time
      await sem.acquire().then(async () => {
        if (this.#closed) {
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

        if (this.#closed) {
          sem.release()
          return
        }

        const events = await pool.querySync(
          relays,
          { kinds: [1222, 1244], authors: [pubkey], until: oldest, limit: 200 },
          { label: `page-${pubkey.substring(0, 6)}` },
        )

        console.debug(
          'paginating to the past',
          pubkey,
          relays,
          { kinds: [1222, 1244], authors: [pubkey], until: oldest },
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

    // after having downloaded more stuff from everybody we needed we can grab stuff from our database
    // and put it in memory
    for await (let evt of this.store.queryEvents({
      kinds: this.kinds,
      authors: this.authors,
      until: ts - 1,
      limit: 100,
    })) {
      this.allEvents.push(evt)
    }
  }
}
