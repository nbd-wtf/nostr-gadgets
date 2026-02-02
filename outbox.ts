import { getSemaphore } from '@henrygd/semaphore'
import { SimplePool } from '@nostr/tools/pool'
import { Filter } from '@nostr/tools/filter'
import { NostrEvent } from '@nostr/tools/core'
import { EventDeletion } from '@nostr/tools/kinds'

import { loadRelayList } from './lists.ts'
import { RedEventStore } from './redstore/index.ts'
import { shuffle } from './utils.ts'
import { BIG_RELAYS_DO_NOT_USE_EVER } from './defaults.ts'
import { purgatory } from './global.ts'

/**
 * OutboxManager handles the pool, store, and bounds for outbox feeds.
 * Use it to create OutboxFeed instances.
 */
export class OutboxManager {
  readonly store: RedEventStore
  readonly baseFilters: Filter[]
  private bounds: { [pubkey: string]: [oldest: number, newest: number] }
  private boundsPromise: null | Promise<{ [pubkey: string]: [number, number] }>
  private pool: SimplePool
  private label: string

  private currentlySyncing: Map<string, () => void>
  private permanentlyLive: Set<string>
  public liveSubscriptions: { url: string; filter: Filter }[]
  private nuclearAbort: AbortController
  private defaultRelaysForConfusedPeople = BIG_RELAYS_DO_NOT_USE_EVER
  private storeRelaysSeenOn: boolean

  private onliveupdate: undefined | ((event: NostrEvent) => void)
  private onsyncupdate: undefined | ((pubkey: string) => void)
  private onbeforeupdate: undefined | ((pubkey: string) => void)
  private ondeletions: undefined | ((ids: string[]) => void)

  constructor(
    baseFilters: Filter[],
    store: RedEventStore,
    opts: {
      pool?: SimplePool
      label?: string
      onliveupdate?: (event: NostrEvent) => void
      onsyncupdate?: (pubkey: string) => void
      onbeforeupdate?: (pubkey: string) => void
      ondeletions?: (ids: string[]) => void
      defaultRelaysForConfusedPeople?: string[]
      storeRelaysSeenOn?: boolean
    },
  ) {
    this.baseFilters = baseFilters
    this.store = store
    this.bounds = {}
    this.boundsPromise = this.getBounds()
    this.pool = opts?.pool || this.pool
    this.label = opts?.label || 'outbox'
    this.onliveupdate = opts?.onliveupdate
    this.onsyncupdate = opts?.onsyncupdate
    this.onbeforeupdate = opts?.onbeforeupdate
    this.ondeletions = opts?.ondeletions
    this.defaultRelaysForConfusedPeople = opts?.defaultRelaysForConfusedPeople || this.defaultRelaysForConfusedPeople
    this.storeRelaysSeenOn = opts?.storeRelaysSeenOn || false
    this.setup()
  }

  setup() {
    this.nuclearAbort = new AbortController()
    this.liveSubscriptions = []
    this.currentlySyncing = new Map()
    this.permanentlyLive = new Set()
  }

  close() {
    this.nuclearAbort.abort('<OutboxManager closed>')
    this.setup()
  }

  private async ensureBoundsLoaded() {
    if (this.boundsPromise) {
      this.bounds = await this.boundsPromise
      this.boundsPromise = null
    }
  }

  private markSyncing(authors: string[]) {
    for (let i = 0; i < authors.length; i++) {
      this.currentlySyncing.set(authors[i], () => {})
    }
  }

  /**
   * Marks a specific public key as not syncing anymore and execute any callbacks that
   * may have registered for that.
   */
  private finishSyncing(pubkey: string) {
    const fn = this.currentlySyncing.get(pubkey)
    this.currentlySyncing.delete(pubkey)
    fn?.()
  }

  /**
   * Returns a promise that is resolved when this pubkey has finished syncing entirely.
   */
  private async waitForSyncingToFinish(pubkey: string): Promise<void> {
    const prev = this.currentlySyncing.get(pubkey)
    if (prev) {
      await new Promise<void>(resolve => {
        // register a new callback here to resolve our promise
        // (this will be called after the item is removed from currentlySyncing)
        this.currentlySyncing.set(pubkey, () => {
          prev()
          resolve()
        })
      })

      // now we check again because someone else may have been waiting too and they
      // may have put this key in a syncing state again
      return this.waitForSyncingToFinish(pubkey)
    }
  }

  /**
   * Returns if a public key is synced up to at least 2 hours ago, which means it
   * can be dealt with by just calling .live().
   */
  async isSynced(pubkey: string): Promise<boolean> {
    await this.ensureBoundsLoaded()
    const bound = this.bounds[pubkey]
    const newest = bound ? bound[1] : undefined
    const now = Math.round(Date.now() / 1000)
    return Boolean(newest && newest > now - 60 * 60 * 2) // 2 hours
  }

  /**
   * Returns true if new notes were discovered during the sync.
   */
  async sync(
    authors: string[],
    opts: {
      signal: AbortSignal
    },
  ): Promise<boolean> {
    await this.ensureBoundsLoaded()

    for (let i = authors.length - 1; i >= 0; i--) {
      if (this.currentlySyncing.has(authors[i])) {
        // swap-delete
        authors[i] = authors[authors.length - 1]
        authors.length = authors.length - 1
      }
    }

    if (authors.length === 0) return false

    this.markSyncing(authors)

    // this prevents the sync process from always starting at the same point
    // which can be bad if we're restarting it all the time (closing and reopening the page)
    shuffle(authors)

    // sync up each of the pubkeys to present
    console.debug('starting sync', authors)
    let addedNewEventsOnSync = false
    const now = Math.round(Date.now() / 1000)
    const promises: Promise<void>[] = []
    for (let i = 0; i < authors.length; i++) {
      if (this.nuclearAbort.signal.aborted || opts.signal.aborted) break

      let pubkey = authors[i]
      let bound = this.bounds[pubkey]
      let newest = bound ? bound[1] : undefined

      if (newest && newest > now - 60 * 60 * 2) {
        // if this person was caught up to 2 hours ago there is no need to repeat this
        // (we'll make up for these missing events in the ongoing live subscription)
        console.debug(
          `${i + 1}/${authors.length} skip`,
          pubkey,
          'synced up to',
          new Date(newest * 1000).toLocaleString(),
          'already',
        )
        this.finishSyncing(pubkey)
        continue
      }

      console.debug(`${i + 1}/${authors.length} syncing`, pubkey)

      // do it only 16 filters at a time because of relay limits
      const sem = getSemaphore('outbox-sync', 16 / this.baseFilters.length)
      promises.push(
        sem.acquire().then(async () => {
          if (this.nuclearAbort.signal.aborted || opts.signal.aborted) {
            this.finishSyncing(pubkey)
            sem.release()
            return
          }

          let relays = (await loadRelayList(pubkey)).items
            .filter(r => r.write && purgatory.allowConnectingToRelay(r.url, ['read', this.baseFilters]))
            .slice(0, 4)
            .map(r => r.url)

          if (relays.length === 0) {
            // someone made a mistake, let's use big relays for them
            relays = this.defaultRelaysForConfusedPeople
          }

          if (this.nuclearAbort.signal.aborted || opts.signal.aborted) {
            this.finishSyncing(pubkey)
            sem.release()
            return
          }

          let events: NostrEvent[]
          try {
            events = (
              await Promise.race([
                new Promise<NostrEvent[]>((_, reject) => setTimeout(() => reject(new Error('<timeout>')), 45000)),
                Promise.all(
                  this.baseFilters.map(f =>
                    this.pool.querySync(
                      relays,
                      { ...f, authors: [pubkey], since: newest, limit: 200 },
                      { label: `sync-${pubkey.substring(0, 6)}`, maxWait: 4000 },
                    ),
                  ),
                ),
              ])
            ).flat()
          } catch (err) {
            console.warn('failed to query events for', pubkey, 'at', relays, '=>', err)
            this.finishSyncing(pubkey)
            sem.release()
            return
          }

          if (this.nuclearAbort.signal.aborted || opts.signal.aborted) {
            this.finishSyncing(pubkey)
            sem.release()
            return
          }

          console.debug(
            `${i + 1}/${authors.length} events downloaded`,
            pubkey,
            relays,
            'newest:',
            newest ? new Date(newest * 1000).toLocaleString() : newest,
            `got ${events.length} events`,
            events,
          )

          if (events.length) {
            // if we didn't get any events we won't have any new events necessarily
            // we also will not update bounds (since this was likely an error)
            let added = await Promise.all(
              events.map(async event => {
                const deletion = event.kind === EventDeletion

                const isNew = await this.store.saveEvent(event, {
                  seenOn: this.storeRelaysSeenOn
                    ? Array.from(this.pool.seenOn.get(event.id) || []).map(relay => relay.url)
                    : undefined,
                })

                if (isNew && deletion) {
                  this.performDeletions(event)
                }

                return isNew
              }),
            )

            if (!addedNewEventsOnSync) {
              addedNewEventsOnSync = added.indexOf(true) !== -1
            }

            // update stored bound bounds for this person since they're caught up to now
            if (bound) {
              bound[1] = now
            } else {
              // didn't have anything before, but now we have all of these
              bound = [events[events.length - 1].created_at, now]
            }
            this.bounds[pubkey] = bound
            await this.setBound(pubkey, bound)
          }

          this.finishSyncing(pubkey)
          this.onsyncupdate?.(pubkey)

          sem.release()
        }),
      )
    }

    await Promise.all(promises)
    console.debug('sync done')
    return addedNewEventsOnSync
  }

  async live(
    authors: string[],
    opts: {
      // this should only be undefined if you want the live() subscription to last forever
      signal: AbortSignal | undefined
    },
  ) {
    await this.ensureBoundsLoaded()

    // do not subscribe live for those that are already subscribed permanently
    for (let i = 0; i < authors.length; i++) {
      const author = authors[i]

      if (this.permanentlyLive.has(author)) {
        // swap-delete
        authors[i] = authors[authors.length - 1]
        authors.length = authors.length - 1
        i--
      } else if (opts.signal === undefined) {
        // mark others as permanently syncing
        this.permanentlyLive.add(author)
      }
    }

    if (authors.length === 0) return

    // wait for these authors to finish syncing
    await Promise.all(authors.map(author => this.waitForSyncingToFinish(author)))
    console.debug('listening live', authors)

    const declaration = await outboxFilterRelayBatch(
      authors,
      ...this.baseFilters.map(f => ({
        ...f,
        since: Math.round(Date.now() / 1000) - 60 * 60 * 2, // since 2 hours ago
      })),
    )

    this.liveSubscriptions.push(...declaration)

    const closer = this.pool.subscribeMap(declaration, {
      label: `live-${this.label}`,
      onevent: async event => {
        const deletion = event.kind === EventDeletion

        const isNew = await this.store.saveEvent(event, {
          seenOn: this.storeRelaysSeenOn
            ? Array.from(this.pool.seenOn.get(event.id) || []).map(relay => relay.url)
            : undefined,
        })

        if (isNew && deletion) {
          this.performDeletions(event)
        }

        if (isNew) {
          const now = Math.round(Date.now() / 1000)

          if (event.pubkey in this.bounds) this.bounds[event.pubkey][1] = now
          else this.bounds[event.pubkey] = [now - 1, now]

          await this.setBound(event.pubkey, this.bounds[event.pubkey])
          this.onliveupdate?.(event)
        }
      },
    })

    if (opts.signal) {
      opts.signal.onabort = () => {
        closer.close()
        this.liveSubscriptions = this.liveSubscriptions.filter(sub => declaration.includes(sub))
      }
    }
    this.nuclearAbort.signal.onabort = () => {
      closer.close()
    }
  }

  async before(
    authors: string[],
    ts: number,
    opts: {
      signal: AbortSignal
    },
  ) {
    await this.ensureBoundsLoaded()

    // wait for these authors to finish syncing
    await Promise.all(authors.map(author => this.waitForSyncingToFinish(author)))

    this.markSyncing(authors)

    // (same as sync(), but not as important)
    shuffle(authors)

    // from all our authors check which ones need a new page fetch
    for (let i = 0; i < authors.length; i++) {
      if (this.nuclearAbort.signal.aborted || opts.signal.aborted) break
      let pubkey = authors[i]

      const sem = getSemaphore('outbox-sync', 15) // do it only 15 pubkeys at a time
      await sem.acquire().then(async () => {
        if (this.nuclearAbort.signal.aborted || opts.signal.aborted) {
          sem.release()
          return
        }

        let bound = this.bounds[pubkey]
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
          .filter(r => r.write && purgatory.allowConnectingToRelay(r.url, ['read', this.baseFilters]))
          .slice(0, 4)
          .map(r => r.url)

        if (this.nuclearAbort.signal.aborted || opts.signal.aborted) {
          sem.release()
          return
        }

        let events: NostrEvent[]
        try {
          events = (
            await Promise.race([
              new Promise<NostrEvent[]>((_, rej) => setTimeout(rej, 5000)),
              Promise.all(
                this.baseFilters.map(f =>
                  this.pool.querySync(
                    relays,
                    { ...f, authors: [pubkey], until: oldest, limit: 200 },
                    { label: `page-${pubkey.substring(0, 6)}` },
                  ),
                ),
              ),
            ])
          ).flat()
        } catch (err) {
          console.warn('failed to query before events for', pubkey, 'at', relays, '=>', err)
          this.finishSyncing(pubkey)
          sem.release()
          return
        }

        console.debug('paginating to the past', pubkey, relays, oldest, events)
        await Promise.all(
          events.map(async event => {
            const deletion = event.kind === EventDeletion

            const isNew = await this.store.saveEvent(event, {
              seenOn: this.storeRelaysSeenOn
                ? Array.from(this.pool.seenOn.get(event.id) || []).map(relay => relay.url)
                : undefined,
            })

            if (isNew && deletion) {
              this.performDeletions(event)
            }

            return isNew
          }),
        )

        // update oldest bound
        if (events.length) {
          // didn't have anything before, but now we have all of these
          bound[0] = events[events.length - 1].created_at
        }
        this.bounds[pubkey] = bound
        await this.setBound(pubkey, bound)
        this.finishSyncing(pubkey)
        this.onbeforeupdate?.(pubkey)

        sem.release()
      })
    }

    console.debug('before done')
  }

  /**
   * retrieves bounds from the syncing store.
   */
  async getBounds(): Promise<{ [pubkey: string]: [number, number] }> {
    return this.store.getOutboxBounds()
  }

  /**
   * saves a single bound to the syncing store.
   */
  async setBound(pubkey: string, bound: [number, number]): Promise<void> {
    console.debug(
      'new bound for',
      pubkey,
      bound.map(d => new Date(d * 1000).toLocaleString()),
    )
    return this.store.setOutboxBound(pubkey, bound)
  }

  private async performDeletions(event: NostrEvent) {
    // event is assumed to be a kind:5
    const filters: Filter[] = []
    for (let t = 0; t < event.tags.length; t++) {
      const tag = event.tags[t]
      switch (tag[0]) {
        case 'e': {
          const filter = { ids: [tag[1]] }
          filters.push(filter)
          break
        }
        case 'a': {
          const spl = tag[1].split(':')
          if (spl.length < 3) continue
          const filter = {
            kinds: [parseInt(spl[0])],
            authors: [spl[1]],
            '#d': [spl.slice(2).join(':')],
          }
          filters.push(filter)
          break
        }
      }
    }

    const ids = await this.store.deleteEventsFilters(filters)
    this.ondeletions?.(ids)
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
  const relayCounts: { [url: string]: Count } = {}
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
            const url = rl.items[i].url
            if (!purgatory.allowConnectingToRelay(url, ['read', baseFilters])) continue
            const count = relayCounts[url] || { count: 0 }
            relayCounts[url] = count
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
