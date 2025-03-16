/**
 * @module
 * Contains functions for optimized fetching of addressable lists associated with a pubkey.
 */

import DataLoader from 'dataloader'
import type { NostrEvent } from '@nostr/tools/core'
import type { Filter } from '@nostr/tools/filter'
import type { SubCloser } from '@nostr/tools/abstract-pool'
import { createStore, getMany, setMany } from 'idb-keyval'

import { pool } from './global'
import { dataloaderCache } from './utils'
import { itemsFromTags, loadRelayList } from './lists'

/**
 * A SetFetcher is a function that can be called to return a map of items indexed by d-tag,
 * where each value contains the event and parsed items.
 */
export type SetFetcher<I> = (pubkey: string, hints?: string[]) => Promise<Result<I>>
type Result<I> = {
  [dTag: string]: {
    event: NostrEvent
    items: I[]
  }
}

/**
 * A SetFetcher for kind:30000 follow sets.
 */
export const loadFollowsList: SetFetcher<string> = makeSetFetcher<string>(
  30000,
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'p') {
      return tag[1]
    }
  }),
)

/**
 * A SetFetcher for kind:30002 relay sets.
 */
export const loadRelaySets: SetFetcher<string> = makeSetFetcher<string>(
  30002,
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'relay') {
      return tag[1]
    }
  }),
)

/**
 * makeSetFetcher is similar to makeListFetcher but handles multiple events per pubkey,
 * differentiated by their "d" tag values.
 */
export function makeSetFetcher<I>(kind: number, process: (event: NostrEvent) => I[]): SetFetcher<I> {
  const cache = dataloaderCache<Result<I>>()
  const store = createStore(`@nostr/gadgets/set:${kind}`, 'cache')

  type Request = { target: string; relays: string[] }

  const dataloader = new DataLoader<Request, Result<I>, string>(
    requests =>
      new Promise(async resolve => {
        let remainingRequests: (Request & { index: number })[] = []
        let now = Math.round(Date.now() / 1000)

        // try to get from idb first -- also set up the results array with defaults
        let results: Result<I>[] = await getMany<Result<I> & { lastAttempt: number }>(
          requests.map(r => r.target),
          store,
        ).then(results =>
          results.map<Result<I>>((res, i) => {
            const req = requests[i] as Request & { index: number }
            req.index = i

            if (!res) {
              remainingRequests.push(req)
              // we don't have anything for this key, fill in with empty object
              return {}
            } else if (!res.lastAttempt || res.lastAttempt < now - 60 * 60 * 24 * 2) {
              remainingRequests.push(req)
              // we have something but it's old (2 days), so we will use it but still try to fetch new versions
              res.lastAttempt = now
              return res
            } else {
              // this one is so good we won't try to fetch it again
              return res
            }
          }),
        )

        if (remainingRequests.length === 0) {
          resolve(results)
          return
        }

        const filtersByRelay: { [url: string]: Filter[] } = {}
        for (let r = 0; r < remainingRequests.length; r++) {
          const req = remainingRequests[r]
          const relays = req.relays.slice(0, Math.min(4, req.relays.length))
          for (let j = 0; j < relays.length; j++) {
            const url = relays[j]
            let filters = filtersByRelay[url]
            if (!filters) {
              filters = [{ kinds: [kind], authors: [] }]
              filtersByRelay[url] = filters
            }
            filters[0]?.authors?.push(req.target)
          }
        }

        try {
          let handle: SubCloser | undefined
          handle = pool.subscribeManyMap(filtersByRelay, {
            label: `kind:${kind}:batch(${remainingRequests.length})`,
            onevent(evt) {
              for (let r = 0; r < remainingRequests.length; r++) {
                const req = remainingRequests[r]
                if (req.target === evt.pubkey) {
                  const dTag = evt.tags.find(t => t[0] === 'd')?.[1] || ''
                  const result = results[req.index]
                  const existing = result[dTag]

                  // only update if this is a newer event for this d tag
                  if (!existing || existing.event.created_at < evt.created_at) {
                    result[dTag] = {
                      event: evt,
                      items: process(evt),
                    }
                  }
                  return
                }
              }
            },
            oneose() {
              handle?.close()
            },
            async onclose() {
              resolve(results)

              // save our updated results to idb
              setMany(
                remainingRequests.map(req => [req.target, results[req.index]]),
                store,
              )
            },
          })
        } catch (err) {
          resolve(results.map(_ => err as Error))
        }
      }),
    {
      cache: true,
      cacheKeyFn: req => req.target,
      cacheMap: cache,
    },
  )

  return async function (pubkey: string, hints: string[] = []): Promise<Result<I>> {
    let relays: string[] = hints

    const rl = await loadRelayList(pubkey, hints)
    relays.push(
      ...rl.items
        .filter(({ write }) => write)
        .map(({ url }) => url)
        .slice(0, 3),
    )
    return await dataloader.load({ target: pubkey, relays })
  }
}
