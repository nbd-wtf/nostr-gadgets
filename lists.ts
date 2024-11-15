/**
 * @module
 * Contains functions for optimized fetching of replaceable lists associated with a pubkey.
 */

import DataLoader from 'dataloader'
import type { NostrEvent } from '@nostr/tools/core'
import type { Filter } from '@nostr/tools/filter'
import type { SubCloser } from '@nostr/tools/abstract-pool'

import { pool } from './global'

import { METADATA_QUERY_RELAYS, RELAYLIST_RELAYS } from './defaults'
import { dataloaderCache, identity } from './utils'
import { createStore, getMany, setMany } from 'idb-keyval'

let serial = 0
let idserial = 0

/**
 * Representation of a relay entry as found in a kind:10002 list.
 */
export type RelayItem = {
  url: string
  read: boolean
  write: boolean
}

/**
 * Representation of a thing that can be muted in a kind:10000 list.
 */
export type MutedEntity = {
  label: 'pubkey' | 'hashtag' | 'word' | 'thread'
  value: string
}

/**
 * A ListFetcher is a function that can be called to return a list of items, these items are parsed out from
 * the Nostr event according to a function given at the time of creation through makeListFetcher().
 *
 * Relays will be chosen smartly based on the pubkey and context: they will be a combination of relays given
 * at makeListFetcher() (generally "global indexer" relays like purplepag.es) and the target user's kind 10002
 * relays list.
 *
 * Results will be cached in memory, so it's safe to call it infinite times in a row with the same pubkey.
 *
 * It is also safe to call it with multiple different pubkeys, requests to the same relay will be batched together.
 */
export type ListFetcher<I> = (pubkey: string, hints?: string[]) => Promise<Result<I>>
type Result<I> = { event: NostrEvent | null; items: I[] }

/**
 * A ListFetcher for kind:3 follow lists.
 *
 * Returns a list of pubkeys as strings.
 */
export const loadFollowsList: ListFetcher<string> = makeListFetcher<string>(
  3,
  METADATA_QUERY_RELAYS,
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'p') {
      return tag[1]
    }
  }),
  _ => [],
)

/**
 * A ListFetcher for kind:10002 relay lists.
 *
 * Returns a list of RelayItem.
 */
export const loadRelayList: ListFetcher<RelayItem> = makeListFetcher<RelayItem>(
  10002,
  RELAYLIST_RELAYS,
  itemsFromTags<RelayItem>((tag: string[]): RelayItem | undefined => {
    if (tag.length === 2) {
      return { url: tag[1], read: true, write: true }
    } else if (tag[2] === 'read') {
      return { url: tag[1], read: true, write: false }
    } else if (tag[2] === 'write') {
      return { url: tag[1], read: false, write: true }
    }
  }),
  _ => [
    { url: 'wss://relay.damus.io', read: true, write: true },
    { url: 'wss://nos.lol', read: true, write: true },
  ],
)

/**
 * A ListFetcher for kind:10000 mute lists.
 *
 * Returns a list of MutedEntity.
 */
export const LoadMuteList: ListFetcher<MutedEntity> = makeListFetcher<MutedEntity>(
  10000,
  [],
  itemsFromTags<MutedEntity>((tag: string[]): MutedEntity | undefined => {
    if (tag.length >= 2) {
      switch (tag[0]) {
        case 'p':
          return { label: 'pubkey', value: tag[1] }
        case 'e':
          return { label: 'thread', value: tag[1] }
        case 't':
          return { label: 'hashtag', value: tag[1] }
        case 'word':
          return { label: 'word', value: tag[1] }
      }
      return undefined
    }
  }),
  _ => [],
)

function itemsFromTags<I>(tagProcessor: (tag: string[]) => I | undefined): (event: NostrEvent | undefined) => I[] {
  return (event: NostrEvent | undefined) => {
    const items = event ? event.tags.map(tagProcessor).filter(identity) : []
    return items as I[]
  }
}

/**
 * makeListFetcher is the function used to create a ListFetcher which should then be used throughout the rest of
 * an application's lifetime.
 *
 * It is generally suited to abstract loading and fetching of NIP-51 lists.
 *
 * Take a look at the loadFollowsList and and loadRelayList implementations for more insight on how to use this.
 */
export function makeListFetcher<I>(
  kind: number,
  hardcodedRelays: string[],
  process: (event: NostrEvent | undefined) => I[],
  defaultTo: (pubkey: string) => I[],
): ListFetcher<I> {
  const cache = dataloaderCache<Result<I>>()
  const store = createStore(`@nostr/gadgets/list:${kind}`, 'cache')

  type Request = { target: string; relays: string[] }

  const dataloader = new DataLoader<Request, Result<I>, string>(
    requests =>
      new Promise(async resolve => {
        let remainingRequests: (Request & { index: number })[] = []

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
              // we don't have anything for this key, fill in with a placeholder
              return { items: defaultTo(req.target), event: null }
            } else if (res.lastAttempt < Date.now() / 1000 - 60 * 60 * 24 * 2) {
              remainingRequests.push(req)
              // we have something but it's old (2 days), so we will use it but still try to fetch a new version
              res.lastAttempt = Math.round(Date.now() / 1000)
              return res
            } else if (res.event === null && res.lastAttempt < Date.now() / 1000 - 60 * 60) {
              remainingRequests.push(req)
              // we have something but and it's not so old (1 hour), but it's empty, so we will try again
              res.lastAttempt = Math.round(Date.now() / 1000)
              return res
            } else {
              // this one is so good we won't try to fetch it again
              return res
            }
          }),
        )

        const filtersByRelay: { [url: string]: Filter[] } = {}
        for (let r = 0; r < remainingRequests.length; r++) {
          const req = remainingRequests[r]
          const relays = req.relays.slice(0, Math.min(4, req.relays.length))
          do {
            relays.push(randomPick(hardcodedRelays))
          } while (relays.length < 3)
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
          // eslint-disable-next-line prefer-const
          handle = pool.subscribeManyMap(filtersByRelay, {
            id: `kind:${kind}:batch(${requests.length})-${idserial++}`,
            onevent(evt) {
              for (let r = 0; r < remainingRequests.length; r++) {
                const req = remainingRequests[r]
                if (req.target === evt.pubkey) {
                  const previous = results[req.index]?.event
                  if (previous?.created_at || 0 > evt.created_at) return
                  results[req.index] = { event: evt, items: process(evt) }
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

    if (kind === 10002) {
      return await dataloader.load({ target: pubkey, relays })
    } else {
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
}

function randomPick<L>(list: L[]): L {
  return list[serial++ % list.length]
}
