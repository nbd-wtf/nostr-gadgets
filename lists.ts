/**
 * @module
 * Contains functions for optimized fetching of replaceable lists associated with a pubkey.
 */

import DataLoader from 'dataloader'
import type { NostrEvent } from '@nostr/tools/core'
import type { Filter } from '@nostr/tools/filter'
import type { SubCloser } from '@nostr/tools/abstract-pool'
import { createStore, getMany, set, setMany } from 'idb-keyval'

import { pool } from './global'

import { METADATA_QUERY_RELAYS, RELAYLIST_RELAYS } from './defaults'
import { dataloaderCache, identity, isHex32 } from './utils'
import { AddressPointer } from '@nostr/tools/nip19'

let serial = 0

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
export type ListFetcher<I> = (
  pubkey: string,
  hints?: string[],
  forceUpdate?: boolean | NostrEvent,
) => Promise<Result<I>>
type Result<I> = { event: NostrEvent | null; items: I[] }

/**
 * A ListFetcher for kind:3 follow lists.
 */
export const loadFollowsList: ListFetcher<string> = makeListFetcher<string>(
  3,
  METADATA_QUERY_RELAYS,
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'p' && isHex32(tag[1])) {
      return tag[1]
    }
  }),
  _ => [],
)

/**
 * A ListFetcher for kind:10101 "good wiki authors" list
 */
export const loadWikiAuthors: ListFetcher<string> = makeListFetcher<string>(
  10101,
  [],
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'p' && isHex32(tag[1])) {
      return tag[1]
    }
  }),
  _ => [],
)

/**
 * A ListFetcher for kind:10102 "good wiki relays" list
 */
export const loadWikiRelays: ListFetcher<string> = makeListFetcher<string>(
  10102,
  [],
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'relay') {
      return tag[1]
    }
  }),
  _ => [],
)

/**
 * A ListFetcher for kind:10012 "favorite relays" list
 */
export const loadFavoriteRelays: ListFetcher<string | AddressPointer> = makeListFetcher<string | AddressPointer>(
  10012,
  [],
  itemsFromTags<string | AddressPointer>((tag: string[]): string | AddressPointer | undefined => {
    if (tag.length >= 2) {
      switch (tag[0]) {
        case 'relay':
          return tag[1]
        case 'a':
          const spl = tag[1].split(':')
          if (!isHex32(spl[1]) || spl[0] !== '30002') return undefined
          return {
            identifier: spl.slice(2).join(':'),
            pubkey: spl[1],
            kind: parseInt(spl[0]),
            relays: tag[2] ? [tag[2]] : [],
          }
      }
    }
  }),
  _ => [],
)

/**
 * A ListFetcher for kind:10002 relay lists.
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
 */
export const loadMuteList: ListFetcher<MutedEntity> = makeListFetcher<MutedEntity>(
  10000,
  [],
  itemsFromTags<MutedEntity>((tag: string[]): MutedEntity | undefined => {
    if (tag.length >= 2) {
      switch (tag[0]) {
        case 'p':
          if (isHex32(tag[1])) {
            return { label: 'pubkey', value: tag[1] }
          }
        case 'e':
          if (isHex32(tag[1])) {
            return { label: 'thread', value: tag[1] }
          }
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

export function itemsFromTags<I>(
  tagProcessor: (tag: string[]) => I | undefined,
): (event: NostrEvent | undefined) => I[] {
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

  type Request = { target: string; relays: string[]; forceUpdate?: boolean | NostrEvent }

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

            if (!res && !req.forceUpdate) {
              remainingRequests.push(req)
              // we don't have anything for this key, fill in with a placeholder
              return { items: defaultTo(req.target), event: null }
            } else if (typeof req.forceUpdate === 'object') {
              // we have the event right here, so just use it
              const final = { event: req.forceUpdate, items: process(req.forceUpdate) }
              set(req.target, final, store)
              return final
            } else if (req.forceUpdate === true || !res.lastAttempt || res.lastAttempt < now - 60 * 60 * 24 * 2) {
              remainingRequests.push(req)
              // we have something but it's old (2 days), so we will use it but still try to fetch a new version
              return res
            } else if (res.event === null && res.lastAttempt < Date.now() / 1000 - 60 * 60) {
              remainingRequests.push(req)
              // we have something and it's not so old (1 hour), but it's empty, so we will try again
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

        const filterByRelay: { [url: string]: Filter } = {}
        for (let r = 0; r < remainingRequests.length; r++) {
          const req = remainingRequests[r]
          const relays = req.relays.slice(0, Math.min(4, req.relays.length))
          do {
            relays.push(randomPick(hardcodedRelays))
          } while (relays.length < 3)
          for (let j = 0; j < relays.length; j++) {
            const url = relays[j]
            let filter = filterByRelay[url]
            if (!filter) {
              filter = { kinds: [kind], authors: [] }
              filterByRelay[url] = filter
            }
            filter.authors?.push(req.target)
          }
        }

        try {
          let handle: SubCloser | undefined
          // eslint-disable-next-line prefer-const
          handle = pool.subscribeMap(
            Object.entries(filterByRelay).map(([url, filter]) => ({ url, filter })),
            {
              label: `kind:${kind}:batch(${remainingRequests.length})`,
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
                  remainingRequests.map(req => [req.target, { ...results[req.index], lastAttempt: now }]),
                  store,
                )
              },
            },
          )
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

  return async function (pubkey: string, hints: string[] = [], forceUpdate?: boolean | NostrEvent): Promise<Result<I>> {
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

      const req = { target: pubkey, relays, forceUpdate }

      if (forceUpdate) {
        dataloader.clear(req)
      }

      return await dataloader.load(req)
    }
  }
}

function randomPick<L>(list: L[]): L {
  return list[serial++ % list.length]
}
