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

let serial = 0

function randomPick<L>(list: L[]): L {
  serial++
  return list[serial % list.length]
}

/**
 * Representation of a relay entry as found in a kind:10002 list.
 */
export type RelayItem = {
  url: string
  read: boolean
  write: boolean
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
export type ListFetcher<I> = (pubkey: string, hints?: string[]) => Promise<{ event: NostrEvent; items: I[] }>

/**
 * A ListFetcher for kind:10002 relay lists.
 *
 * Returns a list of RelayItem.
 */
export const loadRelayList: ListFetcher<RelayItem> = makeListFetcher<RelayItem>(
  10002,
  RELAYLIST_RELAYS,
  itemsFromTags<RelayItem>(nip65RelayFromTag),
)

/**
 * A ListFetcher for kind:3 follow lists.
 *
 * Returns a list of pubkeys as strings.
 */
export const loadFollowsList: ListFetcher<string> = makeListFetcher<string>(
  3,
  METADATA_QUERY_RELAYS,
  itemsFromTags<string>(pFromTag),
)

function itemsFromTags<I>(
  tagProcessor: (tag: string[]) => Promise<I | undefined> | I | undefined,
): (event: NostrEvent | undefined) => Promise<I[]> {
  return async (event: NostrEvent | undefined) => {
    const items = event ? (await Promise.all(event.tags.map(tagProcessor))).filter(identity) : []
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
  process: (event: NostrEvent | undefined) => Promise<I[]> | I[],
): ListFetcher<I> {
  type R = { event: NostrEvent; items: I[] }

  const cache = dataloaderCache<R>()

  const dataloader = new DataLoader<{ target: string; relays: string[] }, R, string>(
    requests =>
      new Promise(async resolve => {
        const results = new Array<NostrEvent | undefined>(requests.length)
        const filtersByRelay: { [url: string]: Filter[] } = {}
        for (let i = 0; i < requests.length; i++) {
          const req = requests[i]
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
            id: `kind:${kind}:batch(${requests.length})`,
            onevent(evt) {
              for (let i = 0; i < requests.length; i++) {
                if (requests[i].target === evt.pubkey) {
                  const res = results[i]
                  if ((res as NostrEvent)?.created_at || 0 > evt.created_at) return
                  results[i] = evt
                  return
                }
              }
            },
            oneose() {
              handle?.close()
            },
            async onclose() {
              resolve(
                await Promise.all(
                  results.map(async event => {
                    if (event) {
                      let items = await process(event)
                      return { event, items }
                    } else {
                      return new Error('not found')
                    }
                  }),
                ),
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

  return async function (pubkey: string, hints: string[] = []): Promise<{ event: NostrEvent; items: I[] }> {
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

function pFromTag(tag: string[]): string | undefined {
  if (tag.length >= 2 && tag[0] === 'p') {
    return tag[1]
  }
}

function nip65RelayFromTag(tag: string[]): RelayItem | undefined {
  if (tag.length === 2) {
    return { url: tag[1], read: true, write: true }
  } else if (tag[2] === 'read') {
    return { url: tag[1], read: true, write: false }
  } else if (tag[2] === 'write') {
    return { url: tag[1], read: false, write: true }
  }
}
