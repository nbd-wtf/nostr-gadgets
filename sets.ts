/**
 * @module
 * Contains functions for optimized fetching of addressable lists associated with a pubkey.
 */

import DataLoader from './dataloader'
import type { NostrEvent } from '@nostr/tools/core'
import type { Filter } from '@nostr/tools/filter'
import type { SubCloser } from '@nostr/tools/abstract-pool'

import { pool, replaceableStore } from './global'
import { isHex32 } from './utils'
import { Emoji, itemsFromTags, loadRelayList } from './lists'

/**
 * A SetFetcher is a function that can be called to return a map of items indexed by d-tag,
 * where each value contains the event and parsed items.
 */
export type SetFetcher<I> = (pubkey: string, hints?: string[], forceUpdate?: boolean) => Promise<SetResult<I>>

export type SetResult<I> = {
  [dTag: string]: {
    event: NostrEvent
    items: I[]
  }
}

/**
 * A SetFetcher for kind:30000 follow sets.
 */
export const loadFollowSets: SetFetcher<string> = makeSetFetcher<string>(
  30000,
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'p' && isHex32(tag[1])) {
      return tag[1]
    }
  }),
)

/**
 * A SetFetcher for kind:39089 follow packs.
 */
export const loadFollowPacks: SetFetcher<string> = makeSetFetcher<string>(
  39089,
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'p' && isHex32(tag[1])) {
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

export const loadEmojiSets: SetFetcher<Emoji> = makeSetFetcher<Emoji>(
  30002,
  itemsFromTags<Emoji>((tag: string[]): Emoji | undefined => {
    if (tag.length < 3 || tag[0] !== 'emoji') return undefined
    return { shortcode: tag[1], url: tag[2] }
  }),
)

/**
 * makeSetFetcher is similar to makeListFetcher but handles multiple events per pubkey,
 * differentiated by their "d" tag values.
 */
export function makeSetFetcher<I>(kind: number, process: (event: NostrEvent) => I[]): SetFetcher<I> {
  type Request = { target: string; relays: string[]; forceUpdate?: boolean }

  const dataloader = new DataLoader<Request, SetResult<I>, string>(
    requests =>
      new Promise(async resolve => {
        let remainingRequests: (Request & { index: number })[] = []
        let now = Math.round(Date.now() / 1000)

        // try to get from store first -- 2-tuple returns all events for 3xxxx kinds as bundle
        const stored = await replaceableStore.loadReplaceables(
          requests.map(r => [kind, r.target] as [number, string]),
        )

        let results = stored.map(([lastAttempt, events], i) => {
          const req = requests[i] as Request & { index: number }
          req.index = i

          // build SetResult from stored events
          const result: SetResult<I> = {}
          for (const evt of events) {
            const dTag = evt.tags.find(t => t[0] === 'd')?.[1] || ''
            const existing = result[dTag]
            if (!existing || existing.event.created_at < evt.created_at) {
              result[dTag] = {
                event: evt,
                items: process(evt),
              }
            }
          }

          if (Object.keys(result).length === 0) {
            remainingRequests.push(req)
            // we don't have anything for this key, fill in with empty object
            return { lastAttempt: now, result: {} }
          } else if (req.forceUpdate || !lastAttempt || lastAttempt < now - 60 * 60 * 24 * 2) {
            remainingRequests.push(req)
            // we have something but it's old (2 days), so we will use it but still try to fetch new versions
            return { lastAttempt: lastAttempt || 0, result }
          } else {
            // this one is so good we won't try to fetch it again
            return { lastAttempt: lastAttempt || 0, result }
          }
        })

        if (remainingRequests.length === 0) {
          resolve(results.map(r => r.result))
          return
        }

        const filterByRelay: { [url: string]: Filter } = {}
        for (let r = 0; r < remainingRequests.length; r++) {
          const req = remainingRequests[r]
          const relays = req.relays.slice(0, Math.min(4, req.relays.length))
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
          const eventsToSave: NostrEvent[] = []
          handle = pool.subscribeMap(
            Object.entries(filterByRelay).map(([url, filter]) => ({ url, filter })),
            {
              label: `kind:${kind}:batch(${remainingRequests.length})`,
              onevent(evt) {
                for (let r = 0; r < remainingRequests.length; r++) {
                  const req = remainingRequests[r]
                  if (req.target === evt.pubkey) {
                    const dTag = evt.tags.find(t => t[0] === 'd')?.[1] || ''
                    const result = results[req.index].result
                    const existing = result[dTag]

                    // only update if this is a newer event for this d tag
                    if (!existing || existing.event.created_at < evt.created_at) {
                      result[dTag] = {
                        event: evt,
                        items: process(evt),
                      }
                      eventsToSave.push(evt)
                    }
                    return
                  }
                }
              },
              oneose() {
                handle?.close()
              },
              async onclose() {
                resolve(results.map(r => r.result))

                // save fetched events to store (this also updates lastAttempt on the bundle)
                for (const evt of eventsToSave) {
                  replaceableStore.saveEvent(evt, { lastAttempt: now })
                }

                // for pubkeys that didn't receive any events, update lastAttempt so we don't keep retrying
                const pubkeysReceived = new Set(eventsToSave.map(e => e.pubkey))
                for (const req of remainingRequests) {
                  if (!pubkeysReceived.has(req.target)) {
                    replaceableStore.saveEvent(
                      {
                        id: '0'.repeat(64),
                        pubkey: req.target,
                        kind: kind,
                        sig: '0'.repeat(128),
                        tags: [],
                        created_at: 0,
                        content: '',
                      },
                      { lastAttempt: now },
                    )
                  }
                }
              },
            },
          )
        } catch (err) {
          resolve(results.map(_ => err as Error))
        }
      }),
    {
      cacheKeyFn: req => req.target,
    },
  )

  return async function (pubkey: string, hints: string[] = [], forceUpdate?: boolean): Promise<SetResult<I>> {
    let relays: string[] = hints

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
