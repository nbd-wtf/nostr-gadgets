/**
 * @module
 * Contains functions for optimized fetching of addressable lists associated with a pubkey.
 */

import type { NostrEvent } from '@nostr/tools/core'
import type { Filter } from '@nostr/tools/filter'
import type { SubCloser } from '@nostr/tools/abstract-pool'

import DataLoader from './dataloader'

import { pool, label, purgatory, replaceableStore } from './global'
import { normalizeURL } from '@nostr/tools/utils'

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
      return normalizeURL(tag[1])
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

  const dataloader = new DataLoader<Request, Promise<SetResult<I>> | SetResult<I>, string>(
    requests =>
      new Promise(async resolve => {
        const toFetch: {
          [target: string]: {
            req: Request & { index: number }
            resolve: (result: SetResult<I>) => void
            resolved: boolean
            since: number // it will be 0 if we are force-updating
            result?: SetResult<I>
          }
        } = {}
        let now = Math.round(Date.now() / 1000)

        // try to get from store first -- 2-tuple returns all events for 3xxxx kinds as bundle
        const stored = (await replaceableStore.loadReplaceables(
          requests.map(r => [kind, r.target] as [number, string]),
        )) as [number, NostrEvent[]][]

        let results = stored.map(([lastAttempt, events], i): SetResult<I> | Promise<SetResult<I>> => {
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
            // we don't have anything for this key, fill in with empty object
            if (req.forceUpdate === false) return {}
            else
              return new Promise(resolve => {
                toFetch[req.target] = {
                  req,
                  resolve,
                  resolved: false,
                  since: req.forceUpdate === true ? 0 : lastAttempt,
                }
              })
          } else if (req.forceUpdate || !lastAttempt || lastAttempt < now - 60 * 60 * 24 * 2) {
            // we have something but it's old (2 days), so we will use it but still try to fetch new versions
            if (req.forceUpdate === false) return result
            else
              return new Promise(resolve => {
                resolve(result)
                toFetch[req.target] = {
                  req,
                  resolve,
                  resolved: true,
                  result,
                  since: req.forceUpdate === true ? 0 : lastAttempt,
                }
              })
          } else {
            // this one is so good we won't try to fetch it again
            return result
          }
        })

        // resolve immediately (pending reqs will return a promise thus resolve later)
        resolve(results)

        if (Object.keys(toFetch).length === 0) return

        const filterByRelay: { [url: string]: Filter } = {}
        for (const {
          req: { target, relays: reqRelays },
        } of Object.values(toFetch)) {
          const relays = reqRelays.slice(0, Math.min(4, reqRelays.length))
          for (let j = 0; j < relays.length; j++) {
            const url = relays[j]
            let filter = filterByRelay[url]
            if (!filter) {
              filter = { kinds: [kind], authors: [] }
              filterByRelay[url] = filter
            }
            filter.authors?.push(target)
          }
        }

        // set since on each relay filter: minimum lastAttempt across targets for each relay
        for (const filter of Object.values(filterByRelay)) {
          let minSince: number = 0
          for (const author of filter.authors || []) {
            const entry = toFetch[author]
            if (minSince === undefined || entry.since < minSince) minSince = entry.since
          }

          if (minSince > 0) {
            filter.since = minSince
          }
        }

        try {
          let handle: SubCloser | undefined
          const savesToAwait: Promise<boolean>[] = []
          const pubkeysThatReceivedEvents: Set<string> = new Set()

          handle = pool.subscribeMap(
            Object.entries(filterByRelay).map(([url, filter]) => ({ url, filter })),
            {
              label: `${label ? label + ':' : ''}kind:${kind}:batch(${Object.keys(toFetch).length})`,
              onevent(evt) {
                const entry = toFetch[evt.pubkey]
                if (!entry) return

                // merge into existing result
                const dTag = evt.tags.find(t => t[0] === 'd')?.[1] || ''
                entry.result = entry.result || {}

                const existing = entry.result[dTag]
                if (!existing || existing.event.created_at < evt.created_at) {
                  entry.result[dTag] = {
                    event: evt,
                    items: process(evt),
                  }
                  savesToAwait.push(replaceableStore.saveEvent(evt, { lastAttempt: now }))
                  pubkeysThatReceivedEvents.add(evt.pubkey)
                }
              },
              oneose() {
                handle?.close()
              },
              async onclose() {
                for (const { req, resolve, resolved, result } of Object.values(toFetch)) {
                  if (resolved) continue

                  // resolve fresh fetches with accumulated result (or empty)
                  resolve(result || {})

                  // for pubkeys that didn't receive any events, update lastAttempt so we don't keep retrying
                  if (!pubkeysThatReceivedEvents.has(req.target)) {
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

                await Promise.all(savesToAwait)
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
        .filter(({ write, url }) => write && purgatory.allowConnectingToRelay(url, ['read', [{ kinds: [kind] }]]))
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
