/**
 * @module
 * Contains functions for optimized fetching of replaceable lists associated with a pubkey.
 */

import DataLoader from './dataloader'
import type { NostrEvent } from '@nostr/tools/core'
import type { Filter } from '@nostr/tools/filter'
import type { SubCloser } from '@nostr/tools/abstract-pool'
import { AddressPointer } from '@nostr/tools/nip19'
import { normalizeURL } from '@nostr/tools/utils'

import { pool, purgatory, replaceableStore } from './global'

import { METADATA_QUERY_RELAYS, RELAYLIST_RELAYS } from './defaults'
import { identity, isHex32 } from './utils'

let serial = 0

export const isFresh = Symbol('event was just downloaded or force-updated, not loaded from cache')

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
  refreshStyle?: boolean | NostrEvent | null,
  defaultItems?: I[],
) => Promise<ListResult<I>>

export type ListResult<I> = { event: NostrEvent | null; items: I[]; [isFresh]: boolean }

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
          return normalizeURL(tag[1])
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
)

/**
 * A ListFetcher for kind:10002 relay lists.
 */
export const loadRelayList: ListFetcher<RelayItem> = makeListFetcher<RelayItem>(
  10002,
  RELAYLIST_RELAYS,
  itemsFromTags<RelayItem>((tag: string[]): RelayItem | undefined => {
    if (tag[0] !== 'r') return undefined

    if (tag.length === 2) {
      return { url: normalizeURL(tag[1]), read: true, write: true }
    } else if (tag[2] === 'read') {
      return { url: normalizeURL(tag[1]), read: true, write: false }
    } else if (tag[2] === 'write') {
      return { url: normalizeURL(tag[1]), read: false, write: true }
    }
  }),
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
          break
        case 'e':
          if (isHex32(tag[1])) {
            return { label: 'thread', value: tag[1] }
          }
          break
        case 't':
          return { label: 'hashtag', value: tag[1] }
        case 'word':
          return { label: 'word', value: tag[1] }
      }
      return undefined
    }
  }),
)

export const loadBookmarks: ListFetcher<string> = makeListFetcher<string>(
  10003,
  [],
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && (tag[0] === 'e' || tag[0] === 'a') && tag[1]) {
      return tag[1]
    }
  }),
)

export const loadBlossomServers: ListFetcher<string> = makeListFetcher<string>(10063, [], event =>
  event
    ? event.tags
        .filter(([k, v]) => k === 'server' && v)
        .map(([, url]) => 'http' + normalizeURL(url).substring(2 /* 'ws' */))
        .filter(Boolean)
    : [],
)

export type Emoji = {
  shortcode: string
  url: string
}

export const loadEmojis: ListFetcher<Emoji | AddressPointer> = makeListFetcher<Emoji | AddressPointer>(
  10030,
  [],
  itemsFromTags<Emoji | AddressPointer>((tag: string[]): Emoji | AddressPointer | undefined => {
    if (tag.length < 2) return
    if (tag[0] === 'a') {
      const spl = tag[1].split(':')
      if (!isHex32(spl[1]) || spl[0] !== '30030') return undefined
      return {
        identifier: spl.slice(2).join(':'),
        pubkey: spl[1],
        kind: parseInt(spl[0]),
        relays: tag[2] ? [tag[2]] : [],
      }
    }
    if (tag.length < 3 || tag[0] !== 'emoji') return undefined
    return { shortcode: tag[1], url: tag[2] }
  }),
)

export const loadPins: ListFetcher<string> = makeListFetcher<string>(
  10001,
  [],
  itemsFromTags<string>((tag: string[]): string | undefined => {
    if (tag.length >= 2 && tag[0] === 'e' && tag[1]) {
      return tag[1]
    }
  }),
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
): ListFetcher<I> {
  type Request = { target: string; relays: string[]; refreshStyle?: boolean | NostrEvent; defaultItems?: I[] }

  const dataloader = new DataLoader<Request, Promise<ListResult<I>> | ListResult<I>, string>(
    requests =>
      new Promise(async resolve => {
        const toFetch: {
          [target: string]: {
            req: Request & { index: number }
            resolve: (result: ListResult<I>) => void
            resolved: boolean
          }
        } = {}
        const pendingSaves: Promise<boolean>[] = []
        const now = Math.round(Date.now() / 1000)

        // try to get from store first -- also set up the results array with defaults
        // use 3-tuple with empty dtag to get single event return type
        const stored = (await replaceableStore.loadReplaceables(
          requests.map(r => [kind, r.target, ''] as [number, string, string]),
        )) as [number, NostrEvent][]

        let results: Array<Promise<ListResult<I>> | ListResult<I> | Error> = stored.map(
          ([lastAttempt, storedEvent], i): ListResult<I> | Promise<ListResult<I>> => {
            const req = requests[i] as Request & { index: number }
            req.index = i

            if (typeof req.refreshStyle === 'object') {
              // we have the event right here, so just use it
              const final = { event: req.refreshStyle, items: process(req.refreshStyle), [isFresh]: true }
              pendingSaves.push(replaceableStore.saveEvent(req.refreshStyle, { lastAttempt: now }))
              return final
            } else if (!storedEvent) {
              // we don't have anything for this key, fill in with a placeholder
              if (req.refreshStyle !== false) {
                return new Promise(resolve => {
                  toFetch[req.target] = { req, resolve, resolved: false }
                })
              } else {
                return { items: req.defaultItems || [], event: null, [isFresh]: false }
              }
            } else if (req.refreshStyle === true || !lastAttempt || lastAttempt < now - 60 * 60 * 24 * 2) {
              // we have something but it's old (2 days), so we will use it but still try to fetch a new version
              const good = { event: storedEvent, items: process(storedEvent), [isFresh]: false }
              if (req.refreshStyle !== false) {
                return new Promise(resolve => {
                  resolve(good)
                  toFetch[req.target] = { req, resolve, resolved: true }
                })
              } else {
                return good
              }
            } else {
              // this one is so good we won't try to fetch it again
              return { event: storedEvent, items: process(storedEvent), [isFresh]: false }
            }
          },
        )

        // wait for pendingSaves (from when an event is passed through refreshStyle)
        if (pendingSaves.length > 0) await Promise.all(pendingSaves)

        // resolve immediately (pending reqs will return a promise thus resolve later)
        resolve(results)

        if (Object.keys(toFetch).length === 0) {
          return
        }

        const filterByRelay: { [url: string]: Filter } = {}
        for (const {
          req: { target, relays: reqRelays },
        } of Object.values(toFetch)) {
          const relays = reqRelays.slice(0, Math.min(4, reqRelays.length))
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
            filter.authors?.push(target)
          }
        }

        try {
          let handle: SubCloser | undefined
          // eslint-disable-next-line prefer-const
          handle = pool.subscribeMap(
            Object.entries(filterByRelay).map(([url, filter]) => ({ url, filter })),
            {
              label: `kind:${kind}:batch(${Object.keys(toFetch).length})`,
              onevent(evt) {
                const { req, resolve, resolved } = toFetch[evt.pubkey]
                if (resolved) return

                resolve({ event: evt, items: process(evt), [isFresh]: true })
                toFetch[evt.pubkey].resolved = true
                replaceableStore.saveEvent(evt, { lastAttempt: now })
              },
              oneose() {
                handle?.close()
              },
              async onclose() {
                // resolve promises for targets that didn't receive any events with their placeholder data
                for (const { req, resolve, resolved } of Object.values(toFetch)) {
                  if (resolved) continue

                  const placeholder = { items: req.defaultItems || [], event: null, [isFresh]: false }
                  resolve(placeholder)

                  // save blank events for targets that didn't receive any events (they won't be really saved)
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
              },
            },
          )
        } catch (err) {
          resolve(results.map(_ => err as Error))
        }
      }),
    {
      cacheKeyFn: req => req.target,
      transformCacheHit(v) {
        v[isFresh] = false
        return v
      },
    },
  )

  return async function (
    pubkey: string,
    hints: string[] = [],
    refreshStyle?: boolean | NostrEvent | null,
    defaultItems?: I[],
  ): Promise<ListResult<I>> {
    if (refreshStyle === null) {
      // refreshStyle === null: reset cache and return empty
      await replaceableStore.deleteReplaceable(kind, pubkey)
      dataloader._cacheMap.delete(pubkey)
      return { items: defaultItems || [], event: null, [isFresh]: true }
    }

    let relays: string[] = hints

    if (kind === 10002) {
      return await dataloader.load({ target: pubkey, relays, refreshStyle, defaultItems })
    } else {
      const rl = await loadRelayList(pubkey, hints, refreshStyle)
      relays.push(
        ...rl.items
          .filter(({ write, url }) => write && purgatory.allowConnectingToRelay(url, ['read', [{ kinds: [kind] }]]))
          .map(({ url }) => url)
          .slice(0, 3),
      )

      const req = { target: pubkey, relays, refreshStyle, defaultItems }

      if (refreshStyle) {
        dataloader.clear(req)
      }

      return await dataloader.load(req)
    }
  }
}

function randomPick<L>(list: L[]): L {
  return list[serial++ % list.length]
}
