/**
 * @module
 * Contains types and a function for optimized loading of profile metadata for any pubkey.
 */

import DataLoader from './dataloader'
import type { NostrEvent } from '@nostr/tools/pure'
import { decode, npubEncode, ProfilePointer } from '@nostr/tools/nip19'

import { pool, purgatory, replaceableStore } from './global'
import { METADATA_QUERY_RELAYS } from './defaults'
import { loadRelayList } from './lists'

let next = 0

/**
 * ProfileMetadata contains information directly parsed from a kind:0 content. nip05 is not verified.
 */
export type ProfileMetadata = {
  name?: string
  picture?: string
  about?: string
  display_name?: string
  website?: string
  banner?: string
  nip05?: string
  lud16?: string
  lud06?: string
}

/**
 * An object containing all necessary information available about a Nostr user.
 *
 * Users for whom no information is unknown will generally have just a pubkey and npub, and shortName
 * will be an ugly short string based on the npub.
 */
export type NostrUser = {
  pubkey: string
  npub: string
  shortName: string
  image?: string
  metadata: ProfileMetadata
  lastUpdated: number
}

/**
 * Generates a placeholder for when we (still) don't have any information about a user.
 */
export function bareNostrUser(input: string): NostrUser {
  let npub: string
  let pubkey: string
  if (input.startsWith('npub1')) {
    let { data } = decode(input)
    pubkey = data as string
    npub = input
  } else if (input.startsWith('nprofile')) {
    let { data } = decode(input)
    pubkey = (data as ProfilePointer).pubkey
    npub = npubEncode(pubkey)
  } else {
    pubkey = input
    npub = npubEncode(input)
  }
  return {
    pubkey,
    npub,
    shortName: npub.substring(0, 8) + '…' + npub.substring(59),
    metadata: {},
    lastUpdated: 0,
  }
}

export type NostrUserRequest = {
  pubkey: string
  relays?: string[]
  refreshStyle?: boolean | NostrEvent | null
}

/**
 * loadNostrUser uses the same approach as ListFetcher -- caching, baching requests etc -- but for profiles
 * based on kind:0.
 */
export async function loadNostrUser(request: NostrUserRequest | string): Promise<NostrUser> {
  if (typeof request === 'string') {
    return metadataLoader.load({ pubkey: request })
  } else {
    if (request.refreshStyle === null) {
      // refreshStyle === null: reset cache and return empty
      await replaceableStore.deleteReplaceable(0, request.pubkey)
      metadataLoader._cacheMap.delete(request.pubkey)
      return bareNostrUser(request.pubkey)
    } else if (request.refreshStyle) {
      metadataLoader.clear(
        // tell ts that refreshStyle can't be null here
        request as Parameters<typeof metadataLoader.clear>[0],
      )
    }
  }

  return metadataLoader.load(
    // tell ts that refreshStyle can't be null here
    request as Parameters<typeof metadataLoader.load>[0],
  )
}

const metadataLoader = new DataLoader<
  NostrUserRequest & { refreshStyle?: NostrEvent | boolean },
  Promise<NostrUser> | NostrUser,
  string
>(
  async requests =>
    new Promise(async resolve => {
      const toFetch: {
        [pubkey: string]: {
          req: NostrUserRequest
          resolve: (result: NostrUser) => void
          resolved: boolean
        }
      } = {}
      const pendingSaves: Promise<boolean>[] = []
      const now = Math.round(Date.now() / 1000)

      // try to get from store first -- also set up the results array with defaults
      // use 3-tuple with empty dtag to get single event return type
      const stored = (await replaceableStore.loadReplaceables(
        requests.map(r => [0, r.pubkey, ''] as [number, string, string]),
      )) as [number, NostrEvent][]

      let results: Array<Promise<NostrUser> | NostrUser | Error> = stored.map(
        ([lastAttempt, storedEvent], i): NostrUser | Promise<NostrUser> => {
          const req = requests[i]

          if (typeof req.refreshStyle === 'object') {
            // we have the event right here, so just use it
            let nu = bareNostrUser(req.pubkey)
            enhanceNostrUserWithEvent(nu, req.refreshStyle)
            pendingSaves.push(replaceableStore.saveEvent(req.refreshStyle, { lastAttempt: now }))
            return nu
          } else if (!storedEvent) {
            // we don't have anything for this key, fill in with a placeholder
            if (req.refreshStyle !== false) {
              return new Promise(resolve => {
                toFetch[req.pubkey] = { req, resolve, resolved: false }
              })
            } else {
              return bareNostrUser(req.pubkey)
            }
          } else if (req.refreshStyle === true || !lastAttempt || lastAttempt < now - 60 * 60 * 24 * 2) {
            // we have something but it's old (2 days), so we will use it but still try to fetch a new version
            const good = nostrUserFromEvent(storedEvent)
            if (req.refreshStyle !== false) {
              return new Promise(resolve => {
                resolve(good)
                toFetch[req.pubkey] = { req, resolve, resolved: true }
              })
            } else {
              return good
            }
          } else {
            const nu = nostrUserFromEvent(storedEvent)
            if (lastAttempt < now - 60 * 60 && !nu.metadata.name && !nu.metadata.picture && !nu.metadata.about) {
              // we have something but and it's not so old (1 hour), but it's empty, so we will try again
              if (req.refreshStyle !== false) {
                return new Promise(resolve => {
                  toFetch[req.pubkey] = { req, resolve, resolved: false }
                })
              }
              return nu
            } else {
              // this one is so good we won't try to fetch it again
              return nu
            }
          }
        },
      )

      // wait for pendingSaves (from when an event is passed through refreshStyle)
      if (pendingSaves.length > 0) await Promise.all(pendingSaves)

      // resolve immediately (pending reqs will return a promise thus resolve later)
      resolve(results)

      // gather relays for each pubkey that needs fetching
      const pubkeysByRelay: { [relay: string]: string[] } = {}

      await Promise.all(
        Object.values(toFetch).map(async ({ req: { pubkey, relays = [] } }) => {
          // start with provided relays (up to 3)
          const selectedRelays = new Set<string>(relays.slice(0, 3))

          try {
            // add relays from their relay list (up to 2 write-enabled relays)
            const { items } = await loadRelayList(pubkey)
            let gathered = 0
            for (let j = 0; j < items.length; j++) {
              if (items[j].write && purgatory.allowConnectingToRelay(items[j].url, ['read', [{ kinds: [0] }]])) {
                selectedRelays.add(items[j].url)
                gathered++
                if (gathered >= 2) break
              }
            }
          } catch (err) {
            console.error('Failed to load relay list for', pubkey, err)
          }

          // ensure we have at least one hardcoded relay
          do {
            selectedRelays.add(METADATA_QUERY_RELAYS[next % METADATA_QUERY_RELAYS.length])
            next++
          } while (selectedRelays.size < 2)

          for (let relay of selectedRelays) {
            if (pubkeysByRelay[relay]) {
              pubkeysByRelay[relay].push(pubkey)
            } else {
              pubkeysByRelay[relay] = [pubkey]
            }
          }
        }),
      )

      try {
        const requestMap = Object.entries(pubkeysByRelay).map(([relay, pubkeys]) => ({
          url: relay,
          filter: { kinds: [0], authors: pubkeys },
        }))

        let h = pool.subscribeMap(requestMap, {
          label: `metadata(${requests.length})`,
          onevent(evt) {
            const nu = nostrUserFromEvent(evt)
            toFetch[evt.pubkey].resolve(nu)
            toFetch[evt.pubkey].resolved = true
            replaceableStore.saveEvent(evt, { lastAttempt: now })
          },
          oneose() {
            h.close()

            // resolve promises for pubkeys that didn't receive any events with their placeholder data
            for (const { req, resolve, resolved } of Object.values(toFetch)) {
              if (resolved) continue

              const placeholder = bareNostrUser(req.pubkey)
              resolve(placeholder)

              // save blank events for pubkeys that didn't receive any events (they won't be really saved)
              replaceableStore.saveEvent(
                {
                  id: '0'.repeat(64),
                  pubkey: req.pubkey,
                  kind: 0,
                  sig: '0'.repeat(128),
                  tags: [],
                  created_at: 0,
                  content: '',
                },
                { lastAttempt: now },
              )
            }
          },
        })
      } catch (err) {
        for (let i = 0; i < results.length; i++) {
          results[i] = err as Error
        }
        if (pendingSaves.length > 0) Promise.all(pendingSaves).then(() => resolve(results))
        else resolve(results)
      }
    }),
  {
    cacheKeyFn: r => r.pubkey,
  },
)

function enhanceNostrUserWithEvent(nu: NostrUser, evt: NostrEvent) {
  let md: any = {}
  try {
    md = JSON.parse(evt.content)
  } catch {
    /**/
  }

  nu.metadata = md
  nu.shortName = md.name || md.display_name || md.nip05?.split('@')?.[0] || nu.shortName
  nu.lastUpdated = evt.created_at

  if (md.picture) nu.image = md.picture
}

export function nostrUserFromEvent(evt: NostrEvent): NostrUser {
  let nu = bareNostrUser(evt.pubkey)
  enhanceNostrUserWithEvent(nu, evt)
  return nu
}
