/**
 * @module
 * Contains types and a function for optimized loading of profile metadata for any pubkey.
 */

import DataLoader from 'dataloader'
import type { Filter } from '@nostr/tools/filter'
import type { NostrEvent } from '@nostr/tools/pure'
import { decode, npubEncode, ProfilePointer } from '@nostr/tools/nip19'
import { createStore, getMany, setMany } from 'idb-keyval'

import { dataloaderCache } from './utils'
import { pool } from './global'
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

const customStore = createStore('@nostr/gadgets/metadata', 'cache')

type NostrUserRequest = {
  pubkey: string
  relays?: string[]
  forceUpdate?: boolean
}

/**
 * loadNostrUser uses the same approach as ListFetcher -- caching, baching requests etc -- but for profiles
 * based on kind:0.
 */
export function loadNostrUser(request: NostrUserRequest | string): Promise<NostrUser> {
  if (typeof request === 'string') {
    return metadataLoader.load({ pubkey: request })
  }
  return metadataLoader.load(request)
}

const metadataLoader = new DataLoader<NostrUserRequest, NostrUser, string>(
  async requests =>
    new Promise(async resolve => {
      const toFetch: NostrUserRequest[] = []
      let now = Math.round(Date.now() / 1000)

      // try to get from idb first -- also set up the results array with defaults
      let results: Array<NostrUser | Error> = await getMany<NostrUser & { lastAttempt: number }>(
        requests.map(r => r.pubkey),
        customStore,
      ).then(results =>
        results.map((res, i) => {
          const req = requests[i]

          if (!res) {
            toFetch.push(req)
            // we don't have anything for this key, fill in with a placeholder
            let nu = blankNostrUser(req.pubkey)
            ;(nu as any).lastAttempt = now
            return nu
          } else if (req.forceUpdate || res.lastAttempt < now - 60 * 60 * 24 * 2) {
            toFetch.push(req)
            // we have something but it's old (2 days), so we will use it but still try to fetch a new version
            res.lastAttempt = now
            return res
          } else if (
            res.lastAttempt < now - 60 * 60 &&
            !res.metadata.name &&
            !res.metadata.picture &&
            !res.metadata.about
          ) {
            toFetch.push(req)
            // we have something but and it's not so old (1 hour), but it's empty, so we will try again
            res.lastAttempt = Math.round(Date.now() / 1000)
            return res
          } else {
            // this one is so good we won't try to fetch it again
            return res
          }
        }),
      )

      // no need to do any requests if we don't have anything to fetch
      if (toFetch.length === 0) {
        resolve(results)
        return
      }

      // gather relays for each pubkey that needs fetching
      const relaysByPubkey: { [pubkey: string]: string[] } = {}

      await Promise.all(
        toFetch.map(async ({ pubkey, relays = [] }) => {
          // start with provided relays (up to 3)
          const selectedRelays = new Set<string>(relays.slice(0, 3))

          try {
            // add relays from their relay list (up to 2 write-enabled relays)
            const { items } = await loadRelayList(pubkey)
            let gathered = 0
            for (let j = 0; j < items.length; j++) {
              if (items[j].write) {
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

          relaysByPubkey[pubkey] = Array.from(selectedRelays)
        }),
      )

      try {
        // create a map of filters by relay
        const filtersByRelay: { [url: string]: Filter[] } = {}
        for (const [pubkey, relays] of Object.entries(relaysByPubkey)) {
          for (const relay of relays) {
            if (!filtersByRelay[relay]) {
              filtersByRelay[relay] = [{ kinds: [0], authors: [] }]
            }
            filtersByRelay[relay][0].authors!.push(pubkey)
          }
        }

        let h = pool.subscribeManyMap(filtersByRelay, {
          label: `metadata(${requests.length})`,
          onevent(evt) {
            for (let i = 0; i < requests.length; i++) {
              if (requests[i].pubkey === evt.pubkey) {
                const nu = results[i] as NostrUser
                if (nu.lastUpdated > evt.created_at) return

                enhanceNostrUserWithEvent(nu, evt)

                return
              }
            }
          },
          oneose() {
            resolve(results)

            h.close()

            // save our updated results to idb
            let idbSave: [IDBValidKey, any][] = []
            for (let i = 0; i < results.length; i++) {
              let res = results[i] as NostrUser
              if (res.pubkey) {
                idbSave.push([res.pubkey, res])
              }
            }

            setMany(idbSave, customStore)
          },
        })
      } catch (err) {
        for (let i = 0; i < results.length; i++) {
          results[i] = err as Error
        }
        resolve(results)
      }
    }),
  {
    cacheKeyFn: r => r.pubkey,
    cache: true,
    cacheMap: dataloaderCache<NostrUser>(),
  },
)

export function blankNostrUser(pubkey: string): NostrUser {
  const npub = npubEncode(pubkey)
  return {
    pubkey,
    npub,
    shortName: npub.substring(0, 8) + '…' + npub.substring(59),
    lastUpdated: 0,
    metadata: {},
  }
}

function enhanceNostrUserWithEvent(nu: NostrUser, evt: NostrEvent) {
  let md: any = {}
  try {
    md = JSON.parse(evt.content)
  } catch {
    /**/
  }

  nu.metadata = md
  nu.shortName = md.name || md.display_name || md.nip05?.split('@')?.[0] || nu.shortName

  if (md.picture) nu.image = md.picture
}

export function nostrUserFromEvent(evt: NostrEvent): NostrUser {
  let nu = blankNostrUser(evt.pubkey)
  enhanceNostrUserWithEvent(nu, evt)
  return nu
}
