/**
 * @module
 * Contains types and a function for optimized loading of profile metadata for any pubkey.
 */

import DataLoader from 'dataloader'
import type { Filter } from '@nostr/tools/filter'
import { decode, npubEncode, ProfilePointer } from '@nostr/tools/nip19'
import { createStore, getMany, setMany } from 'idb-keyval'

import { dataloaderCache } from './utils'
import { pool } from './global'
import { METADATA_QUERY_RELAYS } from './defaults'

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
 * USers for whom no information is unknown will generally have just a pubkey and npub, and shortName
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

const customStore = createStore('@nostr/gadgets', 'metadata')

/**
 * loadNostrUser uses the same approach as ListFetcher -- caching, baching requests etc -- but for profiles
 * based on kind:0.
 */
export function loadNostrUser(pubkey: string): Promise<NostrUser> {
  return metadataLoader.load(pubkey)
}

const metadataLoader = new DataLoader<string, NostrUser, string>(
  async keys =>
    new Promise(async resolve => {
      const filter: Filter = { kinds: [0], authors: [] }
      const resultIndexByKey = new Map<string, number>()

      // try to get from idb first -- also set up the results array with defaults
      let results: Array<NostrUser | Error> = await getMany<NostrUser & { lastAttempt: number }>(
        keys.slice(0),
        customStore,
      ).then(results =>
        results.map((res, i) => {
          const pubkey = keys[i]
          resultIndexByKey.set(pubkey, i)

          if (!res) {
            filter!.authors!.push(pubkey)
            // we don't have anything for this key, fill in with a placeholder
            const npub = npubEncode(pubkey)
            return {
              pubkey,
              npub,
              shortName: npub.substring(0, 8) + '…' + npub.substring(59),
              lastUpdated: 0,
              metadata: {},
              lastAttempt: Math.round(Date.now() / 1000),
            }
          } else if (res.lastAttempt < Date.now() / 1000 - 60 * 60 * 24 * 2) {
            filter!.authors!.push(pubkey)
            // we have something but it's old (2 days), so we will use it but still try to fetch a new version
            res.lastAttempt = Math.round(Date.now() / 1000)
            return res
          } else if (
            res.lastAttempt < Date.now() / 1000 - 60 * 60 &&
            !res.metadata.name &&
            !res.metadata.picture &&
            !res.metadata.about
          ) {
            filter!.authors!.push(pubkey)
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
      if (filter.authors?.length === 0) {
        resolve(results)
        return
      }

      try {
        pool.subscribeManyEose(METADATA_QUERY_RELAYS, [filter], {
          id: `metadata(${keys.length})`,
          onevent(evt) {
            for (let i = 0; i < keys.length; i++) {
              if (keys[i] === evt.pubkey) {
                const nu = results[i] as NostrUser
                if (nu.lastUpdated > evt.created_at) return

                let md: any = {}
                try {
                  md = JSON.parse(evt!.content)
                } catch {
                  /**/
                }

                nu.metadata = md
                nu.shortName = md.name || md.display_name || md.nip05?.split('@')?.[0] || nu.shortName

                if (md.picture) nu.image = md.picture

                return
              }
            }
          },
          onclose() {
            resolve(results)

            // save our updated results to idb
            setMany(
              filter!.authors!.map(pk => [pk, results.find(nu => (nu as NostrUser).pubkey === pk)]),
              customStore,
            )
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
    cache: true,
    cacheMap: dataloaderCache<NostrUser>(),
  },
)
