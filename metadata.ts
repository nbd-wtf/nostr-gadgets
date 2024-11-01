import DataLoader from 'dataloader'
import type { Filter } from '@nostr/tools/filter'
import { decode, npubEncode, ProfilePointer } from '@nostr/tools/nip19'

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

/**
 * loadNostrUser uses the same approach as ListFetcher -- caching, baching requests etc -- but for profiles
 * based on kind:0.
 */
export async function loadNostrUser(pubkey: string): Promise<NostrUser> {
  return await metadataLoader.load(pubkey)
}

const metadataLoader = new DataLoader<string, NostrUser, string>(
  async keys =>
    new Promise(async resolve => {
      const results = new Array<NostrUser | Error>(keys.length)
      const filter: Filter = { kinds: [0], authors: keys.slice(0) }

      // fill in the defaults
      for (let i = 0; i < keys.length; i++) {
        const pubkey = keys[i]
        const npub = npubEncode(pubkey)
        results[i] = {
          pubkey: pubkey,
          npub,
          shortName: npub.substring(0, 8) + '…' + npub.substring(59),
          lastUpdated: 0,
          metadata: {},
        }
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
