/**
 * @module
 * Contains helpers for fetching and caching relay information documents using NIP-11.
 */

import { fetchRelayInformation } from '@nostr/tools/nip11'
import { createStore, getMany, set, UseStore } from 'idb-keyval'
import DataLoader from './dataloader'
import { normalizeURL } from '@nostr/tools/utils'

type RelayInfoDocument = {
  name?: string
  description?: string
  pubkey?: string
  self?: string
  contact?: string
  supported_nips?: number[]
  software?: string
  version?: string
}

interface CachedRelayInfo {
  info: RelayInfoDocument | null
  timestamp: number
}

let store: UseStore

/**
 * Loads and initializes the IndexedDB store for caching relay information.
 */
function getStore(): UseStore {
  if (!store) {
    store = createStore('@nostr/gadgets/relays', 'cache')
  }
  return store
}

type RelayInfoRequest = {
  url: string
  refreshStyle?: boolean | RelayInfoDocument | null
}

const relayInfoLoader = new DataLoader<
  RelayInfoRequest,
  Promise<RelayInfoDocument | null> | RelayInfoDocument | null,
  string
>(
  async reqs => {
    const normalizedUrls = reqs.map(r => normalizeURL(r.url))
    const currentStore = getStore()

    // try to get cached data first for all URLs at once
    let cached: (CachedRelayInfo | undefined)[] = []
    try {
      cached = await getMany(normalizedUrls, currentStore)
    } catch (error) {
      console.warn('error reading relay info from cache:', error)
      cached = new Array(reqs.length).fill(undefined)
    }

    const results: Array<RelayInfoDocument | null | Promise<RelayInfoDocument | null>> = []

    for (let i = 0; i < reqs.length; i++) {
      const req = reqs[i]
      const norm = normalizedUrls[i]
      const cachedInfo = cached[i]

      if (typeof req.refreshStyle === 'object') {
        // just return what we received
        results[i] = req.refreshStyle
        await set(norm, req.refreshStyle, currentStore)
      } else if (
        (cachedInfo && Date.now() < cachedInfo.timestamp + 2 * 24 * 60 * 60 * 1000) ||
        req.refreshStyle === false
      ) {
        // cache is still fresh (or we are forced to not refresh), return cached data
        results[i] = cachedInfo?.info || null
      } else {
        // need to fetch fresh data
        results[i] = fetchRelayInformation(norm)
          .then(async doc => {
            const info = {
              name: doc.name,
              description: doc.description,
              contact: doc.contact,
              self: (doc as any).self,
              pubkey: doc.pubkey,
              software: doc.software,
              version: doc.version,
            } as RelayInfoDocument

            await set(norm, { info, timestamp: Date.now() }, currentStore)
            return info
          })
          .catch(() => {
            // save failure (but fake it as a day in the future, so it refreshes sooner)
            set(norm, { info: null, timestamp: Date.now() + 1000 * 60 * 60 * 24 * 1 }, currentStore)

            // return stale data
            return cachedInfo?.info || null
          })
      }
    }

    return results
  },
  {
    cacheKeyFn: req => normalizeURL(req.url),
  },
)

/**
 * Fetches relay information document for a given URL, with caching and batching.
 *
 * @param url - The relay URL to fetch information for
 * @returns Promise that resolves to the relay information document or null if fetch fails
 */
export async function loadRelayInfo(
  url: string,
  refreshStyle?: boolean | RelayInfoDocument,
): Promise<RelayInfoDocument | null> {
  return relayInfoLoader.load({ url, refreshStyle })
}
