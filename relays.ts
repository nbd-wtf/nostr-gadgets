/**
 * @module
 * Contains helpers for fetching and caching relay information documents using NIP-11.
 */

import { fetchRelayInformation } from '@nostr/tools/nip11'
import DataLoader from './dataloader'
import { normalizeURL } from '@nostr/tools/utils'

export type RelayInfoDocument = {
  url: string
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

const STORAGE_KEY = '@nostr/gadgets/relays'

type RelayInfoCache = {
  [url: string]: CachedRelayInfo
}

function loadCache(): RelayInfoCache {
  const raw = window.localStorage.getItem(STORAGE_KEY)
  if (!raw) return {}
  try {
    const parsed = JSON.parse(raw)
    if (parsed && typeof parsed === 'object') return parsed as RelayInfoCache
  } catch {
    return {}
  }
  return {}
}

function saveCache(cache: RelayInfoCache): void {
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(cache))
}

function saveCacheEntry(url: string, entry: CachedRelayInfo): void {
  const cache = loadCache()
  cache[url] = entry
  saveCache(cache)
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

    // try to get cached data first for all URLs at once
    let cached: (CachedRelayInfo | undefined)[] = []
    try {
      const cache = loadCache()
      cached = normalizedUrls.map(url => cache[url])
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
        saveCacheEntry(norm, { info: req.refreshStyle, timestamp: Date.now() })
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
              url: norm,
              name: doc.name,
              description: doc.description,
              contact: doc.contact,
              self: (doc as any).self,
              pubkey: doc.pubkey,
              software: doc.software,
              version: doc.version,
              supported_nips: doc.supported_nips,
            } as RelayInfoDocument

            saveCacheEntry(norm, { info, timestamp: Date.now() })
            return info
          })
          .catch(() => {
            // save failure (but fake it as a day in the future, so it refreshes sooner)
            saveCacheEntry(norm, { info: null, timestamp: Date.now() + 1000 * 60 * 60 * 24 * 1 })

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
