/**
 * @module
 * Contains helpers for fetching and caching relay information documents using NIP-11.
 */

import { fetchRelayInformation } from '@nostr/tools/nip11'
import DataLoader from './dataloader'
import { normalizeURL } from '@nostr/tools/utils'

export type RelayInfoDocument = {
  url: string
  urlSimple: string
  name?: string
  description?: string
  icon?: string
  pubkey?: string
  self?: string
  contact?: string
  supported_nips?: number[]
  software?: string
  version?: string
}

const relayInfoLoader = new DataLoader<string, RelayInfoDocument | null, string>(
  async urls => {
    return Promise.all(
      urls.map(async url => {
        const norm = normalizeURL(url)
        try {
          const doc = await fetchRelayInformation(norm)
          return {
            url: norm,
            urlSimple: norm
              .replace('wss://', '')
              .replace('ws://', '')
              .replace('https://', '')
              .replace('http://', '')
              .replace(/\/$/, ''),
            name: doc.name,
            description: doc.description,
            icon: doc.icon,
            contact: doc.contact,
            self: (doc as any).self,
            pubkey: doc.pubkey,
            software: doc.software,
            version: doc.version,
            supported_nips: doc.supported_nips,
          } as RelayInfoDocument
        } catch {
          return null
        }
      }),
    )
  },
  {
    cacheKeyFn: normalizeURL,
  },
)

/**
 * Fetches relay information document for a given URL, with caching via DataLoader.
 *
 * @param url - The relay URL to fetch information for
 * @param refreshStyle - If object, save and return it as-is. If true, bust cache and refetch.
 *                       If false, return cached or null. If undefined, use cached if available.
 * @returns Promise that resolves to the relay information document or null if fetch fails
 */
export async function loadRelayInfo(
  url: string,
  refreshStyle?: boolean | RelayInfoDocument,
): Promise<RelayInfoDocument | null> {
  if (typeof refreshStyle === 'object') {
    return refreshStyle as RelayInfoDocument
  }
  if (refreshStyle) {
    relayInfoLoader.clear(url)
  }
  if (refreshStyle === false) {
    const cached = relayInfoLoader._cacheMap.get(normalizeURL(url))
    if (cached) return cached
    return null
  }
  return relayInfoLoader.load(url)
}
