/**
 * @module
 * Contains helpers for fetching and caching relay information documents using NIP-11.
 */

import { fetchRelayInformation } from '@nostr/tools/nip11'
import LRUCache from '@fiatjaf/lru-cache/lru-cache'
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

const cache = new LRUCache<string, RelayInfoDocument | null>(2000)

export async function loadRelayInfo(url: string): Promise<RelayInfoDocument | null> {
  const norm = normalizeURL(url)
  const cached = cache.get(norm)
  if (cached !== undefined) return cached

  try {
    const doc = await fetchRelayInformation(norm)
    if (!doc || typeof doc !== 'object') throw new Error('invalid response')
    const info: RelayInfoDocument = {
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
    }
    cache.set(norm, info)
    return info
  } catch {
    cache.set(norm, null)
    return null
  }
}
