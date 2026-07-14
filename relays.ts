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
  supported_methods?: string[]
  software?: string
  version?: string
}

export async function supportedMethods(url: string): Promise<string[] | null> {
  const httpUrl = normalizeURL(url).replace('wss://', 'https://').replace('ws://', 'http://')
  try {
    const res = await fetch(httpUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/nostr+json+rpc' },
      body: JSON.stringify({ method: 'supportedmethods', params: [] }),
      signal: AbortSignal.timeout(3000),
    })
    if (!res.ok) return null
    const json = await res.json()
    if (json.error) return null
    if (!Array.isArray(json.result)) return null
    return json.result as string[]
  } catch {
    return null
  }
}

const cache = new LRUCache<string, RelayInfoDocument | null>(2000)

async function favicon(url: string): Promise<string | undefined> {
  const httpUrl = normalizeURL(url).replace('wss://', 'https://').replace('ws://', 'http://')
  const u = new URL(httpUrl)
  const fav = `${u.protocol}//${u.hostname}/favicon.ico`
  return new Promise(resolve => {
    const img = new Image()
    img.onload = () => resolve(fav)
    img.onerror = () => resolve(undefined)
    img.src = fav
  })
}

export async function loadRelayInfo(url: string): Promise<RelayInfoDocument | null> {
  const norm = normalizeURL(url)
  const cached = cache.get(norm)
  if (cached !== undefined) return cached

  try {
    const [doc, methods] = await Promise.all([fetchRelayInformation(norm), supportedMethods(norm)])
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
      icon: doc.icon || (await favicon(norm)),
      contact: doc.contact,
      self: (doc as any).self,
      pubkey: doc.pubkey,
      software: doc.software,
      version: doc.version,
      supported_nips: doc.supported_nips,
      supported_methods: methods ?? undefined,
    }
    cache.set(norm, info)
    return info
  } catch {
    cache.set(norm, null)
    return null
  }
}
