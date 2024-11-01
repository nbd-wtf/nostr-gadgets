import type { NostrEvent } from '@nostr/tools/pure'
import { LRUCache } from 'mnemonist'
import type { CacheMap } from 'dataloader'

export function dataloaderCache<V>(): CacheMap<string, Promise<V>> {
  const cache = new LRUCache<string, Promise<V>>(2000)
  ;(cache as any).delete = (_key: string) => {}
  return cache as unknown as CacheMap<string, Promise<V>>
}

export function getTagOr(event: NostrEvent, tagName: string, dflt: string = '') {
  return event.tags.find(([t]) => t === tagName)?.[1] || dflt
}

export function isHex32(input: string): boolean {
  return Boolean(input.match(/^[a-f0-9]{64}$/))
}

export function isATag(input: string): boolean {
  return Boolean(input.match(/^\d+:[0-9a-f]{64}:[^:]+$/))
}

export function urlWithoutScheme(url: string): string {
  return url.replace('wss://', '').replace(/\/+$/, '')
}

export function unique<A>(...arrs: A[][]): A[] {
  const result: A[] = []
  for (let i = 0; i < arrs.length; i++) {
    const arr = arrs[i]
    for (let j = 0; j < arr.length; j++) {
      const item = arr[j]
      if (result.indexOf(item) !== -1) continue
      result.push(item)
    }
  }
  return result
}

export function identity<A>(a: A): boolean {
  return Boolean(a)
}
