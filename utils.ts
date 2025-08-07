import type { NostrEvent } from '@nostr/tools/pure'
import LRUCache from '@fiatjaf/lru-cache/lru-cache'
import type { CacheMap } from 'dataloader'

export function dataloaderCache<V>(): CacheMap<string, Promise<V>> {
  const cache = new LRUCache<string, undefined | Promise<V>>(2000)
  ;(cache as any).delete = (key: string) => {
    cache.set(key, undefined)
  }
  return cache as unknown as CacheMap<string, Promise<V>>
}

/**
 * Gets the value of the first tag with the given name -- or returns a default value.
 */
export function getTagOr(event: NostrEvent, tagName: string, dflt: string = ''): string {
  return event.tags.find(([t]) => t === tagName)?.[1] || dflt
}

/**
 * Checks if a string is a 64-char lowercase hex string as most Nostr ids and pubkeys.
 */
export function isHex32(input: string): boolean {
  for (let i = 0; i < 64; i++) {
    let cc = input.charCodeAt(i)
    if (isNaN(cc) || cc < 48 || cc > 102 || (cc > 57 && cc < 97)) {
      return false
    }
  }
  return true
}

/**
 * Checks if a string matches the structure of an "address", i.e. "<kind>:<pubkey>:<arbitrary-string>".
 */
export function isATag(input: string): boolean {
  return Boolean(input.match(/^\d+:[0-9a-f]{64}:[^:]+$/))
}

/**
 * Just an util to print relay URLs more prettily.
 */
export function urlWithoutScheme(url: string): string {
  return url.replace('wss://', '').replace(/\/+$/, '')
}

/**
 * Creates a new array and copies items over omitting duplicates.
 */
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

export function appendUnique<I>(target: I[], ...newItem: I[]) {
  let max = newItem.length
  for (let i = 0; i < max; i++) {
    let ni = newItem[i]
    if (target.indexOf(ni) === -1) target.push(ni)
  }
}

export function shuffle<I>(arr: I[]) {
  for (let i = 0; i < arr.length; i++) {
    let prev = Math.round(Math.random() * i)
    let tmp = arr[i]
    arr[i] = arr[prev]
    arr[prev] = tmp
  }
}
