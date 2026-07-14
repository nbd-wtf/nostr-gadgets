import type { NostrEvent } from '@nostr/tools/pure'
import { loadRelayList } from './lists'
import { purgatory } from './global'

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

export function getNumberOfRelaysPerUser(totalPubkeys: number): number {
  return totalPubkeys < 100 ? 4 : totalPubkeys < 800 ? 3 : totalPubkeys < 1200 ? 2 : 1
}

export function shuffle<I>(arr: I[]) {
  for (let i = 0; i < arr.length; i++) {
    let prev = Math.round(Math.random() * i)
    let tmp = arr[i]
    arr[i] = arr[prev]
    arr[prev] = tmp
  }
}

/**
 * Returns the top famous relays among the given pubkeys
 **/
export async function globalism(pubkeys: string[]): Promise<string[]> {
  const list: [number, string][] = []
  const rls = await Promise.all(pubkeys.map(pk => loadRelayList(pk)))
  for (let i = 0; i < rls.length; i++) {
    for (let j = 0; j < rls[i].items.length; j++) {
      try {
        const url = rls[i].items[j].url
        const allowed = purgatory.allowConnectingToRelay(url, ['read', [{}]])
        if (!allowed) continue
        let curr = list.find(rs => rs[1] === url)
        if (!curr) {
          curr = [0, url]
          list.push(curr)
        }
        curr[0] += 20 / rls[i].items.length
      } catch (_err) {
        /***/
      }
    }
  }
  list.sort(([a], [b]) => b - a)
  return list.map(rs => rs[1])
}
