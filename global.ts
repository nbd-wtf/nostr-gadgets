import { SimplePool } from '@nostr/tools/pool'
import { Purgatory } from './purgatory'
import { type ReplaceableStore, defaultReplaceableStore } from './replaceable-store'

export type { ReplaceableStore }

export const purgatory: Purgatory = new Purgatory()

/**
 * pool is a global used by all other functions in this library. Feel free to use it directly in your app.
 */
export let pool: SimplePool = new SimplePool({
  allowConnectingToRelay: purgatory.allowConnectingToRelay.bind(purgatory),
  onRelayConnectionFailure: purgatory.onRelayConnectionFailure.bind(purgatory),
  onRelayConnectionSuccess: purgatory.onRelayConnectionSuccess.bind(purgatory),
})

/**
 * setPool sets the global pool -- do not use unless you know what you're doing.
 */
export function setPool(p: SimplePool) {
  pool = p
}

/**
 * replaceableStore is a global store used for caching replaceable Nostr events.
 * By default, it uses localStorage.
 */
export let replaceableStore: ReplaceableStore = defaultReplaceableStore

/**
 * setReplaceableStore sets the global replaceable store.
 */
export function setReplaceableStore(store: ReplaceableStore) {
  replaceableStore = store
}

/**
 * label is prepended to all pool subscription labels for debugging.
 */
export let label: string = ''

/**
 * setLabel sets the global label prepended to pool subscription labels.
 */
export function setLabel(l: string) {
  label = l
}

export type RelayPicker = (items: Array<{ url: string; write: boolean }>, kind: number | number[]) => string[]

export function filterPurgatory(
  items: Array<{ url: string; write: boolean }>,
  kind: number | number[],
): Array<{ url: string; write: boolean }> {
  const kinds = Array.isArray(kind) ? kind : [kind]
  return items.filter(({ write, url }) => write && purgatory.allowConnectingToRelay(url, ['read', [{ kinds }]]))
}

export let relayPicker: RelayPicker = (items, kind) => {
  return items
    .filter(({ write }) => write)
    .map(({ url }) => url)
    .slice(0, 2)
}

export function setRelayPicker(rp: RelayPicker) {
  relayPicker = rp
}
