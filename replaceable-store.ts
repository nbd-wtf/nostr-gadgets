/**
 * @module
 * Contains the ReplaceableStore interface and a default localStorage-based implementation.
 */

import type { NostrEvent } from '@nostr/tools/pure'

/**
 * Interface for storing and retrieving replaceable Nostr events.
 * This is used by metadata.ts, lists.ts, and sets.ts for caching.
 *
 * For kinds 30000-39999 (parameterized replaceables), if dtag is undefined,
 * all events for that kind/pubkey are returned as a bundle.
 */
export interface ReplaceableStore {
  loadReplaceables(
    specs: [kind: number, pubkey: string, dtag?: string][],
  ): Promise<
    (
      | [lastAttempt: number | undefined, events: NostrEvent[]]
      | [lastAttempt: number | undefined, event: NostrEvent | undefined]
    )[]
  >
  saveEvent(event: NostrEvent, options?: { lastAttempt?: number }): Promise<boolean>
  deleteReplaceable(kind: number, pubkey: string): Promise<void>
}

// replaceables (kind 0, 3, 1xxxx, etc.)
type StoredEntry = {
  event: NostrEvent
  lastAttempt: number
}

// addressables (kind 3xxxx) - stored as a bundle
type StoredBundle = {
  events: NostrEvent[]
  lastAttempt: number
}

function isAddressable(kind: number): boolean {
  return kind >= 30000 && kind < 40000
}

function makeKey(kind: number, pubkey: string): string {
  return `@gadgets/repl:${kind}:${pubkey}`
}

/**
 * A ReplaceableStore implementation using window.localStorage.
 * For regular replaceables: stores as "<kind>:<pubkey>" -> {event, lastAttempt}
 * For 3xxxx kinds: stores as "<kind>:<pubkey>" -> {events[], lastAttempt} (bundle)
 */
export class LocalStorageReplaceableStore implements ReplaceableStore {
  async loadReplaceables(
    specs: [kind: number, pubkey: string, dtag?: string][],
  ): Promise<
    (
      | [lastAttempt: number | undefined, events: NostrEvent[]]
      | [lastAttempt: number | undefined, event: NostrEvent | undefined]
    )[]
  > {
    return specs.map(([kind, pubkey, dtag]) => {
      if (isAddressable(kind) && dtag === undefined) {
        // for 3xxxx kinds without dtag, return bundle (array of all events)
        const bundleKey = makeKey(kind, pubkey)
        const raw = window.localStorage.getItem(bundleKey)
        if (!raw) return [undefined, []]
        try {
          const bundle: StoredBundle = JSON.parse(raw)
          return [bundle.lastAttempt, bundle.events]
        } catch {
          return [undefined, []]
        }
      } else if (isAddressable(kind)) {
        // for 3xxxx kinds with dtag, return single event from bundle
        const bundleKey = makeKey(kind, pubkey)
        const raw = window.localStorage.getItem(bundleKey)
        if (!raw) return [undefined, undefined]
        try {
          const bundle: StoredBundle = JSON.parse(raw)
          const event = bundle.events.find(e => (e.tags.find(t => t[0] === 'd')?.[1] || '') === dtag)
          return [bundle.lastAttempt, event]
        } catch {
          return [undefined, undefined]
        }
      } else {
        // regular replaceable - single event storage
        const key = makeKey(kind, pubkey)
        const raw = window.localStorage.getItem(key)
        if (!raw) return [undefined, undefined]
        try {
          const entry: StoredEntry = JSON.parse(raw)
          // skip placeholder events with all-zero IDs
          if (entry.event.id === '0'.repeat(64)) {
            return [entry.lastAttempt, undefined]
          }
          return [entry.lastAttempt, entry.event]
        } catch {
          return [undefined, undefined]
        }
      }
    })
  }

  async saveEvent(event: NostrEvent, options?: { lastAttempt?: number }): Promise<boolean> {
    const dtag = event.tags.find(t => t[0] === 'd')?.[1] || ''
    const now = options?.lastAttempt || Math.round(Date.now() / 1000)

    if (isAddressable(event.kind)) {
      // for 3xxxx kinds, store in bundle
      const bundleKey = makeKey(event.kind, event.pubkey)
      const raw = window.localStorage.getItem(bundleKey)
      let bundle: StoredBundle = { events: [], lastAttempt: now }

      if (raw) {
        try {
          bundle = JSON.parse(raw)
        } catch {
          // Ignore parse errors, start fresh
        }
      }

      // handle placeholder events with all-zero IDs - just update lastAttempt
      if (event.id === '0'.repeat(64) && event.created_at === 0) {
        bundle.lastAttempt = now
        window.localStorage.setItem(bundleKey, JSON.stringify(bundle))
        return false
      }

      // find existing event with same dtag
      const existingIndex = bundle.events.findIndex(e => (e.tags.find(t => t[0] === 'd')?.[1] || '') === dtag)

      if (existingIndex >= 0) {
        const existing = bundle.events[existingIndex]
        if (existing.created_at >= event.created_at) {
          // Existing is newer, just update lastAttempt
          bundle.lastAttempt = now
          window.localStorage.setItem(bundleKey, JSON.stringify(bundle))
          return false
        }
        // replace with newer event
        bundle.events[existingIndex] = event
      } else {
        // add new event to bundle
        bundle.events.push(event)
      }

      bundle.lastAttempt = now
      window.localStorage.setItem(bundleKey, JSON.stringify(bundle))
      return true
    } else {
      // regular replaceable - single event storage
      const key = makeKey(event.kind, event.pubkey)

      // handle placeholder events with all-zero IDs - just save lastAttempt marker
      if (event.id === '0'.repeat(64) && event.created_at === 0) {
        const raw = window.localStorage.getItem(key)
        if (raw) {
          try {
            const entry: StoredEntry = JSON.parse(raw)
            entry.lastAttempt = now
            window.localStorage.setItem(key, JSON.stringify(entry))
          } catch {
            // ignore parse errors
          }
        } else {
          // no existing entry - save a marker with a placeholder event
          const marker: StoredEntry = { event, lastAttempt: now }
          window.localStorage.setItem(key, JSON.stringify(marker))
        }
        return false
      }

      // check if we already have a newer event
      const raw = window.localStorage.getItem(key)
      if (raw) {
        try {
          const existing: StoredEntry = JSON.parse(raw)
          if (existing.event.created_at >= event.created_at) {
            // update lastAttempt if provided
            existing.lastAttempt = now
            window.localStorage.setItem(key, JSON.stringify(existing))
            return false
          }
        } catch {
          // ignore parse errors, will overwrite
        }
      }

      const entry: StoredEntry = { event, lastAttempt: now }
      window.localStorage.setItem(key, JSON.stringify(entry))
      return true
    }
  }

  async deleteReplaceable(kind: number, pubkey: string) {
    if (isAddressable(kind)) {
      // delete the bundle
      window.localStorage.removeItem(makeKey(kind, pubkey))
    } else {
      // delete the single entry
      window.localStorage.removeItem(makeKey(kind, pubkey))
    }
  }
}

/**
 * Default replaceable store instance using localStorage.
 */
export const defaultReplaceableStore: ReplaceableStore = new LocalStorageReplaceableStore()
