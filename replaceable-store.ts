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

const STORAGE_KEY = '@nostr/gadgets/replaceables'

// Storage shape: { [pubkey]: { [kind]: [lastAttempt, event | events[]] } }
type StoredEntry = [lastAttempt: number, event: NostrEvent | undefined]
type StoredBundle = [lastAttempt: number, events: NostrEvent[]]
type StorageData = {
  [pubkey: string]: {
    [kind: string]: StoredEntry | StoredBundle
  }
}

function isAddressable(kind: number): boolean {
  return kind >= 30000 && kind < 40000
}

/**
 * A ReplaceableStore implementation using window.localStorage.
 * All data stored in a single key with structure: {pubkey: {kind: [lastAttempt, event(s)]}}
 * For regular replaceables: [lastAttempt, event | undefined]
 * For 3xxxx kinds: [lastAttempt, events[]] (bundle)
 */
export class LocalStorageReplaceableStore implements ReplaceableStore {
  private loadStorage(): StorageData {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) return {}
    try {
      return JSON.parse(raw)
    } catch {
      return {}
    }
  }

  private saveStorage(data: StorageData): void {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(data))
  }

  async loadReplaceables(
    specs: [kind: number, pubkey: string, dtag?: string][],
  ): Promise<
    (
      | [lastAttempt: number | undefined, events: NostrEvent[]]
      | [lastAttempt: number | undefined, event: NostrEvent | undefined]
    )[]
  > {
    const storage = this.loadStorage()

    return specs.map(([kind, pubkey, dtag]) => {
      const pubkeyData = storage[pubkey]
      if (!pubkeyData) {
        return isAddressable(kind) && dtag === undefined ? [undefined, []] : [undefined, undefined]
      }

      const entry = pubkeyData[kind]
      if (!entry) {
        return isAddressable(kind) && dtag === undefined ? [undefined, []] : [undefined, undefined]
      }

      if (isAddressable(kind) && dtag === undefined) {
        // for 3xxxx kinds without dtag, return bundle (array of all events)
        const [lastAttempt, events] = entry as StoredBundle
        return [lastAttempt, events]
      } else if (isAddressable(kind)) {
        // for 3xxxx kinds with dtag, return single event from bundle
        const [lastAttempt, events] = entry as StoredBundle
        const event = events.find(e => (e.tags.find(t => t[0] === 'd')?.[1] || '') === dtag)
        return [lastAttempt, event]
      } else {
        // regular replaceable - single event storage
        const [lastAttempt, event] = entry as StoredEntry
        // skip placeholder events with all-zero IDs
        if (event && event.id === '0'.repeat(64)) {
          return [lastAttempt, undefined]
        }
        return [lastAttempt, event]
      }
    })
  }

  async saveEvent(event: NostrEvent, options?: { lastAttempt?: number }): Promise<boolean> {
    const storage = this.loadStorage()
    const now = options?.lastAttempt || Math.round(Date.now() / 1000)
    const kind = event.kind
    const pubkey = event.pubkey

    if (!storage[pubkey]) {
      storage[pubkey] = {}
    }

    if (isAddressable(kind)) {
      const dtag = event.tags.find(t => t[0] === 'd')?.[1] || ''
      // for 3xxxx kinds, store in bundle
      let bundle: StoredBundle = storage[pubkey][kind] as StoredBundle
      if (!bundle) {
        bundle = [now, []]
      }

      // handle placeholder events with all-zero IDs - just update lastAttempt
      if (event.id === '0'.repeat(64) && event.created_at === 0) {
        bundle[0] = now
        storage[pubkey][kind] = bundle
        this.saveStorage(storage)
        return false
      }

      // find existing event with same dtag
      const existingIndex = bundle[1].findIndex(e => (e.tags.find(t => t[0] === 'd')?.[1] || '') === dtag)

      if (existingIndex >= 0) {
        const existing = bundle[1][existingIndex]
        if (existing.created_at >= event.created_at) {
          // Existing is newer, just update lastAttempt
          bundle[0] = now
          storage[pubkey][kind] = bundle
          this.saveStorage(storage)
          return false
        }
        // replace with newer event
        bundle[1][existingIndex] = event
      } else {
        // add new event to bundle
        bundle[1].push(event)
      }

      bundle[0] = now
      storage[pubkey][kind] = bundle
      this.saveStorage(storage)
      return true
    } else {
      // regular replaceable - single event storage
      const existing = storage[pubkey][kind] as StoredEntry | undefined

      // handle placeholder events with all-zero IDs - just save lastAttempt marker
      if (event.id === '0'.repeat(64) && event.created_at === 0) {
        if (existing) {
          existing[0] = now
          storage[pubkey][kind] = existing
        } else {
          storage[pubkey][kind] = [now, event]
        }
        this.saveStorage(storage)
        return false
      }

      // check if we already have a newer event
      if (existing && existing[1] && existing[1].created_at >= event.created_at) {
        // update lastAttempt
        existing[0] = now
        storage[pubkey][kind] = existing
        this.saveStorage(storage)
        return false
      }

      storage[pubkey][kind] = [now, event]
      this.saveStorage(storage)
      return true
    }
  }

  async deleteReplaceable(kind: number, pubkey: string) {
    const storage = this.loadStorage()
    if (storage[pubkey]) {
      delete storage[pubkey][kind]
      // clean up empty pubkey objects
      if (Object.keys(storage[pubkey]).length === 0) {
        delete storage[pubkey]
      }
      this.saveStorage(storage)
    }
  }
}

/**
 * Default replaceable store instance using localStorage.
 */
export const defaultReplaceableStore: ReplaceableStore = new LocalStorageReplaceableStore()
