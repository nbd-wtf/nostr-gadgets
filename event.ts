/**
 * @module
 * Contains functions for fetching a specific Nostr event by id, EventPointer, AddressPointer or NIP-19 code.
 */

import type { NostrEvent } from '@nostr/tools/core'
import type { Filter } from '@nostr/tools/filter'
import { AddressPointer, EventPointer, decode } from '@nostr/tools/nip19'
import { normalizeURL } from '@nostr/tools/utils'
import { isReplaceableKind, isAddressableKind } from '@nostr/tools/kinds'

import { pool, label } from './global'
import { ARBITRARY_IDS_RELAYS, BIG_RELAYS_DO_NOT_USE_EVER } from './defaults'
import { isHex32 } from './utils'
import { loadRelayList } from './lists'
import { RedEventStore } from './redstore'

const BIG_RELAYS = [...ARBITRARY_IDS_RELAYS, ...BIG_RELAYS_DO_NOT_USE_EVER]
const TWO_DAYS = 60 * 60 * 24 * 2

export async function loadEvent(
  idOrCode: string | EventPointer | AddressPointer,
  store?: RedEventStore,
): Promise<NostrEvent | undefined> {
  let filter: Filter | undefined
  let relayHints: string[] = []
  let authorHint: string | undefined

  if (typeof idOrCode === 'string') {
    if (isHex32(idOrCode)) {
      filter = { ids: [idOrCode] }
    } else {
      const decoded = decode(idOrCode)
      switch (decoded.type) {
        case 'note':
          filter = { ids: [decoded.data] }
          break
        case 'nevent':
          filter = { ids: [decoded.data.id] }
          if (decoded.data.relays) relayHints = decoded.data.relays
          if (decoded.data.author) authorHint = decoded.data.author
          break
        case 'naddr':
          filter = {
            authors: [decoded.data.pubkey],
            kinds: [decoded.data.kind],
            limit: 1,
          }
          authorHint = decoded.data.pubkey
          if (decoded.data.identifier) {
            filter['#d'] = [decoded.data.identifier]
          }
          if (decoded.data.relays) relayHints = decoded.data.relays
          break
        default:
          throw new Error(`can't fetch ${idOrCode}`)
      }
    }
  } else if ('id' in idOrCode) {
    filter = { ids: [idOrCode.id] }
    if (idOrCode.relays) relayHints = idOrCode.relays
    if (idOrCode.author) authorHint = idOrCode.author
  } else {
    filter = {
      authors: [idOrCode.pubkey],
      kinds: [idOrCode.kind],
      limit: 1,
    }
    authorHint = idOrCode.pubkey
    if (idOrCode.identifier) {
      filter['#d'] = [idOrCode.identifier]
    }
    if (idOrCode.relays) relayHints = idOrCode.relays
  }

  if (!filter) {
    throw new Error(`can't fetch ${typeof idOrCode === 'string' ? idOrCode : 'pointer'}`)
  }

  const now_ = Math.round(Date.now() / 1000)
  const isReplaceable =
    filter.kinds?.length === 1 &&
    filter.authors?.length === 1 &&
    (isReplaceableKind(filter.kinds[0]) || (isAddressableKind(filter.kinds[0]) && filter['#d']?.length === 1))

  let storedEvent: NostrEvent | undefined
  if (store) {
    if (isReplaceable) {
      const dtag = filter['#d']?.[0]
      const stored = (await store.loadReplaceables([[filter.kinds![0], filter.authors![0], dtag]])) as unknown as [
        number | undefined,
        NostrEvent | undefined,
      ][]
      const [lastAttempt, event] = stored[0]

      const isEmpty = event?.id === '0'.repeat(64)

      if (event) {
        if (isEmpty) {
          if (lastAttempt !== undefined && lastAttempt > now_ - TWO_DAYS) {
            return undefined
          }
        } else if (lastAttempt !== undefined && lastAttempt > now_ - TWO_DAYS) {
          return event
        }
      }

      storedEvent = !isEmpty && event ? event : undefined
    } else if (filter.ids?.length === 1) {
      for (const evt of await store.queryEvents(filter, 1)) {
        return evt
      }
    } else {
      throw new Error(`can only fetch replaceables or events by id, got ${filter}`)
    }
  }

  const authorRelays = authorHint ? loadRelayList(authorHint) : undefined

  const normalizedHints = relayHints.map(normalizeURL)
  if (normalizedHints.length) {
    const event = await pool.get(normalizedHints, filter, { label: `${label ? label + ':' : ''}load-event-1` })
    if (event) {
      store?.saveEvent(event, {
        lastAttempt: now_,
        seenOn: Array.from(pool.seenOn.get(event.id) || []).map(r => r.url),
      })
      return event
    }
  }

  let authorRelaysUrls: string[] = []
  if (authorRelays) {
    authorRelaysUrls = (await authorRelays).items
      .filter(r => r.write && !normalizedHints.includes(r.url))
      .map(r => r.url)
    if (authorRelaysUrls.length) {
      const event = await pool.get(authorRelaysUrls, filter, { label: `${label ? label + ':' : ''}load-event-2` })
      if (event) {
        store?.saveEvent(event, {
          lastAttempt: now_,
          seenOn: Array.from(pool.seenOn.get(event.id) || []).map(r => r.url),
        })
        return event
      }
    }
  }

  const bigRelays = BIG_RELAYS.filter(br => !(normalizedHints.includes(br) || authorRelaysUrls.includes(br)))
  if (bigRelays.length) {
    const event = await pool.get(bigRelays, filter, { label: `${label ? label + ':' : ''}load-event-3` })
    if (event) {
      store?.saveEvent(event, {
        lastAttempt: now_,
        seenOn: Array.from(pool.seenOn.get(event.id) || []).map(r => r.url),
      })
      return event
    }
  }

  if (isReplaceable) {
    // we have an old stored, use it
    if (storedEvent) return storedEvent

    // save our failed attempt
    const dtag = filter['#d']?.[0]
    store?.saveEvent(
      {
        id: '0'.repeat(64),
        pubkey: filter.authors![0],
        kind: filter.kinds![0],
        sig: '0'.repeat(128),
        tags: dtag ? [['d', dtag]] : [],
        created_at: 0,
        content: '',
      } as NostrEvent,
      { lastAttempt: now_ },
    )
  }

  return undefined
}
