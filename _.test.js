import { expect, test, describe, beforeAll } from 'vitest'

import { loadNostrUser } from './metadata'
import { loadRelayList, loadFollowsList, makeListFetcher, itemsFromTags, isFresh } from './lists'
import { loadWoT, globalism } from './wot'
import { loadRelaySets } from './sets'
import { outboxFilterRelayBatch } from './outbox'
import { isHex32 } from './utils'
import { eventStore } from './global'
import { RedEventStore } from './redstore'

const TEST_PUBKEYS = {
  fiatjaf: '3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d',
  jb55: '32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245',
  pablo: 'fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52',
  daniele: '7bdef7be22dd8e59f4600e044aa53a1cf975a9dc7d27df5833bc77db784a5805',
}

describe('replaceables', () => {
  beforeAll(async () => {
    const dbs = await RedEventStore.list()
    console.log('before test:', dbs)
    for (let db of dbs) {
      if (db.name === eventStore.name) await RedEventStore.delete(db.name)
    }
  })

  test('loadNostrUser', async () => {
    const users = await Promise.all([
      loadNostrUser(TEST_PUBKEYS.fiatjaf),
      loadNostrUser(TEST_PUBKEYS.jb55),
      loadNostrUser(TEST_PUBKEYS.pablo),
      loadNostrUser(TEST_PUBKEYS.daniele),
    ])

    expect(users.length).toEqual(4)
    expect(users[0].pubkey).toEqual(TEST_PUBKEYS.fiatjaf)
    expect(users[0].shortName).toEqual('fiatjaf')
    expect(users[1].pubkey).toEqual(TEST_PUBKEYS.jb55)
    expect(users[2].pubkey).toEqual(TEST_PUBKEYS.pablo)
    expect(users[3].shortName).toEqual('dtonon')
  })

  test('loadRelayList', async () => {
    const results = await Promise.all([
      loadRelayList(TEST_PUBKEYS.fiatjaf),
      loadRelayList(TEST_PUBKEYS.jb55),
      loadRelayList(TEST_PUBKEYS.pablo),
    ])

    expect(results.length).toEqual(3)
    results.forEach(result => {
      expect(result.items).toBeTruthy()
      expect(Array.isArray(result.items)).toEqual(true)
    })
  })

  test('loadFollowsList_refreshStyle', async () => {
    const pubkey = 'd9e88ea36280ea821c474a5b7d2f7427b4250e2954f49cb38e9ee4684dadf1ad'

    // 1. call with false, should return empty
    let result = await loadFollowsList(pubkey, [], false)
    expect(result.items).toEqual([])
    expect(result.event).toBe(null)
    expect(result[isFresh]).toBeFalsy()

    // 2. call with an event to force it to be cached for that pubkey
    const fakeEvent = {
      id: '2eb602897eef463da34af2f31784d0fe385b0f955ad663a9a9402a9e642037d6',
      pubkey,
      created_at: Math.floor(Date.now() / 1000),
      kind: 3,
      tags: [['p', TEST_PUBKEYS.fiatjaf]],
      content: '',
      sig: 'ac354a9a2f716690f0183189867ca07c338bb1692ebc4b3b6482c9c87a17e7750dfb21a4c40934415fb2858fb01fb1d1a7f074d6e9d04941ed9fc56658a64e2a',
    }
    result = await loadFollowsList(pubkey, [], fakeEvent)
    expect(result.items).toEqual([TEST_PUBKEYS.fiatjaf])
    expect(result.event.id).toEqual(fakeEvent.id)
    expect(result[isFresh]).toBeTruthy()

    // 3. call with false again, should return the same
    result = await loadFollowsList(pubkey, [], false)
    expect(result.items).toEqual([TEST_PUBKEYS.fiatjaf])
    expect(result.event.id).toEqual(fakeEvent.id)
    expect(result[isFresh]).toBeFalsy()

    // 3. call with false again, should return the same
    result = await loadFollowsList(pubkey, [], false)
    expect(result.items).toEqual([TEST_PUBKEYS.fiatjaf])
    expect(result.event.id).toEqual(fakeEvent.id)
    expect(result[isFresh]).toBeFalsy()

    // 4. make a new Fetcher so the memory cache is not reused
    const newFetcher = makeListFetcher(
      3,
      [],
      itemsFromTags(tag => {
        if (tag.length >= 2 && tag[0] === 'p' && isHex32(tag[1])) {
          return tag[1]
        }
      }),
    )
    // Call with false, should still return that fake event
    result = await newFetcher(pubkey, [], false)
    expect(result.items).toEqual([TEST_PUBKEYS.fiatjaf])
    expect(result.event.id).toEqual(fakeEvent.id)
    expect(result[isFresh]).toBeFalsy()
  })

  test('loadRelaySets', async () => {
    const result = await loadRelaySets(TEST_PUBKEYS.fiatjaf)

    expect(result['JGM9mue0UifwnpT8xQIPkScfqYpQACMR'].items.includes('wss://lockbox.fiatjaf.com/')).toBeTruthy()
    expect(result['f4qt86BG85u8POyWO6OMWznNg7innDxp'].items.includes('wss://pyramid.fiatjaf.com/')).toBeTruthy()
    expect(result.lastAttempt).toBeFalsy()
  })
})

describe('wot', () => {
  test('loadWoT', async () => {
    const wot = await loadWoT('96ae9c5b38add45212555f9ed039f2c3f2fba66e9ecd3d76d28746b0ad3df5a5')
    expect(wot.size).toBeGreaterThan(1000)
    expect(wot.has(TEST_PUBKEYS.jb55)).toBe(true)
    expect(wot.has(TEST_PUBKEYS.pablo)).toBe(true)
    expect(wot.has(TEST_PUBKEYS.daniele)).toBe(true)
  })
})

describe('globalism', () => {
  test('globalism', async () => {
    const relays = await globalism([TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.jb55, TEST_PUBKEYS.pablo, TEST_PUBKEYS.daniele])
    expect(relays.length).toBeGreaterThan(8)
    expect(relays.includes('wss://pyramid.fiatjaf.com/')).toBe(true)
    expect(relays.includes('wss://relay.damus.io/')).toBe(true)
  })
})

describe('outbox', () => {
  test('outbox-filter-batch', async () => {
    const result = await outboxFilterRelayBatch(
      [TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.jb55, TEST_PUBKEYS.pablo, TEST_PUBKEYS.daniele],
      {
        kinds: [1],
        limit: 10,
      },
    )

    expect(result.length).toBeGreaterThan(2)
    expect(result.length).toBeLessThan(10)

    const counts = {}

    result.forEach(decl => {
      expect(decl.filter.kinds).toEqual([1])
      expect(decl.filter.limit).toEqual(10)
      expect(Array.isArray(decl.filter.authors)).toBe(true)
      expect(decl.filter.authors.length).toBeGreaterThan(0)

      decl.filter.authors.forEach(pubkey => {
        counts[pubkey] = (counts[pubkey] || 0) + 1
      })
    })

    expect(counts[TEST_PUBKEYS.fiatjaf]).toBeGreaterThan(2)
    expect(counts[TEST_PUBKEYS.pablo]).toBeGreaterThan(2)
    expect(counts[TEST_PUBKEYS.jb55]).toBeGreaterThan(2)
    expect(counts[TEST_PUBKEYS.daniele]).toBeGreaterThan(2)
  })
})
