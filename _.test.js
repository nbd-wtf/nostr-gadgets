import { expect, test } from 'bun:test'
import 'fake-indexeddb/auto'

import { loadNostrUser } from './metadata'
import { loadRelayList } from './lists'
import { loadWoT, globalism } from './wot'
import { loadRelaySets } from './sets'

const TEST_PUBKEYS = {
  fiatjaf: '3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d',
  jb55: '32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245',
  pablo: 'fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52',
  daniele: '7bdef7be22dd8e59f4600e044aa53a1cf975a9dc7d27df5833bc77db784a5805',
}

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

test('loadRelaySets', async () => {
  const result = await loadRelaySets(TEST_PUBKEYS.fiatjaf)

  expect(result['JGM9mue0UifwnpT8xQIPkScfqYpQACMR'].items.includes('wss://lockbox.fiatjaf.com/')).toBeTruthy()
  expect(result['f4qt86BG85u8POyWO6OMWznNg7innDxp'].items.includes('wss://pyramid.fiatjaf.com/')).toBeTruthy()
  expect(result.lastAttempt).toBeFalsy()
})

test('loadWoT', async () => {
  const wot = await loadWoT('96ae9c5b38add45212555f9ed039f2c3f2fba66e9ecd3d76d28746b0ad3df5a5')
  expect(wot.size).toBeGreaterThan(1000)
  expect(wot.has(TEST_PUBKEYS.jb55)).toBeTrue()
  expect(wot.has(TEST_PUBKEYS.pablo)).toBeTrue()
  expect(wot.has(TEST_PUBKEYS.daniele)).toBeTrue()
})

test('globalism', async () => {
  const relays = await globalism([TEST_PUBKEYS.fiatjaf, TEST_PUBKEYS.jb55, TEST_PUBKEYS.pablo, TEST_PUBKEYS.daniele])
  expect(relays.length).toBeGreaterThan(8)
  expect(relays.includes('wss://pyramid.fiatjaf.com/')).toBeTrue()
  expect(relays.includes('wss://relay.damus.io/')).toBeTrue()
})
