import { assertEquals, assertExists } from 'https://deno.land/std/assert/mod.ts'
import 'fake-indexeddb/auto'

import { loadNostrUser } from './metadata'
import { loadRelayList } from './lists'

const TEST_PUBKEYS = {
  fiatjaf: '3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d',
  jb55: '32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245',
  pablo: 'fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52',
}

Deno.test({
  name: 'loadNostrUser handles multiple users concurrently',
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const users = await Promise.all([
      loadNostrUser(TEST_PUBKEYS.fiatjaf),
      loadNostrUser(TEST_PUBKEYS.jb55),
      loadNostrUser(TEST_PUBKEYS.pablo),
      loadNostrUser({
        pubkey: '31f6c99a06bd1f2b3c3b02ce4ad03e7e82910dfd4f2f94b1ffd0a84f9f94f3d3',
        relays: ['offchain.pub'],
      }),
    ])

    assertEquals(users.length, 4)
    assertEquals(users[0].pubkey, TEST_PUBKEYS.fiatjaf)
    assertEquals(users[1].pubkey, TEST_PUBKEYS.jb55)
    assertEquals(users[2].pubkey, TEST_PUBKEYS.pablo)
    assertEquals(users[3].shortName, 'bonanza')
  },
})

Deno.test({
  name: 'loadRelayList handles multiple relay lists concurrently',
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const results = await Promise.all([
      loadRelayList(TEST_PUBKEYS.fiatjaf),
      loadRelayList(TEST_PUBKEYS.jb55),
      loadRelayList(TEST_PUBKEYS.pablo),
    ])

    assertEquals(results.length, 3)
    results.forEach(result => {
      assertExists(result.items)
      assertEquals(Array.isArray(result.items), true)
    })
  },
})
