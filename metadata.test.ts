import { assertEquals, assertExists } from 'https://deno.land/std/assert/mod.ts'
import 'https://deno.land/x/indexeddb@v1.1.0/polyfill_memory.ts'

import { loadNostrUser } from './metadata'
import { loadRelayList } from './lists'

const TEST_PUBKEYS = {
  fiatjaf: '3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d',
  jb55: '32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245',
  pablo: 'fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52',
}

Deno.test('metadata and relay fetching', async t => {
  await t.step('loadNostrUser fetches metadata with default relays', async () => {
    const user = await loadNostrUser(TEST_PUBKEYS.fiatjaf)
    assertEquals(user.pubkey, TEST_PUBKEYS.fiatjaf)
    assertExists(user.metadata)
    assertExists(user.shortName)
  })

  await t.step('loadNostrUser fetches metadata with custom relays', async () => {
    const user = await loadNostrUser({
      pubkey: TEST_PUBKEYS.jb55,
      relays: ['wss://relay.damus.io', 'wss://nos.lol'],
    })
    assertEquals(user.pubkey, TEST_PUBKEYS.jb55)
    assertExists(user.metadata)
  })

  await t.step('loadNostrUser handles multiple users concurrently', async () => {
    const users = await Promise.all([
      loadNostrUser(TEST_PUBKEYS.fiatjaf),
      loadNostrUser(TEST_PUBKEYS.jb55),
      loadNostrUser(TEST_PUBKEYS.pablo),
    ])

    assertEquals(users.length, 3)
    assertEquals(users[0].pubkey, TEST_PUBKEYS.fiatjaf)
    assertEquals(users[1].pubkey, TEST_PUBKEYS.jb55)
    assertEquals(users[2].pubkey, TEST_PUBKEYS.pablo)
  })

  await t.step('loadRelayList handles multiple relay lists concurrently', async () => {
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
  })
})
