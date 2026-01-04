import { expect, test, beforeAll, describe } from 'vitest'
import { hexToBytes } from '@nostr/tools/utils'
import { finalizeEvent } from '@nostr/tools/pure'

import { RedEventStore } from './index.ts'
import { matchFilter } from '@nostr/tools'
import { NostrEvent, getPublicKey } from '@nostr/tools/pure'

const TEST_DB = '_.test.db'

beforeAll(async () => {
  const dbs = await RedEventStore.list()
  console.log('before test:', dbs)
  for (let db of dbs) {
    if (db.name === TEST_DB) await RedEventStore.delete(db.name)
  }
})

const sk1 = hexToBytes('41a7faaa2e37a8ed0ebf6bd4e0c6e28c95b7b087794e15ca98d1374e944eee2b')
const sk2 = hexToBytes('611b5b25b45854a36c3621c94f3508516c9b373c18e2eca59ffd15a6908c96be')
const sk3 = hexToBytes('c7c3d4629c5d53e05373b4a8eb0add486129e5829ebe47cf32aa890d89b04a10')
const sk4 = hexToBytes('2ab02d4c78e7dbe59c36bbd12f93241a072aebc34b9f888108952034e733d8fb')
const sk5 = hexToBytes('4e91489c0cc70387fa217ec7b40e47af6e52abe7ba2d5e784d11dadd6377d263')

describe('redstore', () => {
  test('basic', async () => {
    const store = new RedEventStore(TEST_DB)
    await store.init()

    // try just these two events to start things up
    await Promise.all(
      [sk1, sk2].map(sk =>
        store.saveEvent(
          finalizeEvent(
            {
              kind: 1,
              created_at: 1000,
              content: 'hello',
              tags: [],
            },
            sk,
          ),
        ),
      ),
    )

    let id: string = ''
    {
      let count = 0
      for (let evt of await store.queryEvents({ limit: 5 })) {
        count++
        expect(evt.content).toEqual('hello')
        id = evt.id
      }
      expect(count).toEqual(2)
    }

    let pk: string = ''
    {
      let count = 0
      for (let evt of await store.queryEvents({ ids: [id] })) {
        expect(evt.id).toEqual(id)
        count++
        pk = evt.pubkey
        id = evt.id
      }
      expect(count).toEqual(1)
    }

    {
      let count = 0
      for (let evt of await store.queryEvents({ authors: [pk] })) {
        expect(evt.id).toEqual(id)
        count++
      }
      expect(count).toEqual(1)
    }

    // a slightly tricker case
    const fiveKeys = [sk1, sk2, sk3, sk4, sk5]
    await Promise.all(
      fiveKeys.map(async (sk, f) => {
        for (let i = 0; i < 10; i++) {
          await store.saveEvent(
            finalizeEvent(
              {
                kind: 1111,
                created_at: 1000 + f * 10 + i,
                content: `zpzp ${i}`,
                tags: [],
              },
              sk,
            ),
          )
        }
      }),
    )

    {
      let count = 0
      for (let evt of await store.queryEvents({ limit: 40, authors: fiveKeys.map(getPublicKey), kinds: [1111] })) {
        count++
        expect(evt.content.startsWith('zpzp')).toBe(true)
        expect(evt.pubkey === getPublicKey(sk1)).toBe(false)
      }
      expect(count).toEqual(40)
    }

    await store.close()
  })

  test('more', async () => {
    const store = new RedEventStore(TEST_DB)
    await store.init()

    // add a ton of events and query them in weird ways
    const saves: Promise<boolean>[] = []
    for (let i = 0; i < 800; i++) {
      let [signer, other] = i % 2 === 0 ? [sk1, sk2] : [sk2, sk1]
      const event = {
        created_at: 10000 + i,
        kind: [9, 99, 9999][i % 3],
        content: 'post ' + i,
        tags: [['t', i % 2 === 0 ? 'even' : 'odd']],
      }
      if (i % 10 === 0) {
        event.tags.push(['p', getPublicKey(other)])
      }
      if (i % 200 === 0) {
        event.tags.push(['e', '393d471e4b46848f434583ec79c2c2074af61985acb4fcd772cfd8fc414a4c14'])
      }

      saves.push(store.saveEvent(finalizeEvent(event, signer)))
    }
    await Promise.all(saves)

    {
      let count = 0
      for (let _evt of await store.queryEvents({ since: 10000, limit: 780 })) {
        count++
      }
      expect(count).toEqual(780)

      count = 0
      for (let _evt of await store.queryEvents({ since: 10700 })) {
        count++
      }
      expect(count).toEqual(100)

      count = 0
      for (let evt of await store.queryEvents({ since: 10600, until: 10700 })) {
        count++
        expect(evt.created_at).toBeGreaterThanOrEqual(10600)
        expect(evt.created_at).toBeLessThanOrEqual(10700)
      }
      expect(count).toEqual(101)

      count = 0
      for (let evt of await store.queryEvents({ limit: 32, until: 10700 })) {
        count++
        expect(evt.created_at).toBeGreaterThan(10660)
      }
      expect(count).toEqual(32)
    }

    {
      let count = 0
      for (let _evt of await store.queryEvents({ since: 10500, limit: 4000 })) {
        count++
      }
      expect(count).toEqual(300)
    }

    {
      let count = 0
      let filter = { '#e': ['393d471e4b46848f434583ec79c2c2074af61985acb4fcd772cfd8fc414a4c14'] }
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
      }
      expect(count).toEqual(4)
    }

    {
      let count = 0
      let filter = { authors: [getPublicKey(sk2)], kinds: [9] }
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
      }
      expect(count).toEqual(133 /* 800 / 2 / 3 */)
    }

    {
      let count = 0
      let filter = { authors: [getPublicKey(sk2)], kinds: [9], limit: 12 }
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
      }
      expect(count).toEqual(12)
    }

    {
      let count = 0
      let filter = { authors: [getPublicKey(sk1), getPublicKey(sk2)], kinds: [9, 99], limit: 60 }
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
      }
      expect(count).toEqual(60)
    }

    {
      let count = 0
      let filter = { authors: [getPublicKey(sk1), getPublicKey(sk2)], kinds: [9, 99, 9999], '#t': ['odd'], limit: 800 }
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
      }
      expect(count).toEqual(400)
    }

    let ids: string[] = []
    {
      let count = 0
      let filter = { authors: [getPublicKey(sk2)], kinds: [9], since: 10050, until: 10100 }
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
        ids.push(evt.id)
      }
      expect(count).toEqual(9)
    }

    {
      expect(ids.length).toEqual(9)
      const fromGet = (await store.queryEvents({ ids: ids })).map(evt => evt.id)
      const fromIter = Array.from(await store.queryEvents({ ids })).map(evt => evt.id)
      expect(fromGet).toEqual(ids)
      expect(fromIter).toEqual(ids)
    }

    {
      let count = 0
      let filter = {
        '#e': ['393d471e4b46848f434583ec79c2c2074af61985acb4fcd772cfd8fc414a4c14'],
        since: 10050,
        until: 10200,
      }
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
      }
      expect(count).toEqual(1)

      filter.until = 300
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
      }
      expect(count).toEqual(1)
    }

    {
      let count = 0
      let filter = {
        '#e': ['393d471e4b46848f434583ec79c2c2074af61985acb4fcd772cfd8fc414a4c14'],
        '#t': ['odd'],
        since: 10050,
        until: 10400,
      }
      for (let _evt of await store.queryEvents(filter)) {
        count++
      }
      expect(count).toEqual(0)

      count = 0
      filter['#t'] = ['even']
      for (let evt of await store.queryEvents(filter)) {
        count++
        expect(matchFilter(filter, evt)).toBe(true)
      }
      expect(count).toEqual(2)
    }
    await store.close()
  })

  test('replacing', async () => {
    const store = new RedEventStore(TEST_DB)
    await store.init()

    // test replacing
    {
      let event = finalizeEvent(
        {
          kind: 30023,
          created_at: 1,
          tags: [['d', 'bla']],
          content: 'blablabla',
        },
        sk1,
      )

      await store.saveEvent(event)

      {
        let count = 0
        for (let evt of await store.queryEvents({ '#d': ['bla'], authors: [getPublicKey(sk1)], kinds: [30023] })) {
          count++
          expect(evt.id).toEqual(event.id)
        }
        expect(count).toEqual(1)
      }

      {
        // it shouldn't return anything for the other user
        let count = 0
        for (let _evt of await store.queryEvents({ '#d': ['bla'], authors: [getPublicKey(sk2)], kinds: [30023] })) {
          count++
        }
        expect(count).toEqual(0)
      }

      // since this comes from the other user this should not replace, it should be added as a separate thing
      let fakeReplacement = finalizeEvent(
        {
          kind: 30023,
          created_at: 2,
          tags: [['d', 'bla']],
          content: '# bla\n\nblablabla',
        },
        sk2,
      )

      await store.saveEvent(fakeReplacement)

      {
        let count = 0
        for (let evt of await store.queryEvents({ '#d': ['bla'], authors: [getPublicKey(sk1)], kinds: [30023] })) {
          count++
          expect(evt.id).toEqual(event.id)
        }
        expect(count).toEqual(1)
      }

      {
        let count = 0
        for (let evt of await store.queryEvents({ '#d': ['bla'], authors: [getPublicKey(sk2)], kinds: [30023] })) {
          count++
          expect(evt.id).toEqual(fakeReplacement.id)
        }
        expect(count).toEqual(1)
      }

      // now this should replace the first event
      let actualReplacement = finalizeEvent(
        {
          kind: 30023,
          created_at: 100,
          tags: [['d', 'bla']],
          content: '# bla\n\nblablabla',
        },
        sk1,
      )

      await store.saveEvent(actualReplacement)

      {
        let count = 0
        for (let evt of await store.queryEvents({ '#d': ['bla'], authors: [getPublicKey(sk1)], kinds: [30023] })) {
          count++
          expect(evt.id).toEqual(actualReplacement.id)
        }
        expect(count).toEqual(1)
      }

      {
        let count = 0
        for (let evt of await store.queryEvents({ '#d': ['bla'], authors: [getPublicKey(sk2)], kinds: [30023] })) {
          count++
          expect(evt.id).toEqual(fakeReplacement.id)
        }
        expect(count).toEqual(1)
      }

      // and this has an older timestamp than the actual replacement, so it should not replace anything
      let olderReplacement = finalizeEvent(
        {
          kind: 30023,
          created_at: 50,
          tags: [['d', 'bla']],
          content: '#bla\n\nblablabla',
        },
        sk1,
      )

      await store.saveEvent(olderReplacement)

      {
        let count = 0
        for (let evt of await store.queryEvents({ '#d': ['bla'], authors: [getPublicKey(sk1)], kinds: [30023] })) {
          count++
          expect(evt.id).toEqual(actualReplacement.id)
        }
        expect(count).toEqual(1)
      }

      {
        let count = 0
        for (let evt of await store.queryEvents({ '#d': ['bla'], authors: [getPublicKey(sk2)], kinds: [30023] })) {
          count++
          expect(evt.id).toEqual(fakeReplacement.id)
        }
        expect(count).toEqual(1)
      }
    }
    await store.close()
  })

  test('following', async () => {
    const store = new RedEventStore(TEST_DB)
    await store.init()

    const skA = hexToBytes('41a7faaa2e37a8ed0ebf6bd4e0c6e28c95b7b087794e15ca98d1374e944eee2b')
    const skB = hexToBytes('611b5b25b45854a36c3621c94f3508516c9b373c18e2eca59ffd15a6908c96be')
    const skC = hexToBytes('a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5')
    const skD = hexToBytes('d15fe5f99a1aca0cc209cb2554eedad181043edb498afcab51dd993ba40da37b')

    const pkA = getPublicKey(skA)
    const pkB = getPublicKey(skB)
    const pkC = getPublicKey(skC)
    const pkD = getPublicKey(skD)

    const events: NostrEvent[] = []

    // create events: 10 from each pubkey
    const sks = [skA, skB, skC, skD]
    for (let sk of sks) {
      for (let i = 0; i < 10; i++) {
        const event = finalizeEvent(
          {
            kind: 1,
            created_at: 2000 + i,
            content: 'event ' + i,
            tags: [],
          },
          sk,
        )
        events.push(event)

        // save with follower relationships:
        // A follows B and C
        // B follows C and D
        let followers: string[] = []
        if (sk === skB || sk === skC) {
          followers.push(pkA)
        }
        if (sk === skC || sk === skD) {
          followers.push(pkB)
        }
        await store.saveEvent(event, { followedBy: followers })
      }
    }

    // query by followed by A
    {
      let count = 0
      for (const evt of await store.queryEvents({ followedBy: pkA })) {
        count++
        expect(evt.pubkey === pkB || evt.pubkey === pkC).toBe(true)
      }
      expect(count).toEqual(20)
    }

    // query by followed B
    {
      let count = 0
      for (const evt of await store.queryEvents({ followedBy: pkB })) {
        count++
        expect(evt.pubkey === pkC || evt.pubkey === pkD).toBe(true)
      }
      expect(count).toEqual(20)
    }

    // call markFollow so A starts following D
    await store.markFollow(pkA, pkD)

    // query by followed by A (should return D events too)
    {
      let count = 0
      for (const evt of await store.queryEvents({ followedBy: pkA })) {
        count++
        expect(evt.pubkey === pkB || evt.pubkey === pkC || evt.pubkey === pkD).toBe(true)
      }
      expect(count).toEqual(30)
    }

    // query by followed B (should stay the same)
    {
      let count = 0
      for (const evt of await store.queryEvents({ followedBy: pkB })) {
        count++
        expect(evt.pubkey === pkC || evt.pubkey === pkD).toBe(true)
      }
      expect(count).toEqual(20)
    }

    // call markUnfollow so both A and B stop following C
    await store.markUnfollow(pkA, pkC)
    await store.markUnfollow(pkB, pkC)

    // query by followed by A (should not return C events)
    {
      let count = 0
      for (const evt of await store.queryEvents({ followedBy: pkA })) {
        count++
        expect(evt.pubkey === pkB || evt.pubkey === pkD).toBe(true)
      }
      expect(count).toEqual(20)
    }

    // query by followed B (should not return C events)
    {
      let count = 0
      for (const evt of await store.queryEvents({ followedBy: pkB })) {
        count++
        expect(evt.pubkey === pkD).toBe(true)
      }
      expect(count).toEqual(10)
    }

    // clean followed indexes
    await store.cleanFollowed(pkA, event => event.pubkey === pkB)
    await store.cleanFollowed(pkB, _ => false)

    // query by followed by A (now it should also not return D events)
    {
      let count = 0
      for (const evt of await store.queryEvents({ followedBy: pkA })) {
        count++
        expect(evt.pubkey === pkB).toBe(true)
      }
      expect(count).toEqual(10)
    }

    // query by followed B (should not return any events)
    {
      let count = 0
      for (const _ of await store.queryEvents({ followedBy: pkB })) {
        count++
      }
      expect(count).toEqual(0)
    }

    await store.close()
  })

  test('deletion with followedBy index', async () => {
    const store = new RedEventStore(TEST_DB)
    await store.init()

    const skA = hexToBytes('41a7faaa2e37a8ed0ebf6bd4e0c6e28c95b7b087794e15ca98d1374e944eee2b')
    const skB = hexToBytes('611b5b25b45854a36c3621c94f3508516c9b373c18e2eca59ffd15a6908c96be')

    const pkA = getPublicKey(skA)
    const pkB = getPublicKey(skB)

    // create events from pkB and mark them as followed by pkA
    const events: NostrEvent[] = []
    for (let i = 0; i < 5; i++) {
      const event = finalizeEvent(
        {
          kind: 1,
          created_at: 3000 + i,
          content: 'test event ' + i,
          tags: [],
        },
        skB,
      )
      events.push(event)
      await store.saveEvent(event, { followedBy: [pkA] })
    }

    // verify events are queryable by followedBy
    {
      let count = 0
      for (const evt of await store.queryEvents({ followedBy: pkA })) {
        count++
        expect(evt.pubkey).toEqual(pkB)
      }
      expect(count).toEqual(5)
    }

    // delete the first 3 events
    const idsToDelete = events.slice(0, 3).map(e => e.id)
    const deletedCount = await store.deleteEvents(idsToDelete)
    expect(deletedCount).toEqual(3)

    // verify only 2 events remain and are still queryable by followedBy
    {
      let count = 0
      const remainingIds = new Set()
      for (const evt of await store.queryEvents({ followedBy: pkA })) {
        count++
        expect(evt.pubkey).toEqual(pkB)
        remainingIds.add(evt.id)
      }
      expect(count).toEqual(2)
      expect(remainingIds.has(events[3].id)).toBe(true)
      expect(remainingIds.has(events[4].id)).toBe(true)
    }

    // verify deleted events are no longer queryable by ID
    const deletedEvents = await store.queryEvents({ ids: idsToDelete })
    expect(deletedEvents.length).toEqual(0)

    // verify remaining events are still queryable by ID
    const remainingIds = events.slice(3).map(e => e.id)
    const remainingEvents = await store.queryEvents({ ids: remainingIds })
    expect(remainingEvents.length).toEqual(2)

    await store.close()
  })
})
