import { expect, test, beforeAll, describe } from 'vitest'
import { hexToBytes } from '@nostr/tools/utils'
import { finalizeEvent } from '@nostr/tools/pure'
import { matchFilter } from '@nostr/tools/filter'
import { NostrEvent, getPublicKey } from '@nostr/tools/pure'

import { RedEventStore } from './index.ts'

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
    const store = new RedEventStore(null, TEST_DB)
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
        for (let i = 1; i <= 10; i++) {
          await store.saveEvent(
            finalizeEvent(
              {
                kind: 1111,
                created_at: 1000 + f * 10 + i,
                content: `aiaiaiaiaiai ${i}`,
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
        expect(evt.content.startsWith('aiaiaiaiaiai')).toBe(true)
        expect(evt.pubkey === getPublicKey(sk1)).toBe(false)
      }
      expect(count).toEqual(40)
    }

    await store.close()
  })

  test('uniq', async () => {
    const store = new RedEventStore(null, TEST_DB)
    await store.init()

    const event = finalizeEvent(
      {
        kind: 1888,
        created_at: 1000,
        content: 'goodbye',
        tags: [],
      },
      sk4,
    )

    await store.saveEvent(event)
    await store.saveEvent(event)

    const results = await store.queryEvents({ authors: [getPublicKey(sk4)], kinds: [1888] }, 2)
    expect(results).toHaveLength(1)
  })

  test('more', async () => {
    const store = new RedEventStore(null, TEST_DB)
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
    const store = new RedEventStore(null, TEST_DB)
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

  test('deletion', async () => {
    const store = new RedEventStore(null, TEST_DB)
    await store.init()

    const skB = hexToBytes('0a0869ac5240c995729cdb73626cf393b08759fca883c2e9646ba176f30ad82c')

    // create events from pkB
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
      await store.saveEvent(event)
    }

    // delete the first 3 events
    const idsToDelete = events.slice(0, 3).map(e => e.id)
    const [first] = await store.deleteEvents([idsToDelete[0]])
    const [second, third] = await store.deleteEventsFilters([{ ids: [idsToDelete[1]] }, { ids: [idsToDelete[2]] }])
    expect(idsToDelete).toContain(first)
    expect(idsToDelete).toContain(second)
    expect(idsToDelete).toContain(third)

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
