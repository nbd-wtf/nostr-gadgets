import { expect, test, beforeAll, afterAll, describe } from 'vitest'
import { finalizeEvent, generateSecretKey } from '@nostr/tools/pure'

import { RedEventStore } from './index.ts'

describe('redstore multi-tab', () => {
  describe('two stores sharing a database', () => {
    const TEST_DB = '_.test.multitab.shared.' + Date.now() + '.db'
    let storeA: RedEventStore
    let storeB: RedEventStore

    beforeAll(async () => {
      storeA = new RedEventStore(null, TEST_DB, null)
      storeB = new RedEventStore(null, TEST_DB, null)
    })

    afterAll(async () => {
      try {
        await storeA.close()
      } catch {}
      try {
        await storeB.close()
      } catch {}
      try {
        await RedEventStore.delete(TEST_DB)
      } catch {}
    })

    test('storeA is leader, storeB is follower', async () => {
      const aIsLeader = await storeA.init()
      expect(aIsLeader).toBe(true)

      const bIsLeader = await storeB.init()
      expect(bIsLeader).toBe(false)
    })

    test('follower can save events visible to leader', async () => {
      const sk = generateSecretKey()
      const event = finalizeEvent(
        {
          kind: 1,
          created_at: 1000,
          content: 'from follower B',
          tags: [],
        },
        sk,
      )
      const wasNew = await storeB.saveEvent(event)
      expect(wasNew).toBe(true)

      const fromLeader = await storeA.queryEvents({ ids: [event.id] })
      expect(fromLeader).toHaveLength(1)
      expect(fromLeader[0].content).toEqual('from follower B')
    })

    test('leader can save events visible to follower', async () => {
      const sk = generateSecretKey()
      const event = finalizeEvent(
        {
          kind: 1,
          created_at: 2000,
          content: 'from leader A',
          tags: [],
        },
        sk,
      )
      const wasNew = await storeA.saveEvent(event)
      expect(wasNew).toBe(true)

      const fromFollower = await storeB.queryEvents({ ids: [event.id] })
      expect(fromFollower).toHaveLength(1)
      expect(fromFollower[0].content).toEqual('from leader A')
    })

    test('follower queries for nonexistent event return empty', async () => {
      const fromFollower = await storeB.queryEvents({ ids: ['00'.repeat(32)] })
      expect(fromFollower).toHaveLength(0)
    })

    test('follower deleteEvents goes through leader', async () => {
      const sk = generateSecretKey()
      const event = finalizeEvent(
        {
          kind: 1,
          created_at: 3000,
          content: 'to be deleted by follower',
          tags: [],
        },
        sk,
      )
      await storeA.saveEvent(event)
      const beforeDelete = await storeB.queryEvents({ ids: [event.id] })
      expect(beforeDelete).toHaveLength(1)

      const deleted = await storeB.deleteEvents([event.id])
      expect(deleted).toContain(event.id)

      const afterDelete = await storeA.queryEvents({ ids: [event.id] })
      expect(afterDelete).toHaveLength(0)
    })
  })

  describe('leader handover when leader closes', () => {
    const TEST_DB = '_.test.multitab.handover.' + Date.now() + '.db'
    let storeA: RedEventStore
    let storeB: RedEventStore

    beforeAll(async () => {
      storeA = new RedEventStore(null, TEST_DB, null)
      storeB = new RedEventStore(null, TEST_DB, null)
      const aIsLeader = await storeA.init()
      expect(aIsLeader).toBe(true)
      const bIsLeader = await storeB.init()
      expect(bIsLeader).toBe(false)
    })

    afterAll(async () => {
      try {
        await storeA.close()
      } catch {}
      try {
        await storeB.close()
      } catch {}
      try {
        await RedEventStore.delete(TEST_DB)
      } catch {}
    })

    test('after leader closes, follower can take over and continue saving', async () => {
      const sk = generateSecretKey()

      // save an event before leader closes
      const beforeEvent = finalizeEvent(
        {
          kind: 1,
          created_at: 5000,
          content: 'saved before leader closed',
          tags: [],
        },
        sk,
      )
      await storeA.saveEvent(beforeEvent)
      const seenBefore = await storeB.queryEvents({ ids: [beforeEvent.id] })
      expect(seenBefore).toHaveLength(1)

      // close the leader; follower should take over
      await storeA.close()

      // follower should now be able to save events itself
      const afterEvent = finalizeEvent(
        {
          kind: 1,
          created_at: 6000,
          content: 'saved after leader closed',
          tags: [],
        },
        sk,
      )
      const wasNew = await storeB.saveEvent(afterEvent)
      expect(wasNew).toBe(true)

      // previously saved event should still be queryable
      const stillThere = await storeB.queryEvents({ ids: [beforeEvent.id] })
      expect(stillThere).toHaveLength(1)
      expect(stillThere[0].content).toEqual('saved before leader closed')

      // new event should be queryable
      const newThere = await storeB.queryEvents({ ids: [afterEvent.id] })
      expect(newThere).toHaveLength(1)
      expect(newThere[0].content).toEqual('saved after leader closed')
    })
  })

  describe('concurrent operations across tabs', () => {
    const TEST_DB = '_.test.multitab.concurrent.' + Date.now() + '.db'
    let stores: RedEventStore[]

    beforeAll(async () => {
      stores = [
        new RedEventStore(null, TEST_DB, null),
        new RedEventStore(null, TEST_DB, null),
        new RedEventStore(null, TEST_DB, null),
      ]
      const inits = await Promise.all(stores.map(s => s.init()))
      expect(inits.filter(x => x).length).toBe(1) // exactly one leader
    })

    afterAll(async () => {
      for (const s of stores) {
        try {
          await s.close()
        } catch {}
      }
      try {
        await RedEventStore.delete(TEST_DB)
      } catch {}
    })

    test('all tabs can save and see each others events', async () => {
      const sk = generateSecretKey()
      const events = Array.from({ length: 6 }, (_, i) =>
        finalizeEvent(
          {
            kind: 1,
            created_at: 10000 + i,
            content: `event ${i} from tab ${i % stores.length}`,
            tags: [],
          },
          sk,
        ),
      )

      // round-robin save across tabs
      for (let i = 0; i < events.length; i++) {
        await stores[i % stores.length].saveEvent(events[i])
      }

      // every tab should be able to see every event
      for (const store of stores) {
        const results = await store.queryEvents({ kinds: [1], since: 10000, until: 10010 })
        expect(results).toHaveLength(6)
        const ids = new Set(results.map(e => e.id))
        for (const evt of events) {
          expect(ids.has(evt.id)).toBe(true)
        }
      }
    })
  })
})
