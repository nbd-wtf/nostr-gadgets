import { expect, test } from 'bun:test'
import { finalizeEvent, getPublicKey, } from '@nostr/tools/pure'
import { matchFilter } from '@nostr/tools/filter'
import { hexToBytes } from '@noble/hashes/utils'
import 'fake-indexeddb/auto'

import { loadNostrUser } from './metadata'
import { loadRelayList } from './lists'
import { loadWoT, globalism } from './wot'
import { loadRelaySets } from './sets'
import { outboxFilterRelayBatch } from './outbox'
import { IDBEventStore } from './store'

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

test('outbox filter batch', async () => {
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
        expect(Array.isArray(decl.filter.authors)).toBeTrue()
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

test('idb store', async () => {
    const store = new IDBEventStore()
    await store.init()
    const sk1 = hexToBytes('41a7faaa2e37a8ed0ebf6bd4e0c6e28c95b7b087794e15ca98d1374e944eee2b')
    const sk2 = hexToBytes('611b5b25b45854a36c3621c94f3508516c9b373c18e2eca59ffd15a6908c96be')

    // try just these two events to start things up
    for (let sk of [sk1, sk2]) {
        await store.saveEvent(
            finalizeEvent(
                {
                    kind: 1,
                    created_at: 1000,
                    content: 'hello',
                    tags: [],
                },
                sk,
            ),
        )
    }

    let id
    {
        let count = 0
        for await (let evt of store.queryEvents({ limit: 5 })) {
            count++
            expect(evt.content).toEqual('hello')
            id = evt.id
        }
        expect(count).toEqual(2)
    }

    let pk
    {
        let count = 0
        for await (let evt of store.queryEvents({ ids: [id] })) {
            expect(evt.id).toEqual(id)
            count++
            pk = evt.pubkey
            id = evt.id
        }
        expect(count).toEqual(1)
    }

    {
        let count = 0
        for await (let evt of store.queryEvents({ authors: [pk] })) {
            expect(evt.id).toEqual(id)
            count++
        }
        expect(count).toEqual(1)
    }

    // add a ton of more events and query them in weird ways
    const saves = []
    for (let i = 0; i < 800; i++) {
        let [signer, other] = i % 2 === 0 ? [sk1, sk2] : [sk2, sk1]
        const event = {
            created_at: 10000 + i,
            kind: [1, 11, 1111][i % 3],
            content: 'post ' + i,
            tags: [['t', i % 2 === 0 ? 'even' : 'odd']]
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
        for await (let _evt of store.queryEvents({ since: 10000 }, 780)) {
            count++
        }
        expect(count).toEqual(780)

        count = 0
        for await (let _evt of store.queryEvents({ since: 10700 })) {
            count++
        }
        expect(count).toEqual(100)

        count = 0
        for await (let evt of store.queryEvents({ since: 10600, until: 10700 })) {
            count++
            expect(evt.created_at).toBeGreaterThanOrEqual(10600)
            expect(evt.created_at).toBeLessThanOrEqual(10700)
        }
        expect(count).toEqual(101)

        count = 0
        for await (let evt of store.queryEvents({ limit: 32, until: 10700 })) {
            count++
            expect(evt.created_at).toBeGreaterThan(10660)
        }
        expect(count).toEqual(32)
    }

    {
        let count = 0
        for await (let _evt of store.queryEvents({ since: 10500 })) {
            count++
        }
        expect(count).toEqual(300)
    }

    {
        let count = 0
        let filter = { '#e': ['393d471e4b46848f434583ec79c2c2074af61985acb4fcd772cfd8fc414a4c14'] }
        for await (let evt of store.queryEvents(filter)) {
            count++
            expect(matchFilter(filter, evt)).toBeTrue()
        }
        expect(count).toEqual(4)
    }

    {
        let count = 0
        let filter = { authors: [getPublicKey(sk2)], kinds: [1] }
        for await (let evt of store.queryEvents(filter)) {
            count++
            expect(matchFilter(filter, evt)).toBeTrue()
        }
        expect(count).toEqual(134)
    }

    {
        let count = 0
        let filter = { authors: [getPublicKey(sk2)], kinds: [1], limit: 12 }
        for await (let evt of store.queryEvents(filter)) {
            count++
            expect(matchFilter(filter, evt)).toBeTrue()
        }
        expect(count).toEqual(12)
    }

    let ids = []
    {
        let count = 0
        let filter = { authors: [getPublicKey(sk2)], kinds: [1], since: 10050, until: 10100 }
        for await (let evt of store.queryEvents(filter)) {
            count++
            expect(matchFilter(filter, evt)).toBeTrue()
            ids.push(evt.id)
        }
        expect(count).toEqual(9)
    }

    {
        expect(ids.length).toEqual(9)
        const fromGet = (await store.getByIds(ids)).map(evt => evt.id)
        const fromIter = (await Array.fromAsync(store.queryEvents({ ids }))).map(evt => evt.id)
        expect(fromGet).toEqual(ids)
        expect(fromIter).toEqual(ids)
    }

    {
        let count = 0
        let filter = { '#e': ['393d471e4b46848f434583ec79c2c2074af61985acb4fcd772cfd8fc414a4c14'], since: 10050, until: 10200 }
        for await (let evt of store.queryEvents(filter)) {
            count++
            expect(matchFilter(filter, evt)).toBeTrue()
        }
        expect(count).toEqual(1)

        filter.until = 300
        for await (let evt of store.queryEvents(filter)) {
            count++
            expect(matchFilter(filter, evt)).toBeTrue()
        }
        expect(count).toEqual(1)
    }

    {
        let count = 0
        let filter = { '#e': ['393d471e4b46848f434583ec79c2c2074af61985acb4fcd772cfd8fc414a4c14'], '#t': ['odd'], since: 10050, until: 10400 }
        for await (let _evt of store.queryEvents(filter)) {
            count++
        }
        expect(count).toEqual(0)

        count = 0
        filter['#t'] = ['even']
        for await (let evt of store.queryEvents(filter)) {
            count++
            expect(matchFilter(filter, evt)).toBeTrue()
        }
        expect(count).toEqual(2)
    }
})
