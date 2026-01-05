import { finalizeEvent, generateSecretKey } from '@nostr/tools/pure'
import { RedEventStore } from '../index.ts'

async function main() {
  console.log('example starting...')
  const dbs = await RedEventStore.list()
  console.log(dbs)
  try {
    for (let db of dbs) {
      if (db.name === 'example-db') {
        await RedEventStore.delete(db.name)
        break
      }
    }
  } catch (err) {
    if (String(err).includes("Failed to execute 'removeEntry'")) {
      console.warn('failed to delete:', err)
    } else {
      throw err
    }
  }

  // initialize the store
  const store = new RedEventStore('example-db')
  await store.init()
  console.log('✓ store initialized')

  // store some events
  try {
    const sk = generateSecretKey()
    await Promise.all([
      store.saveEvent(finalizeEvent({ kind: 1, content: 'one', created_at: 1, tags: [] }, sk)),
      store.saveEvent(finalizeEvent({ kind: 1, content: 'two', created_at: 2, tags: [] }, sk)),
      store.saveEvent(finalizeEvent({ kind: 7, content: '', created_at: 3, tags: [] }, sk)),
    ])
    console.log('✓ events saved')
  } catch (error) {
    console.error('failed to save events:', error)
  }

  // query events
  try {
    const events = await store.queryEvents({ kinds: [1] })
    console.log('✓ query results:', {}, events)
  } catch (error) {
    console.error('failed to query events:', error)
  }

  // query events
  try {
    const filter = { kinds: [7] }
    const events = await store.queryEvents(filter)
    console.log('✓ query results:', filter, events)
  } catch (error) {
    console.error('failed to query events:', error)
  }

  console.log('example complete.')
}

main().catch(console.error)
