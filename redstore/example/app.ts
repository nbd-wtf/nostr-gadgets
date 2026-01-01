import { RedEventStore } from '../index.ts'

async function main() {
  console.log('example starting...')

  // initialize the store
  const store = new RedEventStore('example-db')
  await store.init()
  console.log('✓ store initialized')

  // create a sample event
  const sampleEvent = {
    id: '1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef',
    pubkey: 'abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
    created_at: Math.floor(Date.now() / 1000),
    kind: 1,
    tags: [],
    content: 'hello from example',
    sig: '1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345678',
  }

  // save the event
  try {
    await store.saveEvent(sampleEvent, { seenOn: ['wss://relay.damus.io'] })
    console.log('✓ event saved')
  } catch (error) {
    console.error('failed to save event:', error)
  }

  // query events
  try {
    const events = await store.queryEvents({ kinds: [1] })
    console.log('✓ query results:', events.length, 'events found')
    events.forEach((event, i) => {
      console.log(`  event ${i + 1}:`, event.content)
    })
  } catch (error) {
    console.error('failed to query events:', error)
  }

  console.log('example complete.')
}

main().catch(console.error)
