import { SimplePool } from '@nostr/tools'

/**
 * pool is a global used by all other functions in this library. Feel free to use it directly in your app.
 */
export const pool: SimplePool = new SimplePool()
