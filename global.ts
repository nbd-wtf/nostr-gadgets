import { SimplePool } from '@nostr/tools'
import { MemoryHints } from './hints'

/**
 * pool is a global used by all other functions in this library. Feel free to use it directly in your app.
 */
export const pool: SimplePool = new SimplePool()

/**
 * hints is a global used by other functions in this library. Use it directly.
 */
export const hints: MemoryHints = new MemoryHints()
