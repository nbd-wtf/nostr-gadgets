import { SimplePool } from '@nostr/tools/pool'
import { MemoryHints } from './hints'

/**
 * pool is a global used by all other functions in this library. Feel free to use it directly in your app.
 */
export let pool: SimplePool = new SimplePool()

/**
 * setPool sets the global pool -- do not use unless you know what you're doing.
 */
export function setPool(p: SimplePool) {
  pool = p
}

/**
 * hints is a global used by other functions in this library. Use it directly.
 */
export const hints: MemoryHints = new MemoryHints()
