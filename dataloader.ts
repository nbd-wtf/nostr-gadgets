import LRUCache from '@fiatjaf/lru-cache/lru-cache'

// A Function, which when given an Array of keys, returns a Promise of an Array
// of values or Errors.
export type BatchLoadFn<K, V> = (keys: readonly K[]) => Promise<readonly (V | Error)[]>

// Optionally turn off batching or caching or provide a cache key function or a
// custom cache instance.
export type Options<K, V, C = K> = {
  cacheKeyFn: (key: K) => C
  maxBatchSize?: number
  transformCacheHit?: (v: V) => V
}

// If a custom cache is provided, it must be of this type (a subset of ES6 Map).
export type CacheMap<K, V> = {
  get(key: K): V | undefined
  set(key: K, value: V): any
  delete(key: K): any
  clear(): any
}

/**
 * A `DataLoader` creates a public API for loading data from a particular
 * data back-end with unique keys such as the `id` column of a SQL table or
 * document name in a MongoDB database, given a batch loading function.
 *
 * Each `DataLoader` instance contains a unique memoized cache. Use caution when
 * used in long-lived applications or those which serve many users with
 * different access permissions and consider creating a new instance per
 * web request.
 */
class DataLoader<K, V, C = K> {
  // Private
  _batchLoadFn: BatchLoadFn<K, V>
  _maxBatchSize: number
  _cacheKeyFn: (key: K) => C
  _cacheMap: CacheMap<C, Promise<V>>
  _batch: Batch<K, V> | null

  constructor(batchLoadFn: BatchLoadFn<K, V>, options: Options<K, V, C>) {
    if (typeof batchLoadFn !== 'function') {
      throw new TypeError(
        'DataLoader must be constructed with a function which accepts ' +
          `Array<key> and returns Promise<Array<value>>, but got: ${batchLoadFn}.`,
      )
    }
    this._batchLoadFn = batchLoadFn
    this._maxBatchSize = options?.maxBatchSize || Infinity
    this._cacheKeyFn = options?.cacheKeyFn

    const _cache = new LRUCache<C, undefined | Promise<V>>(2000)
    this._cacheMap = {
      get: options.transformCacheHit ? key => _cache.get(key)?.then(options.transformCacheHit) : key => _cache.get(key),
      set(key, value) {
        return _cache.set(key, value)
      },
      delete(key) {
        _cache.set(key, undefined)
      },
      clear() {
        _cache.clear()
      },
    }
    this._batch = null
  }

  /**
   * Loads a key, returning a `Promise` for the value represented by that key.
   */
  load(key: K): Promise<V> {
    if (key === null || key === undefined) {
      throw new TypeError('The loader.load() function must be called with a value, ' + `but got: ${String(key)}.`)
    }

    const batch = getCurrentBatch(this)
    const cacheKey: C = this._cacheKeyFn(key)

    // If caching and there is a cache-hit, return cached Promise.
    const cachedPromise = this._cacheMap.get(cacheKey)
    if (cachedPromise) {
      const cacheHits = batch.cacheHits || (batch.cacheHits = [])
      return new Promise(resolve => {
        cacheHits.push(() => {
          resolve(cachedPromise)
        })
      })
    }

    // Otherwise, produce a new Promise for this key, and enqueue it to be
    // dispatched along with the current batch.
    batch.keys.push(key)
    const promise = new Promise<V>((resolve, reject) => {
      batch.callbacks.push({ resolve, reject })
    })

    // If caching, cache this promise.
    this._cacheMap.set(cacheKey as C, promise)

    return promise
  }

  /**
   * Clears the value at `key` from the cache, if it exists. Returns itself for
   * method chaining.
   */
  clear(key: K): this {
    const cacheMap = this._cacheMap
    if (cacheMap) {
      const cacheKey = this._cacheKeyFn(key)
      cacheMap.delete(cacheKey)
    }
    return this
  }
}

// Private: Describes a batch of requests
type Batch<K, V> = {
  hasDispatched: boolean
  keys: K[]
  callbacks: Array<{
    resolve: (value: V) => void
    reject: (error: Error) => void
  }>
  cacheHits?: Array<() => void>
}

// Private: Either returns the current batch, or creates and schedules a
// dispatch of a new batch for the given loader.
function getCurrentBatch<K, V>(loader: DataLoader<K, V, any>): Batch<K, V> {
  // If there is an existing batch which has not yet dispatched and is within
  // the limit of the batch size, then return it.
  const existingBatch = loader._batch
  if (existingBatch !== null && !existingBatch.hasDispatched && existingBatch.keys.length < loader._maxBatchSize) {
    return existingBatch
  }

  // Otherwise, create a new batch for this loader.
  const newBatch = {
    hasDispatched: false,
    keys: [] as K[],
    callbacks: [] as Array<{ resolve: (value: V) => void; reject: (error: Error) => void }>,
  }

  // Store it on the loader so it may be reused.
  loader._batch = newBatch

  // Then schedule a task to dispatch this batch of requests.
  setTimeout(() => {
    dispatchBatch(loader, newBatch)
  })

  return newBatch
}

function dispatchBatch<K, V>(loader: DataLoader<K, V, any>, batch: Batch<K, V>) {
  // Mark this batch as having been dispatched.
  batch.hasDispatched = true

  // If there's nothing to load, resolve any cache hits and return early.
  if (batch.keys.length === 0) {
    resolveCacheHits(batch)
    return
  }

  // Call the provided batchLoadFn for this loader with the batch's keys and
  // with the loader as the `this` context.
  let batchPromise: Promise<readonly (V | Error)[]>
  try {
    batchPromise = loader._batchLoadFn(batch.keys)
  } catch (e) {
    return failedDispatch(
      loader,
      batch,
      new TypeError(
        'DataLoader must be constructed with a function which accepts ' +
          'Array<key> and returns Promise<Array<value>>, but the function ' +
          `errored synchronously: ${String(e)}.`,
      ),
    )
  }

  // Assert the expected response from batchLoadFn
  if (!batchPromise || typeof batchPromise.then !== 'function') {
    return failedDispatch(
      loader,
      batch,
      new TypeError(
        'DataLoader must be constructed with a function which accepts ' +
          'Array<key> and returns Promise<Array<value>>, but the function did ' +
          `not return a Promise: ${String(batchPromise)}.`,
      ),
    )
  }

  // Await the resolution of the call to batchLoadFn.
  batchPromise
    .then(values => {
      if (values.length !== batch.keys.length) {
        throw new TypeError(
          'DataLoader must be constructed with a function which accepts ' +
            'Array<key> and returns Promise<Array<value>>, but the function did ' +
            'not return a Promise of an Array of the same length as the Array ' +
            'of keys.' +
            `\n\nKeys:\n${String(batch.keys)}` +
            `\n\nValues:\n${String(values)}`,
        )
      }

      // Resolve all cache hits in the same micro-task as freshly loaded values.
      resolveCacheHits(batch)

      // Step through values, resolving or rejecting each Promise in the batch.
      for (let i = 0; i < batch.callbacks.length; i++) {
        const value = values[i]
        if (value instanceof Error) {
          batch.callbacks[i].reject(value)
        } else {
          batch.callbacks[i].resolve(value)
        }
      }
    })
    .catch(error => {
      failedDispatch(loader, batch, error)
    })
}

// Private: do not cache individual loads if the entire batch dispatch fails,
// but still reject each request so they do not hang.
function failedDispatch<K, V>(loader: DataLoader<K, V, any>, batch: Batch<K, V>, error: Error) {
  // Cache hits are resolved, even though the batch failed.
  resolveCacheHits(batch)
  for (let i = 0; i < batch.keys.length; i++) {
    loader.clear(batch.keys[i])
    batch.callbacks[i].reject(error)
  }
}

// Private: Resolves the Promises for any cache hits in this batch.
function resolveCacheHits(batch: Batch<any, any>) {
  if (batch.cacheHits) {
    for (let i = 0; i < batch.cacheHits.length; i++) {
      batch.cacheHits[i]()
    }
  }
}

export default DataLoader
