import init, { Redstore } from './pkg/gadgets_redstore.js'

let syncHandle: any

self.addEventListener('message', async event => {
  const { id, method, data } = event.data

  try {
    let result: any

    if (method === 'init') {
      await init()

      const opfsRoot = await navigator.storage.getDirectory()
      const fileHandle = await opfsRoot.getFileHandle(data, { create: true })

      // @ts-ignore
      syncHandle = await fileHandle.createSyncAccessHandle()

      db = new Redstore(syncHandle)
    } else if (method === 'close') {
      syncHandle.close()
    } else {
      if (!db) throw new Error('Database not initialized')
      switch (method) {
        case 'saveEvents':
          result = db.save_events(
            data /* { events: [{...}, ...], followedBys: [[pubkey, ...], ...] } */,
          ) /* -> [bool, ...] */
          break
        case 'deleteEvents':
          result = db.delete_events(data /* [filter, ...] */) /* -> [count, ...] */
          break
        case 'queryEvents':
          result = db.query_events(data /* [filter, ...] */)
          break
        default:
          throw new Error(`unknown method: ${method}`)
      }
    }
    self.postMessage({ id, success: true, result })
  } catch (error) {
    self.postMessage({ id, success: false, error: String(error) })
  }
})

let db: Redstore | null = null
