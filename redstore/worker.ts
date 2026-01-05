import init, { Redstore } from './pkg/gadgets_redstore.js'

let syncHandle: any

self.addEventListener('message', async event => {
  const { id, method } = event.data

  try {
    let result: any

    if (method === 'init') {
      await init()

      const opfsRoot = await navigator.storage.getDirectory()
      const fileHandle = await opfsRoot.getFileHandle(event.data.fileName, { create: true })

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
            event.data.indexableEvents,
            event.data.followedBys,
            event.data.rawEvents,
          ) /* -> [bool, ...] */
          break
        case 'deleteEvents':
          result = db.delete_events(event.data /* [filter, ...] */) /* -> [count, ...] */
          break
        case 'queryEvents':
          result = db.query_events(event.data /* [filter, ...] */)
          break
        case 'markFollow':
          result = db.mark_follow(event.data.follower, event.data.followed)
          break
        case 'markUnfollow':
          result = db.mark_unfollow(event.data.follower, event.data.followed)
          break
        case 'cleanFollowed':
          result = db.clean_followed(event.data.followedBy, event.data.except)
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
