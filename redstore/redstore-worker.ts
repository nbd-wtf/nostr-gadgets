import init, { Redstore } from './pkg/gadgets_redstore.js'

let syncHandle: any
let db: Redstore | null = null
const pendingRequests: Map<number, { resolve: Function; reject: Function }> = new Map()
var serial = 1
const random = Math.round(Math.random() * 1000000)

self.addEventListener('message', async event => {
  const { id, method } = event.data

  console.debug('~ worker message', event.data)
  const bc = new BroadcastChannel('calls')
  bc.addEventListener('message', async event => {
    console.debug('& bc message', event.data)
    switch (event.data.type) {
      case 'call': {
        // handle if we're the main worker
        if (!db) {
          bc.postMessage({ type: 'response', response: { id, success: false, error: 'not main' } })
          return
        }

        const result = command(event.data.request.method, event.data.request)
        bc.postMessage({ type: 'response', response: { id, success: true, result } })

        break
      }
      case 'response': {
        const pending = pendingRequests.get(event.data.response.id)
        if (pending) {
          if (event.data.response.success) {
            pending.resolve(event.data.response.result)
          } else {
            pending.reject(new Error(event.data.response.error))
          }
          pendingRequests.delete(event.data.response.id)
        }
        break
      }
    }
  })

  if (method === 'init') {
    try {
      await init()
      const opfsRoot = await navigator.storage.getDirectory()
      const fileHandle = await opfsRoot.getFileHandle(event.data.fileName, { create: true })

      // @ts-ignore
      syncHandle = await fileHandle.createSyncAccessHandle()
      db = new Redstore(syncHandle)
      self.postMessage({ id, success: true, result: true })
    } catch (error) {
      // someone else already has the file
      // ...
      console.debug("~ worker: we didn't get the file")
      self.postMessage({ id, success: true, result: false })
    }
  } else {
    try {
      if (db) {
        // handle directly if we're the main worker
        if (method === 'close') {
          syncHandle.close()
          self.postMessage({ id, success: true, result: true })
          bc.postMessage({ type: 'close' })
          bc.close()
        } else {
          const result = command(method, event.data)
          self.postMessage({ id, success: true, result })
        }
      } else {
        // forward to main worker
        const result = await new Promise((resolve, reject) => {
          const id = serial++ + random
          pendingRequests[id] = { resolve, reject }
          bc.postMessage({ id, type: 'call', request: event.data })
        })
        self.postMessage({ id, success: true, result })
      }
    } catch (error) {
      self.postMessage({ id, success: false, error: String(error) })
    }
  }
})

function command(method: string, data: any): any {
  let result: any

  switch (method) {
    case 'saveEvents':
      result = db!.save_events(data.lastAttempts, data.followedBys, data.rawEvents)
      break
    case 'deleteEvents':
      result = db!.delete_events(data)
      break
    case 'queryEvents':
      result = db!.query_events(data)
      break
    case 'loadReplaceables':
      result = db!.load_replaceables(data)
      break
    case 'markFollow':
      result = db!.mark_follow(data.follower, data.followed)
      break
    case 'markUnfollow':
      result = db!.mark_unfollow(data.follower, data.followed)
      break
    case 'cleanFollowed':
      result = db!.clean_followed(data.followedBy, data.except)
      break
    case 'getOutboxBounds':
      result = db!.get_outbox_bounds()
      break
    case 'setOutboxBound':
      result = db!.set_outbox_bound(data.pubkey, data.bound[0], data.bound[1])
      break
    default:
      throw new Error(`unknown method: ${method}`)
  }

  return result
}
