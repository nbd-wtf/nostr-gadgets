import init, { Redstore } from './pkg/gadgets_redstore.js'

let syncHandle: any
let db: Redstore | null = null
const pendingRequests: Map<number, { resolve: Function; reject: Function }> = new Map()
var serial = 1
const random = Math.round(Math.random() * 1000000)

const bc = new BroadcastChannel('calls')
function broadcast(msg: any) {
  console.debug('&> bc', msg)
  bc.postMessage(msg)
}
bc.addEventListener('message', async event => {
  console.debug('&< bc', event.data)
  const [type, proxyId, ...re] = event.data

  switch (type) {
    case 'call': {
      // handle only if we're the main worker
      if (!db) return

      const result = command(re[0], re[1])
      broadcast(['response', proxyId, true, result])

      break
    }
    case 'response': {
      const pending = pendingRequests.get(proxyId)
      if (pending) {
        if (re[0]) {
          pending.resolve(re[1])
        } else {
          pending.reject(new Error(re[1]))
        }
        pendingRequests.delete(proxyId)
      }
      break
    }
  }
})

function sendToPage(msg: any) {
  console.debug('~> page', msg)
  self.postMessage(msg)
}

self.addEventListener('message', async event => {
  console.debug('~< page', event.data)
  const [id, method, data] = event.data

  if (method === 'init') {
    try {
      await init()
      const opfsRoot = await navigator.storage.getDirectory()
      const fileHandle = await opfsRoot.getFileHandle(data.fileName, { create: true })

      // @ts-ignore
      syncHandle = await fileHandle.createSyncAccessHandle()
      db = new Redstore(syncHandle)
      sendToPage([id, true, true])
    } catch (error) {
      // someone else already has the file
      // ...
      console.debug("~ we didn't get the file")
      sendToPage([id, true, false])
    }
  } else {
    try {
      if (db) {
        // handle directly if we're the main worker
        if (method === 'close') {
          syncHandle.close()
          sendToPage([id, true, true])
          broadcast(['close'])
          bc.close()
        } else {
          const result = command(method, data)
          sendToPage([id, true, result])
        }
      } else {
        // forward to main worker
        const result = await new Promise((resolve, reject) => {
          const proxyId = serial++ + random
          pendingRequests.set(proxyId, { resolve, reject })
          broadcast(['call', proxyId, method, data])
        })
        sendToPage([id, true, result])
      }
    } catch (error) {
      sendToPage([id, false, String(error)])
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
