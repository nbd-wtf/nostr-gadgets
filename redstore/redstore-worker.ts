import init, { Redstore } from './pkg/gadgets_redstore.js'

let syncHandle: any
let db: Redstore | null = null
const pendingRequests: Map<number, { resolve: Function; reject: Function }> = new Map()
var serial = 1
const random = Math.round(Math.random() * 1000000)

// leader election state
let fileName: string | null = null
let lastLeaderHeartbeat: number = 0
let heartbeatInterval: ReturnType<typeof setInterval> | null = null
let checkLeaderInterval: ReturnType<typeof setInterval> | null = null
let takeoverInProgress = false

const HEARTBEAT_INTERVAL = 10000 // leader broadcasts every 10 seconds
const CHECK_LEADER_INTERVAL = 15000 // non-leaders check every 15 seconds
const HEARTBEAT_STALE_THRESHOLD = 13000 // heartbeat considered stale after 13 seconds

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
    case 'heartbeat': {
      // update last heartbeat timestamp if we're not the leader
      if (!db) {
        lastLeaderHeartbeat = Date.now()
        console.debug('& received heartbeat from leader')
      }
      break
    }
    case 'close': {
      // leader announced it's closing, try to take over immediately
      if (!db) {
        console.log('& leader closed, attempting immediate takeover...')
        attemptLeadershipTakeover()
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
    fileName = data.fileName
    try {
      await init()
      const opfsRoot = await navigator.storage.getDirectory()
      const fileHandle = await opfsRoot.getFileHandle(data.fileName, { create: true })

      // @ts-ignore
      syncHandle = await fileHandle.createSyncAccessHandle()
      db = new Redstore(syncHandle)
      startLeaderHeartbeat()
      sendToPage([id, true, true])
    } catch (error) {
      // someone else already has the file
      console.log("~ we didn't get the file:", error)
      startCheckingLeader()
      sendToPage([id, true, false])
    }
  } else {
    try {
      if (db) {
        // handle directly if we're the main worker
        if (method === 'close') {
          clearIntervals()
          db!.close()
          syncHandle.close()
          db = null
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

function startLeaderHeartbeat() {
  clearIntervals()

  // broadcast initial heartbeat immediately
  broadcast(['heartbeat'])
  console.debug('& started leader heartbeat')

  // set up recurring heartbeat
  heartbeatInterval = setInterval(() => {
    broadcast(['heartbeat'])
    console.debug('& sent heartbeat')
  }, HEARTBEAT_INTERVAL)
}

function startCheckingLeader() {
  clearIntervals()

  // Initialize last heartbeat to now (assume leader just sent one)
  lastLeaderHeartbeat = Date.now()
  console.debug('& started checking for leader')

  // Set up recurring check
  checkLeaderInterval = setInterval(() => {
    if (db) {
      // We became the leader, stop checking
      if (checkLeaderInterval) clearInterval(checkLeaderInterval)
      checkLeaderInterval = null
      return
    }

    const timeSinceLastHeartbeat = Date.now() - lastLeaderHeartbeat
    if (timeSinceLastHeartbeat > HEARTBEAT_STALE_THRESHOLD) {
      console.log('& leader heartbeat stale, attempting takeover...')
      attemptLeadershipTakeover()
    }
  }, CHECK_LEADER_INTERVAL)
}

async function attemptLeadershipTakeover() {
  if (!fileName) {
    console.error('& cannot attempt takeover: no stored filename')
    return
  }

  if (takeoverInProgress) {
    console.debug('& takeover already in progress, skipping')
    return
  }

  takeoverInProgress = true
  try {
    const opfsRoot = await navigator.storage.getDirectory()
    const fileHandle = await opfsRoot.getFileHandle(fileName, { create: true })

    // @ts-ignore
    syncHandle = await fileHandle.createSyncAccessHandle()
    db = new Redstore(syncHandle)

    console.log('& successfully took over as leader')
    startLeaderHeartbeat()
  } catch (error) {
    // another worker beat us to it or leader recovered
    console.log('& takeover failed, another worker is leader:', error)
    // reset heartbeat timestamp since someone else is now leader
    lastLeaderHeartbeat = Date.now()
  } finally {
    takeoverInProgress = false
  }
}

function clearIntervals() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval)
    heartbeatInterval = null
  }
  if (checkLeaderInterval) {
    clearInterval(checkLeaderInterval)
    checkLeaderInterval = null
  }
}

function command(method: string, data: any): any {
  let result: any

  switch (method) {
    case 'saveEvents':
      result = db!.save_events(data.lastAttempts, data.rawEvents)
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
