import init, { Redstore } from './pkg/gadgets_redstore.js'

let syncHandle: any
let db: Redstore | null = null
const pendingRequests: Map<number, { resolve: Function; reject: Function; method: string; data: any }> = new Map()
var serial = 1
const random = Math.round(Math.random() * 1000000)

// leader election state
let fileName: string | null = null
let wasmUrl: string | null = null
let lastLeaderHeartbeat: number = 0
let heartbeatInterval: ReturnType<typeof setInterval> | null = null
let checkLeaderInterval: ReturnType<typeof setInterval> | null = null
let takeoverInProgress = false

const HEARTBEAT_INTERVAL = 10000 // leader broadcasts every 10 seconds
const CHECK_LEADER_INTERVAL = 5000 // non-leaders check every 5 seconds
const HEARTBEAT_STALE_THRESHOLD = 12000 // heartbeat considered stale after 12 seconds

const bc = new BroadcastChannel('calls')
function broadcast(msg: any) {
  bc.postMessage(msg)
}

// re-run any requests that were forwarded to a leader that disappeared.
// called after a successful takeover so the page sees results without retrying.
function processPendingRequestsAsNewLeader() {
  for (const [proxyId, req] of pendingRequests) {
    try {
      const result = command(req.method, req.data)
      req.resolve(result)
    } catch (error) {
      req.reject(error instanceof Error ? error : new Error(String(error)))
    }
    pendingRequests.delete(proxyId)
  }
}

// fail any in-flight requests — used when the database is being torn down.
function rejectAllPendingRequests(error: Error) {
  for (const [, req] of pendingRequests) {
    req.reject(error)
  }
  pendingRequests.clear()
}

bc.addEventListener('message', async event => {
  const [type, proxyId, ...re] = event.data

  switch (type) {
    case 'call': {
      // handle only if we're the leader
      if (!db) return

      try {
        const result = command(re[0], re[1])
        broadcast(['response', proxyId, true, result])
      } catch (error) {
        broadcast(['response', proxyId, false, String(error)])
      }
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
      // followers track leader liveness
      if (!db) {
        lastLeaderHeartbeat = Date.now()
      }
      break
    }
    case 'close': {
      // leader announced it's closing → try to take over (pending requests
      // will be re-processed locally after the takeover completes)
      if (!db) {
        attemptLeadershipTakeover()
      }
      break
    }
  }
})

function sendToPage(msg: any) {
  self.postMessage(msg)
}

self.addEventListener('message', async event => {
  const [id, method, data] = event.data

  if (method === 'init') {
    fileName = data.fileName
    wasmUrl = data.wasmUrl
    let gotFile = false
    try {
      const opfsRoot = await navigator.storage.getDirectory()
      const fileHandle = await opfsRoot.getFileHandle(data.fileName, { create: true })

      // @ts-ignore
      syncHandle = await fileHandle.createSyncAccessHandle()
      gotFile = true

      // only load the wasm after we hold the file — followers stay light
      await init(data.wasmUrl)
      db = new Redstore(syncHandle)
      startLeaderHeartbeat()
      sendToPage([id, true, true])
    } catch (error) {
      if (gotFile) {
        // we held the file but failed afterwards (e.g. wasm init); release it
        if (syncHandle) {
          try {
            syncHandle.close()
          } catch {}
          syncHandle = null
        }
        sendToPage([id, false, String(error)])
      } else {
        // file lock → another worker is the leader, we become a follower
        startCheckingLeader()
        sendToPage([id, true, false])
      }
    }
  } else if (method === 'close') {
    // close is handled locally by every worker: the leader releases the file
    // and tells others; followers just clean up their own state.
    clearIntervals()
    if (db) {
      try {
        db.close()
      } catch {}
      try {
        syncHandle.close()
      } catch {}
      db = null
      broadcast(['close'])
    }
    rejectAllPendingRequests(new Error('database was closed'))
    sendToPage([id, true, true])
    bc.close()
  } else {
    try {
      if (db) {
        const result = command(method, data)
        sendToPage([id, true, result])
      } else {
        // follower: forward call to the leader
        const result = await new Promise((resolve, reject) => {
          const proxyId = serial++ + random
          pendingRequests.set(proxyId, { resolve, reject, method, data })
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

  // set up recurring heartbeat
  heartbeatInterval = setInterval(() => {
    broadcast(['heartbeat'])
  }, HEARTBEAT_INTERVAL)
}

function startCheckingLeader() {
  clearIntervals()

  // assume the leader is alive; will be corrected by the first heartbeat we hear
  lastLeaderHeartbeat = Date.now()

  checkLeaderInterval = setInterval(() => {
    if (db) {
      // we became the leader ourselves; stop checking
      if (checkLeaderInterval) clearInterval(checkLeaderInterval)
      checkLeaderInterval = null
      return
    }

    if (takeoverInProgress) {
      // a takeover is already running; let it finish before doing anything
      return
    }

    const timeSinceLastHeartbeat = Date.now() - lastLeaderHeartbeat
    if (timeSinceLastHeartbeat > HEARTBEAT_STALE_THRESHOLD) {
      // leader looks dead — try to take over; in-flight requests will be
      // re-processed locally after the takeover completes
      attemptLeadershipTakeover()
    }
  }, CHECK_LEADER_INTERVAL)
}

async function attemptLeadershipTakeover() {
  if (!fileName) {
    return
  }

  if (takeoverInProgress) {
    return
  }

  takeoverInProgress = true
  try {
    const opfsRoot = await navigator.storage.getDirectory()
    const fileHandle = await opfsRoot.getFileHandle(fileName, { create: true })

    // @ts-ignore
    syncHandle = await fileHandle.createSyncAccessHandle()

    // we may have been a follower all along and never loaded the wasm
    await init(wasmUrl!)
    db = new Redstore(syncHandle)
    startLeaderHeartbeat()
    // re-process any requests that were forwarded to the old leader; if the
    // takeover fails, pending requests stay pending until the next attempt.
    processPendingRequestsAsNewLeader()
  } catch (error) {
    // either the previous leader recovered, or another follower beat us to it
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
      result = db!.set_outbox_bound(data.pubkey, data.kind, data.bound[0], data.bound[1])
      break
    default:
      throw new Error(`unknown method: ${method}`)
  }

  return result
}
