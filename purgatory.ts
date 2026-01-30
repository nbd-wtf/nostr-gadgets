import { Filter } from '@nostr/tools/filter'

type PurgedRelay = {
  lastAttempt: number
  failures: number
}

export class Purgatory {
  interval: NodeJS.Timeout
  state: { [url: string]: PurgedRelay } = {}

  // when this class was instantiated
  startTime: number

  // difference between the last time anything was saved (in a previous session) and when this class was instantiated
  discount: number

  constructor(label: string = '@nostr/gadgets/purgatory') {
    this.state = JSON.parse(window.localStorage.getItem(label) || '{}')
    this.startTime = Math.round(Date.now() / 1000)
    this.discount = (this.state as any).lastSave ? this.startTime - (this.state as any).lastSave : 0

    this.interval = setInterval(() => {
      ;(this.state as any).lastSave = Math.round(Date.now() / 1000)
      window.localStorage.setItem(label, JSON.stringify(this.state))
    }, 120_000)
  }

  close() {
    clearInterval(this.interval)
  }

  onRelayConnectionFailure(url: string) {
    const relay = this.state[url] || {
      failures: 0,
      lastAttempt: 0,
    }

    relay.failures++
    relay.lastAttempt = Math.round(Date.now() / 1000)

    this.state[url] = relay
  }

  allowConnectingToRelay(url: string, _operation: ['read', Filter[]] | ['write', Event]): boolean {
    const relay = this.state[url]
    if (!relay) return true

    let purgeTime = relay.failures * 5 * 60 // 5 minutes for each failure
    let lastAttempt =
      relay.lastAttempt < this.startTime
        ? /* last time was in a previous session. apply the discount */ relay.lastAttempt + this.discount
        : relay.lastAttempt

    if (lastAttempt + purgeTime < Math.round(Date.now() / 1000)) {
      // more time has passed than the required purge time
      return true
    }

    return false
  }
}
