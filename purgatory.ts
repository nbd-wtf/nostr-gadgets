import { Filter } from '@nostr/tools/filter'

type PurgedRelay = {
  lastAttempt: number
  failures: number
}

export class Purgatory {
  label: string
  interval: number
  state: { [url: string]: PurgedRelay } = {}

  // when this class was instantiated
  startTime: number

  constructor(label: string = '@nostr/gadgets/purgatory') {
    this.state = JSON.parse(window.localStorage.getItem(label) || '{}')
    this.startTime = Math.round(Date.now() / 1000)
    this.label = label

    this.interval = setInterval(this.save.bind(this), 60_000)
  }

  close() {
    this.save()
    clearInterval(this.interval)
  }

  save() {
    ;(this.state as any).lastSave = Math.round(Date.now() / 1000)
    window.localStorage.setItem(this.label, JSON.stringify(this.state))
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

  onRelayConnectionSuccess(url: string) {
    delete this.state[url]
  }

  allowConnectingToRelay(url: string, _operation: ['read', Filter[]] | ['write', Event]): boolean {
    if (!url) return false

    const relay = this.state[url]
    if (!relay) return true

    let purgeTime = relay.failures * 15 * 60 // 15 minutes for each failure
    let lastAttempt =
      relay.lastAttempt < this.startTime
        ? /* last time was in a previous session. apply the discount */ relay.lastAttempt +
          this.startTime -
          (this.state as any).lastSave
        : relay.lastAttempt

    if (lastAttempt + purgeTime < Math.round(Date.now() / 1000)) {
      // more time has passed than the required purge time
      return true
    }

    return false
  }
}
