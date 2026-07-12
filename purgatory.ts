import { NostrEvent } from '@nostr/tools'
import { Filter } from '@nostr/tools/filter'

type PurgedRelay = {
  lastAttempt: number
  failures: number
}

export class Purgatory {
  label: string
  interval: number
  state: { [url: string]: PurgedRelay } = {}

  constructor(label: string = '@nostr/gadgets/purgatory') {
    this.state = JSON.parse(window.localStorage.getItem(label) || '{}')
    this.label = label
    this.interval = setInterval(this.save.bind(this), 60_000)
  }

  close() {
    this.save()
    clearInterval(this.interval)
  }

  save() {
    window.localStorage.setItem(this.label, JSON.stringify(this.state))
  }

  onRelayConnectionFailure(url: string) {
    const relay = this.state[url] || { failures: 0, lastAttempt: 0 }
    relay.failures++
    relay.lastAttempt = Math.round(Date.now() / 1000)
    this.state[url] = relay
  }

  onRelayConnectionSuccess(url: string) {
    delete this.state[url]
  }

  allowConnectingToRelay(url: string, _operation: ['read', Filter[]] | ['write', NostrEvent]): boolean {
    if (!url) return false

    const relay = this.state[url]
    if (!relay) return true

    const now = Math.round(Date.now() / 1000)
    const purgeTime = Math.min(relay.failures * 2 * 60, 10 * 60)

    if (relay.lastAttempt + purgeTime < now) {
      if (relay.failures > 1) {
        relay.failures--
        relay.lastAttempt = now
      } else {
        delete this.state[url]
      }
      return true
    }

    return false
  }
}
