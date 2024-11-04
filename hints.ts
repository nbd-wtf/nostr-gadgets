import { loadRelayList } from './lists'

interface HintsDB {
  save(pubkey: string, relay: string, key: HintKey, ts: number): void
  topN(pubkey: string, n: number): Promise<string[]>
  printScores(): void
}

/**
 * MemoryHints keeps track of all latest relay hints from all possible sources for each pubkey, as well as
 * our latest attempts to fetch from these relays and our last successes.
 *
 * With this information it's possible to determine the best relays to fetch data from for each pubkey
 * (i.e. their outbox relays) even under high censorship.
 */
export class MemoryHints implements HintsDB {
  private relayBySerial: string[] = []
  private orderedRelaysByPubKey: { [pubkey: string]: RelaysForPubKey } = {}
  private hasLoadedRelaysFor: Set<string> = new Set()

  public export(): string {
    return JSON.stringify({ relayBySerial: this.relayBySerial, orderedRelaysByPubKey: this.orderedRelaysByPubKey })
  }

  public import(exported: string) {
    let { relayBySerial, orderedRelaysByPubKey } = JSON.parse(exported)
    this.relayBySerial = relayBySerial
    this.orderedRelaysByPubKey = orderedRelaysByPubKey
  }

  public save(pubkey: string, relay: string, key: HintKey, ts: number): void {
    const relayIndex = this.relayBySerial.indexOf(relay)
    if (relayIndex === -1) {
      this.relayBySerial.push(relay)
    }

    let rfpk = this.orderedRelaysByPubKey[pubkey]
    if (!rfpk) {
      rfpk = []
    }

    const entryIndex = rfpk.findIndex(re => re.Relay === relayIndex)
    if (entryIndex === -1) {
      const entry = new RelayEntry(relayIndex)
      entry.timestamps[key] = ts
      rfpk.push(entry)
    } else {
      if (rfpk[entryIndex].timestamps[key] < ts) {
        rfpk[entryIndex].timestamps[key] = ts
      }
    }

    this.orderedRelaysByPubKey[pubkey] = rfpk
  }

  public async topN(pubkey: string, n: number): Promise<string[]> {
    // try to fetch relays first
    if (!this.hasLoadedRelaysFor.has(pubkey)) {
      try {
        let {
          event: { created_at },
          items,
        } = await loadRelayList(pubkey)
        items.forEach(rl => {
          if (rl.write) {
            this.save(pubkey, rl.url, HintKey.lastInRelayList, created_at)
          }
        })
      } catch (err) {}
      this.hasLoadedRelaysFor.add(pubkey)
    }

    const urls: string[] = []
    const rfpk = this.orderedRelaysByPubKey[pubkey]

    if (rfpk) {
      // sort entries in descending order based on sum
      rfpk.sort((a, b) => b.sum() - a.sum())

      for (let i = 0; i < n && i < rfpk.entries.length; i++) {
        urls.push(this.relayBySerial[rfpk.entries[i].Relay])
      }
    }

    return urls
  }

  public printScores(): void {
    console.log('= print scores')
    for (let pubkey in this.orderedRelaysByPubKey) {
      let rfpk = this.orderedRelaysByPubKey[pubkey]
      console.log(`== relay scores for ${pubkey}`)
      for (let i = 0; i < rfpk.entries.length; i++) {
        const re = rfpk.entries[i]
        console.log(
          `  ${i.toString().padStart(3)} :: ${this.relayBySerial[re.Relay].padEnd(30)} (${re.Relay}) ::> ${re.sum().toString().padStart(12)}`,
        )
      }
    }
  }
}

type RelaysForPubKey = RelayEntry[]

class RelayEntry {
  public Relay: number
  public timestamps: HintKey[] = new Array(7).fill(0)

  constructor(relay: number) {
    this.Relay = relay
  }

  public sum(): number {
    const now = Date.now() / 1000 + 24 * 60 * 60
    let sum = 0
    for (let i = 0; i < this.timestamps.length; i++) {
      if (this.timestamps[i] === 0) continue

      const value = (hintBasePoints[i] * 10000000000) / Math.pow(Math.max(now - this.timestamps[i], 1), 1.3)
      sum += value
    }
    return sum
  }
}

export enum HintKey {
  lastFetchAttempt,
  mostRecentEventFetched,
  lastInRelayList,
  lastInTag,
  lastInNprofile,
  lastInNevent,
  lastInNIP05,
}

const hintBasePoints = {
  [HintKey.lastFetchAttempt]: -500,
  [HintKey.mostRecentEventFetched]: 700,
  [HintKey.lastInRelayList]: 350,
  [HintKey.lastInTag]: 5,
  [HintKey.lastInNprofile]: 22,
  [HintKey.lastInNevent]: 8,
  [HintKey.lastInNIP05]: 7,
}