import { normalizeURL } from '@nostr/tools/utils'

import { loadRelayList } from './lists.ts'
import { Filter } from '@nostr/tools'

/**
 * Takes a list of public keys and a filter (assumed to not contain an `authors` field).
 *
 * Returns a list of maps that can be passed to @nostr/tools/pool's `subscribeMap()`.
 *
 * It tries to select 2, 3 or 4 (depending on how many pubkeys you're giving -- more
 * pubkeys means less relays are used) outbox relays from each of these pubkeys, based
 * solely on their kind:10002 relay list, and query those. from all hinted outbox relays
 * it will pick the most popular -- considering the other pubkeys -- so the number of
 * relay connections is minimized.
 */
export async function outboxFilterRelayBatch(pubkeys: string[], ...baseFilters: Filter[]) {
  const declaration: { url: string; filter: Filter }[] = []

  type Count = { count: number }
  const relaysByCount: { [url: string]: Count } = {}
  const relaysByPubKey: { [pubkey: string]: { [url: string]: Count } } = {}
  const numberOfRelaysPerUser = pubkeys.length < 100 ? 4 : pubkeys.length < 800 ? 3 : pubkeys.length < 1200 ? 2 : 1

  // get the most popular relays among the list of followed people
  await Promise.all(
    pubkeys.map(async pubkey => {
      const rl = await loadRelayList(pubkey)
      relaysByPubKey[pubkey] = {}

      let w = 0
      for (let i = 0; i < rl.items.length; i++) {
        if (rl.items[i].write) {
          try {
            const url = normalizeURL(rl.items[i].url)
            const count = relaysByCount[url] || { count: 0 }
            relaysByCount[url] = count
            relaysByPubKey[pubkey][url] = count
            count.count++
            w++
          } catch (_err) {
            /***/
          }
        }

        if (w >= 7) break
      }
    }),
  )

  // choose from the most popular first for each user
  for (let i = 0; i < pubkeys.length; i++) {
    const pubkey = pubkeys[i]
    const list: [string, number][] = Object.entries(relaysByPubKey[pubkey]).map(([url, count]) => [url, count.count])
    list.sort((a, b) => b[1] - a[1])

    // we'll get a number of relays per user that will be bigger if we're following less people,
    // smaller otherwise
    const top = list.slice(0, numberOfRelaysPerUser)

    for (let r = 0; r < top.length; r++) {
      const url = top[r][0]
      let found = false
      for (let i = 0; i < declaration.length; i++) {
        const decl = declaration[i]
        if (decl.url === url) {
          // if this relay is found that means it already has all the filters
          // so we just add the pubkey to all of them
          found = true
          decl.filter.authors!.push(pubkey)
        }
      }

      // otherwise we add all the filters to this relay
      if (!found) {
        for (let f = 0; f < baseFilters.length; f++) {
          declaration.push({
            url,
            filter: { ...baseFilters[f], authors: [pubkey] },
          })
        }
      }
    }
  }

  return declaration
}
