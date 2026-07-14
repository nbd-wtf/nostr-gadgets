/**
 * @module
 * Contains helpers for dealing with the default "web-of-trust", i.e. follows of follows.
 */

import { createStore, get, set, UseStore } from 'idb-keyval'
import { loadFollowsList, loadMuteList } from './lists'

var store: UseStore

/**
 * Returns a set of all pubkeys in another pubkey's naïve "web-of-trust",
 * i.e. all follows and follows of follows (except for mutes).
 **/
export async function loadWoT(pubkey: string): Promise<Set<string>> {
  if (!store) store = createStore(`@nostr/gadgets/wot`, 'cache')

  const now = Date.now() / 1000
  let res = await get<{ pubkeys: string[]; lastAttempt: number }>(`${pubkey}`, store)
  if (
    !res ||
    res.lastAttempt < now - 60 * 60 * 24 * 5 /* 5 days old */ ||
    (res.lastAttempt < now * 60 * 60 * 12 && res.pubkeys.length < 5) /* basically empty */
  ) {
    const fl = await loadFollowsList(pubkey).then(fl => fl.items)
    const mutes = await loadMuteList(pubkey).then(fl => fl.items.filter(m => m.label === 'pubkey').map(m => m.value))
    return Promise.all(fl.map(f => loadFollowsList(f).then(fl => fl.items))).then(ffln => {
      const wot = new Set<string>()

      for (let i = 0; i < ffln.length; i++) {
        for (let j = 0; j < ffln[i].length; j++) {
          const pk = ffln[i][j]
          if (!wot.has(pk) && mutes.indexOf(pk) === -1) {
            wot.add(pk)
          }
        }
      }

      set(`${pubkey}`, { pubkeys: Array.from(wot), lastAttempt: now }, store)
      return wot
    })
  } else {
    return new Set(res.pubkeys)
  }
}
