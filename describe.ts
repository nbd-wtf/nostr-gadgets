/**
 * @module
 * Renders a short plaintext description of an event, according to the kind it is from
 * (as registered at https://github.com/nostr-protocol/registry-of-kinds).
 */

import type { NostrEvent } from '@nostr/tools/core'

const KIND_NAMES: { [kind: number]: string } = {
  0: 'User Metadata',
  1: 'Short Text Note',
  3: 'Follows',
  4: 'Encrypted Direct Messages',
  5: 'Event Deletion Request',
  6: 'Repost',
  7: 'Reaction',
  8: 'Badge Award',
  9: 'Chat Message',
  11: 'Forum Thread',
  13: 'Seal',
  14: 'Direct Message',
  15: 'File Message',
  16: 'Generic Repost',
  17: 'Reaction to a Website',
  20: 'Photo',
  21: 'Normal Video Event',
  22: 'Short Video Event',
  24: 'Public Message',
  30: 'Jester Chess Event',
  40: 'Channel Creation',
  41: 'Channel Metadata',
  42: 'Channel Message',
  43: 'Channel Hide Message',
  44: 'Channel Mute User',
  54: 'Podcast Episode',
  62: 'Request to Vanish',
  64: 'Chess (PGN)',
  78: 'Application Data',
  82: 'Medical Data (FHIR)',
  777: 'Spell',
  818: 'Wiki Merge Requests',
  1010: 'Text Note Edit',
  1018: 'Poll Response',
  1021: 'Bid',
  1022: 'Bid Confirmation',
  1040: 'OpenTimestamps',
  1059: 'Gift Wrap',
  1063: 'File Metadata',
  1064: 'Blob Data (NIP-95)',
  1065: 'Blob Header (NIP-95)',
  1068: 'Poll',
  1073: 'Music Track Scrobble',
  1111: 'Comment',
  1163: 'Exclusive Content Membership',
  1222: 'Voice Message',
  1227: 'Scroll',
  1244: 'Voice Message Comment',
  1301: 'Workout Record',
  1311: 'Live Chat Message',
  1312: 'Live Activity Raid',
  1313: 'Live Activity Clip',
  1315: 'Road Event Report',
  1316: 'Road Event Confirmation',
  1337: 'Code Snippet',
  1617: 'Git Patch',
  1618: 'Git Pull Request',
  1619: 'Git Pull Request Update',
  1621: 'Git Issue',
  1622: 'Git Reply',
  1630: 'Git Issue Status Open',
  1631: 'Git Issue Applied/Merged/Resolved',
  1632: 'Git Issue Status Closed',
  1633: 'Git Issue Status Draft',
  1808: 'Audio Header',
  1971: 'Problem Tracker',
  1984: 'Reporting',
  1985: 'Label',
  1986: 'Relay Reviews',
  1987: 'AI Embeddings / Vector Lists',
  2003: 'Torrent',
  2004: 'Torrent Comment',
  2022: 'Coinjoin Pool',
  3063: 'Software Asset',
  4312: 'Nests Admin Command',
  4454: 'Decoupled Key Client Announcement',
  4455: 'Decoupled Encryption Key Distribution',
  4550: 'Community Post Approval',
  5129: 'Napplet Snapshot',
  6969: 'Zap Poll',
  7374: 'Reserved Cashu Wallet Tokens',
  7375: 'Cashu Wallet Tokens',
  7376: 'Cashu Wallet History',
  7516: 'Geocache Log',
  7517: 'Geocache Proof of Find',
  8000: 'Relay Add Member',
  8001: 'Relay Remove Member',
  8333: 'Onchain Zap',
  8828: 'Timeline Act',
  9000: 'Put User (add/update user in group)',
  9001: 'Remove User From Group',
  9002: 'Edit Group Metadata',
  9005: 'Delete Event From Group',
  9007: 'Create Group',
  9008: 'Delete Group',
  9009: 'Create Group Invite',
  9021: 'Join Request',
  9022: 'Leave Request',
  9041: 'Zap Goal',
  9321: 'Nutzap',
  9467: 'Tidal Login',
  9733: 'Private Zap',
  9734: 'Zap Request',
  9735: 'Zap',
  9802: 'Highlights',
  10000: 'Mute List',
  10001: 'Pin List',
  10002: 'Relay List Metadata',
  10003: 'Bookmark List',
  10004: 'Communities List',
  10005: 'Public Chats List',
  10006: 'Blocked Relays List',
  10007: 'Search Relays List',
  10009: 'User Groups',
  10011: 'External Identities',
  10012: 'Favorite Relays List',
  10013: 'Private Event Relay List',
  10015: 'Interests List',
  10019: 'Nutzap Mint Recommendation',
  10020: 'Media Follows',
  10021: 'Favorite Follow Sets',
  10030: 'User Emoji List',
  10031: 'User sticker pack list',
  10044: 'Decoupled Key Announcement',
  10050: 'Relay List to Receive DMs',
  10054: 'Favorite Podcasts',
  10063: 'Blossom Server List',
  10096: 'File Storage Server List',
  10101: 'Good Wiki Authors',
  10102: 'Good Wiki Relays',
  10112: 'Nests Server List',
  10133: 'Payment Targets',
  10154: 'Podcast Metadata',
  10163: 'Exclusive Content Announcement',
  10164: 'Authored Podcasts',
  10166: 'Relay Monitor Announcement',
  10312: 'Room Presence',
  10317: 'User Grasp List',
  10377: 'Proxy Announcement',
  11111: 'Transport Method Announcement',
  11871: 'Attestor Proficiency',
  12473: 'Birdex',
  13194: 'Wallet Info',
  13534: 'Relay Membership List',
  15128: 'Root Nsite',
  15129: 'Root Napplet',
  17375: 'Cashu Wallet Event',
  21000: 'Lightning Pub RPC',
  21001: 'CLINK Offer',
  21002: 'CLINK Debit',
  21003: 'CLINK Manage',
  21059: 'Ephemeral Gift Wrap',
  22242: 'Client Authentication',
  23194: 'Wallet Request',
  23195: 'Wallet Response',
  23196: 'NWC Notification (Legacy)',
  23197: 'NWC Notification',
  23333: 'Ephemeral Chat Room',
  23903: 'Wake Up',
  24133: 'Nostr Connect',
  24242: 'Blobs stored on mediaservers',
  25050: 'Call Offer',
  25051: 'Call Answer',
  25052: 'Call ICE Candidate',
  25053: 'Call Hangup',
  25054: 'Call Reject',
  25055: 'Call Renegotiate',
  27235: 'HTTP Auth',
  28934: 'Relay Join Request',
  28935: 'Relay Invite Request',
  28936: 'Relay Leave Request',
  30000: 'Follow Set',
  30001: 'Old Bookmark Set',
  30002: 'Relay Set',
  30003: 'Bookmark Set',
  30004: 'Curation Set',
  30005: 'Video Set',
  30006: 'Picture Set',
  30007: 'Kind Mute Set',
  30008: 'Profile Badges',
  30009: 'Badge Definition',
  30015: 'Interest Set',
  30017: 'Create or Update a Stall',
  30018: 'Create or Update a Product',
  30019: 'Marketplace UI/UX',
  30020: 'Product Sold as an Auction',
  30023: 'Long-form Content',
  30024: 'Draft Long-form Content',
  30030: 'Emoji Set',
  30031: 'Sticker pack',
  30040: 'Curated Publication Index',
  30041: 'Curated Publication Content',
  30053: 'NNS Name',
  30063: 'Release Artifact Set',
  30078: 'Application-specific Data',
  30166: 'Relay Discovery',
  30267: 'App Curation Set',
  30296: 'Interactive Story Prologue',
  30297: 'Interactive Story Scene',
  30298: 'Interactive Story Reading State',
  30311: 'Live Event',
  30312: 'Interactive Room',
  30313: 'Conference Event',
  30315: 'User Statuses',
  30382: 'User Assertion (NIP-85)',
  30383: 'Event Assertion (NIP-85)',
  30384: 'Addressable Assertion (NIP-85)',
  30385: 'External ID Assertion (NIP-85)',
  30388: 'Slide Set',
  30402: 'Classified Listing',
  30403: 'Draft Classified Listing',
  30617: 'Git Repository Announcement',
  30618: 'Git Repository State Announcement',
  30818: 'Wiki Article',
  30819: 'Wiki Redirects',
  30828: 'Timeline Card',
  31234: 'Draft Event',
  31337: 'Audio Track',
  31388: 'Link Set',
  31436: 'Gopherhole Document',
  31871: 'Attestation',
  31872: 'Attestation Request',
  31873: 'Attestor Recommendation',
  31890: 'Feed',
  31922: 'Date-Based Calendar Event',
  31923: 'Time-Based Calendar Event',
  31924: 'Calendar',
  31925: 'Calendar Event RSVP',
  31989: 'Handler Recommendation',
  31990: 'Handler Information',
  31992: 'Slash Command Definition',
  32267: 'Software Application',
  33401: 'Exercise Template',
  33534: 'Relay Role Definition',
  33863: 'Fundraiser',
  34139: 'Music Playlist',
  34235: 'Video Event (Horizontal)',
  34236: 'Video Event (Vertical)',
  34550: 'Community Definition',
  34551: 'Community Rules',
  35128: 'Named Nsite',
  35129: 'Named Napplet',
  35130: 'Napp',
  36787: 'Music Tracks',
  36820: 'Hitchhiking Ride, per the Hitchhiking Data Standard',
  37515: 'Geocache Listing',
  37516: 'Geocache Log Entry',
  38000: 'Mint Recommendation',
  38172: 'Cashu Mint Announcement',
  38173: 'Fedimint Announcement',
  38383: 'Peer-to-peer Order Events',
  39000: 'Group Metadata',
  39001: 'Group Admins',
  39002: 'Group Members',
  39003: 'Group Roles',
  39089: 'Starter Packs',
  39092: 'Media Starter Packs',
  39701: 'Web Bookmarks',
}

/**
 * The registered name of a kind, as in the kinds registry.
 */
export function kindName(kind: number): string {
  return KIND_NAMES[kind] || `kind ${kind}`
}

function tagValue(event: NostrEvent, tag: string): string {
  return (event.tags.find(([t]) => t === tag)?.[1] || '').trim()
}

function countTags(event: NostrEvent, ...tags: string[]): number {
  let n = 0
  for (let i = 0; i < event.tags.length; i++) {
    if (tags.indexOf(event.tags[i][0]) !== -1) n++
  }
  return n
}

function parseJSON(text: string): any {
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch (_err) {
    return null
  }
}

function imeta(event: NostrEvent): { [key: string]: string } {
  const tag = event.tags.find(([t]) => t === 'imeta')
  if (!tag) return {}
  let result: { [key: string]: string } = {}
  for (let i = 1; i < tag.length; i++) {
    const sep = tag[i].indexOf(' ')
    if (sep > 0) result[tag[i].slice(0, sep)] = tag[i].slice(sep + 1)
  }
  return result
}

function plural(n: number, word: string): string {
  return `${n} ${word}${n === 1 ? '' : 's'}`
}

function labeled(label: string, ...values: (string | undefined)[]): string {
  const value = values.filter(v => v).join(' ')
  return value ? `${label}: ${value}` : label
}

/**
 * Renders a short plaintext representation of an event: notes and comments become their content,
 * everything else gets the name of its kind plus whatever the registry says is important about it.
 */
export function describeEvent(event: NostrEvent): string {
  const name = kindName(event.kind)
  const body = (event.content || '').trim()

  switch (event.kind) {
    case 0: {
      const profile = parseJSON(body)
      return labeled(name, profile?.name)
    }

    case 1:
    case 1111:
      return body

    case 3:
      return labeled(name, plural(countTags(event, 'p'), 'follow'))

    case 4:
    case 14:
      return labeled(name, tagValue(event, 'subject'))

    case 5:
      return labeled(name, plural(countTags(event, 'e', 'a'), 'event'))

    case 6:
    case 16: {
      const reposted = parseJSON(body)
      return (reposted?.content || '').trim() || name
    }

    case 7:
      return labeled(name, body)

    case 8:
    case 13:
    case 1059:
    case 1064:
    case 1065:
    case 8000:
    case 8001:
      return name

    case 9:
    case 24:
    case 30:
    case 42:
    case 62:
    case 64:
    case 82:
    case 818:
    case 1021:
    case 1022:
    case 1040:
    case 1222:
    case 1244:
    case 1311:
    case 1312:
    case 1315:
    case 1617:
    case 1619:
    case 1622:
    case 1630:
    case 1631:
    case 1632:
    case 1633:
    case 1971:
    case 1984:
    case 1985:
    case 1986:
    case 1987:
    case 2004:
    case 2022:
    case 4455:
    case 4550:
    case 6969:
    case 7374:
    case 7375:
    case 7376:
    case 7516:
    case 7517:
    case 8333:
    case 9321:
    case 9467:
    case 9733:
    case 9734:
      return body ? labeled(name, body) : name

    case 11:
    case 1301:
    case 1313:
    case 2003:
    case 8828:
    case 10154:
    case 15128:
    case 15129:
      return labeled(name, tagValue(event, 'title'), body)

    case 15:
      return labeled(name, tagValue(event, 'subject'), body)

    case 17:
      return labeled(name, tagValue(event, 'r'), body)

    case 20:
    case 21:
    case 22:
      return labeled(name, tagValue(event, 'title'), imeta(event).url)

    case 40:
    case 41: {
      const channel = parseJSON(body)
      return labeled(name, tagValue(event, 'title'), channel?.name)
    }

    case 43:
    case 44: {
      const data = parseJSON(body)
      return labeled(name, data?.reason)
    }

    case 54:
      return labeled(name, tagValue(event, 'title'), tagValue(event, 'description'))

    case 78:
    case 1163:
      return labeled(name, tagValue(event, 'd'), body)

    case 777: {
      const spell = parseJSON(body)
      return labeled(name, spell?.method)
    }

    case 1010:
      return labeled(name, tagValue(event, 't'), tagValue(event, 'summary'), body)

    case 1018:
      return labeled(name, tagValue(event, 'response'))

    case 1063:
    case 3063:
      return labeled(name, tagValue(event, 'url'), body)

    case 1068: {
      const options = event.tags.filter(([t]) => t === 'option').map(t => t.slice(2).join(' '))
      return labeled(name, options.join(', '))
    }

    case 1073:
      return labeled(name, tagValue(event, 'title'), tagValue(event, 'artist'))

    case 1227:
      return labeled(name, tagValue(event, 'description'), tagValue(event, 'name'))

    case 1316:
      return labeled(name, tagValue(event, 'status'))

    case 1337:
      return labeled(name, tagValue(event, 'l'), tagValue(event, 'name'), body)

    case 1618:
    case 1621:
    case 31337:
      return labeled(name, tagValue(event, 'subject'))

    case 1808:
      return labeled(name, tagValue(event, 'download_url'), tagValue(event, 'stream_url'))

    case 4312:
      return labeled(name, tagValue(event, 'action'))

    case 4454:
      return labeled(name, tagValue(event, 'client'))

    case 5129:
      return labeled(name, tagValue(event, 'title'), tagValue(event, 'path'))

    case 9000:
    case 9001:
    case 9005:
    case 9007:
    case 9008:
    case 9009:
    case 9021:
    case 9022:
      return labeled(name, tagValue(event, 'h'))

    case 9002:
      return labeled(name, tagValue(event, 'title'), tagValue(event, 'h'))

    case 9041:
      return labeled(name, tagValue(event, 'summary'), body)

    case 9735: {
      const zap = parseJSON(tagValue(event, 'description'))
      return labeled(name, zap?.comment)
    }

    case 9802:
      return labeled(name, tagValue(event, 'comment'), body)
  }

  if (event.kind >= 30000 && event.kind <= 39999) {
    return labeled(name, tagValue(event, 'title') || tagValue(event, 'd'))
  }

  if (event.kind >= 10000) {
    return name
  }

  return body || name
}
