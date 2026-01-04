use redb::TableDefinition;
use sha2::{Digest, Sha256};

use crate::utils::{IndexableEvent, parse_hex_into};

pub const EVENTS: TableDefinition<u32, &[u8]> = TableDefinition::new("events");
pub const INDEX_ID: TableDefinition<&[u8], u32> = TableDefinition::new("index_id");

pub const INDEX_NOTHING: TableDefinition<&[u8], ()> = TableDefinition::new("index_nothing");
pub const INDEX_KIND: TableDefinition<&[u8], ()> = TableDefinition::new("index_kind");
pub const INDEX_PUBKEY: TableDefinition<&[u8], ()> = TableDefinition::new("index_pubkey");
pub const INDEX_PUBKEY_KIND: TableDefinition<&[u8], ()> = TableDefinition::new("index_pubkey_kind");
pub const INDEX_PUBKEY_DTAG: TableDefinition<&[u8], ()> = TableDefinition::new("index_pubkey_dtag");
pub const INDEX_TAG: TableDefinition<&[u8], ()> = TableDefinition::new("index_tag");

pub const INDEX_FOLLOWED: TableDefinition<[u8; 16], ()> = TableDefinition::new("index_followed");

#[derive(Debug)]
pub struct IndexEntry {
    pub table_name: &'static str,
    pub key: Vec<u8>,
}

pub fn compute_indexes(indexable_event: &IndexableEvent, serial: u32) -> Vec<IndexEntry> {
    let mut indexes = Vec::new();
    let timestamp = indexable_event.timestamp;

    // index_id: [8-bytes-at-the-end-of-id] => [u32 serial]
    // (this is different from the rest)
    let mut id_key = vec![0u8; 8];
    parse_hex_into(&indexable_event.id[48..64], &mut id_key[0..8]).expect("invalid hex id");
    indexes.push(IndexEntry {
        table_name: "index_id",
        key: id_key,
    });

    // index_nothing: [4-bytes-of-the-timestamp][4-bytes-serial]
    let mut nothing_key = vec![0u8; 8];
    nothing_key[0..4].copy_from_slice(&timestamp.to_be_bytes());
    nothing_key[4..8].copy_from_slice(&serial.to_be_bytes());
    indexes.push(IndexEntry {
        table_name: "index_nothing",
        key: nothing_key,
    });

    // index_kind: [2-bytes-of-the-kind][4-bytes-of-the-timestamp][4-bytes-serial]
    let mut kind_key = vec![0u8; 10];
    kind_key[0..2].copy_from_slice(&(indexable_event.kind as u16).to_be_bytes());
    kind_key[2..6].copy_from_slice(&timestamp.to_be_bytes());
    kind_key[6..10].copy_from_slice(&serial.to_be_bytes());
    indexes.push(IndexEntry {
        table_name: "index_kind",
        key: kind_key,
    });

    // index_pubkey: [8-bytes-at-the-end-of-pubkey][4-bytes-of-the-timestamp][4-bytes-serial]
    let mut pubkey_key = vec![0u8; 16];
    parse_hex_into(&indexable_event.pubkey[48..64], &mut pubkey_key[0..8])
        .expect("invalid hex pubkey");
    pubkey_key[8..12].copy_from_slice(&timestamp.to_be_bytes());
    pubkey_key[12..16].copy_from_slice(&serial.to_be_bytes());
    indexes.push(IndexEntry {
        table_name: "index_pubkey",
        key: pubkey_key.clone(),
    });

    // index_pubkey_kind: [8-bytes-at-the-end-of-pubkey][2-bytes-of-the-kind][4-bytes-of-the-timestamp][4-bytes-serial]
    let mut pubkey_kind_key = vec![0u8; 18];
    pubkey_kind_key[0..8].copy_from_slice(&pubkey_key[0..8]);
    pubkey_kind_key[8..10].copy_from_slice(&indexable_event.kind.to_be_bytes());
    pubkey_kind_key[10..14].copy_from_slice(&timestamp.to_be_bytes());
    pubkey_kind_key[14..18].copy_from_slice(&serial.to_be_bytes());
    indexes.push(IndexEntry {
        table_name: "index_pubkey_kind",
        key: pubkey_kind_key,
    });

    // index_pubkey_dtag: [8-bytes-at-the-end-of-pubkey][8-initial-bytes-of-sha256-of-d-tag][4-bytes-of-the-timestamp][4-bytes-serial]
    if let Some(d) = &indexable_event.dtag {
        let mut hasher = Sha256::new();
        hasher.update(d.as_bytes());
        let hash = hasher.finalize();

        let mut pubkey_dtag_key = vec![0u8; 24];
        pubkey_dtag_key[0..8].copy_from_slice(&pubkey_key[0..8]);
        pubkey_dtag_key[8..16].copy_from_slice(&hash[0..8]);
        pubkey_dtag_key[16..20].copy_from_slice(&timestamp.to_be_bytes());
        pubkey_dtag_key[20..24].copy_from_slice(&serial.to_be_bytes());
        indexes.push(IndexEntry {
            table_name: "index_pubkey_dtag",
            key: pubkey_dtag_key,
        });
    }

    // index_tag: for each tag, create index entries
    for (tag_letter, tag_value) in &indexable_event.tags {
        let mut tag_key = vec![0u8; 17];
        tag_key[0] = *tag_letter;

        let mut hasher = Sha256::new();
        hasher.update(tag_value.as_bytes());
        let hash = hasher.finalize();

        tag_key[1..9].copy_from_slice(&hash[0..8]);
        tag_key[9..13].copy_from_slice(&timestamp.to_be_bytes());
        tag_key[13..17].copy_from_slice(&serial.to_be_bytes());
        indexes.push(IndexEntry {
            table_name: "index_tag",
            key: tag_key,
        });
    }

    indexes
}
