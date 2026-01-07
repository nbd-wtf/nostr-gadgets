#![feature(ascii_char)]

use std::io::{self};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use redb::{
    Database, ReadTransaction, ReadableDatabase, ReadableTable, StorageBackend, WriteTransaction,
};
use serde_json;
use sha2::{Digest, Sha256};
use wasm_bindgen::prelude::*;
use web_sys::FileSystemSyncAccessHandle;

use crate::indexes::*;
use crate::query::{Plan, Query, execute, prepare};
use crate::utils::{
    IndexableEvent, MAX_U32_BYTES, Querier, Result, extract_id, extract_pubkey, parse_hex_into,
};

mod indexes;
mod query;
mod utils;

struct QueryResultEvent {
    json: Vec<u8>,
    timestamp: Option<u32>,
    serial: u32,
}

#[derive(Debug)]
struct WasmBackend {
    sync_handle: FileSystemSyncAccessHandle,
}

impl WasmBackend {
    fn new(sync_handle: FileSystemSyncAccessHandle) -> Self {
        Self { sync_handle }
    }
}

impl StorageBackend for WasmBackend {
    fn len(&self) -> io::Result<u64> {
        let size = self
            .sync_handle
            .get_size()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
        Ok(size as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), io::Error> {
        let mut bytes_read = 0;
        let options = web_sys::FileSystemReadWriteOptions::new();

        while bytes_read != out.len() {
            options.set_at((offset + bytes_read as u64) as f64);

            let read_result = self
                .sync_handle
                .read_with_u8_array_and_options(&mut out[bytes_read..], &options)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

            bytes_read += read_result as usize;
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.sync_handle
            .truncate_with_f64(len as f64)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.sync_handle
            .flush()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let options = web_sys::FileSystemReadWriteOptions::new();
        let mut bytes_written = 0;

        while bytes_written != data.len() {
            options.set_at((offset + bytes_written as u64) as f64);

            let written = self
                .sync_handle
                .write_with_u8_array_and_options(&data[bytes_written..], &options)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

            bytes_written += written as usize;
        }
        Ok(())
    }
}

unsafe impl Send for WasmBackend {}
unsafe impl Sync for WasmBackend {}

#[wasm_bindgen]
pub struct Redstore {
    db: Arc<Mutex<Database>>,
}

#[wasm_bindgen]
impl Redstore {
    #[wasm_bindgen(constructor)]
    pub fn new(sync_handle: &FileSystemSyncAccessHandle) -> Result<Redstore> {
        console_error_panic_hook::set_once();

        let backend = WasmBackend::new(sync_handle.clone());
        let db = Database::builder()
            .create_with_backend(backend)
            .map_err(|e| JsValue::from_str(&format!("failed to create database: {:?}", e)))?;

        let write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;
        write_txn
            .open_table(EVENTS)
            .map_err(|e| JsValue::from_str(&format!("initial open events: {:?}", e)))?;
        write_txn
            .open_table(INDEX_ID)
            .map_err(|e| JsValue::from_str(&format!("initial open index_id: {:?}", e)))?;
        write_txn
            .open_table(INDEX_NOTHING)
            .map_err(|e| JsValue::from_str(&format!("initial open index_nothing: {:?}", e)))?;
        write_txn
            .open_table(INDEX_KIND)
            .map_err(|e| JsValue::from_str(&format!("initial open index_kind: {:?}", e)))?;
        write_txn
            .open_table(INDEX_PUBKEY)
            .map_err(|e| JsValue::from_str(&format!("initial open index_pubkey: {:?}", e)))?;
        write_txn
            .open_table(INDEX_PUBKEY_KIND)
            .map_err(|e| JsValue::from_str(&format!("initial open index_pubkey_kind: {:?}", e)))?;
        write_txn
            .open_table(INDEX_PUBKEY_DTAG)
            .map_err(|e| JsValue::from_str(&format!("initial open index_pubkey_dtag: {:?}", e)))?;
        write_txn
            .open_table(INDEX_TAG)
            .map_err(|e| JsValue::from_str(&format!("initial open index_tag: {:?}", e)))?;
        write_txn
            .open_table(INDEX_FOLLOWED)
            .map_err(|e| JsValue::from_str(&format!("initial open index_followed: {:?}", e)))?;
        write_txn
            .open_table(OUTBOX_BOUNDS)
            .map_err(|e| JsValue::from_str(&format!("initial open outbox_bounds: {:?}", e)))?;
        write_txn
            .open_table(LAST_ATTEMPT)
            .map_err(|e| JsValue::from_str(&format!("initial open last_attempts: {:?}", e)))?;
        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(Redstore {
            db: Arc::new(Mutex::new(db)),
        })
    }

    // takes a filter and returns an array of events.
    pub fn query_events(&self, filter: js_sys::Object) -> Result<js_sys::Array> {
        #[cfg(debug_assertions)]
        web_sys::console::log_2(&js_sys::JsString::from("query_events"), &filter);

        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        let spec: Querier = (&filter).into();
        let events = self.query_internal(&read_txn, spec)?;
        let array = js_sys::Array::new_with_length(events.len() as u32);
        for (
            i,
            QueryResultEvent {
                json,
                timestamp: _,
                serial: _,
            },
        ) in events.iter().enumerate()
        {
            array.set(i as u32, js_sys::Uint8Array::from(json.as_slice()).into());
        }

        Ok(array)
    }

    // takes a single Uint8Array in the form [kind_u16][pubkey-8bytes][d-tag-hash-8-bytes-or-zeroes][...repeat],
    // returns an array of [last_attempt_timestamp, event_json_as_uint8array]
    pub fn load_replaceables(&self, specs: &[u8]) -> Result<js_sys::Array> {
        #[cfg(debug_assertions)]
        web_sys::console::log_1(&js_sys::JsString::from(format!(
            "load_replaceables {:?}",
            specs
        )));

        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        let results = js_sys::Array::new_with_length(specs.len() as u32 / 18);
        let mut r = 0;
        for i in (0..specs.len()).step_by(18) {
            let result = js_sys::Array::new_with_length(2);
            let key: [u8; 18] = specs[i..i + 18].try_into().expect("18 is not 18");

            let last_attempt_table = read_txn.open_table(LAST_ATTEMPT).map_err(|e| {
                JsValue::from_str(&format!("open last_attempts table error: {:?}", e))
            })?;

            if let Some(ts) = last_attempt_table
                .get(key)
                .map_err(|e| JsValue::from_str(&format!("get from last_attempts error: {:?}", e)))?
            {
                result.set(0, ts.value().into());
            }

            let kind: u16 = u16::from_be_bytes(key[0..2].try_into().unwrap());
            let author = &key[2..10];
            let dtaghash = &key[10..18];

            let mut plan = if dtaghash == [0, 0, 0, 0, 0, 0, 0, 0] {
                // no d-tag
                let mut query_key = vec![0u8; 18];
                query_key[0..8].copy_from_slice(author);
                query_key[8..10].copy_from_slice(&key[0..2]);
                query_key[10..14].copy_from_slice(&MAX_U32_BYTES);
                query_key[14..18].copy_from_slice(&MAX_U32_BYTES);

                Plan {
                    since: 0,
                    extra_kinds: Vec::new(),
                    extra_authors: None,
                    extra_tags: None,
                    queries: vec![Query {
                        table_name: "index_pubkey_kind",
                        results: Vec::with_capacity(1),
                        exhausted: false,
                        curr_key: query_key,
                    }],
                }
            } else {
                let mut query_key = vec![0u8; 24];
                query_key[0..8].copy_from_slice(author);
                query_key[8..16].copy_from_slice(dtaghash);
                query_key[16..20].copy_from_slice(&MAX_U32_BYTES);
                query_key[20..24].copy_from_slice(&MAX_U32_BYTES);

                Plan {
                    since: 0,
                    extra_kinds: vec![kind],
                    extra_authors: None,
                    extra_tags: None,
                    queries: vec![Query {
                        table_name: "index_pubkey_dtag",
                        results: Vec::with_capacity(1),
                        exhausted: false,
                        curr_key: query_key,
                    }],
                }
            };

            let events = execute(&read_txn, &mut plan, 1, 1)?;
            if let Some(QueryResultEvent {
                json,
                timestamp: _,
                serial: _,
            }) = events.first()
            {
                result.set(1, js_sys::Uint8Array::from(json.as_slice()).into());
            }

            results.set(r, result.into());
            r += 1;
        }

        Ok(results)
    }

    fn query_internal(
        &self,
        txn: &ReadTransaction,
        mut spec: Querier,
    ) -> Result<Vec<QueryResultEvent>> {
        // special id query, just get the ids in whatever order and return
        if let Some(ids) = spec.ids {
            let events_table = txn
                .open_table(EVENTS)
                .map_err(|e| JsValue::from_str(&format!("open events table error: {:?}", e)))?;

            let mut results: Vec<QueryResultEvent> = Vec::with_capacity(ids.len());

            for id in ids {
                let mut id_key = [0u8; 8];
                parse_hex_into(&id[48..64], &mut id_key[0..8])
                    .map_err(|e| JsValue::from_str(&format!("id is not valid hex: {:?}", e)))?;

                let ids_index = txn
                    .open_table(INDEX_ID)
                    .map_err(|e| JsValue::from_str(&format!("open index_id error: {:?}", e)))?;

                // get directly
                if let Some(s) = ids_index
                    .get(&id_key[..])
                    .map_err(|e| JsValue::from_str(&format!("get from index_id error: {:?}", e)))?
                {
                    let serial = s.value();
                    if let Some(event_json) = events_table.get(serial).map_err(|e| {
                        JsValue::from_str(&format!("get from events error: {:?}", e))
                    })? {
                        results.push(QueryResultEvent {
                            json: event_json.value().to_owned(),
                            timestamp: None,
                            serial: serial,
                        });
                    }
                }
            }

            return Ok(results);
        }

        // theoretical limit
        if let (Some(authors), Some(kinds)) = (&spec.authors, &spec.kinds) {
            if let Some(d_tags) = &spec.dtags {
                let theoretical_limit = authors.len() * kinds.len() * d_tags.len();
                if theoretical_limit < spec.limit {
                    spec.limit = theoretical_limit;
                }
            } else if kinds
                .iter()
                .all(|kind| *kind == 0 || *kind == 3 || (*kind >= 10000 && *kind < 20000))
            {
                let theoretical_limit = authors.len() * kinds.len();
                if theoretical_limit < spec.limit {
                    spec.limit = theoretical_limit;
                }
            }
        }

        let mut plan = prepare(&mut spec)?;
        execute(&txn, &mut plan, spec.limit, std::cmp::min(20, spec.limit))
    }

    // takes { last_attempts: [], followedBys: [], rawEvents: [] }, returns [bool, ...]
    // last_attempts timestamps used by replaceable loaders only, otherwise should be zero;
    // followedBys are arrays of pubkeys;
    // rawEvents are JSON-encoded raw events, as Uint8Arrays (they can be empty so we'll only store the last_attempt);
    // on the return: true means it was saved, false it wasn't because we already had it or something newer than it)
    pub fn save_events(
        &self,
        last_attempts: js_sys::Array,
        followedbys_arr: js_sys::Array,
        raw_events_arr: js_sys::Array,
    ) -> Result<JsValue> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let mut write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        // get last serial
        let last_serial = {
            let events_table = write_txn
                .open_table(EVENTS)
                .map_err(|e| JsValue::from_str(&format!("serial get table error: {:?}", e)))?;

            let s = events_table
                .last()
                .map_err(|e| JsValue::from_str(&format!("get serial err: {:?}", e)))?
                .map(|l| l.0.value())
                .unwrap_or(0);

            s
        };

        if last_serial % 4 != 0 {
            let _ = write_txn.set_durability(redb::Durability::None);
        }

        #[cfg(debug_assertions)]
        web_sys::console::log_1(
            &js_sys::JsString::from_str(&format!(
                "save_events {:?}",
                (0..raw_events_arr.length())
                    .map(|i| {
                        String::from_utf8(js_sys::Uint8Array::from(raw_events_arr.get(i)).to_vec())
                            .unwrap()
                    })
                    .collect::<Vec<String>>()
            ))
            .unwrap(),
        );

        let result = js_sys::Array::new_with_length(raw_events_arr.length());
        let mut current_serial = last_serial + 1;

        for i in 0..raw_events_arr.length() {
            let last_attempt = last_attempts
                .get(i)
                .as_f64()
                .expect("last_attempt must be number") as u32;

            let raw_event = &js_sys::Uint8Array::from(raw_events_arr.get(i)).to_vec();
            let indexable_event = IndexableEvent::from_json_event(&raw_event)?;

            if last_attempt > 0 {
                // store last attempt if it came
                let mut last_attempts_table = write_txn.open_table(LAST_ATTEMPT).map_err(|e| {
                    JsValue::from_str(&format!("last_attempt insert table error: {:?}", e))
                })?;

                let mut key = [0u8; 18];
                key[0..2].copy_from_slice(&indexable_event.kind.to_be_bytes());
                parse_hex_into(&indexable_event.pubkey[48..64], &mut key[2..10]).map_err(|e| {
                    JsValue::from_str(&format!("invalid pubkey on last_attempt store: {:?}", e))
                })?;

                if let Some(dtag) = &indexable_event.dtag {
                    let mut hasher = Sha256::new();
                    hasher.update(dtag);
                    let hash = hasher.finalize();
                    key[10..18].copy_from_slice(&hash[0..8]);
                }

                last_attempts_table.insert(key, last_attempt).map_err(|e| {
                    JsValue::from_str(&format!("failed to insert last_attempt: {:?}", e))
                })?;
            }

            if indexable_event.id
                == "0000000000000000000000000000000000000000000000000000000000000000"
            {
                // don't store the event if it didn't come
                continue;
            }

            let followed_bys = js_sys::Array::from(&followedbys_arr.get(i));

            // check if event has be replaced
            let replacement_query = if indexable_event.kind == 0
                || indexable_event.kind == 3
                || (indexable_event.kind >= 10000 && indexable_event.kind < 20000)
            {
                // replaceable
                Some(Querier {
                    authors: Some(vec![indexable_event.pubkey.clone()]),
                    limit: 10,
                    ..Default::default()
                })
            } else if indexable_event.kind >= 30000 && indexable_event.kind < 40000 {
                // addressable
                let mut dtag: Option<String> = None;
                for (letter, value) in &indexable_event.tags {
                    // the 'd' tag corresponds to 100
                    if *letter == 100 {
                        dtag = Some(value.clone());
                        break;
                    }
                }

                dtag.map(|d| Querier {
                    authors: Some(vec![indexable_event.pubkey.clone()]),
                    dtags: Some(vec![d]),
                    limit: 10,
                    ..Default::default()
                })
            } else {
                None
            };
            if let Some(rq) = replacement_query {
                let mut should_store = true;
                for QueryResultEvent {
                    json,
                    timestamp,
                    serial,
                } in self.query_internal(&read_txn, rq).map_err(|e| {
                    JsValue::from_str(&format!("pre-replacement query error: {:?}", e))
                })? {
                    if timestamp.expect("query result without ids should always have timestamp")
                        < indexable_event.timestamp
                    {
                        // we have something older stored, delete it
                        let deletable: IndexableEvent =
                            serde_json::from_slice(&json).map_err(|e| {
                                JsValue::from_str(&format!(
                                    "failed to parse old replaceable for deleting: {:?}",
                                    e
                                ))
                            })?;
                        self.delete_internal(&mut write_txn, &deletable, serial)
                            .map_err(|e| {
                                JsValue::from_str(&format!(
                                    "failed to delete old replaceable: {:?}",
                                    e
                                ))
                            })?;
                    } else {
                        // we have something newer (or of the same ts) stored already, so let's not store this event
                        should_store = false;
                    }
                }
                if !should_store {
                    result.set(i, js_sys::Boolean::from(false).into());

                    // even when not storing we still update the followed_bys
                    self.insert_followedby_indexes(
                        &mut write_txn,
                        &indexable_event,
                        current_serial,
                        &followed_bys,
                    )?;

                    continue;
                }
            }

            // insert event and indexes
            let is_new = {
                let mut events_table = write_txn.open_table(EVENTS).map_err(|e| {
                    JsValue::from_str(&format!("events insert table error: {:?}", e))
                })?;

                events_table
                    .insert(current_serial, &raw_event[..])
                    .map_err(|e| JsValue::from_str(&format!("events insert error: {:?}", e)))?
                    .is_none() // it will be None if the key is new, otherwise will return the old value
            };

            // normal indexes (if new)
            if is_new {
                self.insert_indexes(&mut write_txn, &indexable_event, current_serial)?;
            }

            // followed_by indexes (always)
            self.insert_followedby_indexes(
                &mut write_txn,
                &indexable_event,
                current_serial,
                &followed_bys,
            )?;

            current_serial += 1;
            result.set(i, js_sys::Boolean::from(true).into());
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(result.into())
    }

    fn insert_followedby_indexes(
        &self,
        write_txn: &mut WriteTransaction,
        indexable_event: &IndexableEvent,
        serial: u32,
        followed_bys: &js_sys::Array,
    ) -> Result<()> {
        let mut followedby_table = write_txn
            .open_table(INDEX_FOLLOWED)
            .map_err(|e| JsValue::from_str(&format!("followed_by insert table error: {:?}", e)))?;

        for i in 0..followed_bys.length() {
            let follower_hex = followed_bys
                .get(i)
                .as_string()
                .expect("followed_by must be hex pubkey");

            let mut key = vec![0u8; 16];
            parse_hex_into(&follower_hex[48..64], &mut key[0..8])
                .map_err(|e| JsValue::from_str(&format!("followed_by hex error: {:?}", e)))?;
            key[8..12].copy_from_slice(&indexable_event.timestamp.to_be_bytes());
            key[12..16].copy_from_slice(&serial.to_be_bytes());

            followedby_table
                .insert(&key[..], ())
                .map_err(|e| JsValue::from_str(&format!("followed_by insert error: {:?}", e)))?;

            #[cfg(debug_assertions)]
            web_sys::console::log_3(
                &js_sys::JsString::from_str("inserting following").unwrap(),
                &js_sys::JsString::from(follower_hex),
                &js_sys::Uint8Array::from(&key[..]),
            );
        }

        Ok(())
    }

    fn insert_indexes(
        &self,
        write_txn: &mut WriteTransaction,
        indexable_event: &IndexableEvent,
        serial: u32,
    ) -> Result<()> {
        let indexes = compute_indexes(indexable_event, serial);

        for index in indexes {
            match index.table_name {
                "index_id" => {
                    let mut table = write_txn
                        .open_table(INDEX_ID)
                        .map_err(|e| JsValue::from_str(&format!("open index_id error: {:?}", e)))?;
                    table.insert(&index.key[..], serial).map_err(|e| {
                        JsValue::from_str(&format!("insert index_id error: {:?}", e))
                    })?;
                }
                "index_nothing" => {
                    let mut table = write_txn.open_table(INDEX_NOTHING).map_err(|e| {
                        JsValue::from_str(&format!("open index_nothing error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], ()).map_err(|e| {
                        JsValue::from_str(&format!("insert index_nothing error: {:?}", e))
                    })?;
                }
                "index_kind" => {
                    let mut table = write_txn.open_table(INDEX_KIND).map_err(|e| {
                        JsValue::from_str(&format!("open index_kind error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], ()).map_err(|e| {
                        JsValue::from_str(&format!("insert index_kind error: {:?}", e))
                    })?;
                }
                "index_pubkey" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], ()).map_err(|e| {
                        JsValue::from_str(&format!("insert index_pubkey error: {:?}", e))
                    })?;
                }
                "index_pubkey_kind" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY_KIND).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey_kind error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], ()).map_err(|e| {
                        JsValue::from_str(&format!("insert index_pubkey_kind error: {:?}", e))
                    })?;
                }
                "index_pubkey_dtag" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY_DTAG).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey_dtag error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], ()).map_err(|e| {
                        JsValue::from_str(&format!("insert index_pubkey_dtag error: {:?}", e))
                    })?;
                }
                "index_tag" => {
                    let mut table = write_txn.open_table(INDEX_TAG).map_err(|e| {
                        JsValue::from_str(&format!("open index_tag error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], ()).map_err(|e| {
                        JsValue::from_str(&format!("insert index_tag error: {:?}", e))
                    })?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    // takes [filter, ...] and returns [deleted_id, ...]
    pub fn delete_events(&self, filters_arr: js_sys::Array) -> Result<JsValue> {
        #[cfg(debug_assertions)]
        web_sys::console::log_2(
            &js_sys::JsString::from_str("delete_events").unwrap(),
            &filters_arr,
        );

        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let mut write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        let _ = write_txn.set_durability(redb::Durability::None);

        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("read transaction error: {:?}", e)))?;

        let result = js_sys::Array::new();

        // query for the events to delete
        for f in 0..filters_arr.length() {
            for QueryResultEvent {
                json,
                serial,
                timestamp: _,
            } in &self.query_internal(
                &read_txn,
                (&js_sys::Object::from(filters_arr.get(f))).into(),
            )? {
                // parse the event JSON to get the event structure
                let indexable_event: IndexableEvent = serde_json::from_slice(&json)
                    .map_err(|e| JsValue::from_str(&format!("parse event error: {:?}", e)))?;

                // delete the event and its indexes
                let deleted = self.delete_internal(&mut write_txn, &indexable_event, *serial)?;

                #[cfg(debug_assertions)]
                web_sys::console::log_1(&js_sys::JsString::from(format!("deleted {:?}", &deleted)));

                if let Some(id) = deleted {
                    result.push(
                        &js_sys::JsString::from_str(id.as_str())
                            .expect("js string from deleted id"),
                    );

                    // scan the entire INDEX_FOLLOWED for the serial (the last 4 bytes) and delete those entries
                    {
                        let mut followed_table =
                            write_txn.open_table(INDEX_FOLLOWED).map_err(|e| {
                                JsValue::from_str(&format!("open index_followed error: {:?}", e))
                            })?;

                        let mut to_delete: Vec<[u8; 16]> = Vec::new();
                        for item in followed_table.iter().map_err(|e| {
                            JsValue::from_str(&format!("iter index_followed error: {:?}", e))
                        })? {
                            let (key, _) = item.map_err(|e| {
                                JsValue::from_str(&format!(
                                    "get index_followed item error: {:?}",
                                    e
                                ))
                            })?;
                            let key_bytes = key.value();

                            // check if the last 4 bytes match our serial
                            let stored_serial = u32::from_be_bytes([
                                key_bytes[key_bytes.len() - 4],
                                key_bytes[key_bytes.len() - 3],
                                key_bytes[key_bytes.len() - 2],
                                key_bytes[key_bytes.len() - 1],
                            ]);

                            if stored_serial == *serial {
                                to_delete.push(
                                    key_bytes
                                        .try_into()
                                        .expect("index_followed key should have 16 bytes"),
                                );
                            }
                        }

                        // delete the found entries
                        for key in to_delete {
                            followed_table.remove(&key[..]).map_err(|e| {
                                JsValue::from_str(&format!(
                                    "remove index_followed entry error: {:?}",
                                    e
                                ))
                            })?;
                        }
                    }
                }
            }
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(result.into())
    }

    fn delete_internal(
        &self,
        write_txn: &mut WriteTransaction,
        indexable_event: &IndexableEvent,
        serial: u32,
    ) -> Result<Option<String>> {
        // delete from EVENTS table
        let deleted = {
            let mut events_table = write_txn
                .open_table(EVENTS)
                .map_err(|e| JsValue::from_str(&format!("open events table error: {:?}", e)))?;

            events_table
                .remove(serial)
                .map_err(|e| JsValue::from_str(&format!("remove event error: {:?}", e)))?
                .map(|item| extract_id(item.value()))
        };

        // delete from index tables
        let indexes = compute_indexes(indexable_event, serial);

        for index in indexes {
            match index.table_name {
                "index_nothing" => {
                    let mut table = write_txn.open_table(INDEX_NOTHING).map_err(|e| {
                        JsValue::from_str(&format!("open index_nothing error: {:?}", e))
                    })?;
                    table.remove(&index.key[..]).map_err(|e| {
                        JsValue::from_str(&format!("remove index_nothing error: {:?}", e))
                    })?;
                }
                "index_id" => {
                    let mut table = write_txn
                        .open_table(INDEX_ID)
                        .map_err(|e| JsValue::from_str(&format!("open index_id error: {:?}", e)))?;
                    table.remove(&index.key[..]).map_err(|e| {
                        JsValue::from_str(&format!("remove index_id error: {:?}", e))
                    })?;
                }
                "index_kind" => {
                    let mut table = write_txn.open_table(INDEX_KIND).map_err(|e| {
                        JsValue::from_str(&format!("open index_kind error: {:?}", e))
                    })?;
                    table.remove(&index.key[..]).map_err(|e| {
                        JsValue::from_str(&format!("remove index_kind error: {:?}", e))
                    })?;
                }
                "index_pubkey" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey error: {:?}", e))
                    })?;
                    table.remove(&index.key[..]).map_err(|e| {
                        JsValue::from_str(&format!("remove index_pubkey error: {:?}", e))
                    })?;
                }
                "index_pubkey_kind" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY_KIND).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey_kind error: {:?}", e))
                    })?;
                    table.remove(&index.key[..]).map_err(|e| {
                        JsValue::from_str(&format!("remove index_pubkey_kind error: {:?}", e))
                    })?;
                }
                "index_pubkey_dtag" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY_DTAG).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey_dtag error: {:?}", e))
                    })?;
                    table.remove(&index.key[..]).map_err(|e| {
                        JsValue::from_str(&format!("remove index_pubkey_dtag error: {:?}", e))
                    })?;
                }
                "index_tag" => {
                    let mut table = write_txn.open_table(INDEX_TAG).map_err(|e| {
                        JsValue::from_str(&format!("open index_tag error: {:?}", e))
                    })?;
                    table.remove(&index.key[..]).map_err(|e| {
                        JsValue::from_str(&format!("remove index_tag error: {:?}", e))
                    })?;
                }
                _ => {}
            }
        }

        Ok(deleted)
    }

    pub fn mark_follow(
        &self,
        follower: js_sys::JsString,
        followed: js_sys::JsString,
    ) -> Result<()> {
        self.modify_follow_internal(follower, followed, ModifyFollow::Add)
    }

    pub fn mark_unfollow(
        &self,
        follower: js_sys::JsString,
        followed: js_sys::JsString,
    ) -> Result<()> {
        self.modify_follow_internal(follower, followed, ModifyFollow::Remove)
    }

    fn modify_follow_internal(
        &self,
        follower: js_sys::JsString,
        followed: js_sys::JsString,
        action: ModifyFollow,
    ) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("read transaction error: {:?}", e)))?;

        let follower_hex = follower.as_string().expect("follower must be hex pubkey");
        let followed_hex = followed.as_string().expect("followed must be hex pubkey");

        // query all events from the followed pubkey
        let query = Querier {
            authors: Some(vec![followed_hex]),
            limit: 10000, // large limit to get all events
            ..Default::default()
        };

        {
            let mut followedby_table = write_txn
                .open_table(INDEX_FOLLOWED)
                .map_err(|e| JsValue::from_str(&format!("open index_followed error: {:?}", e)))?;

            for QueryResultEvent {
                json: _,
                timestamp,
                serial,
            } in self
                .query_internal(&read_txn, query)
                .map_err(|e| JsValue::from_str(&format!("mark_unfollow query error: {:?}", e)))?
            {
                let ts = timestamp.expect("query result should have timestamp");

                // create key: [follower[48..64]][timestamp][serial]
                let mut key = [0u8; 16];
                parse_hex_into(&follower_hex[48..64], &mut key[0..8])
                    .map_err(|e| JsValue::from_str(&format!("follower hex error: {:?}", e)))?;
                key[8..12].copy_from_slice(&ts.to_be_bytes());
                key[12..16].copy_from_slice(&serial.to_be_bytes());

                match action {
                    ModifyFollow::Add => {
                        followedby_table.insert(&key[..], ()).map_err(|e| {
                            JsValue::from_str(&format!("mark_follow insert error: {:?}", e))
                        })?;

                        #[cfg(debug_assertions)]
                        web_sys::console::log_3(
                            &js_sys::JsString::from_str("new follower").unwrap(),
                            &js_sys::JsString::from(follower_hex.clone()),
                            &js_sys::Uint8Array::from(&key[..]),
                        );
                    }
                    ModifyFollow::Remove => {
                        followedby_table.remove(&key[..]).map_err(|e| {
                            JsValue::from_str(&format!("mark_unfollow insert error: {:?}", e))
                        })?;

                        #[cfg(debug_assertions)]
                        web_sys::console::log_3(
                            &js_sys::JsString::from_str("unfollowed").unwrap(),
                            &js_sys::JsString::from(follower_hex.clone()),
                            &js_sys::Uint8Array::from(&key[..]),
                        );
                    }
                }
            }
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(())
    }

    pub fn clean_followed(&self, follower: js_sys::JsString, except: js_sys::Array) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("read transaction error: {:?}", e)))?;

        // create a set of excepted authors for quick lookup
        let mut excepted_authors = std::collections::HashSet::new();
        for i in 0..except.length() {
            if let Some(author) = except.get(i).as_string() {
                excepted_authors.insert(author);
            }
        }

        // get follower prefix for index lookup
        let follower_hex = follower.as_string().expect("follower must be hex pubkey");
        let mut start_key = [0u8; 16];
        parse_hex_into(&follower_hex[48..64], &mut start_key[0..8]).map_err(|e| {
            JsValue::from_str(&format!("clean_followed follower hex error: {:?}", e))
        })?;

        {
            let mut followedby_table = write_txn.open_table(INDEX_FOLLOWED).map_err(|e| {
                JsValue::from_str(&format!(
                    "clean_followed open index_followed error: {:?}",
                    e
                ))
            })?;
            let events_table = read_txn.open_table(EVENTS).map_err(|e| {
                JsValue::from_str(&format!("clean_followed open events table error: {:?}", e))
            })?;

            let mut to_delete: Vec<[u8; 16]> = Vec::new();

            // iterate through all entries with this follower prefix
            for item in followedby_table
                .range(&start_key[..]..)
                .map_err(|e| JsValue::from_str(&format!("clean_followed range error: {:?}", e)))?
            {
                let (key, _) = item.map_err(|e| {
                    JsValue::from_str(&format!(
                        "clean_followed get index_followed item error: {:?}",
                        e
                    ))
                })?;
                let key_bytes = key.value();

                // stop when we leave our prefix
                if key_bytes[0..8] != start_key[0..8] {
                    break;
                }

                // extract serial from last 4 bytes
                let serial = u32::from_be_bytes([
                    key_bytes[key_bytes.len() - 4],
                    key_bytes[key_bytes.len() - 3],
                    key_bytes[key_bytes.len() - 2],
                    key_bytes[key_bytes.len() - 1],
                ]);

                let mut should_delete = false;

                // get the event to check the author
                if let Some(event_json) = events_table.get(serial).map_err(|e| {
                    JsValue::from_str(&format!("clean_followed get event error: {:?}", e))
                })? {
                    let event_bytes = event_json.value();

                    // extract author hex from position 11..75
                    if event_bytes.len() >= 75 {
                        let author_str = extract_pubkey(event_bytes);

                        // delete if author is not in except list
                        if !excepted_authors.contains(&author_str) {
                            should_delete = true;
                        }
                    }
                } else {
                    // event doesn't exist, delete the index entry
                    should_delete = true;
                };

                if should_delete {
                    to_delete.push(
                        key_bytes
                            .try_into()
                            .expect("index_followed key should have 16 bytes"),
                    );
                }
            }

            // delete the marked entries
            for key in to_delete {
                followedby_table.remove(&key[..]).map_err(|e| {
                    JsValue::from_str(&format!("remove index_followed entry error: {:?}", e))
                })?;

                #[cfg(debug_assertions)]
                web_sys::console::log_2(
                    &js_sys::JsString::from_str("clean_followed deleted").unwrap(),
                    &js_sys::Uint8Array::from(&key[..]),
                );
            }
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(())
    }

    // returns a JSON like `{"<pubkey>": [<start>, <end>]} as an Uint8Array`
    pub fn get_outbox_bounds(&self) -> Result<js_sys::Uint8Array> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("read transaction error: {:?}", e)))?;

        let bounds_table = read_txn
            .open_table(OUTBOX_BOUNDS)
            .map_err(|e| JsValue::from_str(&format!("open bounds table error: {:?}", e)))?;

        let mut map = serde_json::Map::new();
        for item in bounds_table
            .iter()
            .map_err(|e| JsValue::from_str(&format!("bounds iteration error: {:?}", e)))?
        {
            let (pubkey, bound) =
                item.map_err(|e| JsValue::from_str(&format!("bound access error: {:?}", e)))?;
            let (start, end) = bound.value();
            map.insert(
                pubkey.value(),
                serde_json::Value::Array(vec![
                    serde_json::Value::Number(start.into()),
                    serde_json::Value::Number(end.into()),
                ]),
            );
        }

        let json = serde_json::to_vec(&serde_json::Value::Object(map))
            .expect("serialization of serde_json::Value should never fail");

        Ok(js_sys::Uint8Array::from(json.as_slice()))
    }

    pub fn set_outbox_bound(
        &self,
        pubkey: js_sys::JsString,
        bound_start: js_sys::Number,
        bound_end: js_sys::Number,
    ) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let mut write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("write transaction error: {:?}", e)))?;

        let _ = write_txn.set_durability(redb::Durability::None);

        let mut bounds_table = write_txn
            .open_table(OUTBOX_BOUNDS)
            .map_err(|e| JsValue::from_str(&format!("open bounds table error: {:?}", e)))?;

        bounds_table
            .insert(
                pubkey
                    .as_string()
                    .expect("set_outbox_bound pubkey param must be string"),
                (
                    bound_start
                        .as_f64()
                        .expect("set_outbox_bound bound_start param must be a numeric timestamp")
                        as u32,
                    bound_end
                        .as_f64()
                        .expect("set_outbox_bound bound_end param must be a numeric timestamp")
                        as u32,
                ),
            )
            .map_err(|e| JsValue::from_str(&format!("bounds set error: {:?}", e)))?;

        Ok(())
    }
}

enum ModifyFollow {
    Add,
    Remove,
}
