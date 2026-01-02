use std::io::{self};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use gloo_utils::format::JsValueSerdeExt;
use redb::{Database, ReadTransaction, ReadableTable, StorageBackend, WriteTransaction};
use serde_json;
use sha2::{Digest, Sha256};
use wasm_bindgen::prelude::*;
use web_sys::FileSystemSyncAccessHandle;

use crate::indexes::*;
use crate::query::Query;
use crate::utils::{parse_hex_into, NostrEvent, Result, MAX_U32_BYTES};

mod indexes;
mod query;
mod utils;

struct QueryResultEvent {
    json: Vec<u8>,
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

    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];
        let mut bytes_read = 0;
        let options = web_sys::FileSystemReadWriteOptions::new();

        while bytes_read != len {
            options.set_at((offset + bytes_read as u64) as f64);

            let read_result = self
                .sync_handle
                .read_with_u8_array_and_options(&mut buffer[bytes_read..], &options)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

            bytes_read += read_result as usize;
        }
        Ok(buffer)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.sync_handle
            .truncate_with_f64(len as f64)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
        Ok(())
    }

    fn sync_data(&self, _eventual: bool) -> io::Result<()> {
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

        Ok(Redstore {
            db: Arc::new(Mutex::new(db)),
        })
    }

    pub fn query_events(&self, filter: JsValue) -> Result<JsValue> {
        web_sys::console::log_2(&js_sys::JsString::from("query_events"), &filter);
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        let events = self.query_internal(&read_txn, &js_sys::Object::from(filter))?;
        let array = js_sys::Array::new_with_length(events.len() as u32);
        for (i, QueryResultEvent { json, serial: _ }) in events.iter().enumerate() {
            array.set(i as u32, js_sys::Uint8Array::from(json.as_slice()).into());
        }
        Ok(array.into())
    }

    fn query_internal(
        &self,
        txn: &ReadTransaction,
        filter: &js_sys::Object,
    ) -> Result<Vec<QueryResultEvent>> {
        web_sys::console::log_2(&js_sys::JsString::from("query_internal"), &filter);
        // extract filter parameters
        let mut ids: Option<Vec<String>> = None;
        let mut authors: Option<Vec<String>> = None;
        let mut kinds: Option<Vec<u16>> = None;
        let mut dtags: Option<Vec<String>> = None;
        let mut chosen_tagname: Option<String> = None;
        let mut chosen_tagvalues: Option<Vec<String>> = None;
        let mut since: Option<u32> = None;
        let mut until: u32 = (js_sys::Date::now() / 1000.0) as u32;
        let mut limit: usize = 100;

        let keys = js_sys::Object::keys(filter);
        for i in 0..keys.length() {
            let key = keys.get(i);
            if let Ok(value) = js_sys::Reflect::get(&filter, &key) {
                let key_string = key.as_string().expect("object key is not a string?");
                let key_str = key_string.as_str();
                match key_str {
                    "ids" => {
                        let array = js_sys::Array::from(&value);
                        let ids = ids.insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            ids.push(array.get(i).as_string().expect("ids must be strings"));
                        }
                        break; // break here because if we have ids we don't care about anything else
                    }
                    "authors" => {
                        let array = js_sys::Array::from(&value);
                        let authors = authors.insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            authors
                                .push(array.get(i).as_string().expect("authors must be strings"));
                        }
                    }
                    "kinds" => {
                        let array = js_sys::Array::from(&value);
                        let kinds = kinds.insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            kinds
                                .push(array.get(i).as_f64().expect("kinds must be numbers") as u16);
                        }
                    }
                    "since" => {
                        since = Some(value.as_f64().expect("since must be a number") as u32);
                    }
                    "until" => {
                        until = value.as_f64().expect("until must be a number") as u32;
                    }
                    "limit" => {
                        limit = value.as_f64().expect("limit must be a number") as usize;
                    }
                    "#d" => {
                        let array = js_sys::Array::from(&value);
                        let dtags = dtags.insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            dtags.push(array.get(i).as_string().expect("d-tags must be strings"));
                        }
                    }
                    _ => {
                        if chosen_tagvalues.is_none() && key_str.starts_with("#") {
                            chosen_tagname = Some(key_str[1..].to_string());
                            let array = js_sys::Array::from(&value);
                            let chosen_tagvalues = chosen_tagvalues
                                .insert(Vec::with_capacity(array.length() as usize));
                            for i in 0..array.length() {
                                chosen_tagvalues.push(array.get(i).as_string().unwrap());
                            }
                        }
                    }
                }
            }
        }

        let events_table = txn
            .open_table(EVENTS)
            .map_err(|e| JsValue::from_str(&format!("open events table error: {:?}", e)))?;

        if let Some(ids) = ids {
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
                            serial: serial,
                        });
                    }
                }
            }

            return Ok(results);
        }

        // determine which index queries to run
        let mut queries = Vec::new();

        if let (Some(authors), Some(kinds)) = (&authors, &kinds) {
            // use index_pubkey_kind for combined author+kind queries
            for author in authors {
                let mut author_bytes = vec![0u8; 8];
                parse_hex_into(&author[48..64], &mut author_bytes)
                    .map_err(|e| JsValue::from_str(&format!("invalid author hex: {:?}", e)))?;

                for kind in kinds {
                    let mut start_key = vec![0u8; 18];
                    start_key[0..8].copy_from_slice(&author_bytes);
                    start_key[8..10].copy_from_slice(&kind.to_be_bytes());
                    start_key[10..14].copy_from_slice(&until.to_be_bytes());
                    start_key[20..24].copy_from_slice(&MAX_U32_BYTES);
                    queries.push(Query {
                        table_name: "index_pubkey_kind",
                        curr_key: start_key,
                        since,
                        results: Vec::new(),
                        exhausted: false,
                    });
                }
            }
        } else if let (Some(authors), Some(dtags)) = (&authors, &dtags) {
            // use index_pubkey_kind for combined author+kind queries
            for author in authors {
                let mut author_bytes = vec![0u8; 8];
                parse_hex_into(&author[48..64], &mut author_bytes)
                    .map_err(|e| JsValue::from_str(&format!("invalid author hex: {:?}", e)))?;

                for dtag in dtags {
                    let mut start_key = vec![0u8; 24];

                    let mut hasher = Sha256::new();
                    hasher.update(dtag.as_bytes());
                    let hash = hasher.finalize();

                    start_key[0..8].copy_from_slice(&author_bytes);
                    start_key[8..16].copy_from_slice(&hash[0..8]);
                    start_key[16..20].copy_from_slice(&until.to_be_bytes());
                    start_key[20..24].copy_from_slice(&MAX_U32_BYTES);
                    queries.push(Query {
                        table_name: "index_pubkey_kind",
                        curr_key: start_key,
                        since,
                        results: Vec::new(),
                        exhausted: false,
                    });
                }
            }
        } else if let Some(authors) = &authors {
            // use index_pubkey for author-only queries
            for author in authors {
                let mut start_key = vec![0u8; 16];
                parse_hex_into(&author[48..64], &mut start_key[0..8])
                    .map_err(|e| JsValue::from_str(&format!("invalid author hex: {:?}", e)))?;
                start_key[8..12].copy_from_slice(&until.to_be_bytes());
                start_key[12..16].copy_from_slice(&MAX_U32_BYTES);

                queries.push(Query {
                    table_name: "index_pubkey",
                    curr_key: start_key,
                    since,
                    results: Vec::new(),
                    exhausted: false,
                });
            }
        } else if let Some(kinds) = &kinds {
            // use index_kind for kind-only queries
            for kind in kinds {
                let mut start_key = vec![0u8; 10];
                start_key[0..2].copy_from_slice(&kind.to_be_bytes());
                start_key[2..6].copy_from_slice(&until.to_be_bytes());
                start_key[6..10].copy_from_slice(&MAX_U32_BYTES);
                queries.push(Query {
                    table_name: "index_kind",
                    curr_key: start_key,
                    since,
                    results: Vec::new(),
                    exhausted: false,
                });
            }
        } else if let (Some(Some(letter)), Some(values)) =
            (&chosen_tagname.map(|n| n.bytes().next()), &chosen_tagvalues)
        {
            // use index_tag for tag queries
            for value in values {
                let mut start_key = vec![0u8; 17];
                start_key[0] = *letter;

                let mut hasher = Sha256::new();
                hasher.update(value.as_bytes());
                let hash = hasher.finalize();

                start_key[1..9].copy_from_slice(&hash[0..8]);
                start_key[2..6].copy_from_slice(&until.to_be_bytes());
                start_key[6..10].copy_from_slice(&MAX_U32_BYTES);
                queries.push(Query {
                    table_name: "index_tag",
                    curr_key: start_key,
                    since,
                    results: Vec::new(),
                    exhausted: false,
                });
            }
        } else {
            // use index_nothing for general queries
            let mut start_key = vec![0u8; 8];
            start_key[0..4].copy_from_slice(&until.to_be_bytes());
            start_key[4..8].copy_from_slice(&MAX_U32_BYTES);
            queries.push(Query {
                table_name: "index_nothing",
                curr_key: start_key,
                since,
                results: Vec::new(),
                exhausted: false,
            });
        }

        // execute queries and merge results
        let mut remaining_unexhausted = queries.len();

        for query in &mut queries {
            web_sys::console::log_1(&js_sys::JsString::from("pulling"));
            let has_more = query.pull_results(&txn, &mut limit)?;
            if !has_more {
                remaining_unexhausted -= 1;
            }
        }

        // will merge results from all queries
        let mut merged_results: Vec<QueryResultEvent> = Vec::new();
        let mut emitted_count = 0;

        while emitted_count < limit {
            let last_run = remaining_unexhausted == 0;

            // find the query with the highest timestamp
            let mut top_query_idx = None;
            let mut top_query_timestamp = 0;

            for (idx, query) in queries.iter().enumerate() {
                if let Some((timestamp, _)) = query.results.iter().last() {
                    if *timestamp > top_query_timestamp {
                        top_query_timestamp = *timestamp;
                        top_query_idx = Some(idx);
                    }
                }
            }

            if top_query_idx.is_none() {
                break; // no more results
            }

            let top_idx = top_query_idx.unwrap();

            // collect all events with this timestamp from all queries
            let mut batch = Vec::new();

            for query in &mut queries {
                batch.append(&mut query.results);
            }

            // sort by timestamp (newest last)
            batch.sort_by(|(a, _), (b, _)| a.cmp(&b));

            while !batch.is_empty() {
                if emitted_count >= limit {
                    break;
                }

                if let Some((ts, serial)) = batch.pop() {
                    web_sys::console::log_3(
                        &js_sys::JsString::from("really emitting"),
                        &js_sys::Number::from(ts as u32),
                        &js_sys::Number::from(serial as u32),
                    );
                    // go on the EVENTS table and fetch the event JSON using the serial
                    if let Some(event_json) = events_table
                        .get(serial)
                        .map_err(|e| JsValue::from_str(&format!("get event error: {:?}", e)))?
                    {
                        // TODO: extra checks for kind and pubkey if necessary

                        merged_results.push(QueryResultEvent {
                            json: event_json.value().to_owned(),
                            serial,
                        });

                        web_sys::console::log_3(
                            &js_sys::JsString::from("emitted!"),
                            &js_sys::Number::from(ts as u32),
                            &js_sys::Number::from(serial as u32),
                        );

                        emitted_count += 1;
                        if emitted_count == limit {
                            break;
                        }

                        if ts == top_query_timestamp {
                            break;
                        }
                    }
                }
            }

            if emitted_count >= limit {
                break;
            }

            // pull more data from the best query
            web_sys::console::log_1(&js_sys::JsString::from("pulling more"));
            if !queries[top_idx].exhausted {
                // fetch from the top query
                let has_more = queries[top_idx].pull_results(&txn, &mut limit)?;
                if !has_more {
                    remaining_unexhausted -= 1;
                    break;
                }
            } else {
                // fetch from all the other queries
                for query in &mut queries {
                    if !query.exhausted {
                        let has_more = query.pull_results(&txn, &mut limit)?;
                        if !has_more {
                            remaining_unexhausted -= 1;
                        }
                    }
                }
            }

            if last_run {
                break;
            }
        }

        web_sys::console::log_1(&js_sys::JsString::from("done"));

        Ok(merged_results)
    }

    pub fn save_events(&self, data: JsValue) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let mut write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        // get last serial
        let last_serial = {
            let events_table = write_txn
                .open_table(EVENTS)
                .map_err(|e| JsValue::from_str(&format!("table error: {:?}", e)))?;

            let s = events_table
                .last()
                .map_err(|e| JsValue::from_str(&format!("get serial err: {:?}", e)))?
                .map(|l| l.0.value())
                .unwrap_or(0);

            s
        };

        // get events array from data
        let data_obj = js_sys::Object::from(data);
        let events_array = js_sys::Reflect::get(&data_obj, &JsValue::from_str("events"))
            .map_err(|e| JsValue::from_str(&format!("get events error: {:?}", e)))?;

        let events = js_sys::Array::from(&events_array);
        let mut current_serial = last_serial + 1;

        for i in 0..events.length() {
            let event_js = events.get(i);
            let event: NostrEvent = event_js
                .into_serde()
                .map_err(|e| JsValue::from_str(&format!("deserialize event error: {:?}", e)))?;

            // serialize event to bytes
            let event_bytes = serde_json::to_vec(&event)
                .map_err(|e| JsValue::from_str(&format!("serialize event error: {:?}", e)))?;

            // insert event and indexes
            {
                let mut events_table = write_txn
                    .open_table(EVENTS)
                    .map_err(|e| JsValue::from_str(&format!("table error: {:?}", e)))?;

                events_table
                    .insert(current_serial, &event_bytes[..])
                    .map_err(|e| JsValue::from_str(&format!("insert error: {:?}", e)))?;
            }

            self.insert_indexes(&mut write_txn, &event, current_serial)?;

            current_serial += 1;
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;
        Ok(())
    }

    fn insert_indexes(
        &self,
        write_txn: &mut WriteTransaction,
        event: &NostrEvent,
        serial: u32,
    ) -> Result<()> {
        let indexes = compute_indexes(event, serial);

        for index in indexes {
            web_sys::console::log_4(
                &js_sys::JsString::from_str("inserting index").unwrap(),
                &js_sys::JsString::from_str(&serde_json::to_string(&event).unwrap()).unwrap(),
                &js_sys::Uint8Array::from(&index.key[..]),
                &js_sys::JsString::from_str(&index.table_name).unwrap(),
            );

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

    pub fn delete_events(&self, ids: JsValue) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let mut write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        // create filter for the IDs
        let filter_js = js_sys::Object::new();
        js_sys::Reflect::set(&filter_js, &JsValue::from_str("ids"), &ids)
            .map_err(|e| JsValue::from_str(&format!("set ids error: {:?}", e)))?;

        // get the read transaction for querying
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("read transaction error: {:?}", e)))?;

        // query for the events to delete
        for QueryResultEvent { json, serial } in &self.query_internal(&read_txn, &filter_js)? {
            // parse the event JSON to get the event structure
            let event: NostrEvent = serde_json::from_slice(&json)
                .map_err(|e| JsValue::from_str(&format!("parse event error: {:?}", e)))?;

            // delete the event and its indexes
            self.delete_internal(&mut write_txn, &event, *serial)?;
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(())
    }

    fn delete_internal(
        &self,
        write_txn: &mut WriteTransaction,
        event: &NostrEvent,
        serial: u32,
    ) -> Result<()> {
        // delete from EVENTS table
        {
            let mut events_table = write_txn
                .open_table(EVENTS)
                .map_err(|e| JsValue::from_str(&format!("open events table error: {:?}", e)))?;
            events_table
                .remove(serial)
                .map_err(|e| JsValue::from_str(&format!("remove event error: {:?}", e)))?;
        }

        // delete from index tables
        let indexes = compute_indexes(event, serial);

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

        Ok(())
    }
}
