#![feature(ascii_char)]

use std::io::{self};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use redb::{Database, ReadTransaction, ReadableTable, StorageBackend, WriteTransaction};
use serde_json;
use wasm_bindgen::prelude::*;
use web_sys::FileSystemSyncAccessHandle;

use crate::indexes::*;
use crate::query::prepare_queries;
use crate::utils::{IndexableEvent, Querier, Result, parse_hex_into};

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
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(Redstore {
            db: Arc::new(Mutex::new(db)),
        })
    }

    // takes an array of filters (js array of js objects) and returns an array of arrays of events.
    pub fn query_events(&self, filters: JsValue) -> Result<JsValue> {
        web_sys::console::log_2(&js_sys::JsString::from("query_events"), &filters);

        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        let filters_arr = js_sys::Array::from(&filters);
        let results = js_sys::Array::new_with_length(filters_arr.length());
        for f in 0..filters_arr.length() {
            let events = self.query_internal(
                &read_txn,
                (&js_sys::Object::from(filters_arr.get(f))).into(),
            )?;
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

            results.set(f, array.into());
        }

        Ok(results.into())
    }

    fn query_internal(
        &self,
        txn: &ReadTransaction,
        mut spec: Querier,
    ) -> Result<Vec<QueryResultEvent>> {
        let events_table = txn
            .open_table(EVENTS)
            .map_err(|e| JsValue::from_str(&format!("open events table error: {:?}", e)))?;

        // special id query, just get the ids in whatever order and return
        if let Some(ids) = spec.ids {
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

                    web_sys::console::log_2(
                        &js_sys::JsString::from("serial"),
                        &js_sys::Number::from(serial as u32),
                    );

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

        let mut plan = prepare_queries(&mut spec)?;

        // execute queries and merge results
        let mut remaining_unexhausted = plan.queries.len();

        for query in &mut plan.queries {
            let has_more = query.pull_results(&txn, std::cmp::min(20, spec.limit), plan.since)?;
            if !has_more {
                remaining_unexhausted -= 1;
            }
        }

        // will merge results from all queries
        let mut merged_results: Vec<QueryResultEvent> = Vec::new();
        let mut emitted_count = 0;
        let mut batch = Vec::with_capacity(std::cmp::min(20, spec.limit) * plan.queries.len());

        while emitted_count < spec.limit {
            let last_run = remaining_unexhausted == 0;

            web_sys::console::log_7(
                &js_sys::JsString::from("loop"),
                &js_sys::JsString::from("last run?"),
                &js_sys::Boolean::from(last_run),
                &js_sys::JsString::from("remaining?"),
                &js_sys::Number::from(remaining_unexhausted as u32),
                &js_sys::JsString::from("batch?"),
                &js_sys::Number::from(batch.len() as u32),
            );

            // collect all events with this timestamp from all queries
            // and find the query with the highest timestamp
            let mut top_query_idx = None;
            let mut top_query_timestamp = 0;
            for (idx, query) in plan.queries.iter_mut().enumerate() {
                if let Some((timestamp, _)) = query.results.iter().last() {
                    if *timestamp > top_query_timestamp {
                        top_query_timestamp = *timestamp;
                        top_query_idx = Some(idx);
                    }
                }
                batch.append(&mut query.results);
            }

            // sort by timestamp (newest last)
            batch.sort_by(|(a, _), (b, _)| a.cmp(&b));

            while !batch.is_empty() {
                if emitted_count >= spec.limit {
                    break;
                }

                if let Some((ts, serial)) = batch.pop() {
                    // go on the EVENTS table and fetch the event JSON using the serial
                    if let Some(event_j) = events_table
                        .get(serial)
                        .map_err(|e| JsValue::from_str(&format!("get event error: {:?}", e)))?
                    {
                        let event_json = event_j.value();

                        // extra filters
                        if let Some(authors_bf) = &plan.extra_authors {
                            let author_hex = &event_json[11..75]; // saved events always have the pubkey at this pos
                            if !authors_bf.contains(author_hex) {
                                web_sys::console::log_1(&js_sys::JsString::from(format!(
                                    "prevented on extra_authors ({}): {}",
                                    String::from_utf8_lossy(author_hex),
                                    String::from_utf8_lossy(&event_json),
                                )));
                                continue;
                            }
                        }
                        if !plan.extra_kinds.is_empty() {
                            let mut kind = (event_json[156] - 48) as u16; // the first char is always a number
                            for c in &event_json[157..161] {
                                // then the next 4 may or may not be
                                if (*c >= 48/* '0' */) && (*c <= 57/* '9' */) {
                                    kind = kind * 10 + ((*c - 48) as u16)
                                } else {
                                    break;
                                }
                            }
                            if !plan.extra_kinds.contains(&kind) {
                                web_sys::console::log_1(&js_sys::JsString::from(format!(
                                    "prevented on extra_kind ({} & {:?}): {}",
                                    &kind,
                                    &plan.extra_kinds,
                                    String::from_utf8_lossy(&event_json),
                                )));
                                continue;
                            }
                        }
                        if let Some((bf_letters, tags_bf)) = &plan.extra_tags {
                            if let Some(tags_start) = event_json[310..]
                                .iter()
                                .position(|c| *c == 34 /* '"' */)
                                .map(|pos| pos + 310 + 9)
                            {
                                if let Some(tags_end) = event_json[tags_start..]
                                    .iter()
                                    .enumerate()
                                    .position(|(i, c)| {
                                        // search for '],"'
                                        *c == 34 // '"'
                                            && event_json[tags_start + i - 1] == 44 // ','
                                            && event_json[tags_start + i - 2] == 93 // ']'
                                    })
                                    .map(|pos| pos + tags_start - 1)
                                {
                                    if let Ok(tags) = serde_json::from_slice::<Vec<Vec<String>>>(
                                        &event_json[tags_start..tags_end],
                                    ) {
                                        // must contain all tags that are being filtered;
                                        // and for all the tags at least one must be present in the bloom filter
                                        if !bf_letters.iter().all(|letter| {
                                            tags.iter().any(|tag| {
                                                tag.len() >= 2
                                                    && tag[0].len() == 1
                                                    && tag[0].chars().next().unwrap_or('-') as u8
                                                        == *letter
                                                    && tags_bf.contains(&format!(
                                                        "{}=>{}",
                                                        &tag[0], &tag[1]
                                                    ))
                                            })
                                        }) {
                                            web_sys::console::log_1(&js_sys::JsString::from(
                                                format!(
                                                    "prevented on extra_tags ({:?}): {}",
                                                    tags,
                                                    String::from_utf8_lossy(&event_json),
                                                ),
                                            ));

                                            for tag in tags {
                                                if tag[0] == "e" {
                                                    web_sys::console::log_1(
                                                        &js_sys::JsString::from(format!(
                                                            "bf: {} << {}",
                                                            &format!("{}=>{}", &tag[0], &tag[1]),
                                                            tags_bf.contains(&format!(
                                                                "{}=>{}",
                                                                &tag[0], &tag[1]
                                                            ))
                                                        )),
                                                    );
                                                }
                                            }

                                            continue;
                                        }
                                    }
                                }
                            }
                        }

                        web_sys::console::log_1(&js_sys::JsString::from(format!(
                            "emitted {} skipping extras {} {:?} {}",
                            String::from_utf8_lossy(&event_json),
                            plan.extra_authors.is_some(),
                            plan.extra_kinds,
                            plan.extra_tags.is_some(),
                        )));

                        merged_results.push(QueryResultEvent {
                            json: event_json.to_owned(),
                            timestamp: Some(ts),
                            serial,
                        });

                        emitted_count += 1;
                        if emitted_count == spec.limit {
                            break;
                        }

                        if ts == top_query_timestamp {
                            break;
                        }
                    }
                }
            }

            if emitted_count >= spec.limit {
                break;
            }

            // pull more data from the best query
            web_sys::console::log_1(&js_sys::JsString::from("pulling more"));
            if let Some(top_idx) = top_query_idx
                && !plan.queries[top_idx].exhausted
            {
                // fetch from the top query
                let has_more = plan.queries[top_idx].pull_results(
                    &txn,
                    std::cmp::min(20, spec.limit),
                    plan.since,
                )?;
                if !has_more {
                    remaining_unexhausted -= 1;
                }
            } else {
                // fetch from all the other queries
                for query in &mut plan.queries {
                    if !query.exhausted {
                        let has_more =
                            query.pull_results(&txn, std::cmp::min(20, spec.limit), plan.since)?;
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

    // takes { indexableEvents: [], followedBys: [], rawEvents: [] }, returns [bool, ...]
    // indexableEvents are objects with only the data needed for indexing;
    // followedBys are arrays of pubkeys;
    // rawEvents are JSON-encoded raw events, as Uint8Arrays.
    // on the return: true means it was saved, false it wasn't because we already had it or something newer than it)
    pub fn save_events(&self, data: JsValue) -> Result<JsValue> {
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

        // get events array from data
        let data_obj = js_sys::Object::from(data);
        let indexable_events_arr = js_sys::Array::from(
            &js_sys::Reflect::get(&data_obj, &JsValue::from_str("indexableEvents")).map_err(
                |e| JsValue::from_str(&format!("indexable events array error: {:?}", e)),
            )?,
        );
        // let followedbys_arr = js_sys::Array::from(
        //     &js_sys::Reflect::get(&data_obj, &JsValue::from_str("followedBys"))
        //         .map_err(|e| JsValue::from_str(&format!("followedbys array error: {:?}", e)))?,
        // );
        let raw_events_arr = js_sys::Array::from(
            &js_sys::Reflect::get(&data_obj, &JsValue::from_str("rawEvents"))
                .map_err(|e| JsValue::from_str(&format!("raw events array error: {:?}", e)))?,
        );

        web_sys::console::log_3(
            &js_sys::JsString::from_str("save_events").unwrap(),
            &js_sys::Number::from(indexable_events_arr.length()),
            &indexable_events_arr,
        );

        let result = js_sys::Array::new_with_length(indexable_events_arr.length());

        let mut current_serial = last_serial + 1;

        for i in 0..indexable_events_arr.length() {
            let indexable_event: IndexableEvent =
                (&js_sys::Array::from(&indexable_events_arr.get(i))).into();
            let raw_event = (&js_sys::Uint8Array::from(raw_events_arr.get(i))).to_vec();

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
                } in self
                    .query_internal(&read_txn, rq)
                    .map_err(|e| JsValue::from_str(&format!("pre-save id query error: {:?}", e)))?
                {
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
                    continue;
                }
            } else {
                // for non-replaceables, check for identical ids
                let results = self
                    .query_internal(
                        &read_txn,
                        Querier {
                            ids: Some(vec![indexable_event.id.clone()]),
                            ..Default::default()
                        },
                    )
                    .map_err(|e| JsValue::from_str(&format!("pre-save id query error: {:?}", e)))?;
                if !results.is_empty() {
                    // event already exists
                    result.set(i, js_sys::Boolean::from(false).into());
                    continue;
                }
            }

            // insert event and indexes
            {
                let mut events_table = write_txn.open_table(EVENTS).map_err(|e| {
                    JsValue::from_str(&format!("events insert table error: {:?}", e))
                })?;

                events_table
                    .insert(current_serial, &raw_event[..])
                    .map_err(|e| JsValue::from_str(&format!("events insert error: {:?}", e)))?;
            }

            self.insert_indexes(&mut write_txn, &indexable_event, current_serial)?;

            current_serial += 1;
            result.set(i, js_sys::Boolean::from(true).into());
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(result.into())
    }

    fn insert_indexes(
        &self,
        write_txn: &mut WriteTransaction,
        indexable_event: &IndexableEvent,
        serial: u32,
    ) -> Result<()> {
        let indexes = compute_indexes(indexable_event, serial);

        for index in indexes {
            web_sys::console::log_3(
                &js_sys::JsString::from_str("inserting index").unwrap(),
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

    // takes [filter, ...] and returns [deleted_count, ...]
    pub fn delete_events(&self, filters: JsValue) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let mut write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("read transaction error: {:?}", e)))?;

        let filters_arr = js_sys::Array::from(&filters);
        let result = js_sys::Array::new_with_length(filters_arr.length());

        // query for the events to delete
        for f in 0..filters_arr.length() {
            let mut count = 0u32;

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
                self.delete_internal(&mut write_txn, &indexable_event, *serial)?;

                count += 1;
            }

            result.set(f, js_sys::Number::from(count).into());
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        Ok(())
    }

    fn delete_internal(
        &self,
        write_txn: &mut WriteTransaction,
        indexable_event: &IndexableEvent,
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

        Ok(())
    }
}
