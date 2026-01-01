use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use gloo_utils::format::JsValueSerdeExt;
use js_sys::{Array, Object, Uint8Array};
use redb::{
    Database, ReadTransaction, ReadableTable, StorageBackend, TableDefinition, WriteTransaction,
};
use serde::{Deserialize, Serialize};
use serde_json;
use wasm_bindgen::prelude::*;
use web_sys::FileSystemSyncAccessHandle;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NostrEvent {
    pub id: String,
    pub pubkey: String,
    pub created_at: u32,
    pub kind: u32,
    pub tags: Vec<Vec<String>>,
    pub content: String,
    pub sig: String,
}

#[derive(Debug)]
struct IndexEntry {
    table_name: &'static str,
    key: Vec<u8>,
}

type Result<T> = std::result::Result<T, JsValue>;
use wasm_bindgen::prelude::*;

const EVENTS: TableDefinition<u32, &[u8]> = TableDefinition::new("events");
const INDEX_NOTHING: TableDefinition<&[u8], u32> = TableDefinition::new("index_nothing");
const INDEX_ID: TableDefinition<&[u8], u32> = TableDefinition::new("index_id");
const INDEX_KIND: TableDefinition<&[u8], u32> = TableDefinition::new("index_kind");
const INDEX_PUBKEY: TableDefinition<&[u8], u32> = TableDefinition::new("index_pubkey");
const INDEX_PUBKEY_KIND: TableDefinition<&[u8], u32> = TableDefinition::new("index_pubkey_kind");
const INDEX_PUBKEY_DTAG: TableDefinition<&[u8], u32> = TableDefinition::new("index_pubkey_dtag");
const INDEX_TAG: TableDefinition<&[u8], u32> = TableDefinition::new("index_tag");
// const INDEX_FOLLOWED: TableDefinition<&[u8], u32> = TableDefinition::new("index_followed");

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

    pub fn query_events(&self, filter: JsValue) -> Result<Option<js_sys::Uint8Array>> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        let events = self.query_internal(&read_txn, filter)?;

        if events.is_empty() {
            Ok(None)
        } else {
            let events_json = serde_json::to_vec(&events)
                .map_err(|e| JsValue::from_str(&format!("serialize events error: {:?}", e)))?;
            let array = Uint8Array::from(&events_json[..]);
            Ok(Some(array))
        }
    }

    fn query_internal(&self, txn: &ReadTransaction, filter: JsValue) -> Result<Vec<NostrEvent>> {
        let filter_obj = Object::from(filter);

        // Extract filter parameters
        let ids = self.get_string_array(&filter_obj, "ids")?;
        let authors = self.get_string_array(&filter_obj, "authors")?;
        let kinds = self.get_u32_array(&filter_obj, "kinds")?;
        let since = self.get_optional_u32(&filter_obj, "since")?;
        let until = self
            .get_optional_u32(&filter_obj, "until")?
            .unwrap_or(u32::MAX);
        let limit = self.get_optional_u32(&filter_obj, "limit")?.unwrap_or(500) as usize;
        let tags = self.extract_tag_filters(&filter_obj)?;

        // Determine which index queries to run
        let mut query_plans = Vec::new();

        if !ids.is_empty() {
            // Use index_id for ID queries
            for id in ids {
                let mut prefix = Vec::new();
                let id_bytes = id.as_bytes();
                if id_bytes.len() >= 8 {
                    prefix.extend_from_slice(&id_bytes[id_bytes.len() - 8..]);
                } else {
                    let padding = 8 - id_bytes.len();
                    prefix.extend_from_slice(&vec![0u8; padding]);
                    prefix.extend_from_slice(id_bytes);
                }
                query_plans.push(QueryPlan {
                    table_name: "index_id",
                    prefix,
                    since,
                    until: until.unwrap_or(u32::MAX),
                    limit: limit as u32,
                });
            }
        } else if !authors.is_empty() && !kinds.is_empty() {
            // Use index_pubkey_kind for combined author+kind queries
            for author in authors {
                let author_bytes = author.as_bytes();
                let mut author_suffix = Vec::new();
                if author_bytes.len() >= 8 {
                    author_suffix.extend_from_slice(&author_bytes[author_bytes.len() - 8..]);
                } else {
                    let padding = 8 - author_bytes.len();
                    author_suffix.extend_from_slice(&vec![0u8; padding]);
                    author_suffix.extend_from_slice(author_bytes);
                }

                for kind in kinds {
                    let mut prefix = Vec::new();
                    prefix.extend_from_slice(&author_suffix);
                    prefix.extend_from_slice(&(kind as u16).to_be_bytes());
                    query_plans.push(QueryPlan {
                        table_name: "index_pubkey_kind",
                        prefix,
                        since,
                        until: until.unwrap_or(u32::MAX),
                        limit: limit as u32,
                    });
                }
            }
        } else if !authors.is_empty() {
            // Use index_pubkey for author-only queries
            for author in authors {
                let mut prefix = Vec::new();
                let author_bytes = author.as_bytes();
                if author_bytes.len() >= 8 {
                    prefix.extend_from_slice(&author_bytes[author_bytes.len() - 8..]);
                } else {
                    let padding = 8 - author_bytes.len();
                    prefix.extend_from_slice(&vec![0u8; padding]);
                    prefix.extend_from_slice(author_bytes);
                }
                query_plans.push(QueryPlan {
                    table_name: "index_pubkey",
                    prefix,
                    since,
                    until: until.unwrap_or(u32::MAX),
                    limit: limit as u32,
                });
            }
        } else if !kinds.is_empty() {
            // Use index_kind for kind-only queries
            for kind in kinds {
                let mut prefix = Vec::new();
                prefix.extend_from_slice(&(kind as u16).to_be_bytes());
                query_plans.push(QueryPlan {
                    table_name: "index_kind",
                    prefix,
                    since,
                    until: until.unwrap_or(u32::MAX),
                    limit: limit as u32,
                });
            }
        } else if !tags.is_empty() {
            // Use index_tag for tag queries
            for (tag_name, tag_value) in tags {
                let mut prefix = Vec::new();
                prefix.extend_from_slice(tag_name.as_bytes());
                prefix.extend_from_slice(tag_value.as_bytes());
                query_plans.push(QueryPlan {
                    table_name: "index_tag",
                    prefix,
                    since,
                    until: until.unwrap_or(u32::MAX),
                    limit: limit as u32,
                });
            }
        } else {
            // Use index_nothing for general queries
            query_plans.push(QueryPlan {
                table_name: "index_nothing",
                prefix: Vec::new(),
                since,
                until: until.unwrap_or(u32::MAX),
                limit: limit as u32,
            });
        }

        // Execute queries and merge results
        let mut all_events = Vec::new();
        let mut query_states: Vec<QueryState> = Vec::new();

        for plan in query_plans {
            let state = self.execute_query_plan(txn, plan)?;
            query_states.push(state);
        }

        // Merge results from all queries
        let mut merged_results = Vec::new();
        let mut emitted_count = 0;

        while emitted_count < limit {
            // Find the query with the highest timestamp
            let mut best_query_idx = None;
            let mut best_timestamp = 0;

            for (idx, state) in query_states.iter().enumerate() {
                if let Some((timestamp, _)) = state.current_batch.first() {
                    if *timestamp > best_timestamp {
                        best_timestamp = *timestamp;
                        best_query_idx = Some(idx);
                    }
                }
            }

            if best_query_idx.is_none() {
                break; // No more results
            }

            let best_idx = best_query_idx.unwrap();
            let best_timestamp = query_states[best_idx].current_batch[0].0;

            // Collect all events with this timestamp from all queries
            let mut events_with_timestamp = Vec::new();

            for state in &mut query_states {
                while let Some((timestamp, event)) = state.current_batch.first() {
                    if *timestamp == best_timestamp {
                        if let Some((_, event)) = state.current_batch.get(0) {
                            state.current_batch.remove(0);
                            events_with_timestamp.push(event);
                        }
                    } else if *timestamp < best_timestamp {
                        break;
                    } else {
                        // Should not happen as we sort by timestamp descending
                        break;
                    }
                }
            }

            // Sort by timestamp (should already be sorted) and emit
            events_with_timestamp.sort_by(|a, b| b.created_at.cmp(&a.created_at));

            for event in events_with_timestamp {
                if emitted_count >= limit {
                    break;
                }
                merged_results.push(event);
                emitted_count += 1;
            }

            // Pull more data from the best query
            self.pull_more_results(txn, &mut query_states[best_idx])?;
        }

        Ok(merged_results)
    }

    fn get_string_array(&self, obj: &Object, key: &str) -> Result<Vec<String>> {
        if let Ok(value) = js_sys::Reflect::get(obj, &JsValue::from_str(key)) {
            if value.is_undefined() || value.is_null() {
                return Ok(Vec::new());
            }
            let array = Array::from(&value);
            let mut result = Vec::new();
            for i in 0..array.length() {
                let item = array.get(i);
                if item.is_string() {
                    result.push(item.as_string().unwrap());
                }
            }
            Ok(result)
        } else {
            Ok(Vec::new())
        }
    }

    fn get_u32_array(&self, obj: &Object, key: &str) -> Result<Vec<u32>> {
        if let Ok(value) = js_sys::Reflect::get(obj, &JsValue::from_str(key)) {
            if value.is_undefined() || value.is_null() {
                return Ok(Vec::new());
            }
            let array = Array::from(&value);
            let mut result = Vec::new();
            for i in 0..array.length() {
                let item = array.get(i);
                if let Some(num) = item.as_f64() {
                    result.push(num as u32);
                }
            }
            Ok(result)
        } else {
            Ok(Vec::new())
        }
    }

    fn get_optional_u32(&self, obj: &Object, key: &str) -> Result<Option<u32>> {
        if let Ok(value) = js_sys::Reflect::get(obj, &JsValue::from_str(key)) {
            if value.is_undefined() || value.is_null() {
                return Ok(None);
            }
            if let Some(num) = value.as_f64() {
                Ok(Some(num as u32))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn extract_tag_filters(&self, obj: &Object) -> Result<Vec<(String, String)>> {
        let mut tags = Vec::new();
        // Look for properties like "#e", "#p", etc.
        for i in 1..=9 {
            let key = format!("#{}", i);
            if let Ok(value) = js_sys::Reflect::get(obj, &JsValue::from_str(&key)) {
                if !value.is_undefined() && !value.is_null() {
                    let tag_values = self.get_string_array(obj, &key)?;
                    for tag_value in tag_values {
                        tags.push((key.clone(), tag_value));
                    }
                }
            }
        }
        Ok(tags)
    }

    fn execute_query_plan(&self, txn: &ReadTransaction, plan: QueryPlan) -> Result<QueryState> {
        // This would implement the actual table querying logic
        // For now, return empty state
        Ok(QueryState {
            table_name: plan.table_name.to_string(),
            prefix: plan.prefix,
            since: plan.since,
            until: plan.until,
            limit: plan.limit,
            current_batch: Vec::new(),
            exhausted: false,
        })
    }

    fn pull_more_results(&self, txn: &ReadTransaction, state: &mut QueryState) -> Result<()> {
        // This would implement pulling more results from the table
        // For now, mark as exhausted
        state.exhausted = true;
        Ok(())
    }
}

impl Redstore {
    pub fn save_events(&self, data: JsValue) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let mut write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        // Get last serial
        let last_serial = {
            let mut events_table = write_txn
                .open_table(EVENTS)
                .map_err(|e| JsValue::from_str(&format!("table error: {:?}", e)))?;

            events_table
                .last()
                .map_err(|e| JsValue::from_str(&format!("get serial err: {:?}", e)))?
                .map(|l| l.0.value())
                .unwrap_or(0)
        };

        // Get events array from data
        let data_obj = Object::from(data);
        let events_array = js_sys::Reflect::get(&data_obj, &JsValue::from_str("events"))
            .map_err(|e| JsValue::from_str(&format!("get events error: {:?}", e)))?;

        let events = Array::from(&events_array);
        let mut current_serial = last_serial + 1;

        for i in 0..events.length() {
            let event_js = events.get(i);
            let event: NostrEvent = event_js
                .into_serde()
                .map_err(|e| JsValue::from_str(&format!("deserialize event error: {:?}", e)))?;

            // Serialize event to bytes
            let event_bytes = serde_json::to_vec(&event)
                .map_err(|e| JsValue::from_str(&format!("serialize event error: {:?}", e)))?;

            // Insert event and indexes
            {
                let mut events_table = write_txn
                    .open_table(EVENTS)
                    .map_err(|e| JsValue::from_str(&format!("table error: {:?}", e)))?;

                events_table
                    .insert(current_serial, &event_bytes[..])
                    .map_err(|e| JsValue::from_str(&format!("insert error: {:?}", e)))?;

                self.insert_indexes(&mut write_txn, &event, current_serial)?;
            }

            current_serial += 1;
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;
        Ok(())
    }

    fn is_replaceable_event(&self, event: &NostrEvent) -> bool {
        event.kind == 0
            || event.kind == 3
            || (10000..=19999).contains(&event.kind)
            || (30000..=39999).contains(&event.kind)
    }

    fn get_d_tag_for_replaceable(&self, event: &NostrEvent) -> Option<String> {
        if (30000..=39999).contains(&event.kind) {
            event
                .tags
                .iter()
                .find(|tag| tag.len() >= 2 && tag[0] == "d")
                .and_then(|tag| tag.get(1).cloned())
        } else {
            Some(String::new()) // For other replaceable events, use empty string
        }
    }

    fn insert_indexes(
        &self,
        write_txn: &mut WriteTransaction,
        event: &NostrEvent,
        serial: u32,
    ) -> Result<()> {
        let indexes = self.compute_indexes(event, serial);

        for index in indexes {
            match index.table_name {
                "index_nothing" => {
                    let mut table = write_txn.open_table(INDEX_NOTHING).map_err(|e| {
                        JsValue::from_str(&format!("open index_nothing error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], serial).map_err(|e| {
                        JsValue::from_str(&format!("insert index_nothing error: {:?}", e))
                    })?;
                }
                "index_id" => {
                    let mut table = write_txn
                        .open_table(INDEX_ID)
                        .map_err(|e| JsValue::from_str(&format!("open index_id error: {:?}", e)))?;
                    table.insert(&index.key[..], serial).map_err(|e| {
                        JsValue::from_str(&format!("insert index_id error: {:?}", e))
                    })?;
                }
                "index_kind" => {
                    let mut table = write_txn.open_table(INDEX_KIND).map_err(|e| {
                        JsValue::from_str(&format!("open index_kind error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], serial).map_err(|e| {
                        JsValue::from_str(&format!("insert index_kind error: {:?}", e))
                    })?;
                }
                "index_pubkey" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], serial).map_err(|e| {
                        JsValue::from_str(&format!("insert index_pubkey error: {:?}", e))
                    })?;
                }
                "index_pubkey_kind" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY_KIND).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey_kind error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], serial).map_err(|e| {
                        JsValue::from_str(&format!("insert index_pubkey_kind error: {:?}", e))
                    })?;
                }
                "index_pubkey_dtag" => {
                    let mut table = write_txn.open_table(INDEX_PUBKEY_DTAG).map_err(|e| {
                        JsValue::from_str(&format!("open index_pubkey_dtag error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], serial).map_err(|e| {
                        JsValue::from_str(&format!("insert index_pubkey_dtag error: {:?}", e))
                    })?;
                }
                "index_tag" => {
                    let mut table = write_txn.open_table(INDEX_TAG).map_err(|e| {
                        JsValue::from_str(&format!("open index_tag error: {:?}", e))
                    })?;
                    table.insert(&index.key[..], serial).map_err(|e| {
                        JsValue::from_str(&format!("insert index_tag error: {:?}", e))
                    })?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn delete_events(&self, ids: JsValue) -> Result<Option<js_sys::Uint8Array>> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let mut write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        // Create filter for the IDs
        let filter_js = js_sys::Object::new();
        js_sys::Reflect::set(&filter_js, &JsValue::from_str("ids"), &ids)
            .map_err(|e| JsValue::from_str(&format!("set ids error: {:?}", e)))?;

        // Get the read transaction for querying
        let read_txn = db
            .begin_read()
            .map_err(|e| JsValue::from_str(&format!("read transaction error: {:?}", e)))?;

        // Query for the events to delete
        let events_to_delete = self.query_internal(&read_txn, filter_js.into())?;
        let mut deleted_serials = Vec::new();
        let mut deleted_events = Vec::new();

        for event in &events_to_delete {
            // Get the serial for this event
            if let Ok(serial) = self.get_event_serial(&read_txn, &event.id) {
                deleted_serials.push(serial);
                deleted_events.push(event.clone());

                // Delete the event and its indexes
                self.delete_internal(&mut write_txn, &event, serial)?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;

        if deleted_events.is_empty() {
            Ok(None)
        } else {
            let events_json = serde_json::to_vec(&deleted_events).map_err(|e| {
                JsValue::from_str(&format!("serialize deleted events error: {:?}", e))
            })?;
            let array = Uint8Array::from(&events_json[..]);
            Ok(Some(array))
        }
    }

    fn get_event_serial(&self, txn: &ReadTransaction, id: &str) -> Result<u32> {
        let mut id_key = Vec::new();
        let id_bytes = id.as_bytes();
        if id_bytes.len() >= 8 {
            id_key.extend_from_slice(&id_bytes[id_bytes.len() - 8..]);
        } else {
            let padding = 8 - id_bytes.len();
            id_key.extend_from_slice(&vec![0u8; padding]);
            id_key.extend_from_slice(id_bytes);
        }

        let table = txn
            .open_table(INDEX_ID)
            .map_err(|e| JsValue::from_str(&format!("open index_id error: {:?}", e)))?;

        // Search for the entry with this ID prefix
        let mut serial = 0;
        let mut found = false;

        for item in table
            .iter()
            .map_err(|e| JsValue::from_str(&format!("iter error: {:?}", e)))?
        {
            let (key, value) =
                item.map_err(|e| JsValue::from_str(&format!("item error: {:?}", e)))?;
            if key.value().starts_with(&id_key) {
                serial = value.value();
                found = true;
                break;
            }
        }

        if found {
            Ok(serial)
        } else {
            Err(JsValue::from_str("Event not found"))
        }
    }

    fn delete_internal(
        &self,
        write_txn: &mut WriteTransaction,
        event: &NostrEvent,
        serial: u32,
    ) -> Result<()> {
        // Delete from EVENTS table
        {
            let mut events_table = write_txn
                .open_table(EVENTS)
                .map_err(|e| JsValue::from_str(&format!("open events table error: {:?}", e)))?;
            events_table
                .remove(serial)
                .map_err(|e| JsValue::from_str(&format!("remove event error: {:?}", e)))?;
        }

        // Delete from index tables
        let indexes = self.compute_indexes(event, serial);

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

    fn compute_indexes(&self, event: &NostrEvent, serial: u32) -> Vec<IndexEntry> {
        let mut indexes = Vec::new();
        let timestamp = event.created_at;

        // index_nothing: [4-bytes-of-the-timestamp][4-bytes-serial]
        let mut nothing_key = Vec::with_capacity(8);
        nothing_key.extend_from_slice(&timestamp.to_be_bytes());
        nothing_key.extend_from_slice(&serial.to_be_bytes());
        indexes.push(IndexEntry {
            table_name: "index_nothing",
            key: nothing_key,
        });

        // index_id: [8-bytes-at-the-end-of-id][4-bytes-of-the-timestamp][4-bytes-serial]
        let mut id_key = Vec::with_capacity(16);
        let id_bytes = event.id.as_bytes();
        if id_bytes.len() >= 8 {
            id_key.extend_from_slice(&id_bytes[id_bytes.len() - 8..]);
        } else {
            let padding = 8 - id_bytes.len();
            id_key.extend_from_slice(&vec![0u8; padding]);
            id_key.extend_from_slice(id_bytes);
        }
        id_key.extend_from_slice(&timestamp.to_be_bytes());
        id_key.extend_from_slice(&serial.to_be_bytes());
        indexes.push(IndexEntry {
            table_name: "index_id",
            key: id_key,
        });

        // index_kind: [2-bytes-of-the-kind][4-bytes-of-the-timestamp][4-bytes-serial]
        let mut kind_key = Vec::with_capacity(10);
        kind_key.extend_from_slice(&(event.kind as u16).to_be_bytes());
        kind_key.extend_from_slice(&timestamp.to_be_bytes());
        kind_key.extend_from_slice(&serial.to_be_bytes());
        indexes.push(IndexEntry {
            table_name: "index_kind",
            key: kind_key,
        });

        // index_pubkey: [8-bytes-at-the-end-of-pubkey][4-bytes-of-the-timestamp][4-bytes-serial]
        let mut pubkey_key = Vec::with_capacity(16);
        let pubkey_bytes = event.pubkey.as_bytes();
        if pubkey_bytes.len() >= 8 {
            pubkey_key.extend_from_slice(&pubkey_bytes[pubkey_bytes.len() - 8..]);
        } else {
            let padding = 8 - pubkey_bytes.len();
            pubkey_key.extend_from_slice(&vec![0u8; padding]);
            pubkey_key.extend_from_slice(pubkey_bytes);
        }
        pubkey_key.extend_from_slice(&timestamp.to_be_bytes());
        pubkey_key.extend_from_slice(&serial.to_be_bytes());
        indexes.push(IndexEntry {
            table_name: "index_pubkey",
            key: pubkey_key.clone(),
        });

        // index_pubkey_kind: [8-bytes-at-the-end-of-pubkey][2-bytes-of-the-kind][4-bytes-of-the-timestamp][4-bytes-serial]
        let mut pubkey_kind_key = Vec::with_capacity(18);
        pubkey_kind_key.extend_from_slice(&pubkey_key[0..8]);
        pubkey_kind_key.extend_from_slice(&(event.kind as u16).to_be_bytes());
        pubkey_kind_key.extend_from_slice(&timestamp.to_be_bytes());
        pubkey_kind_key.extend_from_slice(&serial.to_be_bytes());
        indexes.push(IndexEntry {
            table_name: "index_pubkey_kind",
            key: pubkey_kind_key,
        });

        // index_pubkey_dtag: [8-bytes-at-the-end-of-pubkey][8-initial-bytes-of-sha256-of-d-tag][4-bytes-of-the-timestamp][4-bytes-serial]
        if (30000..=39999).contains(&event.kind) {
            if let Some(d_tag) = event
                .tags
                .iter()
                .find(|tag| tag.len() >= 2 && tag[0] == "d")
            {
                if let Some(d_value) = d_tag.get(1) {
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};

                    let mut hasher = DefaultHasher::new();
                    d_value.hash(&mut hasher);
                    let hash = hasher.finish();

                    let mut pubkey_dtag_key = Vec::with_capacity(24);
                    pubkey_dtag_key.extend_from_slice(&pubkey_key[0..8]);
                    pubkey_dtag_key.extend_from_slice(&hash.to_be_bytes()[0..8]);
                    pubkey_dtag_key.extend_from_slice(&timestamp.to_be_bytes());
                    pubkey_dtag_key.extend_from_slice(&serial.to_be_bytes());
                    indexes.push(IndexEntry {
                        table_name: "index_pubkey_dtag",
                        key: pubkey_dtag_key,
                    });
                }
            }
        }

        // index_tag: for each tag, create index entries
        for tag in &event.tags {
            if tag.len() >= 2 {
                let tag_name = &tag[0];
                let tag_value = &tag[1];

                let mut tag_key = Vec::with_capacity(16);
                tag_key.extend_from_slice(tag_name.as_bytes());
                tag_key.extend_from_slice(tag_value.as_bytes());
                tag_key.extend_from_slice(&timestamp.to_be_bytes());
                tag_key.extend_from_slice(&serial.to_be_bytes());
                indexes.push(IndexEntry {
                    table_name: "index_tag",
                    key: tag_key,
                });
            }
        }

        indexes
    }
}

#[derive(Debug)]
struct QueryPlan {
    table_name: &'static str,
    prefix: Vec<u8>,
    since: Option<u32>,
    until: u32,
    limit: u32,
}

#[derive(Debug)]
struct QueryState {
    table_name: String,
    prefix: Vec<u8>,
    since: Option<u32>,
    until: u32,
    limit: u32,
    current_batch: Vec<(u32, NostrEvent)>, // (timestamp, event)
    exhausted: bool,
}
