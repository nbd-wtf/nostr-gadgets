use std::io;
use std::sync::{Arc, Mutex};

use redb::{Database, ReadableTable, StorageBackend, TableDefinition};
use wasm_bindgen::prelude::*;
use web_sys::FileSystemSyncAccessHandle;

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
    pub fn new(sync_handle: &FileSystemSyncAccessHandle) -> Result<Redstore, JsValue> {
        console_error_panic_hook::set_once();

        let backend = WasmBackend::new(sync_handle.clone());
        let db = Database::builder()
            .create_with_backend(backend)
            .map_err(|e| JsValue::from_str(&format!("failed to create database: {:?}", e)))?;

        Ok(Redstore {
            db: Arc::new(Mutex::new(db)),
        })
    }

    pub fn query_events(&self, filter: JsValue) -> Result<Option<js_sys::Uint8Array>, JsValue> {
        // TODO: start a transaction and call query_internal with it
    }

    fn query_internal(&self, txn: Transaction, filter: JsValue) -> Iter<Event> {
        // TODO:
        // see the comment on compute_indexes() to understand the shape of the indexes in each index_* table.
        // prepare the queries to run (for example, if the filter contains kinds 2 and 4 pubkeys we'll run 8 queries on the index_pubkey_kind, if the filter contains 3 tags and 2 pubkeys we'll do 3 queries on the relevant tag index but keep the pubkeys around for extra filtering afterwards (same if it includes kinds), if the filter only contains ids we'll use the index_id, if it has nothing we'll use the index_nothing and so on).
        // these queries should all start at a given index with the timestamp as the maximum u32 possible, but if the filter contains an 'until' then that number should be the timestamp in which we'll start.
        // we should also keep track of the 'since' value as u32 and the 'limit' value if it exists, defaulting to 500.
        // then we should begin iterating through each of the prepared queries, backwards.
        // the iteration should stop whenever we reach a timestamp that is below the 'since' value or if the key doesn't match our desired prefix anymore.
        // the iteration should also stop if the number of rows we read from that query reaches the global 'limit'.
        // read 20 records from each iteration, keep track of the last timestamp read from each query
        // do the next steps in a loop:
        // A. from each of the last timestamps read among all queries decide which one is the greatest
        // B. aggregate all the results pulled so far from all queries, then sort them such that the greatest timestamps are first
        // C. from this sorted list take all results until we get to the greatest timestamp we got from step A above, emit all these results
        // D. pull 20 more records from the query that had the greatest timestamp in step A, do not pull from any other query
        // continue the loop (execute step A again, then B etc)
        // the loop ends when we have emitted a number of results equal to the filter global 'limit' (or the default, 500)
    }

    pub fn save_events(&self, data: JsValue) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
        let write_txn = db
            .begin_write()
            .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

        {
            let mut events = write_txn
                .open_table(EVENTS)
                .map_err(|e| JsValue::from_str(&format!("table error: {:?}", e)))?;

            let last = events
                .last()
                .map_err(|e| JsValue::from_str(&format!("get serial err: {:?}", e)))?
                .map(|l| l.0.value())
                .unwrap_or(0);

            // TODO:
            // from inside `data` get the value `events`, which should be an array of JsValues corresponding to Nostr events. for each of these events insert them on the `events` table using the serial as a key (last+1, then add 1 for each event).
            //
            // for each of these events check if they have a kind in the range 30000-39999 or 0 or 3 or 10000-19999, if yes, then query the database using query_internal for authors=[event.author],d=[event.tags.find([k, v] => k === 'd')[1]] (only use 'd' in case the event kind is in the range 30000-39999)

            events
                .insert(key, value)
                .map_err(|e| JsValue::from_str(&format!("insert error: {:?}", e)))?;
        }

        write_txn
            .commit()
            .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;
        Ok(())
    }

    pub fn delete_events(&self, ids: JsValue) -> Result<Option<js_sys::Uint8Array>, JsValue> {
        // TODO:
        // - open a transaction,
        // - call query_internal with the ids
        // - take the serials returned and pass to delete_internal along with the transaction
    }

    fn delete_internal(&self, txn: Transaction, event: JsValue, serial: u32) {
        // TODO:
        // use the serial to delete the entry from the EVENTS table
        // compute the indexes for the given event
        // go on each index table and delete the entries for which indexes were created
    }

    fn compute_indexes(&self, event: JsValue, serial: u32) {
        // TODO:
        // should return a list of indexes containing:
        // - the corresponding index table
        // - the key
        // the key should be like so:
        // for pubkey_kind: [8-bytes-at-the-end-of-pubkey][2-bytes-of-the-kind][4-bytes-of-the-timestamp][4-bytes-serial]
        // for pubkey: [8-bytes-at-the-end-of-pubkey][4-bytes-of-the-timestamp][4-bytes-serial]
        // for pubkey_dtag: [8-bytes-at-the-end-of-pubkey][8-initial-bytes-of-sha256-of-d-tag][4-bytes-of-the-timestamp][4-bytes-serial] if the event is in the range 30000-39999 and contains a "d" tag
        // for index_nothing: [4-bytes-of-the-timestamp][4-bytes-serial]
        // for index_id: [8-bytes-at-the-end-of-id][4-bytes-of-the-timestamp][4-bytes-serial]
        // and so on
    }

    // TODO: eliminate all these useless functions, they're here only for reference:
    // pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), JsValue> {
    //     let db = self
    //         .db
    //         .lock()
    //         .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
    //     let write_txn = db
    //         .begin_write()
    //         .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

    //     {
    //         let mut table = write_txn
    //             .open_table(KV_TABLE)
    //             .map_err(|e| JsValue::from_str(&format!("table error: {:?}", e)))?;

    //         table
    //             .insert(key, value)
    //             .map_err(|e| JsValue::from_str(&format!("insert error: {:?}", e)))?;
    //     }

    //     write_txn
    //         .commit()
    //         .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;
    //     Ok(())
    // }

    // pub fn delete(&self, key: &[u8]) -> Result<bool, JsValue> {
    //     let db = self
    //         .db
    //         .lock()
    //         .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
    //     let write_txn = db
    //         .begin_write()
    //         .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

    //     let existed = {
    //         let mut table = write_txn
    //             .open_table(KV_TABLE)
    //             .map_err(|e| JsValue::from_str(&format!("table error: {:?}", e)))?;

    //         let existed = table
    //             .get(key)
    //             .map_err(|e| JsValue::from_str(&format!("get error: {:?}", e)))?
    //             .is_some();

    //         if existed {
    //             table
    //                 .remove(key)
    //                 .map_err(|e| JsValue::from_str(&format!("remove error: {:?}", e)))?;
    //         }
    //         existed
    //     };

    //     write_txn
    //         .commit()
    //         .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;
    //     Ok(existed)
    // }

    // pub fn clear(&self) -> Result<(), JsValue> {
    //     let db = self
    //         .db
    //         .lock()
    //         .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
    //     let write_txn = db
    //         .begin_write()
    //         .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

    //     {
    //         let mut table = write_txn
    //             .open_table(KV_TABLE)
    //             .map_err(|e| JsValue::from_str(&format!("table error: {:?}", e)))?;

    //         // Clear all entries by collecting keys first, then removing them
    //         let keys_to_remove: Result<Vec<_>, _> = table
    //             .iter()
    //             .map_err(|e| JsValue::from_str(&format!("iter error: {:?}", e)))?
    //             .map(|entry| entry.map(|(k, _v)| k.value().to_vec()))
    //             .collect();

    //         let keys = keys_to_remove
    //             .map_err(|e| JsValue::from_str(&format!("collect error: {:?}", e)))?;
    //         for key in keys {
    //             table
    //                 .remove(&*key)
    //                 .map_err(|e| JsValue::from_str(&format!("remove error: {:?}", e)))?;
    //         }
    //     }

    //     write_txn
    //         .commit()
    //         .map_err(|e| JsValue::from_str(&format!("commit error: {:?}", e)))?;
    //     Ok(())
    // }

    // pub fn keys(&self) -> Result<js_sys::Array, JsValue> {
    //     let db = self
    //         .db
    //         .lock()
    //         .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
    //     let read_txn = db
    //         .begin_read()
    //         .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

    //     if let Ok(table) = read_txn.open_table(KV_TABLE) {
    //         let keys: Result<Vec<_>, _> = table
    //             .iter()
    //             .map_err(|e| JsValue::from_str(&format!("iter error: {:?}", e)))?
    //             .map(|entry| entry.map(|(k, _v)| k.value().to_vec()))
    //             .collect();

    //         let key_vecs =
    //             keys.map_err(|e| JsValue::from_str(&format!("collect error: {:?}", e)))?;
    //         let result = js_sys::Array::new();
    //         for key in key_vecs {
    //             let key_array = js_sys::Uint8Array::from(&key[..]);
    //             result.push(&key_array);
    //         }
    //         Ok(result)
    //     } else {
    //         Ok(js_sys::Array::new())
    //     }
    // }

    // pub fn entries(&self) -> Result<Vec<js_sys::Array>, JsValue> {
    //     let db = self
    //         .db
    //         .lock()
    //         .map_err(|e| JsValue::from_str(&format!("lock error: {:?}", e)))?;
    //     let read_txn = db
    //         .begin_read()
    //         .map_err(|e| JsValue::from_str(&format!("transaction error: {:?}", e)))?;

    //     if let Ok(table) = read_txn.open_table(KV_TABLE) {
    //         let entries: Result<Vec<_>, _> = table
    //             .iter()
    //             .map_err(|e| JsValue::from_str(&format!("iter error: {:?}", e)))?
    //             .map(|entry| {
    //                 let (k, v) =
    //                     entry.map_err(|e| JsValue::from_str(&format!("entry error: {:?}", e)))?;
    //                 let pair = js_sys::Array::new();
    //                 let k_array = js_sys::Uint8Array::from(&k.value().to_vec()[..]);
    //                 let v_array = js_sys::Uint8Array::from(&v.value().to_vec()[..]);
    //                 pair.push(&k_array);
    //                 pair.push(&v_array);
    //                 Ok::<js_sys::Array, JsValue>(pair)
    //             })
    //             .collect();

    //         Ok(entries?)
    //     } else {
    //         Ok(vec![])
    //     }
    // }
}
