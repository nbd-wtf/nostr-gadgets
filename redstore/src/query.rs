use redb::ReadTransaction;
use wasm_bindgen::JsValue;

use crate::indexes::*;
use crate::utils::Result;

#[derive(Debug)]
pub struct Query {
    pub table_name: &'static str,
    pub curr_key: Vec<u8>,
    pub since: Option<u32>,
    pub results: Vec<(u32, u32)>, // (timestamp, serial)
    pub exhausted: bool,
}

impl Query {
    pub fn pull_results(&mut self, txn: &ReadTransaction, limit: &mut usize) -> Result<bool> {
        if self.exhausted {
            return Ok(false);
        }

        let table = match self.table_name {
            "index_nothing" => txn.open_table(INDEX_NOTHING),
            "index_kind" => txn.open_table(INDEX_KIND),
            "index_pubkey" => txn.open_table(INDEX_PUBKEY),
            "index_pubkey_kind" => txn.open_table(INDEX_PUBKEY_KIND),
            "index_pubkey_dtag" => txn.open_table(INDEX_PUBKEY_DTAG),
            "index_tag" => txn.open_table(INDEX_TAG),
            _ => {
                return Err(JsValue::from_str(&format!(
                    "unknown table: {}",
                    self.table_name
                )))
            }
        }
        .map_err(|e| JsValue::from_str(&format!("open table error: {:?}", e)))?;

        let mut count = 0;
        let batch_size = 20.min(*limit);

        web_sys::console::log_2(
            &js_sys::JsString::from("will range"),
            &js_sys::Uint8Array::from(&self.curr_key[..]),
        );

        for item in table
            .range(..&self.curr_key[..])
            .map_err(|e| JsValue::from_str(&format!("iter error: {:?}", e)))?
            .rev()
        {
            let (key, _) = item.map_err(|e| JsValue::from_str(&format!("item error: {:?}", e)))?;
            let key_bytes = key.value();
            let key_len = key_bytes.len();

            // check if key matches our prefix
            if key_len != self.curr_key.len()
                || key_bytes[0..key_len - 8] != self.curr_key[0..key_len - 8]
            {
                web_sys::console::log_5(
                    &js_sys::JsString::from("2"),
                    &js_sys::Number::from(key_len as u32),
                    &js_sys::Boolean::from(key_len != self.curr_key.len()),
                    &js_sys::Uint8Array::from(&key_bytes[0..key_len - 8]),
                    &js_sys::Uint8Array::from(&self.curr_key[0..key_len - 8]),
                );
                self.exhausted = true;
                return Ok(false);
            }

            // extract timestamp from key
            let timestamp = u32::from_be_bytes([
                key_bytes[key_len - 8],
                key_bytes[key_len - 7],
                key_bytes[key_len - 6],
                key_bytes[key_len - 5],
            ]);

            // check if timestamp is in range
            if let Some(since) = self.since {
                if timestamp < since {
                    web_sys::console::log_1(&js_sys::JsString::from("2"));
                    break;
                }
            }

            // extract serial
            let serial = u32::from_be_bytes([
                key_bytes[key_len - 4],
                key_bytes[key_len - 3],
                key_bytes[key_len - 2],
                key_bytes[key_len - 2],
            ]);

            web_sys::console::log_3(
                &js_sys::JsString::from("pulled"),
                &js_sys::Number::from(timestamp as u32),
                &js_sys::Number::from(serial as u32),
            );
            self.results.push((timestamp, serial));
            count += 1;

            self.curr_key.copy_from_slice(key_bytes);
            if count >= batch_size {
                web_sys::console::log_1(&js_sys::JsString::from("1"));
                break;
            }
        }

        if count == 0 {
            self.exhausted = true;
            Ok(false)
        } else {
            Ok(true)
        }
    }
}
