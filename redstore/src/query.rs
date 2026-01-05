use fastbloom::BloomFilter;
use redb::ReadTransaction;
use sha2::{Digest, Sha256};
use wasm_bindgen::JsValue;

use crate::indexes::*;
use crate::utils::{parse_hex_into, Querier, Result, MAX_U32_BYTES};

#[derive(Debug)]
pub struct Plan {
    pub queries: Vec<Query>,
    pub since: u32,
    pub extra_kinds: Vec<u16>,
    pub extra_authors: Option<BloomFilter>,
    pub extra_tags: Option<(Vec<u8>, BloomFilter)>,
}

#[derive(Debug)]
pub struct Query {
    pub table_name: &'static str,
    pub curr_key: Vec<u8>,
    pub results: Vec<(u32, u32)>, // (timestamp, serial)
    pub exhausted: bool,
}

impl Query {
    pub fn pull_results(
        &mut self,
        txn: &ReadTransaction,
        batch_size: usize,
        since: u32,
    ) -> Result<bool> {
        web_sys::console::log_1(&js_sys::JsString::from(format!(
            "> pulling up to {} from {}/{:?}",
            batch_size, self.table_name, self.curr_key
        )));

        if self.exhausted {
            return Ok(false);
        }

        let table = match self.table_name {
            "index_followed" => txn.open_table(INDEX_FOLLOWED),
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
                )));
            }
        }
        .map_err(|e| JsValue::from_str(&format!("open table error: {:?}", e)))?;

        let mut count = 0;

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
                web_sys::console::log_7(
                    &js_sys::JsString::from("exiting on prefix"),
                    &js_sys::Number::from(key_len as u32),
                    &js_sys::Number::from(self.curr_key.len() as u32),
                    &js_sys::Boolean::from(key_len != self.curr_key.len()),
                    &js_sys::Uint8Array::from(&key_bytes[0..key_len - 8]),
                    &js_sys::Uint8Array::from(&self.curr_key[0..key_len - 8]),
                    &js_sys::Boolean::from(
                        key_bytes[0..key_len - 8] != self.curr_key[0..key_len - 8],
                    ),
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
            if timestamp < since {
                web_sys::console::log_1(&js_sys::JsString::from("exiting on 'since'"));
                break;
            }

            // extract serial
            let serial = u32::from_be_bytes([
                key_bytes[key_len - 4],
                key_bytes[key_len - 3],
                key_bytes[key_len - 2],
                key_bytes[key_len - 1],
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

pub fn prepare_queries(spec: &mut Querier) -> Result<Plan> {
    let mut queries = Vec::new();

    if let Some(followed_by) = &spec.followed_by {
        let mut start_key = vec![0u8; 16];
        parse_hex_into(&followed_by[48..64], &mut start_key[0..8])
            .map_err(|e| JsValue::from_str(&format!("invalid followed_by hex: {:?}", e)))?;
        start_key[8..12].copy_from_slice(&spec.until.to_be_bytes());
        start_key[12..16].copy_from_slice(&MAX_U32_BYTES);
        queries.push(Query {
            table_name: "index_followed",
            curr_key: start_key,
            results: Vec::new(),
            exhausted: false,
        });
    } else if let (Some(authors), Some(dtags)) = (&spec.authors, spec.dtags.take()) {
        // use index_pubkey_kind for combined author+kind queries
        for author in authors {
            let mut author_bytes = vec![0u8; 8];
            parse_hex_into(&author[48..64], &mut author_bytes)
                .map_err(|e| JsValue::from_str(&format!("invalid author hex: {:?}", e)))?;

            for dtag in &dtags {
                let mut start_key = vec![0u8; 24];

                let mut hasher = Sha256::new();
                hasher.update(dtag.as_bytes());
                let hash = hasher.finalize();

                start_key[0..8].copy_from_slice(&author_bytes);
                start_key[8..16].copy_from_slice(&hash[0..8]);
                start_key[16..20].copy_from_slice(&spec.until.to_be_bytes());
                start_key[20..24].copy_from_slice(&MAX_U32_BYTES);
                queries.push(Query {
                    table_name: "index_pubkey_dtag",
                    curr_key: start_key,
                    results: Vec::new(),
                    exhausted: false,
                });
            }
        }

        // remove here so we don't use it extra_authors later
        spec.authors.take();
    } else if let Some((letter, values)) = spec.tags.pop() {
        // use index_tag for tag queries
        for value in values {
            let mut start_key = vec![0u8; 17];
            start_key[0] = letter;

            let mut hasher = Sha256::new();
            hasher.update(value.as_bytes());
            let hash = hasher.finalize();

            start_key[1..9].copy_from_slice(&hash[0..8]);
            start_key[9..13].copy_from_slice(&spec.until.to_be_bytes());
            start_key[13..17].copy_from_slice(&MAX_U32_BYTES);
            queries.push(Query {
                table_name: "index_tag",
                curr_key: start_key,
                results: Vec::new(),
                exhausted: false,
            });
        }
    } else if let (Some(authors), Some(kinds)) = (&spec.authors, &spec.kinds) {
        // use index_pubkey_kind for combined author+kind queries
        for author in authors {
            let mut author_bytes = vec![0u8; 8];
            parse_hex_into(&author[48..64], &mut author_bytes)
                .map_err(|e| JsValue::from_str(&format!("invalid author hex: {:?}", e)))?;

            for kind in kinds {
                let mut start_key = vec![0u8; 18];
                start_key[0..8].copy_from_slice(&author_bytes);
                start_key[8..10].copy_from_slice(&kind.to_be_bytes());
                start_key[10..14].copy_from_slice(&spec.until.to_be_bytes());
                start_key[14..18].copy_from_slice(&MAX_U32_BYTES);
                queries.push(Query {
                    table_name: "index_pubkey_kind",
                    curr_key: start_key,
                    results: Vec::new(),
                    exhausted: false,
                });
            }
        }

        // remove here so we don't use it stuff in extra_* later
        spec.authors.take();
        spec.kinds.take();
    } else if let Some(authors) = spec.authors.take() {
        // use index_pubkey for author-only queries
        for author in authors {
            let mut start_key = vec![0u8; 16];
            parse_hex_into(&author[48..64], &mut start_key[0..8])
                .map_err(|e| JsValue::from_str(&format!("invalid author hex: {:?}", e)))?;
            start_key[8..12].copy_from_slice(&spec.until.to_be_bytes());
            start_key[12..16].copy_from_slice(&MAX_U32_BYTES);

            queries.push(Query {
                table_name: "index_pubkey",
                curr_key: start_key,
                results: Vec::new(),
                exhausted: false,
            });
        }
    } else if let Some(kinds) = spec.kinds.take() {
        // use index_kind for kind-only queries
        for kind in kinds {
            let mut start_key = vec![0u8; 10];
            start_key[0..2].copy_from_slice(&kind.to_be_bytes());
            start_key[2..6].copy_from_slice(&spec.until.to_be_bytes());
            start_key[6..10].copy_from_slice(&MAX_U32_BYTES);
            queries.push(Query {
                table_name: "index_kind",
                curr_key: start_key,
                results: Vec::new(),
                exhausted: false,
            });
        }
    } else {
        // use index_nothing for general queries
        let mut start_key = vec![0u8; 8];
        start_key[0..4].copy_from_slice(&spec.until.to_be_bytes());
        start_key[4..8].copy_from_slice(&MAX_U32_BYTES);
        queries.push(Query {
            table_name: "index_nothing",
            curr_key: start_key,
            results: Vec::new(),
            exhausted: false,
        });
    }

    // we'll do these only if kinds/authors remain after planning the queries above (i.e. they weren't used)
    let extra_kinds = spec.kinds.take().unwrap_or(Vec::new());

    let extra_authors = spec.authors.take().map(|authors| {
        let mut bf = BloomFilter::with_false_pos(0.01).expected_items(authors.len());
        for author in authors {
            bf.insert(author.as_bytes());
        }
        bf
    });

    let extra_tags = match (spec.dtags.take(), spec.tags.len() > 0) {
        (Some(dtags), false) => {
            let mut bf = BloomFilter::with_false_pos(0.01).expected_items(dtags.len());
            for dtag in dtags {
                let full = format!("d=>{}", dtag);
                bf.insert(&full);
            }
            Some((vec!['d' as u8], bf))
        }
        (Some(dtags), true) => {
            let mut bf_letters = Vec::with_capacity(1 + spec.tags.len());
            let mut bf =
                BloomFilter::with_false_pos(0.01).expected_items(dtags.len() + spec.tags.len() * 2);
            for dtag in dtags {
                bf_letters.push('d' as u8);
                let full = format!("d=>{}", dtag);
                bf.insert(&full);
            }
            for (letter, values) in &spec.tags {
                bf_letters.push(*letter);
                for value in values {
                    let full = format!("{}=>{}", letter, value);
                    bf.insert(&full);
                }
            }
            Some((bf_letters, bf))
        }
        (None, true) => {
            let mut bf_letters = Vec::with_capacity(1 + spec.tags.len());
            let mut bf = BloomFilter::with_false_pos(0.01).expected_items(spec.tags.len() * 2);
            for (letter, values) in &spec.tags {
                bf_letters.push(*letter);
                for value in values {
                    let full = format!(
                        "{}=>{}",
                        letter.as_ascii().expect("filter tag not ascii").as_str(),
                        value
                    );
                    web_sys::console::log_1(&js_sys::JsString::from(format!("bf ins: {}", full)));
                    bf.insert(&full);
                }
            }
            Some((bf_letters, bf))
        }
        (None, false) => None,
    };

    Ok(Plan {
        queries,
        since: spec.since.unwrap_or(0),
        extra_kinds,
        extra_authors,
        extra_tags,
    })
}
