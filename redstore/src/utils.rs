use serde::Deserialize;
use wasm_bindgen::JsValue;

pub const MAX_U32_BYTES: [u8; 4] = [0xff; 4];

pub type Result<T> = std::result::Result<T, JsValue>;

pub fn parse_hex_into(hex_str: &str, dest: &mut [u8]) -> Result<()> {
    for i in (0..hex_str.len()).step_by(2) {
        let byte_str = &hex_str[i..i + 2];
        let byte = u8::from_str_radix(byte_str, 16)
            .map_err(|_| JsValue::from_str(&format!("invalid hex: {}", byte_str)))?;
        dest[i / 2] = byte;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Querier {
    pub ids: Option<Vec<String>>,
    pub authors: Option<Vec<String>>,
    pub kinds: Option<Vec<u16>>,
    pub dtags: Option<Vec<String>>,
    pub chosen_tagname: Option<String>,
    pub chosen_tagvalues: Option<Vec<String>>,
    pub since: Option<u32>,
    pub until: u32,
    pub limit: usize,
}

impl Default for Querier {
    fn default() -> Self {
        return Self {
            ids: None,
            authors: None,
            kinds: None,
            dtags: None,
            chosen_tagname: None,
            chosen_tagvalues: None,
            since: None,
            until: (js_sys::Date::now() / 1000.0) as u32,
            limit: 100,
        };
    }
}

impl From<&js_sys::Object> for Querier {
    fn from(filter: &js_sys::Object) -> Self {
        let mut querier = Querier::default();

        let keys = js_sys::Object::keys(filter);
        for i in 0..keys.length() {
            let key = keys.get(i);
            if let Ok(value) = js_sys::Reflect::get(&filter, &key) {
                let key_string = key.as_string().expect("object key is not a string?");
                let key_str = key_string.as_str();
                match key_str {
                    "ids" => {
                        let array = js_sys::Array::from(&value);
                        let ids = querier
                            .ids
                            .insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            ids.push(array.get(i).as_string().expect("ids must be strings"));
                        }
                        break; // break here because if we have ids we don't care about anything else
                    }
                    "authors" => {
                        let array = js_sys::Array::from(&value);
                        let authors = querier
                            .authors
                            .insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            authors
                                .push(array.get(i).as_string().expect("authors must be strings"));
                        }
                    }
                    "kinds" => {
                        let array = js_sys::Array::from(&value);
                        let kinds = querier
                            .kinds
                            .insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            kinds
                                .push(array.get(i).as_f64().expect("kinds must be numbers") as u16);
                        }
                    }
                    "since" => {
                        querier.since =
                            Some(value.as_f64().expect("since must be a number") as u32);
                    }
                    "until" => {
                        querier.until = value.as_f64().expect("until must be a number") as u32;
                    }
                    "limit" => {
                        querier.limit = value.as_f64().expect("limit must be a number") as usize;
                    }
                    "#d" => {
                        let array = js_sys::Array::from(&value);
                        let dtags = querier
                            .dtags
                            .insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            dtags.push(array.get(i).as_string().expect("d-tags must be strings"));
                        }
                    }
                    _ => {
                        if querier.chosen_tagvalues.is_none() && key_str.starts_with("#") {
                            querier.chosen_tagname = Some(key_str[1..].to_string());
                            let array = js_sys::Array::from(&value);
                            let chosen_tagvalues = querier
                                .chosen_tagvalues
                                .insert(Vec::with_capacity(array.length() as usize));
                            for i in 0..array.length() {
                                chosen_tagvalues.push(array.get(i).as_string().unwrap());
                            }
                        }
                    }
                }
            }
        }

        return querier;
    }
}

#[derive(Debug)]
pub struct IndexableEvent {
    pub pubkey: String,
    pub kind: u16,
    pub id: String,
    pub dtag: Option<String>,
    pub tags: Vec<(u8, String)>,
    pub timestamp: u32,
}

impl From<&js_sys::Array> for IndexableEvent {
    fn from(indexables_arr: &js_sys::Array) -> Self {
        // the indexables_arr has the shape [id, pubkey, kind, timestamp, tags]

        let indexable_tags = js_sys::Array::from(&indexables_arr.get(4));
        let mut tags = Vec::with_capacity(indexable_tags.length() as usize);
        let mut dtag = None;

        for t in 0..indexable_tags.length() {
            let indexable_tag = js_sys::Array::from(&indexable_tags.get(t));
            let letter = indexable_tag.get(0).as_f64().unwrap() as u8;
            let value = indexable_tag.get(1).as_string().unwrap();
            if letter == 100 {
                dtag = Some(value)
            } else {
                tags.push((letter, value));
            }
        }

        Self {
            id: indexables_arr.get(0).as_string().unwrap(),
            pubkey: indexables_arr.get(1).as_string().unwrap(),
            kind: indexables_arr.get(2).as_f64().unwrap() as u16,
            timestamp: indexables_arr.get(3).as_f64().unwrap() as u32,
            dtag,
            tags,
        }
    }
}

impl<'de> Deserialize<'de> for IndexableEvent {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TempEvent {
            pubkey: String,
            kind: u16,
            id: String,
            created_at: u32,
            tags: Vec<Vec<String>>,
        }
        let temp = TempEvent::deserialize(deserializer)?;

        let mut tags = Vec::with_capacity(temp.tags.len());
        let mut dtag = None;

        for mut tag in temp.tags.into_iter() {
            if tag.len() >= 2 {
                let first = tag.swap_remove(0);
                if first.len() == 1 {
                    let second = tag.swap_remove(1);
                    if first.as_str() == "d" {
                        dtag = Some(second);
                    } else {
                        tags.push((first.bytes().next().unwrap(), second));
                    }
                }
            }
        }

        Ok(IndexableEvent {
            pubkey: temp.pubkey,
            kind: temp.kind,
            id: temp.id,
            timestamp: temp.created_at,
            tags,
            dtag,
        })
    }
}
