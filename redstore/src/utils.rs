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
    pub tags: Vec<(u8, String)>,
    pub timestamp: u32,
}

impl From<&js_sys::Object> for IndexableEvent {
    fn from(event_obj: &js_sys::Object) -> Self {
        let indexable_tags = js_sys::Array::from(
            &js_sys::Reflect::get(&event_obj, &JsValue::from_str("indexable_tags")).unwrap(),
        );
        let mut tags = Vec::with_capacity(indexable_tags.length() as usize);
        for t in 0..indexable_tags.length() {
            let indexable_tag = js_sys::Array::from(&indexable_tags.get(t));
            let letter = indexable_tag.get(0).as_f64().unwrap() as u8;
            let value = indexable_tag.get(1).as_string().unwrap();
            tags.push((letter, value));
        }

        Self {
            pubkey: js_sys::Reflect::get(&event_obj, &JsValue::from_str("pubkey"))
                .unwrap()
                .as_string()
                .unwrap(),
            kind: js_sys::Reflect::get(&event_obj, &JsValue::from_str("kind"))
                .unwrap()
                .as_f64()
                .unwrap() as u16,
            id: js_sys::Reflect::get(&event_obj, &JsValue::from_str("id"))
                .unwrap()
                .as_string()
                .unwrap(),
            timestamp: js_sys::Reflect::get(&event_obj, &JsValue::from_str("timestamp"))
                .unwrap()
                .as_f64()
                .unwrap() as u32,
            tags,
        }
    }
}

impl Deserialize for IndexableEvent {}
