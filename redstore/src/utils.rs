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
    pub tags: Vec<(u8, Vec<String>)>,
    pub since: Option<u32>,
    pub until: u32,
    pub limit: usize,
    pub followed_by: Option<String>,
}

impl Default for Querier {
    fn default() -> Self {
        return Self {
            ids: None,
            authors: None,
            kinds: None,
            dtags: None,
            tags: Vec::new(),
            since: None,
            until: (js_sys::Date::now() / 1000.0) as u32,
            limit: 250,
            followed_by: None,
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
                    "followedBy" => {
                        querier.followed_by =
                            Some(value.as_string().expect("followedBy must be a string"));
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
                        if key_str.starts_with("#") {
                            let name = key_str[1..].to_string();
                            let array = js_sys::Array::from(&value);
                            let mut values = Vec::with_capacity(array.length() as usize);
                            for i in 0..array.length() {
                                values.push(array.get(i).as_string().unwrap());
                            }
                            querier.tags.push((name.bytes().next().unwrap(), values));
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

impl IndexableEvent {
    pub fn from_json_event(event_bytes: &[u8]) -> Result<Self> {
        let kind = extract_kind(event_bytes);

        let extracted_tags = extract_tags(event_bytes)?;
        let mut tags = Vec::with_capacity(extracted_tags.len());
        let mut dtag = None;
        for mut tag in extracted_tags {
            if tag.len() < 2 {
                continue;
            }
            if tag[0].len() != 1 {
                continue;
            }

            let letter = tag[0]
                .chars()
                .next()
                .expect("very weird tag")
                .as_ascii()
                .unwrap() as u8;

            let value = tag.swap_remove(1);
            if letter == 100 {
                if kind >= 30000 && kind < 40000 {
                    dtag = Some(value.clone())
                }
            }

            tags.push((letter, value));
        }

        Ok(Self {
            id: extract_id(event_bytes),
            pubkey: extract_pubkey(event_bytes),
            kind,
            tags,
            dtag,
            timestamp: extract_created_at(event_bytes)?,
        })
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
                let name = &tag[0];
                if name.len() == 1 {
                    let letter = name.bytes().next().unwrap();
                    let value = tag.swap_remove(1);
                    if letter == 100 {
                        // 'd' tag
                        dtag = Some(value.clone());
                    }
                    tags.push((letter, value));
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

// weird functions that depend on the JSON being always formed in the same way:

#[inline]
pub fn extract_pubkey(event_json: &[u8]) -> String {
    // saved events always have the pubkey at this pos
    let author_hex = &event_json[11..75];
    String::from_utf8_lossy(author_hex).to_string()
}

#[inline]
pub fn extract_pubkey_bytes<'a>(event_json: &'a [u8]) -> &'a [u8] {
    &event_json[11..75]
}

#[inline]
pub fn extract_id(event_json: &[u8]) -> String {
    String::from_utf8_lossy(&event_json[83..147]).to_string()
}

#[inline]
pub fn extract_kind(event_json: &[u8]) -> u16 {
    let mut kind = (event_json[156] - 48) as u16; // the first char is always a number
    for c in &event_json[157..161] {
        // then the next 4 may or may not be
        if (*c >= 48/* '0' */) && (*c <= 57/* '9' */) {
            kind = kind * 10 + ((*c - 48) as u16)
        } else {
            break;
        }
    }
    kind
}

#[inline]
pub fn extract_tags(event_json: &[u8]) -> Result<Vec<Vec<String>>> {
    if let Some(tags_start) = event_json[305..]
        .iter()
        .position(|c| *c == 34 /* '"' */)
        .map(|pos| pos + 305 + 9)
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
            return serde_json::from_slice::<Vec<Vec<String>>>(&event_json[tags_start..tags_end])
                .map_err(|e| JsValue::from_str(&format!("invalid tags json extracted: {:?}", e,)));
        }
    }

    Err(JsValue::from("failed to extract tags"))
}

#[inline]
pub fn extract_created_at(event_json: &[u8]) -> Result<u32> {
    if let Some(start) = event_json[169..]
        .iter()
        .position(|c| *c == 58 /* ':' */)
        .map(|pos| pos + 169 + 1)
    {
        let mut ts = (event_json[start] - 48) as u32; // the first char is always a number
        for c in &event_json[start + 1..start + 11] {
            // then the next may or may not be
            if (*c >= 48/* '0' */) && (*c <= 57/* '9' */) {
                ts = ts * 10 + ((*c - 48) as u32)
            } else {
                break;
            }
        }
        Ok(ts)
    } else {
        Err(JsValue::from("failed to extract created_at"))
    }
}
