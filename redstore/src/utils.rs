use serde::Deserialize;
use wasm_bindgen::JsValue;

pub const MAX_U32_BYTES: [u8; 4] = [0xff; 4];

pub type Result<T> = std::result::Result<T, JsValue>;

#[inline]
fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[inline]
fn is_hex(bytes: &[u8]) -> bool {
    bytes.iter().all(|c| hex_val(*c).is_some())
}

// takes a 64-char hex id or pubkey and parses its last 8 bytes into dest
pub fn parse_hex_suffix_into(hex_str: &str, dest: &mut [u8]) -> Result<()> {
    let bytes = hex_str.as_bytes();
    if bytes.len() != 64 || dest.len() != 8 {
        return Err(JsValue::from_str(&format!(
            "expected 64-char hex, got: {}",
            hex_str
        )));
    }
    for (i, pair) in bytes[48..64].as_chunks::<2>().0.iter().enumerate() {
        match (hex_val(pair[0]), hex_val(pair[1])) {
            (Some(hi), Some(lo)) => dest[i] = (hi << 4) | lo,
            _ => return Err(JsValue::from_str(&format!("invalid hex: {}", hex_str))),
        }
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
}

impl Default for Querier {
    fn default() -> Self {
        Self {
            ids: None,
            authors: None,
            kinds: None,
            dtags: None,
            tags: Vec::new(),
            since: None,
            until: u32::MAX,
            limit: 250,
        }
    }
}

impl TryFrom<&js_sys::Object> for Querier {
    type Error = JsValue;

    fn try_from(filter: &js_sys::Object) -> Result<Self> {
        let mut querier = Querier::default();

        let keys = js_sys::Object::keys(filter);
        for i in 0..keys.length() {
            let key = keys.get(i);
            if let Ok(value) = js_sys::Reflect::get(filter, &key) {
                let key_string = key
                    .as_string()
                    .ok_or_else(|| JsValue::from_str("object key is not a string"))?;
                let key_str = key_string.as_str();
                if value.is_undefined() {
                    continue;
                }
                match key_str {
                    "ids" => {
                        let array = js_sys::Array::from(&value);
                        let ids = querier
                            .ids
                            .insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            ids.push(array.get(i).as_string().ok_or_else(|| {
                                JsValue::from_str("ids must be strings")
                            })?);
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
                                .push(
                                    array
                                        .get(i)
                                        .as_string()
                                        .ok_or_else(|| JsValue::from_str("authors must be strings"))?,
                                );
                        }
                    }
                    "kinds" => {
                        let array = js_sys::Array::from(&value);
                        let kinds = querier
                            .kinds
                            .insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            kinds
                                .push(
                                    array
                                        .get(i)
                                        .as_f64()
                                        .ok_or_else(|| JsValue::from_str("kinds must be numbers"))?
                                        as u16,
                                );
                        }
                    }
                    "since" => {
                        querier.since = Some(
                            value
                                .as_f64()
                                .ok_or_else(|| JsValue::from_str("since must be a number"))?
                                as u32,
                        );
                    }
                    "until" => {
                        querier.until = value
                            .as_f64()
                            .ok_or_else(|| JsValue::from_str("until must be a number"))?
                            as u32;
                    }
                    "limit" => {
                        querier.limit = value
                            .as_f64()
                            .ok_or_else(|| JsValue::from_str("limit must be a number"))?
                            as usize;
                    }
                    "#d" => {
                        let array = js_sys::Array::from(&value);
                        let dtags = querier
                            .dtags
                            .insert(Vec::with_capacity(array.length() as usize));
                        for i in 0..array.length() {
                            dtags.push(
                                array
                                    .get(i)
                                    .as_string()
                                    .ok_or_else(|| JsValue::from_str("d-tags must be strings"))?,
                            );
                        }
                    }
                    _ => {
                        if let Some(name) = key_str.strip_prefix("#") {
                            let array = js_sys::Array::from(&value);
                            if let Some(letter) = name.bytes().next() {
                                if !letter.is_ascii() {
                                    return Err(JsValue::from_str(&format!(
                                        "tag #{} is not ascii",
                                        name
                                    )));
                                }
                                let mut values = Vec::with_capacity(array.length() as usize);
                                for i in 0..array.length() {
                                    values.push(
                                        array
                                            .get(i)
                                            .as_string()
                                            .ok_or_else(|| {
                                                JsValue::from_str(&format!(
                                                    "tag #{} values must be strings",
                                                    name
                                                ))
                                            })?,
                                    );
                                }
                                querier.tags.push((letter, values));
                            }
                        }
                    }
                }
            }
        }

        Ok(querier)
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
        // the extract_* functions below rely on this exact layout
        if event_bytes.len() < 305
            || !event_bytes.starts_with(b"{\"pubkey\":\"")
            || !is_hex(&event_bytes[11..75])
            || &event_bytes[75..83] != b"\",\"id\":\""
            || !is_hex(&event_bytes[83..147])
            || &event_bytes[147..156] != b"\",\"kind\":"
            || !event_bytes[156].is_ascii_digit()
        {
            return Err(JsValue::from_str(
                "event json is not in the expected format",
            ));
        }

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

            // a 1-byte utf-8 string is always ascii
            let letter = tag[0].as_bytes()[0];

            let value = tag.swap_remove(1);
            if letter == 100 && (30000..40000).contains(&kind) {
                dtag = Some(value.clone())
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
                if name.len() == 1
                    && let Some(letter) = name.bytes().next()
                {
                    let value = tag.swap_remove(1);
                    if letter == 100 {
                        // 'd' tag - only extract for addressable events (kind 30000-40000)
                        if temp.kind >= 30000 && temp.kind < 40000 {
                            dtag = Some(value.clone());
                        }
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
pub fn extract_pubkey_bytes(event_json: &[u8]) -> &[u8] {
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
    if let Some(tags_start) = event_json
        .get(305..)
        .and_then(|rest| rest.iter().position(|c| *c == 34 /* '"' */))
        .map(|pos| pos + 305 + 9)
        && let Some(tags_end) = event_json
            .get(tags_start..)
            .unwrap_or_default()
            .iter()
            .enumerate()
            .position(|(i, c)| {
                // search for '],"'
                *c == 34 // '"'
                && event_json[tags_start + i - 1] == 44 // ','
                && event_json[tags_start + i - 2] == 93 // ']'
            })
            .map(|pos| pos + tags_start - 2 + 1 /* we'll match the end of '],"', so we have to go 2 back, but add 1 so we include the ']' */)
        && let Some(tags_json) = event_json.get(tags_start..tags_end)
        {
            return serde_json::from_slice::<Vec<Vec<String>>>(tags_json)
                .map_err(|e| JsValue::from_str(&format!("invalid tags json extracted: {:?}", e,)));
        }

    Err(JsValue::from("failed to extract tags"))
}

#[inline]
pub fn extract_created_at(event_json: &[u8]) -> Result<u32> {
    let Some(start) = event_json
        .get(169..)
        .and_then(|rest| rest.iter().position(|c| *c == 58 /* ':' */))
        .map(|pos| pos + 169 + 1)
    else {
        return Err(JsValue::from("failed to extract created_at"));
    };

    let mut ts: u32 = 0;
    let mut digits = 0;
    for c in event_json[start..].iter().take(10) {
        if !c.is_ascii_digit() {
            break;
        }
        ts = ts
            .checked_mul(10)
            .and_then(|ts| ts.checked_add((*c - 48) as u32))
            .ok_or_else(|| JsValue::from("created_at out of bounds"))?;
        digits += 1;
    }
    if digits == 0 {
        return Err(JsValue::from("failed to extract created_at"));
    }
    Ok(ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_tags_on_preformatted_event() {
        let evtj = r#"{"pubkey":"c5cdd5737e47f5426c9dea243012112ed62fba7b534788681606f79f5ab9682a","id":"908801a73409d38b1420b70edb21e380afa1a0fbb444d96f164b3f4926e1cc0a","kind":1,"created_at":1714060747,"sig":"9bfb9a8499ae3ebad287452abd777bad4daf65ed99e63149c3c38e11e54b997577ae18edf5439fd31d21593cea6c153b49549fba9177542c98f81156098f7595","tags":[["e","45e72174dad9971b8c9197295a3f871a87775af046df8686e1a2686ca8b6ef89","wss://relay.wellorder.net","root"],["p","f728d9e6e7048358e70930f5ca64b097770d989ccd86854fe618eda9c8a38106"]],"content":"Which lie is Ukraine war based on? Go on, enlighten me, please.","seen_on":["wss://nos.lol/"]}"#.as_bytes();
        let tags = extract_tags(evtj).unwrap();
        assert_eq!(tags[0][0], "e");
        assert_eq!(
            tags[0][1],
            "45e72174dad9971b8c9197295a3f871a87775af046df8686e1a2686ca8b6ef89"
        );
        assert_eq!(tags[0][2], "wss://relay.wellorder.net");
        assert_eq!(
            tags[1][1],
            "f728d9e6e7048358e70930f5ca64b097770d989ccd86854fe618eda9c8a38106"
        );
    }

    #[test]
    fn indexable_event_from_preformatted_event() {
        let evtj = r#"{"pubkey":"c5cdd5737e47f5426c9dea243012112ed62fba7b534788681606f79f5ab9682a","id":"908801a73409d38b1420b70edb21e380afa1a0fbb444d96f164b3f4926e1cc0a","kind":30023,"created_at":1714060747,"sig":"9bfb9a8499ae3ebad287452abd777bad4daf65ed99e63149c3c38e11e54b997577ae18edf5439fd31d21593cea6c153b49549fba9177542c98f81156098f7595","tags":[["d","hello"],["p","f728d9e6e7048358e70930f5ca64b097770d989ccd86854fe618eda9c8a38106"]],"content":"x"}"#.as_bytes();
        let evt = IndexableEvent::from_json_event(evtj).unwrap();
        assert_eq!(evt.kind, 30023);
        assert_eq!(evt.timestamp, 1714060747);
        assert_eq!(evt.dtag.as_deref(), Some("hello"));
        assert_eq!(evt.tags.len(), 2);

        let mut dest = [0u8; 8];
        parse_hex_suffix_into(&evt.id, &mut dest).unwrap();
        assert_eq!(dest, [0x16, 0x4b, 0x3f, 0x49, 0x26, 0xe1, 0xcc, 0x0a]);
    }
}
