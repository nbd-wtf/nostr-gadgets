use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;

pub const MAX_U32_BYTES: [u8; 4] = [0xff; 4];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NostrEvent {
    pub pubkey: String,
    pub kind: u32,
    pub id: String,
    pub created_at: u32,
    pub tags: Vec<Vec<String>>,
    pub content: String,
    pub sig: String,
}

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
