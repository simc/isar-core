use crate::index::Index;
use crate::index::MAX_STRING_INDEX_SIZE;
use std::hash::Hasher;
use wyhash::{wyhash, WyHash};

#[derive(Clone)]
pub struct IndexKey<'a> {
    pub(crate) index: &'a Index,
    pub(crate) bytes: Vec<u8>,
}

impl<'a> IndexKey<'a> {
    pub(crate) fn new(index: &'a Index) -> Self {
        IndexKey {
            index: &index,
            bytes: index.get_prefix(),
        }
    }

    pub(crate) fn with_buffer(index: &'a Index, mut buffer: Vec<u8>) -> Self {
        buffer.clear();
        buffer.extend_from_slice(index.get_prefix().as_slice());
        IndexKey {
            index: &index,
            bytes: buffer,
        }
    }

    pub fn add_byte(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn add_int(&mut self, value: i32) {
        let unsigned = unsafe { std::mem::transmute::<i32, u32>(value) };
        let bytes: [u8; 4] = (unsigned ^ 1 << 31).to_be_bytes();
        self.bytes.extend_from_slice(&bytes);
    }

    pub fn add_long(&mut self, value: i64) {
        let unsigned = unsafe { std::mem::transmute::<i64, u64>(value) };
        let bytes: [u8; 8] = (unsigned ^ 1 << 63).to_be_bytes();
        self.bytes.extend_from_slice(&bytes);
    }

    pub fn add_float(&mut self, value: f32) {
        let bytes: [u8; 4] = if !value.is_nan() {
            let bits = if value.is_sign_positive() {
                value.to_bits() + 2u32.pow(31)
            } else {
                !(-value).to_bits() - 2u32.pow(31)
            };
            bits.to_be_bytes()
        } else {
            [0; 4]
        };
        self.bytes.extend_from_slice(&bytes);
    }

    pub fn add_double(&mut self, value: f64) {
        let bytes: [u8; 8] = if !value.is_nan() {
            let bits = if value.is_sign_positive() {
                value.to_bits() + 2u64.pow(63)
            } else {
                !(-value).to_bits() - 2u64.pow(63)
            };
            bits.to_be_bytes()
        } else {
            [0; 8]
        };
        self.bytes.extend_from_slice(&bytes);
    }

    pub fn add_string_hash(&mut self, value: Option<&str>, case_sensitive: bool) {
        let hash = if let Some(value) = value {
            let mut hasher = WyHash::default();
            hasher.write_usize(value.len());
            if case_sensitive {
                hasher.write(value.as_bytes());
            } else {
                let lower_case = value.to_lowercase();
                hasher.write(lower_case.as_bytes());
            }
            hasher.finish()
        } else {
            0
        };
        let bytes: [u8; 8] = hash.to_be_bytes();
        self.bytes.extend_from_slice(&bytes);
    }

    pub fn add_string_value(&mut self, value: Option<&str>, case_sensitive: bool) {
        if let Some(value) = value {
            let value = if case_sensitive {
                value.to_string()
            } else {
                value.to_lowercase()
            };
            let bytes = value.as_bytes();
            self.bytes.push(1);
            if bytes.len() >= MAX_STRING_INDEX_SIZE {
                self.bytes
                    .extend_from_slice(&bytes[0..MAX_STRING_INDEX_SIZE]);
                self.bytes.push(0);
                let hash = wyhash(bytes, 0);
                self.bytes.extend_from_slice(&u64::to_le_bytes(hash));
            } else {
                self.bytes.extend_from_slice(bytes);
                self.bytes.push(0);
            }
        } else {
            self.bytes.push(0);
        }
    }

    pub fn add_string_word(&mut self, value: &str, case_sensitive: bool) {
        if case_sensitive {
            self.bytes.extend_from_slice(value.as_bytes());
        } else {
            let lower_case = value.to_lowercase();
            self.bytes.extend_from_slice(lower_case.as_bytes());
        }
    }
}
