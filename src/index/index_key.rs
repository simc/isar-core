use crate::index::{Index, MAX_STRING_INDEX_SIZE};
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
            index: index,
            bytes: index.get_prefix(),
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

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn truncate(&mut self, len: usize) {
        assert!(len >= 2);
        self.bytes.truncate(len);
    }
}

#[cfg(test)]
mod tests {
    use crate::object::isar_object::IsarObject;

    use super::*;
    use float_next_after::NextAfter;

    #[test]
    fn test_add_byte() {
        let pairs = vec![
            (IsarObject::NULL_BYTE, vec![0]),
            (123, vec![123]),
            (255, vec![255]),
        ];

        let index = Index::new(0, 0, vec![], false, false);
        for (val, bytes) in pairs {
            let mut index_key = IndexKey::new(&index);
            index_key.add_byte(val);
            assert_eq!(&index_key.bytes[2..], &bytes);
        }
    }

    #[test]
    fn test_add_int() {
        let pairs = vec![
            (i32::MIN, vec![0, 0, 0, 0]),
            (i32::MIN + 1, vec![0, 0, 0, 1]),
            (-1, vec![127, 255, 255, 255]),
            (0, vec![128, 0, 0, 0]),
            (1, vec![128, 0, 0, 1]),
            (i32::MAX - 1, vec![255, 255, 255, 254]),
            (i32::MAX, vec![255, 255, 255, 255]),
        ];

        let index = Index::new(0, 0, vec![], false, false);
        for (val, bytes) in pairs {
            let mut index_key = IndexKey::new(&index);
            index_key.add_int(val);
            assert_eq!(&index_key.bytes[2..], &bytes);
        }
    }

    #[test]
    fn test_add_long() {
        let pairs = vec![
            (i64::MIN, vec![0, 0, 0, 0, 0, 0, 0, 0]),
            (i64::MIN + 1, vec![0, 0, 0, 0, 0, 0, 0, 1]),
            (-1, vec![127, 255, 255, 255, 255, 255, 255, 255]),
            (0, vec![128, 0, 0, 0, 0, 0, 0, 0]),
            (1, vec![128, 0, 0, 0, 0, 0, 0, 1]),
            (i64::MAX - 1, vec![255, 255, 255, 255, 255, 255, 255, 254]),
            (i64::MAX, vec![255, 255, 255, 255, 255, 255, 255, 255]),
        ];

        let index = Index::new(0, 0, vec![], false, false);
        for (val, bytes) in pairs {
            let mut index_key = IndexKey::new(&index);
            index_key.add_long(val);
            assert_eq!(&index_key.bytes[2..], &bytes);
        }
    }

    #[test]
    fn test_add_float() {
        let pairs = vec![
            (f32::NAN, vec![0, 0, 0, 0]),
            (f32::NEG_INFINITY, vec![0, 127, 255, 255]),
            (f32::MIN, vec![0, 128, 0, 0]),
            (f32::MIN.next_after(f32::MAX), vec![0, 128, 0, 1]),
            ((-0.0).next_after(f32::MIN), vec![127, 255, 255, 254]),
            (-0.0, vec![127, 255, 255, 255]),
            (0.0, vec![128, 0, 0, 0]),
            (0.0.next_after(f32::MAX), vec![128, 0, 0, 1]),
            (f32::MAX.next_after(f32::MIN), vec![255, 127, 255, 254]),
            (f32::MAX, vec![255, 127, 255, 255]),
            (f32::INFINITY, vec![255, 128, 0, 0]),
        ];

        let index = Index::new(0, 0, vec![], false, false);
        for (val, bytes) in pairs {
            let mut index_key = IndexKey::new(&index);
            index_key.add_float(val);
            assert_eq!(&index_key.bytes[2..], &bytes);
        }
    }

    #[test]
    fn test_add_double() {
        let pairs = vec![
            (f64::NAN, vec![0, 0, 0, 0, 0, 0, 0, 0]),
            (f64::NEG_INFINITY, vec![0, 15, 255, 255, 255, 255, 255, 255]),
            (f64::MIN, vec![0, 16, 0, 0, 0, 0, 0, 0]),
            (f64::MIN.next_after(f64::MAX), vec![0, 16, 0, 0, 0, 0, 0, 1]),
            (
                (-0.0).next_after(f64::MIN),
                vec![127, 255, 255, 255, 255, 255, 255, 254],
            ),
            (-0.0, vec![127, 255, 255, 255, 255, 255, 255, 255]),
            (0.0, vec![128, 0, 0, 0, 0, 0, 0, 0]),
            (0.0.next_after(f64::MAX), vec![128, 0, 0, 0, 0, 0, 0, 1]),
            (
                f64::MAX.next_after(f64::MIN),
                vec![255, 239, 255, 255, 255, 255, 255, 254],
            ),
            (f64::MAX, vec![255, 239, 255, 255, 255, 255, 255, 255]),
            (f64::INFINITY, vec![255, 240, 0, 0, 0, 0, 0, 0]),
        ];

        let index = Index::new(0, 0, vec![], false, false);
        for (val, bytes) in pairs {
            let mut index_key = IndexKey::new(&index);
            index_key.add_double(val);
            assert_eq!(&index_key.bytes[2..], &bytes);
        }
    }

    #[test]
    fn test_add_string_hash() {
        let long_str = (0..850).map(|_| "aB").collect::<String>();

        let pairs: Vec<(Option<&str>, Vec<u8>, Vec<u8>)> = vec![
            (
                None,
                vec![0, 0, 0, 0, 0, 0, 0, 0],
                vec![0, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                Some(""),
                vec![183, 56, 242, 170, 183, 88, 42, 211],
                vec![183, 56, 242, 170, 183, 88, 42, 211],
            ),
            (
                Some("hELLo"),
                vec![195, 215, 64, 163, 175, 255, 28, 49],
                vec![255, 175, 47, 252, 56, 169, 22, 4],
            ),
            (
                Some("this is just a test"),
                vec![156, 13, 228, 133, 209, 47, 168, 125],
                vec![156, 13, 228, 133, 209, 47, 168, 125],
            ),
            (
                Some(&long_str[..]),
                vec![232, 213, 235, 242, 9, 163, 151, 208],
                vec![245, 5, 235, 221, 71, 240, 88, 127],
            ),
        ];

        let index = Index::new(0, 0, vec![], false, false);
        for (str, hash, hash_lc) in pairs {
            let mut index_key = IndexKey::new(&index);
            index_key.add_string_hash(str, true);
            assert_eq!(index_key.bytes[2..], hash);

            let mut index_key = IndexKey::new(&index);
            index_key.add_string_hash(str, false);
            assert_eq!(index_key.bytes[2..], hash_lc);
        }
    }

    #[test]
    fn test_get_string_value_key() {
        let long_str = (0..850).map(|_| "aB").collect::<String>();
        let long_str_lc = long_str.to_lowercase();

        let mut long_str_bytes = vec![1];
        long_str_bytes.extend_from_slice(long_str.as_bytes());
        long_str_bytes.push(0);

        let mut long_str_lc_bytes = vec![1];
        long_str_lc_bytes.extend_from_slice(long_str_lc.as_bytes());
        long_str_lc_bytes.push(0);

        let mut hello_bytes = vec![1];
        hello_bytes.extend_from_slice(b"hELLO");
        hello_bytes.push(0);

        let mut hello_bytes_lc = vec![1];
        hello_bytes_lc.extend_from_slice(b"hello");
        hello_bytes_lc.push(0);

        let pairs: Vec<(Option<&str>, Vec<u8>, Vec<u8>)> = vec![
            (None, vec![0], vec![0]),
            (Some(""), vec![1, 0], vec![1, 0]),
            (
                Some("hello"),
                hello_bytes_lc.clone(),
                hello_bytes_lc.clone(),
            ),
            (Some("hELLO"), hello_bytes.clone(), hello_bytes_lc.clone()),
            //(Some(&long_str), long_str_bytes, long_str_lc_bytes),
        ];

        let index = Index::new(0, 0, vec![], false, false);
        for (str, bytes, bytes_lc) in pairs {
            let mut index_key = IndexKey::new(&index);
            index_key.add_string_value(str, true);
            assert_eq!(index_key.bytes[2..], bytes);

            let mut index_key = IndexKey::new(&index);
            index_key.add_string_value(str, false);
            assert_eq!(index_key.bytes[2..], bytes_lc);
        }
    }

    #[test]
    fn test_get_string_word_keys() {
        let pairs: Vec<(&str, Vec<u8>, Vec<u8>)> = vec![
            ("", b"".to_vec(), b"".to_vec()),
            ("hello", b"hello".to_vec(), b"hello".to_vec()),
            ("tESt", b"tESt".to_vec(), b"test".to_vec()),
        ];

        let index = Index::new(0, 0, vec![], false, false);
        for (str, bytes, bytes_lc) in pairs {
            let mut index_key = IndexKey::new(&index);
            index_key.add_string_word(str, true);
            assert_eq!(index_key.bytes[2..], bytes);

            let mut index_key = IndexKey::new(&index);
            index_key.add_string_word(str, false);
            assert_eq!(index_key.bytes[2..], bytes_lc);
        }
    }
}
