use crate::mdbx::Key;
use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::convert::TryInto;

pub struct IdKey<'a> {
    bytes: Cow<'a, [u8]>,
}

impl<'a> IdKey<'a> {
    pub fn new(id: i64) -> Self {
        let unsigned: u64 = unsafe { std::mem::transmute(id) };
        let bytes = (unsigned ^ 1 << 63).to_le_bytes().to_vec();
        IdKey {
            bytes: Cow::Owned(bytes),
        }
    }

    pub fn from_bytes(bytes: &'a [u8]) -> IdKey<'a> {
        IdKey {
            bytes: Cow::Borrowed(bytes),
        }
    }

    pub fn get_unsigned_id(&self) -> u64 {
        u64::from_le_bytes(self.as_bytes().try_into().unwrap())
    }

    pub fn get_id(&self) -> i64 {
        let unsigned = self.get_unsigned_id();
        let signed: i64 = unsafe { std::mem::transmute(unsigned) };
        signed ^ 1 << 63
    }
}

impl<'a> Key for IdKey<'a> {
    fn as_bytes(&self) -> &[u8] {
        self.bytes.borrow()
    }

    fn cmp_bytes(&self, other: &[u8]) -> Ordering {
        let other_key = IdKey::from_bytes(other);
        self.get_unsigned_id().cmp(&other_key.get_unsigned_id())
    }
}

#[cfg(test)]
mod tests {
    use crate::id_key::IdKey;
    use crate::mdbx::Key;

    #[test]
    fn test_new() {
        assert_eq!(IdKey::new(i64::MIN).as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            IdKey::new(i64::MIN + 1).as_bytes(),
            &[1, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            IdKey::new(i64::MAX).as_bytes(),
            &[255, 255, 255, 255, 255, 255, 255, 255]
        );
        assert_eq!(
            IdKey::new(i64::MAX - 1).as_bytes(),
            &[254, 255, 255, 255, 255, 255, 255, 255]
        );
    }

    #[test]
    fn test_from_bytes() {
        assert_eq!(
            IdKey::from_bytes(&[1, 2, 3, 4, 5, 6, 7, 8]).as_bytes(),
            &[1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn test_get_unsigned_int() {
        assert_eq!(IdKey::new(i64::MIN).get_unsigned_id(), 0);
        assert_eq!(IdKey::new(i64::MIN + 1).get_unsigned_id(), 1);
        assert_eq!(IdKey::new(i64::MAX).get_unsigned_id(), u64::MAX);
        assert_eq!(IdKey::new(i64::MAX - 1).get_unsigned_id(), u64::MAX - 1);
    }

    #[test]
    fn test_get_id() {
        assert_eq!(IdKey::new(i64::MIN).get_id(), i64::MIN);
        assert_eq!(IdKey::new(i64::MIN + 1).get_id(), i64::MIN + 1);
        assert_eq!(IdKey::new(i64::MAX).get_id(), i64::MAX);
        assert_eq!(IdKey::new(i64::MAX - 1).get_id(), i64::MAX - 1);
    }
}
