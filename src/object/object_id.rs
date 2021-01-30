use crate::index::Index;
use crate::object::data_type::DataType;
use byteorder::{BigEndian, ByteOrder};
use std::borrow::Cow;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectId<'a> {
    oid_type: DataType,
    bytes: Cow<'a, [u8]>,
}

impl<'a> ObjectId<'a> {
    pub(crate) fn from_int(col_id: u16, int: i32) -> Self {
        let mut bytes = col_id.to_be_bytes().to_vec();
        bytes.extend_from_slice(&Index::create_int_key(int));
        ObjectId {
            oid_type: DataType::Int,
            bytes: Cow::Owned(bytes),
        }
    }

    pub(crate) fn from_long(col_id: u16, long: i64) -> Self {
        let mut bytes = col_id.to_be_bytes().to_vec();
        bytes.extend_from_slice(&Index::create_long_key(long));
        ObjectId {
            oid_type: DataType::Long,
            bytes: Cow::from(bytes),
        }
    }

    pub(crate) fn from_str(col_id: u16, str: &str) -> Self {
        let mut bytes = col_id.to_be_bytes().to_vec();
        bytes.extend_from_slice(str.as_bytes());
        ObjectId {
            oid_type: DataType::String,
            bytes: Cow::from(bytes),
        }
    }

    pub(crate) fn from_bytes(oid_type: DataType, bytes: &'a [u8]) -> Self {
        ObjectId {
            oid_type,
            bytes: Cow::from(bytes),
        }
    }

    pub(crate) fn get_col_id(&self) -> u16 {
        BigEndian::read_u16(self.as_bytes())
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn get_type(&self) -> DataType {
        self.oid_type
    }

    pub fn get_int(&self) -> Option<i32> {
        if self.oid_type == DataType::Int {
            Some(Index::get_int_from_key(&self.bytes[2..]))
        } else {
            None
        }
    }

    pub fn get_long(&self) -> Option<i64> {
        if self.oid_type == DataType::Long {
            Some(Index::get_long_from_key(&self.bytes[2..]))
        } else {
            None
        }
    }

    pub fn get_string(&self) -> Option<&str> {
        if self.oid_type == DataType::String {
            Some(std::str::from_utf8(&self.bytes[2..]).unwrap())
        } else {
            None
        }
    }

    pub fn to_owned(&self) -> ObjectId<'static> {
        ObjectId {
            oid_type: self.oid_type,
            bytes: Cow::from(self.as_bytes().to_vec()),
        }
    }
}

impl<'a> Hash for ObjectId<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_bytes())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_as_bytes() {
        /*let mut oid = ObjectId::new(123, 222);
        assert_eq!(
            oid.as_bytes(99),
            &[99, 0, 0, 0, 0, 123, 222, 0, 0, 0, 0, 0, 0, 0]
        )*/
    }
}
