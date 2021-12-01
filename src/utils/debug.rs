use crate::collection::IsarCollection;
use crate::index::Index;
use crate::instance::IsarInstance;
use crate::lmdb::cursor::Cursor;
use crate::lmdb::Key;
use crate::lmdb::{ByteKey, IntKey};
use crate::object::isar_object::IsarObject;
use crate::object::object_builder::ObjectBuilder;
use crate::txn::IsarTxn;
use hashbrown::HashSet;

#[macro_export]
macro_rules! map (
    ($($key:expr => $value:expr),+) => {
        {
            let mut m = ::hashbrown::HashMap::new();
            $(m.insert($key, $value);)+
            m
        }
    };
);

#[macro_export]
macro_rules! set (
    [$($val:expr),+] => {
        {
            let mut s = ::hashbrown::HashSet::new();
            $(s.insert($val);)+
            s
        }
    };
);

#[macro_export]
macro_rules! txn (
    ($isar:expr, $txn:ident) => {
        let mut $txn = $isar.begin_txn(true, false).unwrap();
    };
);

pub trait WriteToObject {
    fn write_to_object(&self, builder: &mut ObjectBuilder);
}

impl WriteToObject for u8 {
    fn write_to_object(&self, builder: &mut ObjectBuilder) {
        builder.write_byte(*self)
    }
}

impl WriteToObject for bool {
    fn write_to_object(&self, builder: &mut ObjectBuilder) {
        builder.write_bool(*self)
    }
}

impl WriteToObject for i32 {
    fn write_to_object(&self, builder: &mut ObjectBuilder) {
        builder.write_int(*self)
    }
}

impl WriteToObject for f32 {
    fn write_to_object(&self, builder: &mut ObjectBuilder) {
        builder.write_float(*self)
    }
}

impl WriteToObject for i64 {
    fn write_to_object(&self, builder: &mut ObjectBuilder) {
        builder.write_long(*self)
    }
}

impl WriteToObject for f64 {
    fn write_to_object(&self, builder: &mut ObjectBuilder) {
        builder.write_double(*self)
    }
}

impl WriteToObject for &str {
    fn write_to_object(&self, builder: &mut ObjectBuilder) {
        builder.write_string(Some(self))
    }
}

impl WriteToObject for Option<&str> {
    fn write_to_object(&self, builder: &mut ObjectBuilder) {
        builder.write_string(*self)
    }
}

#[macro_export]
macro_rules! object (
    ($col:expr, $obj:ident, $builder:ident, $($value:expr),+) => {
        let mut $builder = $col.new_object_builder(None);
        $(
            $crate::utils::debug::WriteToObject::write_to_object(&$value, &mut $builder);
        )+
        let $obj = $builder.finish();
    };
);

#[macro_export]
macro_rules! put_object (
    ($col:expr, $txn:expr, $obj:ident, $builder:ident, $($value:expr),+) => {
        $crate::object!($col, $obj, $builder, $($value),+);
        $col.put(&mut $txn, $obj).unwrap();
    };
);

pub fn verify_col(col: &IsarCollection, txn: &mut IsarTxn, objects: &[IsarObject]) {
    for index in col.debug_get_indexes() {
        verify_index(index, txn, objects)
    }

    let mut map = hashbrown::HashMap::new();
    for object in objects {
        map.insert(object.read_id(), object.as_bytes().to_vec());
    }
    assert_eq!(col.debug_dump(txn), map);
}

pub(crate) fn verify_index(index: &Index, txn: &mut IsarTxn, objects: &[IsarObject]) {
    let mut set = hashbrown::HashSet::new();
    for object in objects {
        let id_bytes = IntKey::new(index.col_id, object.read_id())
            .as_bytes()
            .to_vec();
        let keys = index.debug_create_keys(*object);
        for key in keys {
            set.insert((key, id_bytes.clone()));
        }
    }
    assert_eq!(index.debug_dump(txn), set);
}

pub fn dump_db(cursor: &mut Cursor, prefix: Option<&[u8]>) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let mut set = HashSet::new();

    let mut upper = prefix.unwrap_or(&[]).to_vec();
    upper.extend_from_slice(&u64::MAX.to_le_bytes());
    cursor
        .iter_between(
            ByteKey::new(prefix.unwrap_or(&[])),
            ByteKey::new(&upper),
            false,
            true,
            |_, k, v| {
                set.insert((k.to_vec(), v.to_vec()));
                Ok(true)
            },
        )
        .unwrap();
    set
}

pub fn dump_db_oid(cursor: &mut Cursor, prefix: u16) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let mut set = HashSet::new();

    cursor
        .iter_between(
            IntKey::new(prefix, IsarInstance::MIN_ID),
            IntKey::new(prefix, IsarInstance::MAX_ID),
            false,
            true,
            |_, k, v| {
                set.insert((k.to_vec(), v.to_vec()));
                Ok(true)
            },
        )
        .unwrap();

    set
}
