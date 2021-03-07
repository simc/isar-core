use crate::collection::IsarCollection;
use crate::error::Result;
use crate::index::Index;
use crate::lmdb::{ByteKey, IntKey};
use crate::object::isar_object::IsarObject;
use crate::query::Sort;
use crate::schema::collection_schema::IndexType;
use crate::txn::Cursors;
use std::mem::ManuallyDrop;

#[derive(Clone)]
pub struct IdWhereClause {
    prefix: u16,
    lower: i64,
    upper: i64,
    sort: Sort,
}

impl IdWhereClause {
    pub(crate) fn new(prefix: u16, lower: i64, upper: i64, sort: Sort) -> Self {
        IdWhereClause {
            prefix,
            lower,
            upper,
            sort,
        }
    }

    pub(crate) fn get_prefix(&self) -> u16 {
        self.prefix
    }

    pub fn is_empty(&self) -> bool {
        self.upper < self.lower
    }

    pub(crate) fn id_matches(&self, oid: i64) -> bool {
        self.lower <= oid && self.upper >= oid
    }

    pub(crate) fn iter<'txn, F>(&self, cursors: &mut Cursors<'txn>, mut callback: F) -> Result<bool>
    where
        F: FnMut(&mut Cursors<'txn>, IntKey, &'txn [u8]) -> Result<bool>,
    {
        let mut cursors_clone = ManuallyDrop::new(cursors.clone());
        cursors.primary.iter_between(
            IntKey::new(self.prefix, self.lower),
            IntKey::new(self.prefix, self.upper),
            false,
            self.sort == Sort::Ascending,
            |_, oid, object| callback(&mut cursors_clone, IntKey::from_bytes(oid), object),
        )
    }
}

#[derive(Clone)]
pub struct IndexWhereClause {
    lower_key: Vec<u8>,
    upper_key: Vec<u8>,
    index: Index,
    skip_duplicates: bool,
    sort: Sort,
}

impl IndexWhereClause {
    const PREFIX_LEN: usize = 2;

    pub(crate) fn new(prefix: &[u8], index: Index, mut skip_duplicates: bool, sort: Sort) -> Self {
        if index.is_unique() {
            skip_duplicates = false;
        }
        IndexWhereClause {
            lower_key: prefix.to_vec(),
            upper_key: prefix.to_vec(),
            index,
            skip_duplicates,
            sort,
        }
    }

    pub fn is_empty(&self) -> bool {
        ByteKey::new(&self.lower_key) > ByteKey::new(&self.upper_key)
    }

    pub fn is_unique(&self) -> bool {
        self.index.is_unique()
    }

    pub fn is_from_collection(&self, collection: &IsarCollection) -> bool {
        collection.get_indexes().contains(&self.index)
    }

    pub(crate) fn object_matches(&self, object: IsarObject) -> bool {
        let mut key_matches = false;
        self.index
            .create_keys(object, |key| {
                let key = ByteKey::new(key);
                key_matches =
                    key >= ByteKey::new(&self.lower_key) && key <= ByteKey::new(&self.upper_key);
                Ok(!key_matches)
            })
            .unwrap();
        key_matches
    }

    pub(crate) fn iter<'txn, F>(&self, cursors: &mut Cursors<'txn>, mut callback: F) -> Result<bool>
    where
        F: FnMut(&mut Cursors<'txn>, IntKey) -> Result<bool>,
    {
        let mut cursors_clone = ManuallyDrop::new(cursors.clone());
        let secondary = &mut cursors.secondary;
        let secondary_dup = &mut cursors.secondary_dup;

        let cursor = if self.index.is_unique() {
            secondary
        } else {
            secondary_dup
        };

        cursor.iter_between(
            ByteKey::new(&self.lower_key),
            ByteKey::new(&self.upper_key),
            self.skip_duplicates,
            self.sort == Sort::Ascending,
            |_, _, oid| callback(&mut cursors_clone, IntKey::from_bytes(oid)),
        )
    }

    pub(crate) fn try_exclude(&mut self, include_lower: bool, include_upper: bool) -> bool {
        if !include_lower {
            let mut increased = false;
            for i in (Self::PREFIX_LEN..self.lower_key.len()).rev() {
                if let Some(added) = self.lower_key[i].checked_add(1) {
                    self.lower_key[i] = added;
                    increased = true;
                    break;
                }
            }
            if !increased {
                return false;
            }
        }
        if !include_upper {
            let mut decreased = false;
            for i in (Self::PREFIX_LEN..self.upper_key.len()).rev() {
                if let Some(subtracted) = self.upper_key[i].checked_sub(1) {
                    self.upper_key[i] = subtracted;
                    decreased = true;
                    break;
                }
            }
            if !decreased {
                return false;
            }
        }
        true
    }

    pub fn add_byte(&mut self, lower: u8, upper: u8) {
        self.lower_key
            .extend_from_slice(&Index::create_byte_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_byte_key(upper));
    }

    pub fn add_int(&mut self, lower: i32, upper: i32) {
        self.lower_key
            .extend_from_slice(&Index::create_int_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_int_key(upper));
    }

    pub fn add_float(&mut self, lower: f32, upper: f32) {
        self.lower_key
            .extend_from_slice(&Index::create_float_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_float_key(upper));
    }

    pub fn add_long(&mut self, lower: i64, upper: i64) {
        self.lower_key
            .extend_from_slice(&Index::create_long_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_long_key(upper));
    }

    pub fn add_double(&mut self, lower: f64, upper: f64) {
        self.lower_key
            .extend_from_slice(&Index::create_double_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_double_key(upper));
    }

    pub fn add_string(
        &mut self,
        lower: Option<&str>,
        lower_unbounded: bool,
        upper: Option<&str>,
        upper_unbounded: bool,
        case_sensitive: bool,
        index_type: IndexType,
    ) {
        let get_bytes = |value: Option<&str>| {
            let value = if case_sensitive {
                value.map(|s| s.to_string())
            } else {
                value.map(|s| s.to_lowercase())
            };
            match index_type {
                IndexType::Value => Index::create_string_value_key(value.as_deref()),
                IndexType::Hash => Index::create_string_hash_key(value.as_deref()),
                IndexType::Words => value.map_or(vec![], |s| s.as_bytes().to_vec()),
            }
        };

        if lower_unbounded {
            match index_type {
                IndexType::Value => self.lower_key.push(0),
                IndexType::Hash => self.lower_key.extend_from_slice(&0u64.to_le_bytes()),
                IndexType::Words => {}
            };
        } else {
            self.lower_key.extend_from_slice(&get_bytes(lower));
        }

        if upper_unbounded {
            self.upper_key.extend_from_slice(&u64::MAX.to_le_bytes());
        } else {
            self.upper_key.extend_from_slice(&get_bytes(upper));
        }
    }

    pub fn add_max_upper(&mut self) {
        self.upper_key.extend_from_slice(&u64::MAX.to_le_bytes());
    }
}

/*#[cfg(test)]
mod tests {
    //use super::*;
    //use itertools::Itertools;

    #[macro_export]
    macro_rules! exec_wc (
        ($txn:ident, $col:ident, $wc:ident, $res:ident) => {
            let mut cursor = $col.debug_get_index(0).debug_get_db().cursor(&$txn).unwrap();
            let $res = $wc.iter(&mut cursor)
                .unwrap()
                .map(Result::unwrap)
                .map(|(_, v)| v)
                .collect_vec();
        };
    );

    /*fn get_str_obj(col: &IsarCollection, str: &str) -> Vec<u8> {
        let mut ob = col.new_object_builder();
        ob.write_string(Some(str));
        ob.finish()
    }*/

    #[test]
    fn test_iter() {
        /*isar!(isar, col => col!(field => String; ind!(field)));

        let txn = isar.begin_txn(true, false).unwrap();
        let oid1 = col.put(&txn, None, &get_str_obj(&col, "aaaa")).unwrap();
        let oid2 = col.put(&txn, None, &get_str_obj(&col, "aabb")).unwrap();
        let oid3 = col.put(&txn, None, &get_str_obj(&col, "bbaa")).unwrap();
        let oid4 = col.put(&txn, None, &get_str_obj(&col, "bbbb")).unwrap();

        let all_oids = &[
            oid1.as_ref(),
            oid2.as_ref(),
            oid3.as_ref(),
            oid4.as_ref(),
        ];

        let mut wc = col.new_where_clause(Some(0)).unwrap();
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, all_oids);

        wc.add_lower_string_value(Some("aa"), true);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, all_oids);

        let mut wc = col.new_where_clause(Some(0)).unwrap();
        wc.add_lower_string_value(Some("aa"), false);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, &[oid3.as_ref(), oid4.as_ref()]);

        wc.add_upper_string_value(Some("bba"), true);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, &[oid3.as_ref()]);

        let mut wc = col.new_where_clause(Some(0)).unwrap();
        wc.add_lower_string_value(Some("x"), false);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, &[] as &[&[u8]]);*/
    }

    #[test]
    fn test_add_upper_oid() {}
}
*/
