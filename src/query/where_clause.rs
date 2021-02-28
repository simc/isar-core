use crate::collection::IsarCollection;
use crate::error::Result;
use crate::index::Index;
use crate::lmdb::Key;
use crate::object::isar_object::IsarObject;
use crate::query::Sort;
use crate::schema::collection_schema::IndexType;
use crate::txn::Cursors;
use crate::utils::{oid_from_bytes, oid_to_bytes};
use std::mem::ManuallyDrop;

#[derive(Clone)]
pub struct WhereClause {
    lower_key: Vec<u8>,
    upper_key: Vec<u8>,
    index: Option<Index>,
    skip_duplicates: bool,
    sort: Sort,
}

impl WhereClause {
    const PREFIX_LEN: usize = 2;

    pub(crate) fn new_primary(
        prefix: u16,
        lower_oid: i64,
        upper_oid: i64,
        sort: Sort,
    ) -> Result<Self> {
        Ok(WhereClause {
            lower_key: oid_to_bytes(lower_oid, prefix)?.to_vec(),
            upper_key: oid_to_bytes(upper_oid, prefix)?.to_vec(),
            index: None,
            skip_duplicates: false,
            sort,
        })
    }

    pub(crate) fn new_secondary(
        prefix: &[u8],
        index: Index,
        mut skip_duplicates: bool,
        sort: Sort,
    ) -> Self {
        if index.is_unique() {
            skip_duplicates = false;
        }
        WhereClause {
            lower_key: prefix.to_vec(),
            upper_key: prefix.to_vec(),
            index: Some(index),
            skip_duplicates,
            sort,
        }
    }

    pub(crate) fn new_empty() -> Self {
        WhereClause {
            lower_key: vec![1],
            upper_key: vec![0],
            index: None,
            skip_duplicates: false,
            sort: Sort::Ascending,
        }
    }

    pub fn is_empty(&self) -> bool {
        Key(&self.lower_key) > Key(&self.upper_key)
    }

    pub fn is_primary(&self) -> bool {
        self.index.is_none()
    }

    pub fn is_unique(&self) -> bool {
        self.index.as_ref().map_or(true, |i| i.is_unique())
    }

    pub fn is_from_collection(&self, collection: &IsarCollection) -> bool {
        if let Some(index) = &self.index {
            collection.get_indexes().contains(index)
        } else {
            collection.get_id() == oid_from_bytes(&self.lower_key).1
        }
    }

    pub(crate) fn object_matches(&self, oid: i64, object: IsarObject) -> bool {
        if let Some(index) = &self.index {
            let mut key_matches = false;
            index
                .create_keys(object, |key| {
                    key_matches =
                        Key(key) >= Key(&self.lower_key) && Key(key) <= Key(&self.upper_key);
                    Ok(!key_matches)
                })
                .unwrap();
            key_matches
        } else {
            let (lower_oid, _) = oid_from_bytes(&self.lower_key);
            let (upper_oid, _) = oid_from_bytes(&self.upper_key);
            oid >= lower_oid && oid <= upper_oid
        }
    }

    pub(crate) fn iter<'txn, F>(&self, cursors: &mut Cursors<'txn>, mut callback: F) -> Result<bool>
    where
        F: FnMut(&mut Cursors<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        let mut cursors_clone = ManuallyDrop::new(cursors.clone());
        let primary = &mut cursors.primary;
        let secondary = &mut cursors.secondary;
        let secondary_dup = &mut cursors.secondary_dup;

        let cursor = if self.is_primary() {
            primary
        } else if self.is_unique() {
            secondary
        } else {
            secondary_dup
        };

        cursor.iter_between(
            Key(&self.lower_key),
            Key(&self.upper_key),
            self.skip_duplicates,
            self.sort == Sort::Ascending,
            |_, oid, object| callback(&mut cursors_clone, oid.0, object),
        )
    }

    pub(crate) fn try_exclude(&mut self, include_lower: bool, include_upper: bool) -> bool {
        assert!(self.index.is_some());

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
        assert!(self.index.is_some());
        self.lower_key
            .extend_from_slice(&Index::create_byte_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_byte_key(upper));
    }

    pub fn add_int(&mut self, lower: i32, upper: i32) {
        assert!(self.index.is_some());
        self.lower_key
            .extend_from_slice(&Index::create_int_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_int_key(upper));
    }

    pub fn add_float(&mut self, lower: f32, upper: f32) {
        assert!(self.index.is_some());
        self.lower_key
            .extend_from_slice(&Index::create_float_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_float_key(upper));
    }

    pub fn add_long(&mut self, lower: i64, upper: i64) {
        assert!(self.index.is_some());
        self.lower_key
            .extend_from_slice(&Index::create_long_key(lower));
        self.upper_key
            .extend_from_slice(&Index::create_long_key(upper));
    }

    pub fn add_double(&mut self, lower: f64, upper: f64) {
        assert!(self.index.is_some());
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
        assert!(self.index.is_some());
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
