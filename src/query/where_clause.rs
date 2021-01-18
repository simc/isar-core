use crate::error::Result;
use crate::index::Index;
use crate::lmdb::check_below_upper_key;
use crate::lmdb::cursor::Cursor;
use crate::object::object_id::ObjectId;
use std::convert::TryInto;

#[derive(Clone)]
pub struct WhereClause {
    lower_key: Vec<u8>,
    upper_key: Vec<u8>,
    index: Option<Index>,
}

impl WhereClause {
    const PREFIX_LEN: usize = 2;

    pub(crate) fn new_primary(collection_id: u16) -> Self {
        WhereClause {
            lower_key: collection_id.to_le_bytes().to_vec(),
            upper_key: collection_id.to_le_bytes().to_vec(),
            index: None,
        }
    }

    pub(crate) fn new_secondary(index: Index) -> Self {
        WhereClause {
            lower_key: index.get_id().to_le_bytes().to_vec(),
            upper_key: index.get_id().to_le_bytes().to_vec(),
            index: Some(index),
        }
    }

    pub(crate) fn new_empty() -> Self {
        WhereClause {
            lower_key: vec![1],
            upper_key: vec![0],
            index: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        !check_below_upper_key(&self.lower_key, &self.upper_key)
    }

    pub fn is_primary(&self) -> bool {
        self.index.is_none()
    }

    pub fn is_unique(&self) -> bool {
        self.index.as_ref().map_or(true, |i| i.is_unique())
    }

    pub fn get_prefix(&self) -> u16 {
        if self.lower_key.len() < 2 {
            0 // empty
        } else {
            u16::from_le_bytes(self.lower_key[0..2].try_into().unwrap())
        }
    }

    pub(crate) fn object_matches(&self, oid: &[u8], object: &[u8]) -> bool {
        if let Some(index) = &self.index {
            let index_key = index.create_key(object);
            self.lower_key <= index_key && check_below_upper_key(&index_key, &self.upper_key)
        } else {
            &self.lower_key[..] <= oid && check_below_upper_key(oid, &self.upper_key)
        }
    }

    pub(crate) fn iter<'txn, F>(&self, cursor: &mut Cursor<'txn>, callback: F) -> Result<bool>
    where
        F: FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        cursor.iter_between(&self.lower_key, &self.upper_key, callback)
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

    /*pub(super) fn merge(&self, other: &WhereClause) -> Option<WhereClause> {
        unimplemented!()
    }*/

    pub fn add_oid(&mut self, oid: ObjectId) {
        let bytes = oid.as_bytes_without_prefix();
        self.lower_key.extend_from_slice(bytes);
        self.upper_key.extend_from_slice(bytes);
    }

    pub fn add_oid_time(&mut self, lower: u32, upper: u32) {
        self.lower_key.extend_from_slice(&lower.to_be_bytes());
        self.upper_key.extend_from_slice(&upper.to_be_bytes());
    }

    pub fn add_byte(&mut self, lower: u8, upper: u8) {
        self.lower_key
            .extend_from_slice(&Index::get_byte_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_byte_key(upper));
    }

    pub fn add_int(&mut self, lower: i32, upper: i32) {
        self.lower_key.extend_from_slice(&Index::get_int_key(lower));
        self.upper_key.extend_from_slice(&Index::get_int_key(upper));
    }

    pub fn add_float(&mut self, lower: f32, upper: f32) {
        self.lower_key
            .extend_from_slice(&Index::get_float_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_float_key(upper));
    }

    pub fn add_long(&mut self, lower: i64, upper: i64) {
        self.lower_key
            .extend_from_slice(&Index::get_long_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_long_key(upper));
    }

    pub fn add_double(&mut self, lower: f64, upper: f64) {
        self.lower_key
            .extend_from_slice(&Index::get_double_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_double_key(upper));
    }

    pub fn add_string_hash(&mut self, value: Option<&str>) {
        let hash = Index::get_string_hash_key(value);
        self.lower_key.extend_from_slice(&hash);
        self.upper_key.extend_from_slice(&hash);
    }

    pub fn add_string_value(&mut self, lower: Option<&str>, upper: Option<&str>) {
        self.lower_key
            .extend_from_slice(&Index::get_string_value_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_string_value_key(upper));
    }
}

#[cfg(test)]
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

        let txn = isar.begin_txn(true).unwrap();
        let oid1 = col.put(&txn, None, &get_str_obj(&col, "aaaa")).unwrap();
        let oid2 = col.put(&txn, None, &get_str_obj(&col, "aabb")).unwrap();
        let oid3 = col.put(&txn, None, &get_str_obj(&col, "bbaa")).unwrap();
        let oid4 = col.put(&txn, None, &get_str_obj(&col, "bbbb")).unwrap();

        let all_oids = &[
            oid1.as_bytes(),
            oid2.as_bytes(),
            oid3.as_bytes(),
            oid4.as_bytes(),
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
        assert_eq!(&oids, &[oid3.as_bytes(), oid4.as_bytes()]);

        wc.add_upper_string_value(Some("bba"), true);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, &[oid3.as_bytes()]);

        let mut wc = col.new_where_clause(Some(0)).unwrap();
        wc.add_lower_string_value(Some("x"), false);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, &[] as &[&[u8]]);*/
    }

    #[test]
    fn test_add_upper_oid() {}
}
