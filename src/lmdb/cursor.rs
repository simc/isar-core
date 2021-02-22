use crate::error::Result;
use crate::lmdb::db::Db;
use crate::lmdb::error::{lmdb_result, LmdbError};
use crate::lmdb::txn::Txn;
use crate::lmdb::{from_mdb_val, to_mdb_val, Key, KeyVal, EMPTY_KEY, EMPTY_VAL};
use core::ptr;
use lmdb_sys as ffi;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct Cursor<'txn> {
    cursor: *mut ffi::MDB_cursor,
    write: bool,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> Cursor<'txn> {
    pub(crate) fn open(txn: &'txn Txn, db: &Db) -> Result<Cursor<'txn>> {
        let mut cursor: *mut ffi::MDB_cursor = ptr::null_mut();

        unsafe { lmdb_result(ffi::mdb_cursor_open(txn.txn, db.dbi, &mut cursor))? }

        Ok(Cursor {
            cursor,
            write: txn.write,
            _marker: PhantomData,
        })
    }

    #[allow(clippy::try_err)]
    fn op_get(
        &self,
        op: u32,
        key: Option<Key>,
        val: Option<&[u8]>,
    ) -> Result<Option<KeyVal<'txn>>> {
        let mut key = key.map_or(EMPTY_KEY, |key| unsafe { to_mdb_val(key.0) });
        let mut data = val.map_or(EMPTY_VAL, |val| unsafe { to_mdb_val(val) });

        let result =
            unsafe { lmdb_result(ffi::mdb_cursor_get(self.cursor, &mut key, &mut data, op)) };

        match result {
            Ok(()) => {
                let key = unsafe { from_mdb_val(key) };
                let data = unsafe { from_mdb_val(data) };
                Ok(Some((Key(key), data)))
            }
            Err(LmdbError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e)?,
        }
    }

    pub fn move_to(&mut self, key: Key) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_SET_KEY, Some(key), None)
    }

    pub fn move_to_key_val(&mut self, key: Key, val: &[u8]) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_GET_BOTH, Some(key), Some(val))
    }

    pub fn move_to_gte(&mut self, key: Key) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_SET_RANGE, Some(key), None)
    }

    pub fn move_to_dup(&mut self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_NEXT_DUP, None, None)
    }

    pub fn move_to_prev(&mut self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_PREV, None, None)
    }

    pub fn move_to_prev_key(&mut self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_PREV_NODUP, None, None)
    }

    pub fn move_to_last(&mut self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_LAST, None, None)
    }

    pub fn put(&self, key: Key, data: &[u8]) -> Result<()> {
        self.put_internal(key, data, 0)?;
        Ok(())
    }

    #[allow(clippy::try_err)]
    pub fn put_no_override(&self, key: Key, data: &[u8]) -> Result<bool> {
        let result = self.put_internal(key, data, ffi::MDB_NOOVERWRITE);
        match result {
            Ok(()) => Ok(true),
            Err(LmdbError::KeyExist {}) => Ok(false),
            Err(e) => Err(e)?,
        }
    }

    fn put_internal(
        &self,
        key: Key,
        data: &[u8],
        flags: u32,
    ) -> std::result::Result<(), LmdbError> {
        assert!(self.write);
        unsafe {
            let mut key = to_mdb_val(key.0);
            let mut data = to_mdb_val(data);
            lmdb_result(ffi::mdb_cursor_put(self.cursor, &mut key, &mut data, flags))?;
        }
        Ok(())
    }

    /// Requires the cursor to have a valid position
    pub fn delete_current(&mut self) -> Result<()> {
        assert!(self.write);
        unsafe { lmdb_result(ffi::mdb_cursor_del(self.cursor, 0))? };

        Ok(())
    }

    #[inline(never)]
    fn iter_between_first(
        &mut self,
        lower_key: Key,
        upper_key: Key,
        ascending: bool,
    ) -> Result<Option<KeyVal<'txn>>> {
        if upper_key < lower_key {
            return Ok(None);
        }

        let first_entry = if !ascending {
            if let Some(first_entry) = self.move_to_gte(upper_key)? {
                Some(first_entry)
            } else {
                // If some key between upper_key and lower_key happens to be the last key in the db
                self.move_to_last()?
            }
        } else {
            self.move_to_gte(lower_key)?
        };

        if let Some((key, _)) = first_entry {
            if key > upper_key {
                if !ascending {
                    if let Some((prev_key, prev_val)) = self.move_to_prev()? {
                        if prev_key >= lower_key {
                            return Ok(Some((prev_key, prev_val)));
                        }
                    }
                }
                Ok(None)
            } else {
                Ok(first_entry)
            }
        } else {
            Ok(None)
        }
    }

    pub fn iter_between(
        &mut self,
        lower_key: Key,
        upper_key: Key,
        skip_duplicates: bool,
        ascending: bool,
        mut callback: impl FnMut(&mut Cursor<'txn>, Key<'txn>, &'txn [u8]) -> Result<bool>,
    ) -> Result<bool> {
        if upper_key < lower_key {
            return Ok(true);
        }

        if let Some((key, val)) = self.iter_between_first(lower_key, upper_key, ascending)? {
            if !callback(self, key, val)? {
                return Ok(false);
            }
        } else {
            return Ok(true);
        }

        let next = match (ascending, skip_duplicates) {
            (true, true) => ffi::MDB_NEXT_NODUP,
            (true, false) => ffi::MDB_NEXT,
            (false, true) => ffi::MDB_PREV_NODUP,
            (false, false) => ffi::MDB_PREV,
        };
        loop {
            if let Some((key, val)) = self.op_get(next, None, None)? {
                if (ascending && key > upper_key) || (!ascending && key < lower_key) {
                    return Ok(true);
                } else if !callback(self, key, val)? {
                    return Ok(false);
                }
            } else {
                return Ok(true);
            }
        }
    }

    pub fn iter_dups(
        &mut self,
        key: Key,
        mut callback: impl FnMut(&mut Cursor<'txn>, Key<'txn>, &'txn [u8]) -> Result<bool>,
    ) -> Result<bool> {
        if let Some((key, val)) = self.move_to(key)? {
            if !callback(self, key, val)? {
                return Ok(true);
            }
        } else {
            return Ok(true);
        }
        loop {
            if let Some((key, val)) = self.move_to_dup()? {
                if !callback(self, key, val)? {
                    return Ok(false);
                }
            } else {
                return Ok(true);
            }
        }
    }
}

impl<'txn> Drop for Cursor<'txn> {
    fn drop(&mut self) {
        if !self.write {
            unsafe { ffi::mdb_cursor_close(self.cursor) }
        }
    }
}

#[cfg(test)]
mod tests {
    /*use crate::lmdb::db::Db;
    use crate::lmdb::env::tests::get_env;
    use crate::lmdb::env::Env;
    use itertools::Itertools;
    use std::sync::{Arc, Mutex};

    fn get_filled_db() -> (Env, Db) {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", false, false).unwrap();
        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key2", b"val2").unwrap();
        db.put(&txn, b"key3", b"val3").unwrap();
        db.put(&txn, b"key4", b"val4").unwrap();
        txn.commit().unwrap();
        (env, db)
    }

    fn get_filled_db_dup() -> (Env, Db) {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", true, false).unwrap();
        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key1", b"val1b").unwrap();
        db.put(&txn, b"key1", b"val1c").unwrap();
        db.put(&txn, b"key2", b"val2").unwrap();
        db.put(&txn, b"key2", b"val2b").unwrap();
        db.put(&txn, b"key2", b"val2c").unwrap();
        txn.commit().unwrap();
        (env, db)
    }

    fn get_empty_db() -> (Env, Db) {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", true, false).unwrap();
        txn.commit().unwrap();
        (env, db)
    }

    #[test]
    fn test_get() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        let entry = cur.get().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        cur.move_to_next().unwrap();
        let entry = cur.get().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_get_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        let entry = cur.get().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        cur.move_to_next().unwrap();
        let entry = cur.get().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1b"[..])));
    }

    #[test]
    fn test_move_to_first() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let first = cur.move_to_first().unwrap();
        assert_eq!(first, Some((&b"key1"[..], &b"val1"[..])));

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_move_to_first_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let first = cur.move_to_first().unwrap();
        assert_eq!(first, None);

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, None);
    }

    #[test]
    fn test_move_to_last() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let last = cur.move_to_last().unwrap();
        assert_eq!(last, Some((&b"key4"[..], &b"val4"[..])));

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, None);
    }

    #[test]
    fn test_move_to_last_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let last = cur.move_to_last().unwrap();
        assert_eq!(last, Some((&b"key2"[..], &b"val2c"[..])));
    }

    #[test]
    fn test_move_to_last_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_last().unwrap();
        assert!(entry.is_none());

        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn test_move_to() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to(b"key2").unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));

        let entry = cur.move_to(b"key1").unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, Some((&b"key2"[..], &b"val2"[..])));

        let entry = cur.move_to(b"key5").unwrap();
        assert_eq!(entry, None);
    }

    #[test]
    fn test_move_to_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to(b"key1").unwrap();
        assert!(entry.is_none());
        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn test_move_to_gte() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_gte(b"key2").unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));

        let entry = cur.move_to_gte(b"k").unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn move_to_gte_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_gte(b"key1").unwrap();
        assert!(entry.is_none());

        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn test_move_to_next() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        let entry = cur.move_to_next().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_move_to_next_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        let entry = cur.move_to_next().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1b"[..])));

        let entry = cur.move_to_next().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1c"[..])));

        let entry = cur.move_to_next().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_move_to_next_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());

        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn test_delete_current() {
        let (env, db) = get_filled_db();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        cur.delete_current(false).unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_delete_current_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        cur.delete_current(false).unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1b"[..])));

        cur.delete_current(true).unwrap();
        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_delete_while() {
        let (env, db) = get_filled_db();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entries = Arc::new(Mutex::new(vec![(b"key1", b"val1"), (b"key2", b"val2")]));

        cur.move_to_first().unwrap();
        cur.delete_while(
            |k, v| {
                let mut entries = entries.lock().unwrap();
                if entries.is_empty() {
                    return false;
                }
                let (rk, rv) = entries.remove(0);
                assert_eq!((&rk[..], &rv[..]), (k, v));
                true
            },
            false,
        )
        .unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key3"[..], &b"val3"[..])));
    }

    #[test]
    fn test_delete_while_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        cur.delete_current(false).unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1b"[..])));

        cur.delete_current(true).unwrap();
        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_iter() {
        let (env, db) = get_filled_db();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        cur.move_to_next().unwrap();
        let keys = cur
            .iter()
            .map(|r| {
                let (k, _) = r.unwrap();
                k
            })
            .collect_vec();
        assert_eq!(vec![b"key2", b"key3", b"key4"], keys);
    }

    #[test]
    fn test_get_put_delete() {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", false, false).unwrap();
        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key2", b"val2").unwrap();
        db.put(&txn, b"key3", b"val3").unwrap();
        db.put(&txn, b"key2", b"val4").unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        assert_eq!(b"val1", db.get(&txn, b"key1").unwrap().unwrap());
        assert_eq!(b"val4", db.get(&txn, b"key2").unwrap().unwrap());
        assert_eq!(b"val3", db.get(&txn, b"key3").unwrap().unwrap());
        assert_eq!(db.get(&txn, b"key").unwrap(), None);

        db.delete(&txn, b"key1", None).unwrap();
        assert_eq!(db.get(&txn, b"key1").unwrap(), None);
        txn.abort();
    }

    #[test]
    fn test_put_get_del_multi() {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", true, false).unwrap();

        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key1", b"val2").unwrap();
        db.put(&txn, b"key1", b"val3").unwrap();
        db.put(&txn, b"key2", b"val4").unwrap();
        db.put(&txn, b"key2", b"val5").unwrap();
        db.put(&txn, b"key2", b"val6").unwrap();
        db.put(&txn, b"key3", b"val7").unwrap();
        db.put(&txn, b"key3", b"val8").unwrap();
        db.put(&txn, b"key3", b"val9").unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        {
            //let mut cur = db.cursor(&txn).unwrap();
            //assert_eq!(cur.set(b"key2").unwrap(), true);
            //let iter = cur.iter_dup();
            //let vals = iter.map(|x| x.1).collect_vec();
            //assert!(iter.error.is_none());
            //assert_eq!(vals, vec![b"val4", b"val5", b"val6"]);
        }
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        db.delete(&txn, b"key1", Some(b"val2")).unwrap();
        db.delete(&txn, b"key2", None).unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        {
            let mut cur = db.cursor(&txn).unwrap();
            cur.move_to_first().unwrap();
            let iter = cur.iter();
            let vals: Result<Vec<&[u8]>> = iter.map_ok(|x| x.1).collect();
            assert_eq!(
                vals.unwrap(),
                vec![b"val1", b"val3", b"val7", b"val8", b"val9"]
            );
        }
        txn.commit().unwrap();
    }

    #[test]
    fn test_put_no_override() {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", false, false).unwrap();
        db.put(&txn, b"key", b"val").unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        assert_eq!(db.put_no_override(&txn, b"key", b"err").unwrap(), false);
        assert_eq!(db.put_no_override(&txn, b"key2", b"val2").unwrap(), true);
        assert_eq!(db.get(&txn, b"key").unwrap(), Some(&b"val"[..]));
        assert_eq!(db.get(&txn, b"key2").unwrap(), Some(&b"val2"[..]));
        txn.abort();
    }

    #[test]
    fn test_put_no_dup_data() {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", true, false).unwrap();
        db.put(&txn, b"key", b"val").unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        assert_eq!(db.put_no_dup_data(&txn, b"key", b"val").unwrap(), false);
        assert_eq!(db.put_no_dup_data(&txn, b"key2", b"val2").unwrap(), true);
        assert_eq!(db.get(&txn, b"key2").unwrap(), Some(&b"val2"[..]));
        txn.abort();
    }

    #[test]
    fn test_clear_db() {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", false, false).unwrap();
        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key2", b"val2").unwrap();
        db.put(&txn, b"key3", b"val3").unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        db.clear(&txn).unwrap();
        txn.commit().unwrap();

        let txn = env.txn(false).unwrap();
        {
            let mut cursor = db.cursor(&txn).unwrap();
            assert!(cursor.move_to_first().unwrap().is_none());
        }
        txn.abort();
    }*/
}
