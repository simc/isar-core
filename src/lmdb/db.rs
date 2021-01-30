use crate::error::Result;
use crate::lmdb::cursor::Cursor;
use crate::lmdb::error::lmdb_result;
use crate::lmdb::txn::Txn;
use lmdb_sys as ffi;
use std::ffi::CString;

#[derive(Copy, Clone)]
pub struct Db {
    pub dbi: ffi::MDB_dbi,
    pub dup: bool,
}

impl Db {
    pub fn open(txn: &Txn, name: &str, dup: bool, dup_fixed_vals: bool) -> Result<Self> {
        let name = CString::new(name.as_bytes()).unwrap();
        let mut flags = ffi::MDB_CREATE;
        if dup {
            flags |= ffi::MDB_DUPSORT;
            if dup_fixed_vals {
                flags |= ffi::MDB_DUPFIXED;
            }
        }

        let mut dbi: ffi::MDB_dbi = 0;
        unsafe {
            lmdb_result(ffi::mdb_dbi_open(txn.txn, name.as_ptr(), flags, &mut dbi))?;
        }
        Ok(Self { dbi, dup })
    }

    pub fn cursor<'txn>(&self, txn: &'txn Txn) -> Result<Cursor<'txn>> {
        Cursor::open(txn, &self)
    }

    #[cfg(test)]
    pub fn debug_new(dup: bool) -> Db {
        Db { dbi: 0, dup }
    }
}

#[cfg(test)]
mod tests {
    use crate::lmdb::env::tests::get_env;

    use super::*;

    #[test]
    fn test_open() {
        let env = get_env();

        let read_txn = env.txn(false).unwrap();
        assert!(Db::open(&read_txn, "test", false, false).is_err());
        read_txn.abort();

        let flags = vec![
            (false, false, 0),
            (false, true, 0),
            (true, false, ffi::MDB_DUPSORT),
            (true, true, ffi::MDB_DUPSORT | ffi::MDB_DUPFIXED),
        ];

        for (i, (dup, fixed_vals, flags)) in flags.iter().enumerate() {
            let txn = env.txn(true).unwrap();
            let db = Db::open(&txn, format!("test{}", i).as_str(), *dup, *fixed_vals).unwrap();
            txn.commit().unwrap();

            let mut actual_flags: u32 = 0;
            let txn = env.txn(false).unwrap();
            unsafe {
                ffi::mdb_dbi_flags(txn.txn, db.dbi, &mut actual_flags);
            }
            txn.abort();
            assert_eq!(*flags, actual_flags);
        }
    }
}
