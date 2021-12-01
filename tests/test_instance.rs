/*use crate::object::isar_object::IsarObject;
use crate::{col, isar};
use tempfile::tempdir;

#[test]
fn test_open_new_instance() {
    isar!(isar, col => col!());

    let mut ob = col.new_object_builder(None);
    ob.write_long(123);
    let o = ob.finish();

    let mut txn = isar.begin_txn(true, false).unwrap();
    col.put(&mut txn, o).unwrap();
    txn.commit().unwrap();
    let mut txn = isar.begin_txn(false, false).unwrap();
    assert_eq!(col.get(&mut txn, 123).unwrap().unwrap(), o);
    txn.abort();
    isar.close();
}

#[test]
fn test_open_instance_added_collection() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    isar!(path: path, isar, col1 => col!("col1",));

    let mut ob = col1.new_object_builder(None);
    ob.write_long(123);
    let object = ob.finish();
    let object_bytes = object.as_bytes().to_vec();

    let mut txn = isar.begin_txn(true, false).unwrap();
    col1.put(&mut txn, object).unwrap();
    txn.commit().unwrap();

    assert!(isar.close());

    isar!(path: path, isar2, col1 => col!("col1"), col2 => col!("col2"));
    let mut txn = isar2.begin_txn(false, false).unwrap();
    let object = IsarObject::from_bytes(&object_bytes);
    assert_eq!(col1.get(&mut txn, 123).unwrap(), Some(object));
    assert_eq!(col2.new_query_builder().build().count(&mut txn).unwrap(), 0);
    txn.abort();
    isar2.close();
}

#[test]
fn test_open_instance_removed_collection() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    isar!(path: path, isar, col1 => col!("col1"), _col2 => col!("col2"));
    let mut ob = col1.new_object_builder(None);
    ob.write_long(123);
    let o = ob.finish();
    let mut txn = isar.begin_txn(true, false).unwrap();
    //col1.put(&txn, None, o.as_ref()).unwrap();
    col1.put(&mut txn, o).unwrap();
    txn.commit().unwrap();
    isar.close();

    isar!(path: path, isar, _col2 => col!("col2"));
    isar.close();

    isar!(path: path, isar, col1 => col!("col1"), _col2 => col!("col2"));
    let mut txn = isar.begin_txn(false, false).unwrap();
    assert_eq!(col1.new_query_builder().build().count(&mut txn).unwrap(), 0);
    txn.abort();
    isar.close();
}
*/
