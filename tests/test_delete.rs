use crossbeam_channel::unbounded;
use isar_core::schema::link_schema::LinkSchema;

use crate::common::test_obj::TestObj;

mod common;

#[test]
fn test_delete() {
    isar!(isar, col => TestObj::default_schema());
    txn!(isar, txn);

    // put new object with id 1 and 2
    put!(col, txn, id, obj1 => 1, obj2 => 2);
    verify!(txn, col, obj1, obj2);

    // delete object with id 1
    col.delete(&mut txn, 1).unwrap();
    verify!(txn, col, obj2);

    // delete object with id 2
    col.delete(&mut txn, 2).unwrap();
    verify!(txn, col);

    txn.abort();
    isar.close();
}

#[test]
fn test_delete_clears_links() {
    let link_schema = LinkSchema::new("link", "obj");
    let schema = TestObj::schema("obj", &[], &[link_schema]);
    isar!(isar, col => schema);
    txn!(isar, txn);

    // put new objects
    put!(id: col, txn, obj1 => 1, obj2 => 2, obj3 => 3);
    col.link(&mut txn, schema.debug_link_id(0), 1, 2).unwrap();
    col.link(&mut txn, schema.debug_link_id(0), 2, 3).unwrap();
    col.link(&mut txn, schema.debug_link_id(0), 3, 1).unwrap();
    verify!(txn, col, obj1, obj2, obj3; "link", 1 => 2, 2 => 3, 3 => 1);

    // delete obj 1
    col.delete(&mut txn, 1).unwrap();
    verify!(txn, col, obj2, obj3; "link", 2 => 3);

    // delete obj 3
    col.delete(&mut txn, 3).unwrap();
    verify!(txn, col, obj2);

    txn.abort();
    isar.close();
}

#[test]
fn test_delete_calls_notifiers() {
    isar!(isar, col => TestObj::default_schema());

    // create a new objects with id 1 and id 2
    txn!(isar, txn);
    put!(col, txn, id, obj1 => 1, obj2 => 2, obj3 => 3);
    txn.commit().unwrap();

    // watch object 1
    let (tx, rx) = unbounded();
    let handle = isar.watch_object(col, 1, Box::new(move || tx.send(true).unwrap()));

    // delete object with id 2
    let mut txn = isar.begin_txn(true, false).unwrap();
    col.delete(&mut txn, 2).unwrap();
    txn.commit().unwrap();

    // assert that no rx is not notified
    assert_eq!(rx.len(), 0);

    // delete object with id 1
    let mut txn = isar.begin_txn(true, false).unwrap();
    col.delete(&mut txn, 1).unwrap();
    txn.commit().unwrap();

    // assert that the rx channel has received true
    assert_eq!(rx.len(), 1);
    assert!(rx.try_recv().unwrap());

    // clear the collection
    let mut txn = isar.begin_txn(true, false).unwrap();
    col.clear(&mut txn).unwrap();
    txn.commit().unwrap();

    // assert that the rx channel has received true
    assert_eq!(rx.len(), 1);
    assert!(rx.try_recv().unwrap());

    handle.stop();
    isar.close();
}
