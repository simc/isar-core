use crossbeam_channel::unbounded;

use crate::common::test_obj::TestObj;

mod common;

#[test]
fn test_delete() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    // put new object with id 1
    let obj1 = TestObj::default(1);
    obj1.save(col, &mut txn);
    TestObj::verify(col, &mut txn, &[&obj1]);

    // put object with id 2
    let obj2 = TestObj::default(2);
    obj2.save(col, &mut txn);
    TestObj::verify(col, &mut txn, &[&obj1, &obj2]);

    // delete object with id 1
    col.delete(&mut txn, 1).unwrap();
    TestObj::verify(col, &mut txn, &[&obj2]);

    // delete object with id 2
    col.delete(&mut txn, 2).unwrap();
    TestObj::verify(col, &mut txn, &[]);

    txn.abort();
    isar.close();
}

#[test]
fn test_delete_calls_notifiers() {
    isar!(isar, col, TestObj::default_schema());

    // create a new objects with id 1 and id 2
    txn!(isar, txn);
    let obj1 = TestObj::default(1);
    obj1.save(col, &mut txn);
    let obj2 = TestObj::default(2);
    obj2.save(col, &mut txn);
    txn.commit().unwrap();

    // watch the collection
    let (tx, rx) = unbounded();
    let handle = isar.watch_collection(col, Box::new(move || tx.send(true).unwrap()));

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
