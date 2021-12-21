use isar_core::instance::IsarInstance;

use crate::common::test_obj::TestObj;

mod common;

#[test]
fn test_id_only() {
    isar!(isar, col =>TestObj::default_schema());

    txn!(isar, txn);
    put!(col, txn, id, obj1 => 1);
    txn.commit().unwrap();

    txn!(isar, txn);
    verify!(txn, col, obj1);
    txn.commit().unwrap();

    txn!(isar, txn);
    put!(col, txn, id, obj2 => 2, obj3 => 3, obj4 => 4);
    txn.commit().unwrap();

    txn!(isar, txn);
    verify!(txn, col, obj1, obj2, obj3, obj4);
    txn.commit().unwrap();

    isar.close();
}

#[test]
fn test_put_new() {
    isar!(isar, col =>TestObj::default_schema());
    txn!(isar, txn);

    assert_eq!(col.auto_increment(&mut txn).unwrap(), i64::MIN + 1);

    // put new object with id 1
    let obj1 = TestObj::default(1);
    obj1.save(&mut txn, col);
    assert_eq!(col.auto_increment(&mut txn).unwrap(), 2);

    // put new object with id 2
    let obj2 = TestObj::default(3);
    obj2.save(&mut txn, col);
    assert_eq!(col.auto_increment(&mut txn).unwrap(), 4);

    // both objects should be in the database
    verify!(txn, col, obj1, obj2);

    txn.abort();
    isar.close();
}

#[test]
fn test_put_existing() {
    isar!(isar, col =>TestObj::default_schema());
    txn!(isar, txn);

    // put new object with id 1
    let mut obj1 = TestObj::default(1);
    obj1.int = 1;
    obj1.save(&mut txn, col);
    assert_eq!(col.auto_increment(&mut txn).unwrap(), 2);
    verify!(txn, col, obj1);

    // overwrite object with id 1
    let mut obj2 = TestObj::default(1);
    obj2.int = 2;
    obj2.save(&mut txn, col);
    assert_eq!(col.auto_increment(&mut txn).unwrap(), 3);
    verify!(txn, col, obj2);

    // put new object with id 333
    let mut obj3 = TestObj::default(333);
    obj3.int = 3;
    obj3.save(&mut txn, col);
    assert_eq!(col.auto_increment(&mut txn).unwrap(), 334);
    verify!(txn, col, obj2, obj3);

    txn.abort();
    isar.close();
}

#[test]
fn test_many() {
    isar!(isar, col =>TestObj::default_schema());
    txn!(isar, txn);

    let str_val = "some random string to store in the database";
    for i in 0..1000i32 {
        let mut obj = TestObj::default(i as i64);
        obj.int = i * i;
        obj.string = Some(str_val.to_string());
        obj.save(&mut txn, col);
    }
    txn.commit().unwrap();

    txn!(isar, txn);
    let count = col.new_query_builder().build().count(&mut txn).unwrap();
    assert_eq!(count, 1000);

    let obj = TestObj::get(col, &mut txn, 100).unwrap();
    assert_eq!(obj.id, 100);
    assert_eq!(obj.int, 10000);
    assert_eq!(obj.string, Some(str_val.to_string()));

    txn.abort();
    isar.close();
}

/*#[test]
fn test_put_calls_notifiers() {
    isar!(isar, col =>TestObj::default_schema());

    let mut txn = isar.begin_txn(true, false).unwrap();

    // create a query that retuern all objects with id 1
    let mut qb1 = col.new_query_builder();
    qb1.set_filter(Filter::long(TestObj::ID_PROP, 1, 1).unwrap());
    let q1 = qb1.build();

    // create a query that retuern all objects with id 2
    let mut qb2 = col.new_query_builder();
    qb2.set_filter(Filter::long(TestObj::ID_PROP, 2, 2).unwrap());
    let q2 = qb2.build();

    // watch query 1 and send true to the rx1 channel
    let (tx1, rx1) = unbounded();
    let handle1 = isar.watch_query(col, q1, Box::new(move || tx1.send(true).unwrap()));

    // watch query 2 and send true to the rx2 channel
    let (tx2, rx2) = unbounded();
    let handle2 = isar.watch_query(col, q2, Box::new(move || tx2.send(true).unwrap()));

    // assert rx1 channel has received true after putting object with id 1
    TestObj::default(1).save(&mut txn, col);
    assert_eq!(rx1.len(), 1);
    assert_eq!(rx2.len(), 0);
    assert!(rx1.try_recv().unwrap());

    // assert rx1 and rx2 channel has received true after putting object with id 1 and id 2
    TestObj::default(1).save(&mut txn, col);
    TestObj::default(2).save(&mut txn, col);
    assert_eq!(rx1.len(), 1);
    assert_eq!(rx2.len(), 1);
    assert!(rx1.try_recv().unwrap());
    assert!(rx2.try_recv().unwrap());

    handle1.stop();
    handle2.stop();
    txn.abort();
    isar.close();
}
*/
