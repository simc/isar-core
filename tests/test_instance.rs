use crate::common::test_obj::TestObj;
use isar_core::schema::link_schema::LinkSchema;
use isar_core::verify::verify_isar;

mod common;

#[test]
fn test_open_instance_added_collection() {
    let indexes = TestObj::default_indexes();
    let link = LinkSchema::new("testlink", "obj1");
    let schema1 = TestObj::schema("obj1", &indexes, &[link]);
    let schema2 = TestObj::schema("obj2", &indexes, &[]);

    // empty database
    isar!(isar);
    let path = isar.path.clone();
    txn!(isar, txn);
    verify_isar(&mut txn, vec![]);
    txn.abort();
    isar.close();

    // database with one collection
    isar!(path, isar, col1 => schema1);
    txn!(isar, txn);
    put!(id: col1, txn, obj1 => 1, obj2 => 2);
    col1.link(&mut txn, 0, false, 1, 2).unwrap();
    verify!(txn, col1, obj1, obj2; "testlink", 1 => 2);
    txn.commit().unwrap();
    isar.close();

    // database with two collections
    isar!(path, isar, col1 => schema1, col2 => schema2);
    txn!(isar, txn);
    put!(id: col2, txn, obj3 => 3);
    verify!(txn, col!(col1, obj1, obj2; "testlink", 1 => 2); col!(col2, obj3));
    txn.commit().unwrap();
    isar.close();
}

#[test]
fn test_open_instance_removed_collection() {
    let indexes = TestObj::default_indexes();
    let link1 = LinkSchema::new("testlink1", "obj1");
    let link2 = LinkSchema::new("testlink2", "obj2");
    let schema1 = TestObj::schema("obj1", &indexes, &[link1]);
    let schema2 = TestObj::schema("obj2", &indexes, &[link2]);

    // database with two collections
    isar!(isar, col1 => schema1, col2 => schema2);
    let path = isar.path.clone();
    txn!(isar, txn);
    put!(id: col1, txn, obj1 => 1, obj2 => 2);
    put!(id: col2, txn, obj3 => 3, obj4 => 4);
    col1.link(&mut txn, 0, false, 1, 2).unwrap();
    col2.link(&mut txn, 0, false, 3, 4).unwrap();
    verify!(txn, col!(col1, obj1, obj2; "testlink1", 1 => 2); col!(col2, obj3, obj4; "testlink2", 3 => 4));
    txn.commit().unwrap();
    isar.close();

    // database with one collection
    isar!(path, isar, col2 => schema2);
    txn!(isar, txn);
    verify!(txn, col2, obj3, obj4; "testlink2", 3 => 4);
    txn.commit().unwrap();
    isar.close();

    // empty database
    isar!(path, isar);
    txn!(isar, txn);
    verify!(txn);
    txn.abort();
    isar.close();
}
