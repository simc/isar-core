mod common;

use isar_core::collection::IsarCollection;
use isar_core::schema::link_schema::LinkSchema;
use isar_core::txn::IsarTxn;

use crate::common::test_obj::TestObj;

#[test]
fn test_create_aborts_if_object_not_existing() {
    let col_schema = TestObj::schema("col1", &[], &[LinkSchema::new("l1", "col1")]);
    isar!(isar, col => col_schema);
    txn!(isar, txn);

    // create object with id 1
    put!(col, txn, id, obj => 1);

    // source object does not exist
    let linked = col.link(&mut txn, 0, false, 5, 1).unwrap();
    assert!(!linked);

    // target object does not exist
    let linked = col.link(&mut txn, 0, false, 1, 5).unwrap();
    assert!(!linked);

    verify!(txn, col, obj);

    txn.abort();
    isar.close();
}

#[test]
fn test_link() {
    let col1_schema = TestObj::schema(
        "col1",
        &[],
        &[
            LinkSchema::new("self", "col1"),
            LinkSchema::new("other", "col2"),
        ],
    );
    let col2_schema = TestObj::schema("col2", &[], &[]);
    isar!(isar, col1 => col1_schema, col2 => col2_schema);
    txn!(isar, txn);

    put!(col1, txn, id, obj1a => 1, obj1b => 2);
    put!(col2, txn, id, obj2a => 3, obj2b => 4);

    // same collection same object
    let linked = col1.link(&mut txn, 0, false, obj1a.id, obj1a.id).unwrap();
    assert!(linked);

    // same collection different object
    let linked = col1.link(&mut txn, 0, false, obj1a.id, obj1b.id).unwrap();
    assert!(linked);

    // different collection
    let linked = col1.link(&mut txn, 1, false, obj1a.id, obj2b.id).unwrap();
    assert!(linked);

    verify!(txn,
        col!(col1, obj1a, obj1b;
            "self", obj1a.id => obj1a.id, obj1a.id => obj1b.id;
            "other", obj1a.id => obj2b.id
        );
        col!(col2, obj2a, obj2b)
    );

    txn.abort();
    isar.close();
}

#[test]
fn test_link_backlink() {
    let col1_schema = TestObj::schema(
        "col1",
        &[],
        &[
            LinkSchema::new("self", "col1"),
            LinkSchema::new("other", "col2"),
        ],
    );
    let col2_schema = TestObj::schema("col2", &[], &[]);
    isar!(isar, col1 => col1_schema, col2 => col2_schema);
    txn!(isar, txn);

    put!(col1, txn, id, obj1a => 1, obj1b => 2);
    put!(col2, txn, id, obj2a => 3, obj2b => 4);

    // same collection same object
    let linked = col1.link(&mut txn, 0, true, obj1a.id, obj1a.id).unwrap();
    assert!(linked);

    // same collection different object
    let linked = col1.link(&mut txn, 0, true, obj1a.id, obj1b.id).unwrap();
    assert!(linked);

    // different collection
    let linked = col2.link(&mut txn, 0, true, obj2b.id, obj1a.id).unwrap();
    assert!(linked);

    verify!(txn,
        col!(col1, obj1a, obj1b;
            "self", obj1a.id => obj1a.id, obj1b.id => obj1a.id;
            "other", obj1a.id => obj2b.id
        );
        col!(col2, obj2a, obj2b)
    );

    txn.abort();
    isar.close();
}

fn verify_linked(
    txn: &mut IsarTxn,
    col: &IsarCollection,
    link: usize,
    bl: bool,
    id: i64,
    linked_ids: Vec<i64>,
) {
    let mut linked = vec![];
    col.get_linked_objects(txn, link, bl, id, |id, _| {
        linked.push(id);
        true
    })
    .unwrap();
    assert_eq!(linked, linked_ids);
}

#[test]
fn test_get_linked_objects() {
    let col1_schema = TestObj::schema(
        "col1",
        &[],
        &[
            LinkSchema::new("self", "col1"),
            LinkSchema::new("other", "col2"),
        ],
    );
    let col2_schema = TestObj::schema("col2", &[], &[]);
    isar!(isar, col1 => col1_schema, col2 => col2_schema);
    txn!(isar, txn);

    put!(col1, txn, id, obj1a => 1, obj1b => 2);
    put!(col2, txn, id, obj2a => 3, obj2b => 4);
    col1.link(&mut txn, 0, false, obj1a.id, obj1a.id).unwrap();
    col1.link(&mut txn, 0, false, obj1a.id, obj1b.id).unwrap();
    col1.link(&mut txn, 1, false, obj1a.id, obj2a.id).unwrap();
    col1.link(&mut txn, 1, false, obj1a.id, obj2b.id).unwrap();
    col1.link(&mut txn, 1, false, obj1b.id, obj2b.id).unwrap();

    verify_linked(&mut txn, col1, 0, false, obj1a.id, vec![obj1a.id, obj1b.id]);
    verify_linked(&mut txn, col1, 0, false, obj1b.id, vec![]);
    verify_linked(&mut txn, col1, 0, true, obj1a.id, vec![obj1a.id]);
    verify_linked(&mut txn, col1, 0, true, obj1b.id, vec![obj1a.id]);

    verify_linked(&mut txn, col1, 1, false, obj1a.id, vec![obj2a.id, obj2b.id]);
    verify_linked(&mut txn, col1, 1, false, obj1b.id, vec![obj2b.id]);
    verify_linked(&mut txn, col2, 0, true, obj2a.id, vec![obj1a.id]);
    verify_linked(&mut txn, col2, 0, true, obj2b.id, vec![obj1a.id, obj1b.id]);

    txn.abort();
    isar.close();
}
