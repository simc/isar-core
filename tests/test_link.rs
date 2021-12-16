mod common;

use isar_core::verify::verify_isar;

use crate::common::test_obj::TestObj;

/*#[test]
fn test_create_aborts_if_object_not_existing() {
    let col_schema = TestObj::schema("col1", &[], &[LinkSchema::new("l1", "col1")]);
    isar!(isar, col1, col_schema);
    txn!(isar, txn);

    let obj1 = TestObj::default(1);
    obj1.save(&mut txn, col1);

    // source object does not exist
    let linked = col1.link(&mut txn, 0, false, 1, 5).unwrap();
    assert!(!linked);

    // target object does not exist
    let linked = col1.link(&mut txn, 0, false, 5, 2).unwrap();
    assert!(!linked);

    verify_isar(&mut txn, vec![(col1, vec![obj1.to_bytes()], vec![])]);

    txn.abort();
    isar.close();
}

#[test]
fn test_create() {
    let col1_schema = TestObj::schema(
        "col1",
        &[],
        &[LinkSchema::new("l1", "col1"), LinkSchema::new("l2", "col2")],
    );
    let col2_schema = TestObj::schema("col2", &[], &[]);
    isar!(isar, col1, col1_schema, col2, col2_schema);
    txn!(isar, txn);

    let obj1 = TestObj::default(1);
    obj1.save(col1, &mut txn);

    let obj2 = TestObj::default(2);
    obj2.save(col2, &mut txn);

    // same collection
    let linked = col1.link(&mut txn, 0, false, 1, 1).unwrap();
    assert!(linked);

    // different collection
    let linked = col1.link(&mut txn, 1, false, 1, 2).unwrap();
    assert!(linked);

    verify_isar(
        &mut txn,
        vec![
            (
                col1,
                vec![obj1.to_bytes()],
                vec![("l1", 1, 1), ("l2", 1, 2)],
            ),
            (col2, vec![obj2.to_bytes()], vec![]),
        ],
    );

    txn.abort();
    isar.close();
}

/*
#[test]
fn test_create_aborts_if_target_object_not_existing() {
    isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

    create_objects(&isar, col1);

    let link = IsarLink::new(0, 1, col1.id, col2.id);
    let mut txn = isar.begin_txn(true, false).unwrap();

    let success = txn
        .write(|c, _| link.create(&mut c.data, &mut c.links, 0, 555))
        .unwrap();
    assert!(!success);
    assert!(link.debug_dump(&mut txn).is_empty());

    txn.abort();
    isar.close();
}

#[test]
fn test_create() {
    isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

    create_objects(&isar, col1);
    create_objects(&isar, col2);

    let link = IsarLink::new(123, 456, col1.id, col2.id);
    let mut txn = isar.begin_txn(true, false).unwrap();

    txn.write(|c, _| {
        assert!(link.create(&mut c.data, &mut c.links, 1, 1).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 1, 2).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());
        Ok(())
    })
    .unwrap();

    assert_eq!(
        link.debug_dump(&mut txn),
        map!(1 => set![1, 2], 2 => set![2])
    );

    assert_eq!(
        link.to_backlink().debug_dump(&mut txn),
        map!(1 => set![1], 2 => set![1, 2])
    );

    txn.abort();
    isar.close();
}

#[test]
fn test_create_same_collection() {
    isar!(isar, col => col!());

    create_objects(&isar, col);

    let link = IsarLink::new(123, 456, col.id, col.id);
    let mut txn = isar.begin_txn(true, false).unwrap();

    txn.write(|c, _| {
        assert!(link.create(&mut c.data, &mut c.links, 1, 1).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 1, 2).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());
        Ok(())
    })
    .unwrap();

    assert_eq!(
        link.debug_dump(&mut txn),
        map!(1 => set![1, 2], 2 => set![2])
    );

    assert_eq!(
        link.to_backlink().debug_dump(&mut txn),
        map!(1 => set![1], 2 => set![1, 2])
    );

    txn.abort();
    isar.close();
}

#[test]
fn test_delete() {
    isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

    create_objects(&isar, col1);
    create_objects(&isar, col2);

    let link = IsarLink::new(123, 456, col1.id, col2.id);
    let mut txn = isar.begin_txn(true, false).unwrap();

    txn.write(|c, _| {
        assert!(link.create(&mut c.data, &mut c.links, 1, 1).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 1, 2).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());

        assert!(link.delete(&mut c.links, 1, 2).unwrap());
        assert!(link.delete(&mut c.links, 2, 2).unwrap());
        assert!(!link.delete(&mut c.links, 2, 2).unwrap());
        assert!(!link.delete(&mut c.links, 3, 2).unwrap());
        Ok(())
    })
    .unwrap();

    assert_eq!(link.debug_dump(&mut txn), map!(1 => set![1]));

    assert_eq!(link.to_backlink().debug_dump(&mut txn), map!(1 => set![1]));

    txn.abort();
    isar.close();
}

#[test]
fn test_delete_same_collection() {
    isar!(isar, col => col!());

    create_objects(&isar, col);

    let link = IsarLink::new(123, 456, col.id, col.id);
    let mut txn = isar.begin_txn(true, false).unwrap();

    txn.write(|c, _| {
        assert!(link.create(&mut c.data, &mut c.links, 1, 1).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 1, 2).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());

        assert!(link.delete(&mut c.links, 1, 2).unwrap());
        assert!(link.delete(&mut c.links, 2, 2).unwrap());
        assert!(!link.delete(&mut c.links, 2, 2).unwrap());
        assert!(!link.delete(&mut c.links, 3, 2).unwrap());
        Ok(())
    })
    .unwrap();

    assert_eq!(link.debug_dump(&mut txn), map!(1 => set![1]));

    assert_eq!(link.to_backlink().debug_dump(&mut txn), map!(1 => set![1]));

    txn.abort();
    isar.close();
}

#[test]
fn test_delete_all_for_object() {
    isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

    create_objects(&isar, col1);
    create_objects(&isar, col2);

    let link = IsarLink::new(123, 456, col1.id, col2.id);
    let mut txn = isar.begin_txn(true, false).unwrap();

    txn.write(|c, _| {
        assert!(link.create(&mut c.data, &mut c.links, 2, 3).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 3, 2).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 3, 1).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());

        link.delete_all_for_object(&mut c.links, 2).unwrap();
        Ok(())
    })
    .unwrap();

    assert_eq!(link.debug_dump(&mut txn), map!(3 => set![1, 2]));

    assert_eq!(
        link.to_backlink().debug_dump(&mut txn),
        map!(1 => set![3], 2 => set![3])
    );

    txn.abort();
    isar.close();
}

#[test]
fn test_clear() {
    isar!(isar, col => col!());

    create_objects(&isar, col);

    let link = IsarLink::new(123, 456, col.id, col.id);
    let mut txn = isar.begin_txn(true, false).unwrap();

    txn.write(|c, _| {
        assert!(link.create(&mut c.data, &mut c.links, 2, 3).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 3, 2).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 3, 1).unwrap());
        assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());

        link.clear(&mut c.links).unwrap();
        Ok(())
    })
    .unwrap();

    assert!(link.debug_dump(&mut txn).is_empty());

    assert!(link.to_backlink().debug_dump(&mut txn).is_empty());

    txn.abort();
    isar.close();
}
*/*/
