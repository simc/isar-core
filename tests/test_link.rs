/*#[cfg(test)]
mod tests {

    fn create_objects(isar: &IsarInstance, col: &IsarCollection) {
        let mut ob1 = col.new_object_builder(None);
        ob1.write_long(1);

        let mut ob2 = col.new_object_builder(None);
        ob2.write_long(2);

        let mut ob3 = col.new_object_builder(None);
        ob3.write_long(3);

        let mut txn = isar.begin_txn(true, true).unwrap();
        col.put(&mut txn, ob1.finish()).unwrap();
        col.put(&mut txn, ob2.finish()).unwrap();
        col.put(&mut txn, ob3.finish()).unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn test_create_aborts_if_object_not_existing() {
        isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

        create_objects(&isar, col2);

        let link = Link::new(0, 1, col1.id, col2.id);
        let mut txn = isar.begin_txn(true, false).unwrap();

        let success = txn
            .write(|c, _| link.create(&mut c.data, &mut c.links, 555, 0))
            .unwrap();
        assert!(!success);
        assert!(link.debug_dump(&mut txn).is_empty());

        txn.abort();
        isar.close();
    }

    #[test]
    fn test_create_aborts_if_target_object_not_existing() {
        isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

        create_objects(&isar, col1);

        let link = Link::new(0, 1, col1.id, col2.id);
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

        let link = Link::new(123, 456, col1.id, col2.id);
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

        let link = Link::new(123, 456, col.id, col.id);
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

        let link = Link::new(123, 456, col1.id, col2.id);
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

        let link = Link::new(123, 456, col.id, col.id);
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

        let link = Link::new(123, 456, col1.id, col2.id);
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

        let link = Link::new(123, 456, col.id, col.id);
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
}
*/
