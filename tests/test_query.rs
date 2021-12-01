/*#[test]
fn test_filter_unsorted() -> Result<()> {
    let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
    let col = isar.get_collection(0).unwrap();
    let mut txn = isar.begin_txn(false, false)?;

    let int_property = *col.properties.get(1).unwrap();
    let mut qb = col.new_query_builder();
    qb.set_filter(Filter::or(vec![
        Filter::int(int_property, 2, 3)?,
        Filter::not(Filter::int(int_property, 0, 4)?),
    ]));

    assert_eq!(
        find(&mut txn, col, qb.build()),
        vec![(1, 5), (4, 3), (5, 2), (6, 2)]
    );

    txn.abort();
    isar.close();
    Ok(())
}

#[test]
fn test_filter_sorted() -> Result<()> {
    let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
    let col = isar.get_collection(0).unwrap();
    let mut txn = isar.begin_txn(false, false)?;

    let int_property = *col.properties.get(1).unwrap();
    let mut qb = col.new_query_builder();
    qb.set_filter(Filter::or(vec![
        Filter::int(int_property, 2, 3)?,
        Filter::not(Filter::int(int_property, 0, 4)?),
    ]));
    qb.add_sort(int_property, Sort::Ascending);

    assert_eq!(
        find(&mut txn, col, qb.build()),
        vec![(5, 2), (6, 2), (4, 3), (1, 5)]
    );

    txn.abort();
    isar.close();
    Ok(())
}

#[test]
fn test_distinct_unsorted() -> Result<()> {
    let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
    let col = isar.get_collection(0).unwrap();
    let mut txn = isar.begin_txn(false, false)?;

    let int_property = *col.properties.get(1).unwrap();
    let mut qb = col.new_query_builder();
    qb.add_distinct(int_property, false);

    assert_eq!(
        find(&mut txn, col, qb.build()),
        vec![(1, 5), (2, 4), (4, 3), (5, 2), (7, 1)]
    );

    txn.abort();
    isar.close();
    Ok(())
}

#[test]
fn test_distinct_sorted() -> Result<()> {
    let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
    let col = isar.get_collection(0).unwrap();
    let mut txn = isar.begin_txn(false, false)?;

    let int_property = *col.properties.get(1).unwrap();
    let mut qb = col.new_query_builder();
    qb.add_distinct(int_property, false);
    qb.add_sort(int_property, Sort::Ascending);

    assert_eq!(
        find(&mut txn, col, qb.build()),
        vec![(7, 1), (5, 2), (4, 3), (2, 4), (1, 5)]
    );

    txn.abort();
    isar.close();
    Ok(())
}
*/
