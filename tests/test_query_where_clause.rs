mod common;

use std::vec;

use isar_core::key::IndexKey;
use itertools::Itertools;

use crate::common::test_obj::TestObj;
use crate::common::util::assert_find;

#[test]
fn test_no_where_clauses() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    put!(col, txn, byte, obj1 => 1, obj2 => 2, obj3 => 3, obj4 => 4);

    let q = col.new_query_builder().build();
    assert_find(&mut txn, q, &[&obj1, &obj2, &obj3, &obj4]);

    txn.abort();
    isar.close();
}

#[test]
fn test_single_id_where_clause() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    put!(id col, txn, obj0 => 0, obj1 => 1, obj2 => 2, obj3 => 3, obj4 => 5);

    let mut qb = col.new_query_builder();
    qb.add_id_where_clause(1, 3).unwrap();
    assert_find(&mut txn, qb.build(), &[&obj1, &obj2, &obj3]);

    let mut qb = col.new_query_builder();
    qb.add_id_where_clause(3, 1).unwrap();
    assert_find(&mut txn, qb.build(), &[&obj3, &obj2, &obj1]);

    txn.abort();
    isar.close();
}

#[test]
fn test_single_index_where_clause() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let mut lower = IndexKey::new();
    lower.add_byte(1);
    let mut upper = IndexKey::new();
    upper.add_byte(3);

    put!(col, txn, byte, obj0 => 0, obj1 => 1, obj2 => 2, obj3 => 3, obj4 => 4);

    let results = vec![
        (&lower, true, &upper, true, vec![&obj1, &obj2, &obj3]),
        (&lower, false, &upper, true, vec![&obj2, &obj3]),
        (&lower, true, &upper, false, vec![&obj1, &obj2]),
        (&lower, false, &upper, false, vec![&obj2]),
    ];
    for (lower, incl_lower, upper, incl_upper, objects) in results {
        // verify that the query returns the expected objects
        let mut qb = col.new_query_builder();
        qb.add_index_where_clause(
            0,
            lower.clone(),
            incl_lower,
            upper.clone(),
            incl_upper,
            false,
        )
        .unwrap();
        assert_find(&mut txn, qb.build(), &objects);

        // verify that the reversed query returns the expected objects in reverse order
        let mut qb = col.new_query_builder();
        qb.add_index_where_clause(
            0,
            upper.clone(),
            incl_upper,
            lower.clone(),
            incl_lower,
            false,
        )
        .unwrap();
        assert_find(
            &mut txn,
            qb.build(),
            &objects.into_iter().rev().collect_vec(),
        );
    }

    txn.abort();
    isar.close();
}
