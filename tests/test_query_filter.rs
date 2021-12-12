use std::vec;

use isar_core::collection::IsarCollection;
use isar_core::query::filter::Filter;
use isar_core::txn::IsarTxn;

use crate::common::test_obj::TestObj;

mod common;

fn expect_filter(col: &IsarCollection, txn: &mut IsarTxn, filter: Filter, objects: &[&TestObj]) {
    let mut qb = col.new_query_builder();
    qb.set_filter(filter);
    let q = qb.build();
    let result = q.find_all_vec(txn).unwrap();
    assert_eq!(objects.len(), result.len());
    for (o, (_, r)) in objects.iter().zip(result.into_iter()) {
        assert_eq!(TestObj::from(r), **o);
    }
}

#[test]
fn test_byte_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let p = TestObj::BYTE_PROP;

    put_objects!(col, txn, byte, obj1, 1, obj2, 2, obj3, 3, obj4, 4);

    let results = vec![
        (0, 0, vec![]),
        (5, 5, vec![]),
        (2, 1, vec![]),
        (0, 2, vec![&obj1, &obj2]),
        (1, 1, vec![&obj1]),
        (2, 3, vec![&obj2, &obj3]),
        (4, 6, vec![&obj4]),
    ];
    for (lower, upper, objects) in results {
        expect_filter(
            col,
            &mut txn,
            Filter::byte(p, lower, upper).unwrap(),
            &objects,
        );
    }

    txn.abort();
    isar.close();
}

#[test]
fn test_int_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let p = TestObj::INT_PROP;

    put_objects!(col, txn, int, obj1, 1, obj2, 2, obj3, 3, obj4, 4);

    let results = vec![
        (0, 0, vec![]),
        (5, 5, vec![]),
        (2, 1, vec![]),
        (0, 2, vec![&obj1, &obj2]),
        (1, 1, vec![&obj1]),
        (2, 3, vec![&obj2, &obj3]),
        (4, 6, vec![&obj4]),
    ];
    for (lower, upper, objects) in results {
        expect_filter(
            col,
            &mut txn,
            Filter::int(p, lower, upper).unwrap(),
            &objects,
        );
    }

    txn.abort();
    isar.close();
}

#[test]
fn test_long_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let p = TestObj::ID_PROP;

    put_objects!(col, txn, id, obj1, 1, obj2, 2, obj3, 3, obj4, 4);

    let results = vec![
        (0, 0, vec![]),
        (5, 5, vec![]),
        (2, 1, vec![]),
        (0, 2, vec![&obj1, &obj2]),
        (1, 1, vec![&obj1]),
        (2, 3, vec![&obj2, &obj3]),
        (4, 6, vec![&obj4]),
    ];
    for (lower, upper, objects) in results {
        expect_filter(
            col,
            &mut txn,
            Filter::long(p, lower, upper).unwrap(),
            &objects,
        );
    }

    txn.abort();
    isar.close();
}

#[test]
fn test_float_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let p = TestObj::FLOAT_PROP;

    put_objects!(col, txn, float, obj1, 1.0, obj2, 2.0, obj3, 3.0, obj4, 4.0);

    let results = vec![
        (0.0, 0.0, vec![]),
        (5.0, 5.0, vec![]),
        (2.0, 1.0, vec![]),
        (0.0, 2.0, vec![&obj1, &obj2]),
        (1.0, 1.0, vec![&obj1]),
        (2.0, 3.0, vec![&obj2, &obj3]),
        (4.0, 6.0, vec![&obj4]),
    ];
    for (lower, upper, objects) in results {
        expect_filter(
            col,
            &mut txn,
            Filter::float(p, lower, upper).unwrap(),
            &objects,
        );
    }

    txn.abort();
    isar.close();
}

#[test]
fn test_double_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let p = TestObj::DOUBLE_PROP;

    put_objects!(col, txn, double, obj1, 1.0, obj2, 2.0, obj3, 3.0, obj4, 4.0);

    let results = vec![
        (0.0, 0.0, vec![]),
        (5.0, 5.0, vec![]),
        (2.0, 1.0, vec![]),
        (0.0, 2.0, vec![&obj1, &obj2]),
        (1.0, 1.0, vec![&obj1]),
        (2.0, 3.0, vec![&obj2, &obj3]),
        (4.0, 6.0, vec![&obj4]),
    ];
    for (lower, upper, objects) in results {
        expect_filter(
            col,
            &mut txn,
            Filter::double(p, lower, upper).unwrap(),
            &objects,
        );
    }

    txn.abort();
    isar.close();
}

#[test]
fn test_string_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let p = TestObj::STRING_PROP;

    let mut obj1 = TestObj::default(0);
    obj1.string = None;

    let mut obj2 = TestObj::default(1);
    obj2.string = Some("a".to_string());
    obj2.save(col, &mut txn);

    let mut obj3 = TestObj::default(2);
    obj3.string = Some("aA".to_string());
    obj3.save(col, &mut txn);

    let mut obj4 = TestObj::default(3);
    obj4.string = Some("aa".to_string());
    obj4.save(col, &mut txn);

    let mut obj5 = TestObj::default(4);
    obj5.string = Some("ab".to_string());
    obj5.save(col, &mut txn);

    expect_filter(
        col,
        &mut txn,
        Filter::string(p, None, None, false).unwrap(),
        &[],
    );

    obj1.save(col, &mut txn);

    let results = vec![
        (None, None, false, vec![&obj1]),
        (None, None, true, vec![&obj1]),
        (Some("x"), Some("y"), false, vec![]),
        (Some("x"), Some("y"), true, vec![]),
        (Some("ab"), Some("aa"), false, vec![]),
        (Some("ab"), Some("aa"), true, vec![]),
        (Some("a"), None, false, vec![]),
        (Some("a"), None, true, vec![]),
        (None, Some("a"), false, vec![&obj1, &obj2]),
        (None, Some("a"), true, vec![&obj1, &obj2]),
        (None, Some("aA"), false, vec![&obj1, &obj2, &obj3, &obj4]),
        (None, Some("aA"), true, vec![&obj1, &obj2, &obj3]),
        (Some("aa"), Some("xx"), false, vec![&obj3, &obj4, &obj5]),
        (Some("aa"), Some("xx"), true, vec![&obj4, &obj5]),
    ];

    for (lower, upper, case_sensitive, objects) in results {
        expect_filter(
            col,
            &mut txn,
            Filter::string(p, lower, upper, case_sensitive).unwrap(),
            &objects,
        );
    }

    txn.abort();
    isar.close();
}

#[test]
fn test_string_starts_ends_with_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let p = TestObj::STRING_PROP;

    let mut obj1 = TestObj::default(0);
    obj1.string = None;
    obj1.save(col, &mut txn);

    let mut obj2 = TestObj::default(1);
    obj2.string = Some("hello".to_string());
    obj2.save(col, &mut txn);

    let mut obj3 = TestObj::default(2);
    obj3.string = Some("hello World".to_string());
    obj3.save(col, &mut txn);

    let mut obj4 = TestObj::default(3);
    obj4.string = Some("hello WORLD".to_string());
    obj4.save(col, &mut txn);

    let mut obj5 = TestObj::default(4);
    obj5.string = Some("Hello WORLD".to_string());
    obj5.save(col, &mut txn);

    let starts_with_result = vec![
        ("", false, vec![&obj2, &obj3, &obj4, &obj5]),
        ("", true, vec![&obj2, &obj3, &obj4, &obj5]),
        (" ", false, vec![]),
        (" ", true, vec![]),
        ("hello", false, vec![&obj2, &obj3, &obj4, &obj5]),
        ("hello", true, vec![&obj2, &obj3, &obj4]),
        ("hello  ", false, vec![]),
        ("hello  ", true, vec![]),
        ("hello WO", false, vec![&obj3, &obj4, &obj5]),
        ("hello WO", true, vec![&obj4]),
        ("hello World", false, vec![&obj3, &obj4, &obj5]),
        ("hello World", true, vec![&obj3]),
    ];

    for (value, case_sensitive, objects) in starts_with_result {
        expect_filter(
            col,
            &mut txn,
            Filter::string_starts_with(p, value, case_sensitive).unwrap(),
            &objects,
        );
    }

    let ends_with_result = vec![
        ("", false, vec![&obj2, &obj3, &obj4, &obj5]),
        ("", true, vec![&obj2, &obj3, &obj4, &obj5]),
        (" ", false, vec![]),
        (" ", true, vec![]),
        ("WORLD", false, vec![&obj3, &obj4, &obj5]),
        ("WORLD", true, vec![&obj4, &obj5]),
        ("  World", false, vec![]),
        ("  World ", true, vec![]),
        ("o WORLD", false, vec![&obj3, &obj4, &obj5]),
        ("o WORLD", true, vec![&obj4, &obj5]),
        ("hello World", false, vec![&obj3, &obj4, &obj5]),
        ("hello World", true, vec![&obj3]),
    ];

    for (value, case_sensitive, objects) in ends_with_result {
        expect_filter(
            col,
            &mut txn,
            Filter::string_ends_with(p, value, case_sensitive).unwrap(),
            &objects,
        );
    }

    txn.abort();
    isar.close();
}

#[test]
fn test_string_matches_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let p = TestObj::STRING_PROP;

    let mut obj1 = TestObj::default(0);
    obj1.string = None;
    obj1.save(col, &mut txn);

    let mut obj2 = TestObj::default(1);
    obj2.string = Some("ab12abc".to_string());
    obj2.save(col, &mut txn);

    let mut obj3 = TestObj::default(2);
    obj3.string = Some("aBB11".to_string());
    obj3.save(col, &mut txn);

    let mut obj4 = TestObj::default(3);
    obj4.string = Some("bbaa".to_string());
    obj4.save(col, &mut txn);

    let starts_with_result = vec![
        ("", false, vec![]),
        ("", true, vec![]),
        (" ", false, vec![]),
        (" ", true, vec![]),
        ("ab*", false, vec![&obj2, &obj3]),
        ("ab*", true, vec![&obj2]),
        ("*b*", false, vec![&obj2, &obj3, &obj4]),
        ("*b*", true, vec![&obj2, &obj4]),
        ("Bba?", false, vec![&obj4]),
        ("Bba?", true, vec![]),
    ];

    for (value, case_sensitive, objects) in starts_with_result {
        expect_filter(
            col,
            &mut txn,
            Filter::string_matches(p, value, case_sensitive).unwrap(),
            &objects,
        );
    }

    txn.abort();
    isar.close();
}

#[test]
fn test_and_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let obj1 = TestObj::default(0);
    obj1.save(col, &mut txn);

    let obj2 = TestObj::default(1);
    obj2.save(col, &mut txn);

    expect_filter(
        col,
        &mut txn,
        Filter::and(vec![Filter::stat(true), Filter::stat(false)]),
        &[],
    );

    expect_filter(
        col,
        &mut txn,
        Filter::and(vec![Filter::stat(true)]),
        &[&obj1, &obj2],
    );

    expect_filter(col, &mut txn, Filter::and(vec![]), &[&obj1, &obj2]);

    txn.abort();
    isar.close();
}

#[test]
fn test_or_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let obj1 = TestObj::default(0);
    obj1.save(col, &mut txn);

    let obj2 = TestObj::default(1);
    obj2.save(col, &mut txn);

    expect_filter(
        col,
        &mut txn,
        Filter::or(vec![Filter::stat(true), Filter::stat(false)]),
        &[&obj1, &obj2],
    );

    expect_filter(col, &mut txn, Filter::or(vec![Filter::stat(false)]), &[]);

    expect_filter(col, &mut txn, Filter::or(vec![]), &[]);

    txn.abort();
    isar.close();
}

#[test]
fn test_not_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let obj1 = TestObj::default(0);
    obj1.save(col, &mut txn);

    let obj2 = TestObj::default(1);
    obj2.save(col, &mut txn);

    expect_filter(
        col,
        &mut txn,
        Filter::not(Filter::stat(false)),
        &[&obj1, &obj2],
    );

    expect_filter(col, &mut txn, Filter::not(Filter::stat(true)), &[]);

    txn.abort();
    isar.close();
}

#[test]
fn test_static_filter() {
    isar!(isar, col, TestObj::default_schema());
    txn!(isar, txn);

    let obj1 = TestObj::default(0);
    obj1.save(col, &mut txn);

    let obj2 = TestObj::default(1);
    obj2.save(col, &mut txn);

    expect_filter(col, &mut txn, Filter::stat(true), &[&obj1, &obj2]);

    expect_filter(col, &mut txn, Filter::stat(false), &[]);

    txn.abort();
    isar.close();
}
