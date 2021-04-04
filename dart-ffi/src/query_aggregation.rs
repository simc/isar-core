use crate::txn::IsarDartTxn;
use enum_ordinalize::Ordinalize;
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::object::data_type::DataType;
use isar_core::object::isar_object::Property;
use isar_core::query::Query;
use isar_core::txn::IsarTxn;
use std::cmp::Ordering;

pub enum AggregationResult {
    Long(i64),
    Double(f64),
}

#[derive(Ordinalize, PartialEq)]
#[repr(u8)]
pub enum AggregationOp {
    Min,
    Max,
    Sum,
    Average,
    Count,
}

fn aggregate(
    query: &Query,
    txn: &mut IsarTxn,
    op: AggregationOp,
    property: Option<Property>,
) -> Result<AggregationResult> {
    let mut count = 0usize;

    let (mut long_value, mut double_value) = if op == AggregationOp::Min {
        (i64::MAX, f64::INFINITY)
    } else if op == AggregationOp::Max {
        (i64::MIN, f64::NEG_INFINITY)
    } else {
        (0, 0.0)
    };

    let min_max_cmp = if op == AggregationOp::Max {
        Ordering::Greater
    } else {
        Ordering::Less
    };

    query.find_while(txn, |obj| {
        match op {
            AggregationOp::Count => count += 1,
            AggregationOp::Min | AggregationOp::Max => {
                let property = property.unwrap();
                match property.data_type {
                    DataType::Int => {
                        let value = obj.read_int(property) as i64;
                        if value.cmp(&long_value) == min_max_cmp {
                            long_value = value;
                        }
                    }
                    DataType::Float => {
                        let value = obj.read_float(property) as f64;
                        if value > double_value && min_max_cmp == Ordering::Greater {
                            double_value = value;
                        } else if value < double_value && min_max_cmp == Ordering::Less {
                            double_value = value;
                        }
                    }
                    DataType::Long => {
                        let value = obj.read_long(property);
                        if value.cmp(&long_value) == min_max_cmp {
                            long_value = value;
                        }
                    }
                    DataType::Double => {
                        let value = obj.read_double(property);
                        if value > double_value && min_max_cmp == Ordering::Greater {
                            double_value = value;
                        } else if value < double_value && min_max_cmp == Ordering::Less {
                            double_value = value;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            AggregationOp::Sum | AggregationOp::Average => {
                count += 1;
                let property = property.unwrap();
                match property.data_type {
                    DataType::Int => long_value += obj.read_int(property) as i64,
                    DataType::Float => double_value += obj.read_float(property) as f64,
                    DataType::Long => long_value += obj.read_long(property),
                    DataType::Double => double_value += obj.read_double(property),
                    _ => unreachable!(),
                }
            }
        }
        true
    })?;

    match op {
        AggregationOp::Average => {
            let result = match property.unwrap().data_type {
                DataType::Int | DataType::Long => (long_value as f64) / (count as f64),
                DataType::Float | DataType::Double => double_value / (count as f64),
                _ => unreachable!(),
            };
            return Ok(AggregationResult::Double(result));
        }
        AggregationOp::Count => return Ok(AggregationResult::Long(count as i64)),
        _ => {}
    };

    let result = match property.unwrap().data_type {
        DataType::Int | DataType::Long => AggregationResult::Long(long_value),
        DataType::Float | DataType::Double => AggregationResult::Double(double_value),
        _ => unreachable!(),
    };

    Ok(result)
}

pub struct AggregationResultSend(*mut *const AggregationResult);

unsafe impl Send for AggregationResultSend {}

#[no_mangle]
pub unsafe extern "C" fn isar_q_aggregate(
    collection: &IsarCollection,
    query: &'static Query,
    txn: &mut IsarDartTxn,
    operation: u8,
    property_index: u32,
    result: *mut *const AggregationResult,
) -> i32 {
    let op = AggregationOp::from_ordinal(operation).unwrap();
    let property = if op != AggregationOp::Count {
        let (_, p) = collection
            .get_properties()
            .get(property_index as usize)
            .unwrap();
        Some(*p)
    } else {
        None
    };
    let result = AggregationResultSend(result);
    isar_try_txn!(txn, move |txn| {
        let aggregate_result = aggregate(query, txn, op, property)?;
        result.0.write(Box::into_raw(Box::new(aggregate_result)));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_aggregate_long_result(result: &AggregationResult) -> i64 {
    match result {
        AggregationResult::Long(long) => *long,
        AggregationResult::Double(double) => *double as i64,
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_aggregate_double_result(result: &AggregationResult) -> f64 {
    match result {
        AggregationResult::Long(long) => *long as f64,
        AggregationResult::Double(double) => *double,
    }
}
