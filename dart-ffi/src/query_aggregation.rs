use enum_ordinalize::Ordinalize;
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::object::data_type::DataType;
use isar_core::object::isar_object::Property;
use isar_core::query::Query;
use isar_core::txn::IsarTxn;
use std::cmp::Ordering;
use std::hint::unreachable_unchecked;

pub enum AggregationResult {
    Int(i32),
    Float(f32),
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

    let mut int_value = 0;
    let mut float_value = 0.0;
    let mut long_value = 0;
    let mut double_value = 0.0;

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
                        let value = obj.read_int(property);
                        if value.cmp(&int_value) == min_max_cmp {
                            int_value = value;
                        }
                    }
                    DataType::Float => {
                        let value = obj.read_float(property);
                        if value > float_value && min_max_cmp == Ordering::Greater {
                            float_value = value;
                        } else if value < float_value && min_max_cmp == Ordering::Less {
                            float_value = value;
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
                    DataType::Int => int_value += obj.read_int(property),
                    DataType::Float => float_value += obj.read_float(property),
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
            match property.unwrap().data_type {
                DataType::Int => int_value /= count as i32,
                DataType::Float => float_value /= count as f32,
                DataType::Long => long_value /= count as i64,
                DataType::Double => double_value /= count as f64,
                _ => unreachable!(),
            };
        }
        AggregationOp::Count => return Ok(AggregationResult::Int(count as i32)),
        _ => {}
    };

    let result = match property.unwrap().data_type {
        DataType::Int => AggregationResult::Int(int_value),
        DataType::Float => AggregationResult::Float(float_value),
        DataType::Long => AggregationResult::Long(long_value),
        DataType::Double => AggregationResult::Double(double_value),
        _ => unreachable!(),
    };

    Ok(result)
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_aggregate(
    collection: &IsarCollection,
    query: &Query,
    txn: &mut IsarTxn,
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
    isar_try! {
        let aggregate_result = aggregate(query, txn, op, property)?;
        result.write(Box::into_raw(Box::new(aggregate_result)));
    }
}
