use crate::error::{illegal_arg, Result};
use crate::object::isar_object::{IsarObject, Property};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
#[derive(Clone)]
pub enum Filter {
    IsNull(IsNull),

    ByteBetween(ByteBetween),
    ByteNotEqual(ByteNotEqual),
    IntBetween(IntBetween),
    IntNotEqual(IntNotEqual),
    LongBetween(LongBetween),
    LongNotEqual(LongNotEqual),
    FloatBetween(FloatBetween),
    DoubleBetween(DoubleBetween),

    ByteListContains(ByteListContains),
    IntListContains(IntListContains),
    LongListContains(LongListContains),

    StringEqual(StringEqual),
    StringStartsWith(StringStartsWith),
    StringEndsWith(StringEndsWith),
    StringContains(StringContains),

    StringListContains(StringListContains),

    And(And),
    Or(Or),
    Not(Not),
    Static(Static),
}

#[enum_dispatch(Filter)]
pub trait Condition {
    fn evaluate(&self, object: IsarObject) -> bool;
}

#[derive(Clone)]
pub struct IsNull {
    property: Property,
    is_null: bool,
}

impl Condition for IsNull {
    fn evaluate(&self, object: IsarObject) -> bool {
        object.is_null(self.property) == self.is_null
    }
}

impl IsNull {
    pub fn filter(property: Property, is_null: bool) -> Filter {
        Filter::IsNull(Self { property, is_null })
    }
}

#[macro_export]
macro_rules! filter_between_struct {
    ($name:ident, $data_type:ident, $type:ty) => {
        #[derive(Clone)]
        pub struct $name {
            upper: $type,
            lower: $type,
            property: Property,
        }

        impl $name {
            pub fn filter(property: Property, lower: $type, upper: $type) -> Result<Filter> {
                if property.data_type == crate::object::data_type::DataType::$data_type {
                    Ok(Filter::$name(Self {
                        property,
                        lower,
                        upper,
                    }))
                } else {
                    illegal_arg("Property does not support this filter.")
                }
            }
        }
    };
}

#[macro_export]
macro_rules! primitive_filter_between {
    ($name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_between_struct!($name, $data_type, $type);

        impl Condition for $name {
            fn evaluate(&self, object: IsarObject) -> bool {
                let val = object.$prop_accessor(self.property);
                self.lower <= val && self.upper >= val
            }
        }
    };
}

#[macro_export]
macro_rules! float_filter_between {
    ($name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_between_struct!($name, $data_type, $type);

        impl Condition for $name {
            fn evaluate(&self, object: IsarObject) -> bool {
                let val = object.$prop_accessor(self.property);
                if self.upper.is_nan() {
                    self.lower.is_nan() && val.is_nan()
                } else if self.lower.is_nan() {
                    self.upper >= val || val.is_nan()
                } else {
                    self.lower <= val && self.upper >= val
                }
            }
        }
    };
}

primitive_filter_between!(ByteBetween, Byte, u8, read_byte);
primitive_filter_between!(IntBetween, Int, i32, read_int);
primitive_filter_between!(LongBetween, Long, i64, read_long);
float_filter_between!(FloatBetween, Float, f32, read_float);
float_filter_between!(DoubleBetween, Double, f64, read_double);

#[macro_export]
macro_rules! filter_not_equal_struct {
    ($name:ident, $data_type:ident, $type:ty) => {
        #[derive(Clone)]
        pub struct $name {
            value: $type,
            property: Property,
        }

        impl $name {
            pub fn filter(property: Property, value: $type) -> Result<Filter> {
                if property.data_type == crate::object::data_type::DataType::$data_type {
                    Ok(Filter::$name(Self { property, value }))
                } else {
                    illegal_arg("Property does not support this filter.")
                }
            }
        }
    };
}

#[macro_export]
macro_rules! primitive_filter_not_equal {
    ($not_equal_name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_not_equal_struct!($not_equal_name, $data_type, $type);

        impl Condition for $not_equal_name {
            fn evaluate(&self, object: IsarObject) -> bool {
                let val = object.$prop_accessor(self.property);
                self.value != val
            }
        }
    };
}

#[macro_export]
macro_rules! primitive_list_filter {
    ($name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_not_equal_struct!($name, $data_type, $type);

        impl Condition for $name {
            fn evaluate(&self, object: IsarObject) -> bool {
                let list = object.$prop_accessor(self.property);
                if let Some(list) = list {
                    list.contains(&self.value)
                } else {
                    false
                }
            }
        }
    };
}

primitive_filter_not_equal!(ByteNotEqual, Byte, u8, read_byte);
primitive_filter_not_equal!(IntNotEqual, Int, i32, read_int);
primitive_filter_not_equal!(LongNotEqual, Long, i64, read_long);

primitive_list_filter!(ByteListContains, Byte, u8, read_byte_list);
primitive_list_filter!(IntListContains, Int, i32, read_int_list);
primitive_list_filter!(LongListContains, Long, i64, read_long_list);

#[macro_export]
macro_rules! string_filter_struct {
    ($name:ident) => {
        #[derive(Clone)]
        pub struct $name {
            property: Property,
            value: Option<String>,
            ignore_case: bool,
        }

        impl $name {
            pub fn filter(
                property: Property,
                value: Option<&str>,
                ignore_case: bool,
            ) -> Result<Filter> {
                let value = if ignore_case {
                    value.map(|s| s.to_lowercase())
                } else {
                    value.map(|s| s.to_string())
                };
                if property.data_type == crate::object::data_type::DataType::String {
                    Ok(Filter::$name($name {
                        property,
                        value,
                        ignore_case,
                    }))
                } else {
                    illegal_arg("Property does not support this filter.")
                }
            }
        }
    };
}

#[macro_export]
macro_rules! string_filter {
    ($name:ident) => {
        string_filter_struct!($name);

        impl Condition for $name {
            fn evaluate(&self, object: IsarObject) -> bool {
                let other_str = object.read_string(self.property);
                if let (Some(filter_str), Some(other_str)) = (self.value.as_ref(), other_str) {
                    if self.ignore_case {
                        let lowercase_string = other_str.to_lowercase();
                        let lowercase_str = &lowercase_string;
                        string_filter!($name filter_str, lowercase_str)
                    } else {
                        string_filter!($name filter_str, other_str)
                    }
                } else {
                    self.value.is_none() && other_str.is_none()
                }
            }
        }
    };

    (StringEqual $filter_str:ident, $other_str:ident) => {
        $filter_str == $other_str
    };

    (StringStartsWith $filter_str:ident, $other_str:ident) => {
        $other_str.starts_with($filter_str)
    };

    (StringEndsWith $filter_str:ident, $other_str:ident) => {
        $other_str.ends_with($filter_str)
    };

    (StringContains $filter_str:ident, $other_str:ident) => {
        twoway::find_str($other_str, $filter_str).is_some()
    };
}

string_filter!(StringEqual);
string_filter!(StringStartsWith);
string_filter!(StringEndsWith);
string_filter!(StringContains);

string_filter_struct!(StringListContains);

impl Condition for StringListContains {
    fn evaluate(&self, object: IsarObject) -> bool {
        let list = object.read_string_list(self.property);
        if let Some(list) = list {
            list.contains(&self.value.as_deref())
        } else {
            false
        }
    }
}

#[derive(Clone)]
pub struct And {
    filters: Vec<Filter>,
}

impl Condition for And {
    fn evaluate(&self, object: IsarObject) -> bool {
        for filter in &self.filters {
            if !filter.evaluate(object) {
                return false;
            }
        }
        true
    }
}

impl And {
    pub fn filter(filters: Vec<Filter>) -> Filter {
        Filter::And(And { filters })
    }
}

#[derive(Clone)]
pub struct Or {
    filters: Vec<Filter>,
}

impl Condition for Or {
    fn evaluate(&self, object: IsarObject) -> bool {
        for filter in &self.filters {
            if filter.evaluate(object) {
                return true;
            }
        }
        false
    }
}

impl Or {
    pub fn filter(filters: Vec<Filter>) -> Filter {
        Filter::Or(Or { filters })
    }
}

#[derive(Clone)]
pub struct Not {
    filter: Box<Filter>,
}

impl Condition for Not {
    fn evaluate(&self, object: IsarObject) -> bool {
        self.filter.evaluate(object)
    }
}

impl Not {
    pub fn filter(filter: Filter) -> Filter {
        Filter::Not(Not {
            filter: Box::new(filter),
        })
    }
}

#[derive(Clone)]
pub struct Static {
    value: bool,
}

impl Condition for Static {
    fn evaluate(&self, _: IsarObject) -> bool {
        self.value
    }
}

impl Static {
    pub fn filter(value: bool) -> Filter {
        Filter::Static(Static { value })
    }
}
