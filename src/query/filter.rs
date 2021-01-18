use crate::error::{illegal_arg, Result};
use crate::object::property::Property;
use enum_dispatch::enum_dispatch;

#[derive(Eq, PartialEq)]
pub enum Case {
    Sensitive,
    Insensitive,
}

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
    /*StrAnyOf(StrAnyOf),
    StrStartsWith(),
    StrEndsWith(),
    StrContains(),*/
    And(And),
    Or(Or),
    Not(Not),
}

#[enum_dispatch(Filter)]
pub trait Condition {
    fn evaluate(&self, object: &[u8]) -> bool;
}

#[derive(Clone)]
pub struct IsNull {
    property: Property,
    is_null: bool,
}

impl Condition for IsNull {
    fn evaluate(&self, object: &[u8]) -> bool {
        self.property.is_null(object) == self.is_null
    }
}

impl IsNull {
    pub fn filter(property: Property, is_null: bool) -> Filter {
        Filter::IsNull(Self { property, is_null })
    }
}

#[macro_export]
macro_rules! filter_between {
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
        filter_between!($name, $data_type, $type);

        impl Condition for $name {
            fn evaluate(&self, object: &[u8]) -> bool {
                let val = self.property.$prop_accessor(object);
                self.lower <= val && self.upper >= val
            }
        }
    };
}

#[macro_export]
macro_rules! float_filter_between {
    ($name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_between!($name, $data_type, $type);

        impl Condition for $name {
            fn evaluate(&self, object: &[u8]) -> bool {
                let val = self.property.$prop_accessor(object);
                if self.upper.is_nan() {
                    self.lower.is_nan() && val.is_nan()
                } else if self.lower.is_nan() {
                    self.upper >= val
                } else {
                    self.lower <= val && self.upper >= val
                }
            }
        }
    };
}

primitive_filter_between!(ByteBetween, Byte, u8, get_byte);
primitive_filter_between!(IntBetween, Int, i32, get_int);
primitive_filter_between!(LongBetween, Long, i64, get_long);
float_filter_between!(FloatBetween, Float, f32, get_float);
float_filter_between!(DoubleBetween, Double, f64, get_double);

#[macro_export]
macro_rules! filter_not_equal {
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
        filter_not_equal!($not_equal_name, $data_type, $type);

        impl Condition for $not_equal_name {
            fn evaluate(&self, object: &[u8]) -> bool {
                let val = self.property.$prop_accessor(object);
                self.value != val
            }
        }
    };
}

primitive_filter_not_equal!(ByteNotEqual, Byte, u8, get_byte);
primitive_filter_not_equal!(IntNotEqual, Int, i32, get_int);
primitive_filter_not_equal!(LongNotEqual, Long, i64, get_long);

/*pub struct StrAnyOf {
    property: Property,
    values: Vec<Option<Vec<u8>>>,
    case: Case,
}

impl StrAnyOf {
    pub fn new(property: Property, values: &[Option<&str>], case: Case) -> StrAnyOf {
        let values = if case == Case::Insensitive {
            values
                .iter()
                .map(|s| s.to_lowercase().into_bytes())
                .collect_vec()
        } else {
            values.iter().map(|s| s.as_bytes().to_vec()).collect_vec()
        };
        StrAnyOf {
            property,
            values,
            case,
        }
    }
}

impl Condition for StrAnyOf {
    fn evaluate(&self, object: &[u8]) -> bool {
        let string_bytes = self.property.get_bytes(object);
        match self.case {
            Case::Sensitive => self
                .values
                .iter()
                .any(|item| item.as_slice() == string_bytes),
            Case::Insensitive => unsafe {
                let lowercase_string = std::str::from_utf8_unchecked(object).to_lowercase();
                let lowercase_bytes = lowercase_string.as_bytes();
                self.values
                    .iter()
                    .any(|item| item.as_slice() == lowercase_bytes)
            },
        }
    }
}

impl StrAnyOf {
    pub fn filter(property: Property, values: Vec<Vec<u8>>, case: Case) -> Filter {
        Filter::StrAnyOf(StrAnyOf {
            property,
            values,
            case,
        })
    }
}*/

#[derive(Clone)]
pub struct And {
    filters: Vec<Filter>,
}

impl Condition for And {
    fn evaluate(&self, object: &[u8]) -> bool {
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
    fn evaluate(&self, object: &[u8]) -> bool {
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
    fn evaluate(&self, object: &[u8]) -> bool {
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
