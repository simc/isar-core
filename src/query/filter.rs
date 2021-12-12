use crate::collection::IsarCollection;
use crate::cursor::IsarCursors;
use crate::error::{illegal_arg, IsarError, Result};
use crate::key::IdKey;
use crate::link::IsarLink;
use crate::object::isar_object::{IsarObject, Property};
use crate::query::fast_wild_match::fast_wild_match;
use enum_dispatch::enum_dispatch;
use itertools::Itertools;
use paste::paste;

#[derive(Clone)]
pub struct Filter(FilterCond);

impl Filter {
    pub fn id(lower: i64, upper: i64) -> Result<Filter> {
        let filter_cond = IdBetweenCond::filter(lower, upper)?;
        Ok(Filter(filter_cond))
    }

    pub fn byte(property: Property, lower: u8, upper: u8) -> Result<Filter> {
        let filter_cond = ByteBetweenCond::filter(property, lower, upper)?;
        Ok(Filter(filter_cond))
    }

    pub fn int(property: Property, lower: i32, upper: i32) -> Result<Filter> {
        let filter_cond = IntBetweenCond::filter(property, lower, upper)?;
        Ok(Filter(filter_cond))
    }

    pub fn long(property: Property, lower: i64, upper: i64) -> Result<Filter> {
        let filter_cond = LongBetweenCond::filter(property, lower, upper)?;
        Ok(Filter(filter_cond))
    }

    pub fn float(property: Property, lower: f32, upper: f32) -> Result<Filter> {
        let filter_cond = FloatBetweenCond::filter(property, lower, upper)?;
        Ok(Filter(filter_cond))
    }

    pub fn double(property: Property, lower: f64, upper: f64) -> Result<Filter> {
        let filter_cond = DoubleBetweenCond::filter(property, lower, upper)?;
        Ok(Filter(filter_cond))
    }

    pub fn string(
        property: Property,
        lower: Option<&str>,
        upper: Option<&str>,
        case_sensitive: bool,
    ) -> Result<Filter> {
        let filter_cond = StringBetweenCond::filter(property, lower, upper, case_sensitive)?;
        Ok(Filter(filter_cond))
    }

    pub fn string_starts_with(
        property: Property,
        value: &str,
        case_sensitive: bool,
    ) -> Result<Filter> {
        let filter_cond = StringStartsWithCond::filter(property, value, case_sensitive)?;
        Ok(Filter(filter_cond))
    }

    pub fn string_ends_with(
        property: Property,
        value: &str,
        case_sensitive: bool,
    ) -> Result<Filter> {
        let filter_cond = StringEndsWithCond::filter(property, value, case_sensitive)?;
        Ok(Filter(filter_cond))
    }

    pub fn string_matches(property: Property, value: &str, case_sensitive: bool) -> Result<Filter> {
        let filter_cond = StringMatchesCond::filter(property, value, case_sensitive)?;
        Ok(Filter(filter_cond))
    }

    pub fn and(filters: Vec<Filter>) -> Filter {
        let filters = filters.into_iter().map(|f| f.0).collect_vec();
        let filter_cond = AndCond::filter(filters);
        Filter(filter_cond)
    }

    pub fn or(filters: Vec<Filter>) -> Filter {
        let filters = filters.into_iter().map(|f| f.0).collect_vec();
        let filter_cond = OrCond::filter(filters);
        Filter(filter_cond)
    }

    pub fn not(filter: Filter) -> Filter {
        let filter_cond = NotCond::filter(filter.0);
        Filter(filter_cond)
    }

    pub fn stat(value: bool) -> Filter {
        let filter_cond = StaticCond::filter(value);
        Filter(filter_cond)
    }

    pub fn link(
        collection: &IsarCollection,
        link_index: usize,
        backlink: bool,
        filter: Filter,
    ) -> Result<Filter> {
        let filter_cond = LinkCond::filter(collection, link_index, backlink, filter.0)?;
        Ok(Filter(filter_cond))
    }

    pub(crate) fn evaluate(
        &self,
        id: &IdKey,
        object: IsarObject,
        cursors: Option<&IsarCursors>,
    ) -> Result<bool> {
        self.0.evaluate(id, object, cursors)
    }
}

#[enum_dispatch]
#[derive(Clone)]
enum FilterCond {
    IdBetween(IdBetweenCond),
    ByteBetween(ByteBetweenCond),
    IntBetween(IntBetweenCond),
    LongBetween(LongBetweenCond),
    FloatBetween(FloatBetweenCond),
    DoubleBetween(DoubleBetweenCond),

    StringBetween(StringBetweenCond),
    StringStartsWith(StringStartsWithCond),
    StringEndsWith(StringEndsWithCond),
    StringMatches(StringMatchesCond),

    And(AndCond),
    Or(OrCond),
    Not(NotCond),
    Static(StaticCond),
    IsarLink(LinkCond),
}

#[enum_dispatch(FilterCond)]
trait Condition {
    fn evaluate(
        &self,
        id: &IdKey,
        object: IsarObject,
        cursors: Option<&IsarCursors>,
    ) -> Result<bool>;
}

#[derive(Clone)]
struct IdBetweenCond {
    lower: i64,
    upper: i64,
}

impl IdBetweenCond {
    fn filter(lower: i64, upper: i64) -> Result<FilterCond> {
        Ok(FilterCond::IdBetween(IdBetweenCond { lower, upper }))
    }
}

impl Condition for IdBetweenCond {
    fn evaluate(&self, id: &IdKey, _object: IsarObject, _: Option<&IsarCursors>) -> Result<bool> {
        let id = id.get_id();
        Ok(self.lower <= id && self.upper >= id)
    }
}

#[macro_export]
macro_rules! filter_between_struct {
    ($name:ident, $data_type:ident, $type:ty) => {
        paste! {
            #[derive(Clone)]
            struct [<$name Cond>] {
                upper: $type,
                lower: $type,
                property: Property,
            }

            impl [<$name Cond>] {
                 fn filter(property: Property, lower: $type, upper: $type) -> Result<FilterCond> {
                    if property.data_type == crate::object::data_type::DataType::$data_type {
                        Ok(FilterCond::$name(Self {
                            property,
                            lower,
                            upper,
                        }))
                    } else {
                        illegal_arg("Property does not support this filter.")
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! primitive_filter_between {
    ($name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_between_struct!($name, $data_type, $type);
        paste! {
            impl Condition for [<$name Cond>] {
                fn evaluate(&self, _id: &IdKey, object: IsarObject, _: Option<&IsarCursors>) -> Result<bool> {
                    let val = object.$prop_accessor(self.property);
                    Ok(self.lower <= val && self.upper >= val)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! float_filter_between {
    ($name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_between_struct!($name, $data_type, $type);
        paste! {
            impl Condition for [<$name Cond>] {
                fn evaluate(&self, _id: &IdKey, object: IsarObject, _: Option<&IsarCursors>) -> Result<bool> {
                    let val = object.$prop_accessor(self.property);
                    let result = if self.upper.is_nan() {
                        self.lower.is_nan() && val.is_nan()
                    } else if self.lower.is_nan() {
                        self.upper >= val || val.is_nan()
                    } else {
                        self.lower <= val && self.upper >= val
                    };
                    Ok(result)
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

#[derive(Clone)]
struct StringBetweenCond {
    property: Property,
    lower: Option<String>,
    upper: Option<String>,
    case_sensitive: bool,
}

impl StringBetweenCond {
    fn filter(
        property: Property,
        lower: Option<&str>,
        upper: Option<&str>,
        case_sensitive: bool,
    ) -> Result<FilterCond> {
        let lower = if case_sensitive {
            lower.map(|s| s.to_string())
        } else {
            lower.map(|s| s.to_lowercase())
        };
        let upper = if case_sensitive {
            upper.map(|s| s.to_string())
        } else {
            upper.map(|s| s.to_lowercase())
        };
        if property.data_type == crate::object::data_type::DataType::String {
            Ok(FilterCond::StringBetween(StringBetweenCond {
                property,
                lower,
                upper,
                case_sensitive,
            }))
        } else {
            illegal_arg("Property does not support this filter.")
        }
    }
}

impl Condition for StringBetweenCond {
    fn evaluate(&self, _id: &IdKey, object: IsarObject, _: Option<&IsarCursors>) -> Result<bool> {
        let obj_str = object.read_string(self.property);
        let result = if let Some(obj_str) = obj_str {
            let mut matches = true;
            if self.case_sensitive {
                if let Some(ref lower) = self.lower {
                    matches = lower.as_str() <= obj_str;
                }
                matches &= if let Some(ref upper) = self.upper {
                    upper.as_str() >= obj_str
                } else {
                    false
                };
            } else {
                let obj_str = obj_str.to_lowercase();
                if let Some(ref lower) = self.lower {
                    matches = lower.as_str() <= obj_str.as_str();
                }
                matches &= if let Some(ref upper) = self.upper {
                    upper.as_str() >= obj_str.as_str()
                } else {
                    false
                };
            }
            matches
        } else {
            self.lower.is_none()
        };
        Ok(result)
    }
}

#[macro_export]
macro_rules! string_filter_struct {
    ($name:ident, $data_type:ident) => {
        paste! {
            #[derive(Clone)]
             struct [<$name Cond>] {
                property: Property,
                value: String,
                case_sensitive: bool,
            }

            impl [<$name Cond>] {
                 fn filter(
                    property: Property,
                    value: &str,
                    case_sensitive: bool,
                ) -> Result<FilterCond> {
                    let value = if case_sensitive {
                        value.to_string()
                    } else {
                        value.to_lowercase()
                    };
                    if property.data_type == crate::object::data_type::DataType::$data_type {
                        Ok(FilterCond::$name([<$name Cond>] {
                            property,
                            value,
                            case_sensitive,
                        }))
                    } else {
                        illegal_arg("Property does not support this filter.")
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! string_filter {
    ($name:ident) => {
        string_filter_struct!($name, String);
        paste! {
            impl Condition for [<$name Cond>] {
                fn evaluate(&self, _id: &IdKey, object: IsarObject, _: Option<&IsarCursors>) -> Result<bool> {
                    let other_str = object.read_string(self.property);
                    let result = if let Some(other_str) = other_str {
                        if self.case_sensitive {
                            string_filter!($name &self.value, other_str)
                        } else {
                            let lowercase_string = other_str.to_lowercase();
                            let lowercase_str = &lowercase_string;
                            string_filter!($name &self.value, lowercase_str)
                        }
                    } else {
                        false
                    };
                    Ok(result)
                }
            }
        }
    };

    (StringStartsWith $filter_str:expr, $other_str:ident) => {
        $other_str.starts_with($filter_str)
    };

    (StringEndsWith $filter_str:expr, $other_str:ident) => {
        $other_str.ends_with($filter_str)
    };

    (StringMatches $filter_str:expr, $other_str:ident) => {
        fast_wild_match($other_str, $filter_str)
    };
}

string_filter!(StringStartsWith);
string_filter!(StringEndsWith);
string_filter!(StringMatches);

#[derive(Clone)]
struct AndCond {
    filters: Vec<FilterCond>,
}

impl Condition for AndCond {
    fn evaluate(
        &self,
        id: &IdKey,
        object: IsarObject,
        cursors: Option<&IsarCursors>,
    ) -> Result<bool> {
        for filter in &self.filters {
            if !filter.evaluate(id, object, cursors)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl AndCond {
    pub fn filter(filters: Vec<FilterCond>) -> FilterCond {
        FilterCond::And(AndCond { filters })
    }
}

#[derive(Clone)]
struct OrCond {
    filters: Vec<FilterCond>,
}

impl Condition for OrCond {
    fn evaluate(
        &self,
        id: &IdKey,
        object: IsarObject,
        cursors: Option<&IsarCursors>,
    ) -> Result<bool> {
        for filter in &self.filters {
            if filter.evaluate(id, object, cursors)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl OrCond {
    fn filter(filters: Vec<FilterCond>) -> FilterCond {
        FilterCond::Or(OrCond { filters })
    }
}

#[derive(Clone)]
struct NotCond {
    filter: Box<FilterCond>,
}

impl Condition for NotCond {
    fn evaluate(
        &self,
        id: &IdKey,
        object: IsarObject,
        cursors: Option<&IsarCursors>,
    ) -> Result<bool> {
        Ok(!self.filter.evaluate(id, object, cursors)?)
    }
}

impl NotCond {
    pub fn filter(filter: FilterCond) -> FilterCond {
        FilterCond::Not(NotCond {
            filter: Box::new(filter),
        })
    }
}

#[derive(Clone)]
struct StaticCond {
    value: bool,
}

impl Condition for StaticCond {
    fn evaluate(&self, _id: &IdKey, _: IsarObject, _: Option<&IsarCursors>) -> Result<bool> {
        Ok(self.value)
    }
}

impl StaticCond {
    pub fn filter(value: bool) -> FilterCond {
        FilterCond::Static(StaticCond { value })
    }
}

#[derive(Clone)]
struct LinkCond {
    link: IsarLink,
    filter: Box<FilterCond>,
}

impl Condition for LinkCond {
    fn evaluate(
        &self,
        id: &IdKey,
        _object: IsarObject,
        cursors: Option<&IsarCursors>,
    ) -> Result<bool> {
        if let Some(cursors) = cursors {
            self.link
                .iter(cursors, id, |id, object| {
                    self.filter
                        .evaluate(&id, object, None)
                        .map(|matches| !matches)
                })
                .map(|none_matches| !none_matches)
        } else {
            Err(IsarError::VersionError {})
        }
    }
}

impl LinkCond {
    fn filter(
        collection: &IsarCollection,
        link_index: usize,
        backlink: bool,
        filter: FilterCond,
    ) -> Result<FilterCond> {
        let link = collection.get_link_backlink(link_index, backlink)?;
        Ok(FilterCond::IsarLink(LinkCond {
            link,
            filter: Box::new(filter),
        }))
    }
}
