#![allow(clippy::missing_safety_doc)]

#[macro_use]
pub mod debug;

use time::OffsetDateTime;

pub fn seconds_since_epoch() -> u64 {
    OffsetDateTime::now_utc().unix_timestamp() as u64
}

#[macro_export]
macro_rules! option (
    ($option:expr, $value:expr) => {
        if $option {
            Some($value)
        } else {
            None
        }
    };
);

#[macro_export]
macro_rules! map_option (
    ($option:expr, $var:ident, $map:expr) => {
        if let Some($var) = $option {
            Some($map)
        } else {
            None
        }
    };
);
