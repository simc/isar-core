#![allow(clippy::missing_safety_doc)]

#[macro_use]
pub mod debug;

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
