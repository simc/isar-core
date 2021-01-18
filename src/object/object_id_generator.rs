use crate::object::object_id::ObjectId;
use crate::utils::seconds_since_epoch;
use rand::random;
use std::sync::atomic::{AtomicU32, Ordering};

pub struct ObjectIdGenerator {
    counter: AtomicU32,
    time: fn() -> u64,
    random: fn() -> u32,
}

impl ObjectIdGenerator {
    pub fn new() -> Self {
        ObjectIdGenerator {
            counter: AtomicU32::new(random()),
            time: seconds_since_epoch,
            random: rand::random,
        }
    }

    #[cfg(test)]
    pub fn new_debug(time: fn() -> u64, random: fn() -> u32) -> Self {
        ObjectIdGenerator {
            counter: AtomicU32::new(random()),
            time,
            random,
        }
    }

    pub fn generate(&self) -> ObjectId {
        let time = ((self.time)() & 0xFFFFFFFF) as u32;
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let random: u32 = (self.random)();

        ObjectId::new(time, counter, random)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate() {
        let oidg = ObjectIdGenerator::new_debug(|| 123, || 100);

        let oid = oidg.generate();
        assert_eq!(oid.get_time(), 123);
        assert_eq!(oid.get_counter(), 100);
        assert_eq!(oid.get_rand(), 100);

        let oid = oidg.generate();
        assert_eq!(oid.get_time(), 123);
        assert_eq!(oid.get_counter(), 101);
        assert_eq!(oid.get_rand(), 100);

        let oid = oidg.generate();
        assert_eq!(oid.get_time(), 123);
        assert_eq!(oid.get_counter(), 102);
        assert_eq!(oid.get_rand(), 100);
    }
}
