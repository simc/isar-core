use std::hash::{Hash, Hasher};
use std::mem;

#[derive(Copy, Clone, Debug)]
#[repr(packed)]
pub struct ObjectId {
    prefix: u16,
    time: u32,    // big endian
    counter: u32, // big endian
    rand: u32,
}

impl ObjectId {
    pub const fn get_size() -> usize {
        mem::size_of::<ObjectId>()
    }

    pub fn new(time: u32, counter: u32, rand: u32) -> Self {
        ObjectId {
            prefix: 0,
            time: time.to_be(),
            counter: counter.to_be(),
            rand,
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> &Self {
        let (_, body, _) = unsafe { bytes.align_to::<Self>() };
        &body[0]
    }

    pub(crate) fn get_prefix(&self) -> u16 {
        let prefix = self.prefix;
        assert_ne!(prefix, 0);
        prefix
    }

    pub(crate) fn set_prefix(&mut self, prefix: u16) {
        assert_ne!(prefix, 0);
        self.prefix = prefix;
    }

    pub fn get_time(&self) -> u32 {
        self.time.to_be()
    }

    pub fn get_counter(&self) -> u32 {
        self.counter.to_be()
    }

    pub fn get_rand(&self) -> u32 {
        self.rand
    }

    #[inline]
    fn as_bytes_internal(&self) -> &[u8] {
        let bytes = unsafe {
            ::std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                ::std::mem::size_of::<Self>(),
            )
        };
        &bytes
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        let prefix = self.prefix;
        assert_ne!(prefix, 0);
        self.as_bytes_internal()
    }

    #[inline]
    pub(crate) fn as_bytes_without_prefix(&self) -> &[u8] {
        &self.as_bytes_internal()[2..]
    }
}

impl PartialEq for ObjectId {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.counter == other.counter && self.rand == other.rand
    }
}

impl Eq for ObjectId {}

impl Hash for ObjectId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.time);
        state.write_u32(self.counter);
        state.write_u32(self.rand);
    }
}

impl ToString for ObjectId {
    fn to_string(&self) -> String {
        hex::encode(self.as_bytes_without_prefix())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_as_bytes() {
        /*let mut oid = ObjectId::new(123, 222);
        assert_eq!(
            oid.as_bytes(99),
            &[99, 0, 0, 0, 0, 123, 222, 0, 0, 0, 0, 0, 0, 0]
        )*/
    }
}
