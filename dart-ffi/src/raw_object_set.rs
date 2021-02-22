use isar_core::error::Result;
use isar_core::object::isar_object::IsarObject;
use isar_core::query::Query;
use isar_core::txn::IsarTxn;
use std::{ptr, slice};

#[repr(C)]
pub struct RawObject {
    oid: i64,
    buffer: *mut u8,
    buffer_length: u32,
}

#[repr(C)]
pub struct RawObjectSend(pub &'static mut RawObject);

unsafe impl Send for RawObjectSend {}

impl RawObject {
    pub fn new() -> Self {
        RawObject {
            oid: i64::MIN,
            buffer: std::ptr::null_mut(),
            buffer_length: 0,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_bytes(&self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.buffer, self.buffer_length as usize) }
    }

    pub fn get_oid(&mut self) -> i64 {
        self.oid
    }

    pub fn set_oid(&mut self, oid: i64) {
        self.oid = oid;
    }

    pub fn set_object(&mut self, object: Option<IsarObject>) {
        if let Some(object) = object {
            let bytes = object.as_bytes();
            let buffer_length = bytes.len() as u32;
            let buffer = bytes as *const _ as *mut u8;
            self.buffer = buffer;
            self.buffer_length = buffer_length;
        } else {
            self.buffer = ptr::null_mut();
            self.buffer_length = 0;
        }
    }
}

#[repr(C)]
pub struct RawObjectSet {
    objects: *mut RawObject,
    length: u32,
}

#[repr(C)]
pub struct RawObjectSetSend(pub &'static mut RawObjectSet);

unsafe impl Send for RawObjectSetSend {}

impl RawObjectSet {
    pub fn fill_from_query(
        &mut self,
        query: &Query,
        txn: &mut IsarTxn,
        limit: usize,
    ) -> Result<()> {
        let mut objects = vec![];
        let mut count = 0;
        query.find_while(txn, |object| {
            let mut raw_obj = RawObject::new();
            raw_obj.set_object(Some(object));
            objects.push(raw_obj);
            count += 1;
            count < limit
        })?;

        self.fill_from_vec(objects);
        Ok(())
    }

    pub fn fill_from_vec(&mut self, objects: Vec<RawObject>) {
        let mut objects = objects.into_boxed_slice();
        self.objects = objects.as_mut_ptr();
        self.length = objects.len() as u32;
        std::mem::forget(objects);
    }

    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_objects(&self) -> &mut [RawObject] {
        std::slice::from_raw_parts_mut(self.objects, self.length as usize)
    }

    pub fn get_length(&self) -> usize {
        self.length as usize
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_raw_obj_list(ros: &mut RawObjectSet) {
    Vec::from_raw_parts(ros.objects, ros.length as usize, ros.length as usize);
    ros.objects = ptr::null_mut();
    ros.length = 0;
}
