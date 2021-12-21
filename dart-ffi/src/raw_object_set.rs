use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::object::isar_object::IsarObject;
use isar_core::query::Query;
use isar_core::txn::IsarTxn;
use std::{ptr, slice};

#[repr(C)]
pub struct RawObject {
    id: i64,
    buffer: *mut u8,
    buffer_length: u32,
}

unsafe impl Send for RawObject {}

impl RawObject {
    pub fn new() -> Self {
        RawObject {
            id: i64::MIN,
            buffer: std::ptr::null_mut(),
            buffer_length: 0,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_object(&self) -> IsarObject {
        let bytes = unsafe { slice::from_raw_parts(self.buffer, self.buffer_length as usize) };
        IsarObject::from_bytes(bytes)
    }

    pub fn get_id(&mut self) -> i64 {
        self.id
    }

    pub fn set_id(&mut self, id: i64) {
        self.id = id;
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

unsafe impl Send for RawObjectSet {}

impl RawObjectSet {
    pub fn fill_from_query(
        &mut self,
        query: &Query,
        txn: &mut IsarTxn,
        limit: usize,
    ) -> Result<()> {
        let mut objects = vec![];
        let mut count = 0;
        query.find_while(txn, |id, object| {
            let mut raw_obj = RawObject::new();
            raw_obj.set_id(id);
            raw_obj.set_object(Some(object));
            objects.push(raw_obj);
            count += 1;
            count < limit
        })?;

        self.fill_from_vec(objects);
        Ok(())
    }

    pub fn fill_from_link(
        &mut self,
        collection: &IsarCollection,
        txn: &mut IsarTxn,
        link_index: usize,
        backlink: bool,
        id: i64,
    ) -> Result<()> {
        let mut objects = vec![];
        collection.get_linked_objects(txn, link_index, backlink, id, |id, object| {
            let mut raw_obj = RawObject::new();
            raw_obj.set_id(id);
            raw_obj.set_object(Some(object));
            objects.push(raw_obj);
            true
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
