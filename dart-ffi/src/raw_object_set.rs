use isar_core::error::Result;
use isar_core::object::object_builder::IsarObjectAllocator;
use isar_core::object::object_id::ObjectId;
use isar_core::query::query::Query;
use isar_core::txn::IsarTxn;
use std::{ptr, slice};

#[repr(C)]
pub struct RawObject {
    oid_time: u32,
    oid_counter: u32,
    oid_rand: u32,
    data: *const u8,
    data_length: u32,
    data_capacity: u32,
}

#[repr(C)]
pub struct RawObjectSend(pub &'static mut RawObject);

unsafe impl Send for RawObjectSend {}

impl RawObject {
    pub fn new(oid: ObjectId, object: &[u8]) -> Self {
        RawObject {
            oid_time: oid.get_time(),
            oid_counter: oid.get_counter(),
            oid_rand: oid.get_rand(),
            data: object as *const _ as *const u8,
            data_length: object.len() as u32,
            data_capacity: 0,
        }
    }

    pub fn set_object_id(&mut self, oid: ObjectId) {
        self.oid_time = oid.get_time();
        self.oid_counter = oid.get_time();
        self.oid_rand = oid.get_rand();
    }

    pub fn set_object(&mut self, object: &[u8]) {
        let data_length = object.len() as u32;
        let data = object as *const _ as *const u8;
        self.data = data;
        self.data_length = data_length;
    }

    pub fn object_as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.data_length as usize) }
    }

    pub fn get_object_id(&self) -> Option<ObjectId> {
        if self.oid_time != 0 {
            Some(ObjectId::new(
                self.oid_time,
                self.oid_counter,
                self.oid_rand,
            ))
        } else {
            None
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
    pub fn fill_from_query(&mut self, query: &Query, txn: &mut IsarTxn) -> Result<()> {
        let mut objects = vec![];
        query.find_while(txn, |oid, object| {
            objects.push(RawObject::new(*oid, object));
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
}

#[no_mangle]
pub extern "C" fn isar_alloc_raw_obj_buffer(object: &mut RawObject, size: u32) {
    assert_eq!((size as usize + ObjectId::get_size()) % 8, 0);
    let buffer = Vec::with_capacity_in(size as usize, IsarObjectAllocator {});
    let ptr = buffer.as_ptr();
    let capactity = buffer.capacity();
    std::mem::forget(buffer);
    object.data = ptr;
    object.data_length = size;
    object.data_capacity = capactity as u32;
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_raw_obj_buffer(object: &mut RawObject) {
    let object = Box::from_raw(object);
    Vec::from_raw_parts(
        object.data as *mut u8,
        object.data_length as usize,
        object.data_capacity as usize,
    );
}

#[no_mangle]
pub unsafe extern "C" fn isar_alloc_raw_obj_list(ros: &mut RawObjectSet, size: u32) {
    let mut ros = Box::from_raw(ros);
    let mut objects = Vec::with_capacity(size as usize);
    ros.objects = objects.as_mut_ptr();
    ros.length = objects.len() as u32;
    std::mem::forget(objects);
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_raw_obj_list(ros: &mut RawObjectSet) {
    let mut ros = Box::from_raw(ros);
    let mut objects = Vec::from_raw_parts(ros.objects, ros.length as usize, ros.length as usize);
    for object in &mut objects {
        isar_free_raw_obj_buffer(object)
    }
    ros.objects = ptr::null_mut();
    ros.length = 0;
}
