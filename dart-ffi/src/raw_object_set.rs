use isar_core::error::Result;
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

    pub fn get_length(&self) -> u32 {
        self.data_length
    }

    pub fn clear(&mut self) {
        self.oid_time = 0;
        self.oid_counter = 0;
        self.oid_rand = 0;
        self.data = ptr::null();
        self.data_length = 0;
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
        query.find_all(txn, |oid, object| {
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

    pub unsafe fn clear(&mut self) {
        if !self.objects.is_null() {
            Vec::from_raw_parts(self.objects, self.length as usize, self.length as usize);
        }
        self.objects = ptr::null_mut();
        self.length = 0;
    }

    pub fn length(&self) -> u32 {
        self.length
    }
}

#[no_mangle]
pub extern "C" fn isar_alloc_raw_obj(size: u32) -> *mut RawObject {
    assert_eq!((size as usize + ObjectId::get_size()) % 8, 0);
    let padding = ObjectId::get_size() % 8;
    let buffer_size = size as usize + padding;
    let buffer = vec![0u8; buffer_size];
    let ptr = buffer[padding..].as_ptr();
    std::mem::forget(buffer);
    let raw_obj = RawObject {
        oid_time: 0,
        oid_counter: 0,
        oid_rand: 0,
        data: ptr,
        data_length: size,
    };
    Box::into_raw(Box::new(raw_obj))
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_raw_obj(object: &mut RawObject) {
    let object = Box::from_raw(object);
    let padding = ObjectId::get_size() % 8;
    let buffer_size = object.data_length as usize + padding;

    let data = object.data.sub(padding);
    Vec::from_raw_parts(data as *mut u8, buffer_size, buffer_size);
}

#[no_mangle]
pub extern "C" fn isar_alloc_raw_obj_set() -> *mut RawObjectSet {
    let raw_obj_set = RawObjectSet {
        objects: ptr::null_mut(),
        length: 0,
    };
    Box::into_raw(Box::new(raw_obj_set))
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_raw_obj_set(ros: &mut RawObjectSet) {
    let mut ros = Box::from_raw(ros);
    ros.clear();
}
