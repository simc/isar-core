use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::object::data_type::DataType;
use isar_core::object::isar_object::IsarObject;
use isar_core::object::object_id::ObjectId;
use isar_core::query::Query;
use isar_core::txn::IsarTxn;
use std::{ptr, slice};

#[repr(C)]
pub struct RawObject {
    oid_str: *const u8,
    oid_str_len: u32,
    oid_num: i64,
    data: *const u8,
    data_length: u32,
}

#[repr(C)]
pub struct RawObjectSend(pub &'static mut RawObject);

unsafe impl Send for RawObjectSend {}

impl RawObject {
    pub fn new() -> Self {
        RawObject {
            oid_num: 0,
            oid_str: std::ptr::null(),
            oid_str_len: 0,
            data: std::ptr::null(),
            data_length: 0,
        }
    }

    pub fn get_object_id(&self, col: &IsarCollection) -> Option<ObjectId> {
        if self.oid_str_len > 0 && self.oid_num > 0 {
            return None;
        }
        let oid_type = col.get_oid_type();
        let oid = match oid_type {
            DataType::Int => col.new_int_oid(self.oid_num as i32),
            DataType::Long => col.new_long_oid(self.oid_num),
            DataType::String => unsafe {
                let slice = std::slice::from_raw_parts(self.oid_str, self.oid_str_len as usize);
                col.new_string_oid(std::str::from_utf8(slice).unwrap())
            },
            _ => unreachable!(),
        };
        Some(oid)
    }

    pub fn reset_object_id(&mut self) {
        self.oid_num = i64::MAX;
        self.oid_str = std::ptr::null();
        self.oid_str_len = u32::MAX;
    }

    pub fn set_object_id(&mut self, oid: ObjectId) {
        self.reset_object_id();
        match oid.get_type() {
            DataType::Int => {
                self.oid_num = oid.get_int().unwrap() as i64;
                self.oid_str_len = 0;
            }
            DataType::Long => {
                self.oid_num = oid.get_long().unwrap();
                self.oid_str_len = 0;
            }
            DataType::String => {
                let str = oid.get_string().unwrap();
                self.oid_str = str.as_ptr();
                self.oid_str_len = str.len() as u32;
                self.oid_num = 0;
            }
            _ => unreachable!(),
        }
    }

    pub fn get_object(&self) -> IsarObject {
        let bytes = unsafe { slice::from_raw_parts(self.data, self.data_length as usize) };
        IsarObject::new(bytes)
    }

    pub fn set_object(&mut self, object: IsarObject) {
        let bytes = object.as_bytes();
        let data_length = bytes.len() as u32;
        let data = bytes as *const _ as *const u8;
        self.data = data;
        self.data_length = data_length;
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
            let mut raw_obj = RawObject::new();
            raw_obj.set_object_id(oid);
            raw_obj.set_object(object);
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
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_raw_obj_list(ros: &mut RawObjectSet) {
    Vec::from_raw_parts(ros.objects, ros.length as usize, ros.length as usize);
    ros.objects = ptr::null_mut();
    ros.length = 0;
}
