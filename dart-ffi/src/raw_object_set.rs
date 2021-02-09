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
    oid_str_length: u32,
    oid_num: i64,
    buffer: *const u8,
    buffer_length: u32,
}

#[repr(C)]
pub struct RawObjectSend(pub &'static mut RawObject);

unsafe impl Send for RawObjectSend {}

impl RawObject {
    pub fn new() -> Self {
        RawObject {
            oid_num: 0,
            oid_str: std::ptr::null(),
            oid_str_length: 0,
            buffer: std::ptr::null(),
            buffer_length: 0,
        }
    }

    pub fn get_object_id(&self, col: &IsarCollection) -> Option<ObjectId<'static>> {
        match col.get_oid_type() {
            DataType::Int => {
                if self.oid_num == 0 {
                    None
                } else {
                    Some(col.new_int_oid(self.oid_num as i32))
                }
            }
            DataType::Long => {
                if self.oid_num == 0 {
                    None
                } else {
                    Some(col.new_long_oid(self.oid_num))
                }
            }
            DataType::String => unsafe {
                if self.oid_str.is_null() {
                    None
                } else {
                    let slice =
                        std::slice::from_raw_parts(self.oid_str, self.oid_str_length as usize);
                    Some(col.new_string_oid(std::str::from_utf8(slice).unwrap()))
                }
            },
            _ => unreachable!(),
        }
    }

    pub fn set_object_id(&mut self, oid: &ObjectId) {
        match oid.get_type() {
            DataType::Int => {
                self.oid_num = oid.get_int().unwrap() as i64;
                self.oid_str = ptr::null();
                self.oid_str_length = 0;
            }
            DataType::Long => {
                self.oid_num = oid.get_long().unwrap();
                self.oid_str = ptr::null();
                self.oid_str_length = 0;
            }
            DataType::String => {
                self.oid_num = 0;
                let bytes = oid.get_string().unwrap().as_bytes();
                self.oid_str = bytes.as_ptr();
                self.oid_str_length = bytes.len() as u32;
            }
            _ => unreachable!(),
        }
    }

    pub fn get_object(&self) -> IsarObject {
        let bytes = unsafe { slice::from_raw_parts(self.buffer, self.buffer_length as usize) };
        IsarObject::new(bytes)
    }

    pub fn set_object(&mut self, object: Option<IsarObject>) {
        if let Some(object) = object {
            let bytes = object.as_bytes();
            let buffer_length = bytes.len() as u32;
            let buffer = bytes as *const _ as *const u8;
            self.buffer = buffer;
            self.buffer_length = buffer_length;
        } else {
            self.buffer = ptr::null();
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
        query.find_while(txn, |oid, object| {
            let mut raw_obj = RawObject::new();
            raw_obj.set_object_id(&oid);
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
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_raw_obj_list(ros: &mut RawObjectSet) {
    Vec::from_raw_parts(ros.objects, ros.length as usize, ros.length as usize);
    ros.objects = ptr::null_mut();
    ros.length = 0;
}
