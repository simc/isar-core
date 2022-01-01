use std::ffi::c_void;

pub type DartPort = i64;

pub type DartFinalizerCallback = unsafe extern "C" fn(
    isolate_callback_data: *mut c_void,
    peer: *mut c_void
);
pub type DartHandleFinalizer = Option<DartFinalizerCallback>;

pub type DartHandle = *mut c_void;

pub type DartFinalizableHandle = *mut c_void;

#[link(name = "dart")]
extern "C" {
    pub fn Dart_InitializeApiDL(data: *mut c_void) -> usize;
    pub fn Dart_NewFinalizableHandle_DL(
        handle: DartHandle,
        peer: *mut c_void,
        external_allocation_size: usize,
        callback: DartHandleFinalizer,
    ) -> DartFinalizableHandle;
    pub fn Dart_DeleteFinalizableHandle(
         object: DartFinalizableHandle,
         strong_ref_to_object: DartHandle
    );
    pub fn Dart_PostInteger_DL(
         port_id: DartPort,
         message: i64
    ) -> bool;
}

#[no_mangle]
pub unsafe extern "C" fn isar_connect_dart_api(data: *mut c_void) {
    Dart_InitializeApiDL(data);
}
