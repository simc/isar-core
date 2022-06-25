use jni::objects::{JObject, JString, JValue};

pub fn get_dir() -> Option<String> {
    let android_context = ndk_context::android_context();
    let vm = unsafe { jni::JavaVM::from_raw(android_context.vm().cast()).ok()? };
    let env = vm.attach_current_thread().ok()?;
    let context = JObject::from(android_context.context().cast());
    let dir = env
        .call_method(*context, "getFilesDir", "()Ljava/io/File;", &[])
        .ok()?;
    if let JValue::Object(dir) = dir {
        let path = env
            .call_method(dir, "getPath", "()Ljava/lang/String;", &[])
            .ok()?;
        if let JValue::Object(path) = path {
            let j_str = JString::from(path);
            let java_str = env.get_string(j_str).ok()?;
            let str = java_str.to_str().ok()?;
            return Some(str.to_string());
        }
    }
    None
}

pub fn get_app_id() -> Option<String> {
    None
}
