use jni::objects::{JObject, JString, JValue};
use jni::JNIEnv;

fn get_dir() -> Option<String> {
    let android_context = ndk_context::android_context();
    let vm = unsafe { jni::JavaVM::from_raw(android_context.vm().cast()) }?;
    let env = vm.attach_current_thread()?;
    let context = jni::objects::JObject::from(android_context.context().cast());
    let dir = env
        .call_method(*context, "getFilesDir", "()Ljava/io/File;", &[])
        .ok()?;
    let path = env
        .call_method(dir, "getPath", "()Ljava/lang/String;", &[])
        .ok()?;
    if let JValue::Object(obj) = path {
        let j_str = JString::from(obj);
        let java_str = env.get_string(j_str).ok()?;
        let str = java_str.to_str().ok()?;
        return Some(str.to_string());
    }
    None
}

pub fn get_app_id() -> Option<String> {
    None
}
