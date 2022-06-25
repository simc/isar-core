use std::env::current_exe;
use std::path::PathBuf;

#[cfg(any(target_os = "ios", target_os = "macos"))]
mod apple;
#[cfg(any(target_os = "ios", target_os = "macos"))]
use crate::apple::*;

#[cfg(target_os = "android")]
mod android;
#[cfg(target_os = "android")]
use crate::android::*;

#[cfg(not(any(target_os = "ios", target_os = "macos", target_os = "android")))]
mod other;
#[cfg(not(any(target_os = "ios", target_os = "macos", target_os = "android")))]
use crate::other::*;

pub fn get_app_dir() -> Option<String> {
    let mut dir = PathBuf::from(get_dir()?);
    if let Some(app_id) = get_app_id() {
        if app_id.is_empty() {
            let exe_path = current_exe().ok()?;
            let exe = exe_path.iter().last()?.to_str()?;
            dir.push(exe);
        } else {
            dir.push(&app_id);
        }
    }
    Some(dir.to_str()?.to_string())
}
