pub fn get_dir() -> Option<String> {
    dirs::config_dir()?.to_str()?.to_string()
}

pub fn get_app_id() -> Option<String> {
    String::new()
}
