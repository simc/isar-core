pub type WatcherCallback = Box<dyn Fn() + Send + Sync + 'static>;

pub(super) struct Watcher {
    id: usize,
    callback: WatcherCallback,
}

impl Watcher {
    pub fn new(id: usize, callback: WatcherCallback) -> Self {
        Watcher { id, callback }
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub fn notify(&self) {
        (*self.callback)()
    }
}
