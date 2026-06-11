use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::{Mutex, OnceLock};

use crossbeam_deque::{Injector, Stealer, Worker};

pub(crate) static STATE: SharedState = SharedState {
    slots: OnceLock::new(),
    injector: OnceLock::new(),
    idle: AtomicU32::new(0),
    shutdown: AtomicBool::new(false),
};

pub struct SharedState {
    pub slots: OnceLock<Box<[Slot]>>,
    pub injector: OnceLock<Injector<StealPtr>>,
    pub idle: AtomicU32,
    pub shutdown: AtomicBool,
}

impl SharedState {
    pub fn slots(&'static self) -> &'static [Slot] {
        self.slots.get().expect("thread pool not initialized")
    }

    pub fn injector(&'static self) -> &'static Injector<StealPtr> {
        self.injector.get().expect("thread pool not initialized")
    }
}

#[repr(align(64))]
pub struct Padded<T>(pub T);

#[derive(Clone, Copy)]
pub struct PinnedPtr(pub u32);

#[derive(Clone, Copy)]
pub struct StealPtr(pub u32);

pub struct LocalDeque(pub Worker<StealPtr>);

unsafe impl Sync for LocalDeque {}

pub struct PendingSpawn(Box<dyn FnOnce(u32) + 'static>);

unsafe impl Send for PendingSpawn {}

impl PendingSpawn {
    pub fn new<F: FnOnce(u32) + 'static>(f: F) -> Self {
        Self(Box::new(f))
    }

    /// Run the thunk on the calling (owner) worker.
    pub fn run(self, worker_id: u32) {
        (self.0)(worker_id)
    }
}

pub struct Slot {
    pub incoming: Mutex<VecDeque<PendingSpawn>>,
    pub ready: Mutex<VecDeque<PinnedPtr>>,
    pub local: LocalDeque,
    pub stealer: Stealer<StealPtr>,
    pub load: Padded<AtomicU32>,
    pub notify: Padded<AtomicU32>,
}

impl Slot {
    /// Build a fresh slot. Not `const`: `crossbeam_deque::Worker::new_lifo()` allocates. Slots are
    /// only ever constructed at runtime (in `ThreadPool::new`/`for_test`), so this is fine.
    pub fn new() -> Self {
        let local = Worker::new_lifo();
        let stealer = local.stealer();
        Self {
            incoming: Mutex::new(VecDeque::new()),
            ready: Mutex::new(VecDeque::new()),
            local: LocalDeque(local),
            stealer,
            load: Padded(AtomicU32::new(0)),
            notify: Padded(AtomicU32::new(0)),
        }
    }
}
