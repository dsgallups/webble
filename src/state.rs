use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::{Mutex, OnceLock};

use crossbeam_deque::{Injector, Stealer, Worker};

/// Lives in our WASM linear memory (a `SharedArrayBuffer` when compiled with `+atomics`),
/// so every worker sees this struct.
pub(crate) static STATE: SharedState = SharedState {
    slots: OnceLock::new(),
    injector: OnceLock::new(),
    idle: AtomicU32::new(0),
    shutdown: AtomicBool::new(false),
};

pub struct SharedState {
    /// Per-worker slots. It's sized a single time in `ThreadPool::new`.
    pub slots: OnceLock<Box<[Slot]>>,
    /// The global injector for stealable work produced off a worker,
    /// and the fallback a worker drains when its own deque and its siblings'
    /// are empty. Sized alongside `slots`.
    pub injector: OnceLock<Injector<StealPtr>>,
    /// Bitmask of **parked** workers. bit `i` set means that worker `i` is idle in
    /// `Atomics.waitAsync`. That worker is waiting for a notify to re-enter
    /// `__worker_drain`. `schedule_stealable` drives this bit. A producer
    /// will wake a single parked worker (claiming its bit). Supports
    /// up to 32 workers. See `__worker_drain` to see checks on a lost-wakeup race.
    pub idle: AtomicU32,
    pub shutdown: AtomicBool,
}

impl SharedState {
    /// the per-worker slots. Panics if the pool has not been initialized.
    pub fn slots(&'static self) -> &'static [Slot] {
        self.slots.get().expect("thread pool not initialized")
    }

    /// The global stealable injector. Panics if the pool has not been initialized.
    pub fn injector(&'static self) -> &'static Injector<StealPtr> {
        self.injector.get().expect("thread pool not initialized")
    }
}

/// a 64-byte aligned wrapper to keep per-worker atomics off each other's cache lines.
#[repr(align(64))]
pub struct Padded<T>(pub T);

/// A woken **pinned**, `?Send` `Runnable<u32>` raw pointer, handed back
/// to owner worker's [`Slot::ready`]. Distinct from [`StealPtr`] so that
/// the two types can never be mixed up.
#[derive(Clone, Copy)]
pub struct PinnedPtr(pub u32);

/// A stealable, `Send` `Runnable<()>` raw pointer. lives in the per-worker
/// deques ([`Slot::local`]/[`Slot::stealer"]) and the global [`SharedState::injector`].
///
/// Any worker may pop and poll this.
#[derive(Clone, Copy)]
pub struct StealPtr(pub u32);

/// The owner half of a worker's stealable deque.
pub(crate) struct LocalDeque(pub Worker<StealPtr>);

/// SAFETY:
///
/// `crossbeam_deque::Worker` is `Send + !Sync`: it is meant to be driven
/// by a single thread. We store is inside the shared [`Slot`] slice (`&'static`), which
/// requires `Sync`.
/// `slot.local` is only ever touched by the worker whose `thread_id == slot index`. It
/// pushes/pops its own deque and uses it as the dest of steals.
///
/// Every *other* worker touches only [`Slot::stealer`] (which is genuinely `Send + Sync`).
/// `__worker_drain` enforces the single-owner discipline by indexing `slots[worker_id]`.
///
// this mirrors Forte's per-seat `unsafe impl Sync fo Seats`.
unsafe impl Sync for LocalDeque {}

/// A unit of `?Send` work destined for a specific worker via [`Slot::incoming`].
pub struct PendingSpawn(Box<dyn FnOnce(u32) + 'static>);

/// # Safety
/// The boxed closure is invoked is invoked only on the owner worker, with that worker's
/// id, and is responsible for performing the `async_task` spawn + first poll + detach.
///
/// The future it captures is `?Send`;
///
/// It crosses thread boundaries exactly once here (dispatcher -> owner) and is never
/// polled anywhere but the owner thereafter. Stealable `Send` work *does not* use this type.
/// It lives in the deques as raw [`StealPtr`]s.
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

/// Per-worker scheduling state. Lives in the shared boxed slice, so every worker
/// that instantiates the same module+memory sees the same slots at stable addresses.
pub struct Slot {
    /// Pinned work placed on this worker, not yet spawned.
    pub(crate) incoming: Mutex<VecDeque<PendingSpawn>>,
    /// Woken `Runnable<u32>` raw pointers handed back from another worker (cross-worker wake cold path).
    pub(crate) ready: Mutex<VecDeque<PinnedPtr>>,
    /// Owner half of this worker's stealable deque. Pushed/popped only by this worker. other workers
    /// steal this through [`Slot::stealer`]. See [`LocalDeque`].
    pub(crate) local: LocalDeque,
    /// Steal half of this worker's stealable deque. Shared: any worker may steal from it.
    pub stealer: Stealer<StealPtr>,
    /// Number of live **pinned** futures owned by this worker. It serves as the least-loaded
    /// placement metric.
    ///
    /// Stealable futures do not count here (they self-balance via stealing)
    pub load: Padded<AtomicU32>,
    /// `Atomics.waitAsync` futex word. Producers bump it then `memory.atomic.notify`.
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
