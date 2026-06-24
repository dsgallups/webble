use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};

/// Lives in our WASM linear memory (a `SharedArrayBuffer` when compiled with `+atomics`),
/// so every worker sees this struct.
pub(crate) static STATE: SharedState = SharedState {
    slots: OnceLock::new(),
    main_slot: OnceLock::new(),
    injector: OnceLock::new(),
    idle: AtomicU32::new(0),
    state: AtomicU8::new(Lifecycle::Uninit as u8),
};

/// The runtime's lifecycle.
///
/// `Uninit → Running` on [`init`](crate::WebbleBuilder::init), `Running → Shutdown` on
/// [`shutdown`](crate::shutdown), and `Shutdown → Running` again on a fresh `init`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Lifecycle {
    /// Never initialized (or reset). No workers exist.
    Uninit = 0,
    /// `init` succeeded; workers are live and accepting work.
    Running = 1,
    /// `shutdown` was called; workers are terminated. A fresh `init` restarts the runtime.
    Shutdown = 2,
}

/// Sentinel worker id for the **main thread**. The main thread participates on the pinned
/// track only (it runs `on_main` closures), so it is addressed through [`SharedState::main_slot`]
/// rather than the `slots` array. Chosen as `u32::MAX` so it can never collide with a real worker
/// index (capped at 32).
pub const MAIN_ID: u32 = u32::MAX;

pub struct SharedState {
    /// Per-worker slots. It's sized a single time in `Webble::builder().init()`.
    pub slots: OnceLock<Box<[Slot]>>,
    /// The main thread's pinned-track slot. Kept separate from `slots` so worker-only machinery
    /// (`pick_worker`, the steal loops, the 32-bit idle bitmask) never has to special-case it.
    /// Addressed via [`MAIN_ID`].
    pub main_slot: OnceLock<Slot>,
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
    /// The runtime [`Lifecycle`], stored as its `u8` discriminant. Drives the double-init guard, the
    /// worker/main drain stop signal, and restart.
    pub state: AtomicU8,
}

impl SharedState {
    /// the per-worker slots. Panics if the pool has not been initialized.
    pub fn slots(&'static self) -> &'static [Slot] {
        self.slots.get().expect("thread pool not initialized")
    }

    /// The current [`Lifecycle`] state.
    pub fn lifecycle(&self) -> Lifecycle {
        match self.state.load(Ordering::Acquire) {
            0 => Lifecycle::Uninit,
            1 => Lifecycle::Running,
            _ => Lifecycle::Shutdown,
        }
    }

    /// Whether the runtime is shutting down. The worker and main drain loops poll this to stop.
    pub fn is_shutdown(&self) -> bool {
        self.state.load(Ordering::Acquire) == Lifecycle::Shutdown as u8
    }

    /// The global stealable injector. Panics if the pool has not been initialized.
    pub fn injector(&'static self) -> &'static Injector<StealPtr> {
        self.injector.get().expect("thread pool not initialized")
    }

    /// Resolve a worker id (or [`MAIN_ID`]) to its scheduling slot. The futex wake path
    /// (`notify_worker`, `__notify_index`) routes through here so the main thread is woken
    /// exactly like a worker.
    pub fn slot_for(&'static self, id: u32) -> &'static Slot {
        if id == MAIN_ID {
            self.main_slot.get().expect("main slot not initialized")
        } else {
            &self.slots()[id as usize]
        }
    }
}

/// Clear all per-slot queues/counters, the main slot, the global injector, and the idle bitmask,
/// returning the executor to a pristine state.
///
/// Used by a restart (reusing the existing `OnceLock` slots) and by the test harness. Any leftover
/// `async_task` allocations are leaked, which is fine(?) here. They are never re-run.
pub(crate) fn clear_runtime_state() {
    for slot in STATE.slots() {
        slot.incoming.lock().unwrap().clear();
        slot.ready.lock().unwrap().clear();
        while slot.local.0.pop().is_some() {}
        slot.load.0.store(0, Ordering::Release);
        slot.notify.0.store(0, Ordering::Release);
        slot.busy.0.store(false, Ordering::Release);
    }
    let main = STATE.slot_for(MAIN_ID);
    main.incoming.lock().unwrap().clear();
    main.ready.lock().unwrap().clear();
    while main.local.0.pop().is_some() {}
    main.load.0.store(0, Ordering::Release);
    main.notify.0.store(0, Ordering::Release);
    while !matches!(STATE.injector().steal(), Steal::Empty) {}
    STATE.idle.store(0, Ordering::Release);
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
pub struct LocalDeque(pub Worker<StealPtr>);

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
    pub incoming: Mutex<VecDeque<PendingSpawn>>,
    /// Woken `Runnable<u32>` raw pointers handed back from another worker (cross-worker wake cold path).
    pub ready: Mutex<VecDeque<PinnedPtr>>,
    /// Owner half of this worker's stealable deque. Pushed/popped only by this worker. other workers
    /// steal this through [`Slot::stealer`]. See [`LocalDeque`].
    pub local: LocalDeque,
    /// Steal half of this worker's stealable deque. Shared: any worker may steal from it.
    pub stealer: Stealer<StealPtr>,
    /// Number of live **pinned** futures owned by this worker. It serves as the least-loaded
    /// placement metric.
    ///
    /// Stealable futures do not count here (they self-balance via stealing)
    pub load: Padded<AtomicU32>,
    /// `Atomics.waitAsync` futex word. Producers bump it then `memory.atomic.notify`.
    pub notify: Padded<AtomicU32>,
    /// `true` while this worker is inside `__worker_drain` actively touching the shared deques, and
    /// `false` at a safe point (parked, or stopped on shutdown). [`shutdown`](crate::shutdown) waits
    /// for every slot to read `false` before calling `Worker.terminate()`, so a worker is never
    /// killed mid-deque-op (which would corrupt the shared work-stealing structures, reused across
    /// restarts). See the Dekker handshake in `__worker_drain` / `shutdown`.
    pub busy: Padded<AtomicBool>,
}

impl Slot {
    /// Build a fresh slot. Not `const`: `crossbeam_deque::Worker::new_lifo()` allocates. Slots are
    /// only ever constructed at runtime (in `WebbleBuilder::init`/`test_reset`), so this is fine.
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
            busy: Padded(AtomicBool::new(false)),
        }
    }
}
