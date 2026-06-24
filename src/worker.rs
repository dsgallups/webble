use std::cell::Cell;
use std::sync::atomic::{Ordering, fence};

use crossbeam_deque::Steal;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::Worker;

use crate::{
    exec::{idle_clear, idle_set, rearm_self, run_runnable_ptr, run_steal_ptr},
    state::{MAIN_ID, STATE, Slot, StealPtr},
};

/// The runtime thread the current code is running on.
///
/// Every thread that participates in the runtime has an identity: the [`Main`](ThreadId::Main) thread
/// (the one that called `init`, which runs [`on_main`](crate::on_main) work) and each
/// [`Worker`](ThreadId::Worker). [`current_thread`] returns `None` only for a thread that is *not*
/// part of the runtime at all — e.g. before `init`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum ThreadId {
    /// The main thread.
    Main,
    /// Worker number `0..num_workers`.
    Worker(u32),
}

impl ThreadId {
    /// The raw scheduling id: a worker index, or [`MAIN_ID`] for the main thread.
    pub(crate) fn to_raw(self) -> u32 {
        match self {
            ThreadId::Main => MAIN_ID,
            ThreadId::Worker(i) => i,
        }
    }
}

thread_local! {
    static THREAD: Cell<Option<ThreadId>> = const { Cell::new(None) };
}

/// Record the identity of the current runtime thread. Called on each drain entry — `__worker_drain`
/// for a worker, `start_main_loop` for main.
///
/// In production, a thread is only ever one identity, so this is idempotent.
///
/// However, the test harness reuses the main thread as virtual worker 0 by relabelling it here,
/// which is why it overwrites rather than writing once.
pub(crate) fn set_thread_id(thread: ThreadId) {
    THREAD.with(|v| v.set(Some(thread)));
}

/// The current runtime thread, or `None` if this thread is not part of the runtime (e.g. before
/// `init`, or a thread that is neither a webble worker nor the `init` caller).
pub fn current_thread() -> Option<ThreadId> {
    THREAD.with(|v| v.get())
}

/// The current **worker index**, or `None` on the main thread / off-runtime. This is the
/// worker-flavored view of [`current_thread`]; use that to tell the main thread apart from a
/// non-runtime thread.
#[cfg(debug_assertions)]
pub fn thread_id() -> Option<u32> {
    match current_thread() {
        Some(ThreadId::Worker(i)) => Some(i),
        _ => None,
    }
}

/// The raw scheduling id (worker index or [`MAIN_ID`]) of the current thread. Internal: the
/// schedulers use it to decide whether a runnable can run locally.
pub(crate) fn current_raw() -> Option<u32> {
    current_thread().map(ThreadId::to_raw)
}

pub struct ThreadWorker {
    pub(crate) id: u32,
    pub(crate) inner: Worker,
}
impl ThreadWorker {
    pub fn id(&self) -> u32 {
        self.id
    }
}

/// Drain all currently-available work for this worker.
///
/// **Non-blocking**: the worker never parks the thread here. The idle
/// wait lives in JS (`Atomics.waitAsync`), which keeps the event loop
/// alive so already-spawned futures and their JS callbacks keep progressing.
///
///
/// ## Order:
/// 1. pinned runnables woken from another worker
/// 2. pinned work newly placed on this worker
/// 3. one stealable task (local deque -> steal from a sibling -> global injector), polled one.
///    on `Pending` a stealable task re-queues itself via its waker, so it may migrate to another
///    worker; if more stealable work is locally available, we re-arm deferred to take it the following
///    tick.
///
/// Returns `false` to signal a shutdown (the JS loop should stop).
///
/// ## WARNING
///
/// **NEVER** call this from the main thread.
#[wasm_bindgen]
pub fn __worker_drain(worker_id: u32) -> bool {
    set_thread_id(ThreadId::Worker(worker_id));

    if STATE.is_shutdown() {
        return false;
    }

    let slots = STATE.slots();
    let slot = &slots[worker_id as usize];

    // Quiescence handshake with `shutdown()`. Mark ourselves busy BEFORE touching the shared
    // deques, then re-check shutdown. The `SeqCst` fence pairs with `shutdown`'s store-Shutdown-
    // then-read-busy: by the single total order, either we observe the shutdown here (and bail
    // without touching the deques) or `shutdown` observes our `busy` flag (and waits for us to
    // reach a safe point). This is what stops `Worker.terminate()` from killing us mid-deque-op and
    // corrupting the shared work-stealing structures, which are reused across restarts.
    slot.busy.0.store(true, Ordering::SeqCst);
    fence(Ordering::SeqCst);
    if STATE.is_shutdown() {
        slot.busy.0.store(false, Ordering::SeqCst);
        return false;
    }

    // We are actively draining now and not parekd. Drop ourselves
    // from the idle set so a stealable producer's `wake_one` doesn't waste
    // its wake on us.
    idle_clear(worker_id);

    // these are the pinned runnables handed to us by another worker's schedule (a cross-worker wake).
    loop {
        let ptr = slot.ready.lock().unwrap().pop_front();
        match ptr {
            Some(ptr) => run_runnable_ptr(ptr),
            None => break,
        }
    }

    // this is newly placed pinned work. we spawn each of these on ourselves.
    loop {
        let pending = slot.incoming.lock().unwrap().pop_front();
        match pending {
            Some(p) => p.run(worker_id),
            None => break,
        }
    }

    // for stealable work, we take one runnable task and poll it once.
    //
    // Batch size 1 + a deferred re-arm is our fairness mechanism. It lets
    // sibling workers claim their share between our polls instead of one worker
    // draining the whole queue.
    if let Some(ptr) = find_stealable_work(worker_id, slot) {
        run_steal_ptr(ptr);
    }

    // We prepare to return to the JS loop, which will park us in `Atomics.waitAsync`.
    // We mark ourselves idle FIRST, and then re-check the queues before sleeping.
    // This is the worker half of the Dekker handhskae with `schedule_stealable`
    // (which pushes, then reads the idle set). if work is still avalable,
    // left over after the poll above, or pushed concurrently while we were deciding
    // to park, we CANNOT PARK. we have to clear our idle bit and re-arm via a
    // deferred self-wake so we re-drain the next tick.
    //
    // The `SeqCst` fence order our idle-set before the queue reads, closing
    // the lost-wakeup window.
    idle_set(worker_id);
    fence(Ordering::SeqCst);
    if !slot.local.0.is_empty() || !STATE.injector().is_empty() {
        idle_clear(worker_id);
        rearm_self(worker_id);
    }

    // Reached a safe point: about to return to the JS loop and park in `Atomics.waitAsync`, no
    // longer touching the deques. Release our `busy` flag so `shutdown` may terminate us.
    slot.busy.0.store(false, Ordering::SeqCst);

    true
}

/// Find one stealable task for `worker_id`. We check our own local
/// deque first (this doesn't have any contention).
///
/// If there is nothing here, we will steal a batch from a sibling
/// into our local deque. Otherwise, we pull from the global injector.
/// Stolen batches land in our local deque so subsequent ticks pop them cheaply.
fn find_stealable_work(worker_id: u32, slot: &Slot) -> Option<StealPtr> {
    if let Some(ptr) = slot.local.0.pop() {
        return Some(ptr);
    }

    let slots = STATE.slots();
    let n = slots.len();
    // Rotate the start so workers don't all hammer slot 0.
    for k in 1..n {
        let j = (worker_id as usize + k) % n;
        loop {
            match slots[j].stealer.steal_batch_and_pop(&slot.local.0) {
                Steal::Success(ptr) => return Some(ptr),
                Steal::Retry => continue,
                Steal::Empty => break,
            }
        }
    }

    loop {
        match STATE.injector().steal_batch_and_pop(&slot.local.0) {
            Steal::Success(ptr) => return Some(ptr),
            Steal::Retry => continue,
            Steal::Empty => return None,
        }
    }
}
