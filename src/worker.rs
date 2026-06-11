use std::sync::{
    OnceLock,
    atomic::{Ordering, fence},
};

use crossbeam_deque::Steal;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::Worker;

use crate::{
    exec::{idle_clear, idle_set, rearm_self, run_runnable_ptr, run_steal_ptr},
    state::{STATE, Slot, StealPtr},
};

thread_local! {
    pub static THREAD_ID: OnceLock<u32> = const { OnceLock::new() };
}

pub fn thread_id() -> Option<u32> {
    THREAD_ID.with(|v| v.get().copied())
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
    THREAD_ID.with(|v| {
        let _ = v.set(worker_id);
    });

    if STATE.shutdown.load(Ordering::Acquire) {
        return false;
    }

    let slots = STATE.slots();
    let slot = &slots[worker_id as usize];

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
