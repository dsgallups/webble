use core::ptr::NonNull;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering, fence};

use wasm_bindgen::prelude::*;
use web_sys::js_sys;

use crate::prelude::*;

/// A schedulable, worker-pinned task whose metadata is its owner worker id. This alias is canonical:
/// every `into_raw`/`from_raw` round-trip of a pinned task must use it, or the metadata type
/// mismatches (UB encounter).
pub type Run = async_task::Runnable<u32>;

/// A schedulable, stealable task (default `()` metadata. It has no owner, so any worker may poll it).
pub type Steal = async_task::Runnable;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = queueMicrotask)]
    fn queue_microtask(cb: &js_sys::Function);
}

thread_local! {
    static REARM: RefCell<Option<Closure<dyn FnMut()>>> = const { RefCell::new(None) };
}

/// Wake the worker `id` if it is parked in `Atomics.waitAsync` on its notify word. Producers MUST
/// push their work to the queue BEFORE calling this.
pub fn notify_worker(id: u32) {
    let slot = STATE.slot_for(id);
    slot.notify.0.fetch_add(1, Ordering::Release);
    let _addr = &slot.notify.0 as *const AtomicU32 as *mut i32;

    // SAFETY: `addr` points at a live i32 in shared linear memory; wake the (single) waiter.
    #[cfg(target_arch = "wasm32")]
    unsafe {
        core::arch::wasm32::memory_atomic_notify(_addr, 1);
    }
}

/// The `Int32Array` word index of worker `id`'s notify futex, for JS `Atomics.waitAsync`.
#[wasm_bindgen]
pub fn __notify_index(id: u32) -> u32 {
    let slot = STATE.slot_for(id);
    let addr = (&slot.notify.0 as *const AtomicU32).expose_provenance();
    (addr / 4) as u32
}

struct Micro {
    pending: VecDeque<PinnedPtr>,
    scheduled: bool,
    closure: Option<Closure<dyn FnMut()>>,
}

thread_local! {
    static MICRO: RefCell<Micro> = RefCell::new(Micro {
        pending: VecDeque::new(),
        scheduled: false,
        closure: None,
    });
}

/// Queue a woken pinned `Runnable` to run on this worker's microtask queue.
fn push_microtask(ptr: PinnedPtr) {
    MICRO.with(|m| {
        let mut m = m.borrow_mut();
        m.pending.push_back(ptr);
        if m.scheduled {
            return;
        }
        m.scheduled = true;
        if m.closure.is_none() {
            m.closure = Some(Closure::wrap(Box::new(drain_microtasks) as Box<dyn FnMut()>));
        }
        let f: &js_sys::Function = m.closure.as_ref().unwrap().as_ref().unchecked_ref();
        queue_microtask(f);
    });
}

/// Drain teh currently-pending pinned runnables. New reschedules produced while running are
/// picked up by a fresh microtask, so we always yield to the event loop between batches.
fn drain_microtasks() {
    let batch: Vec<PinnedPtr> = MICRO.with(|m| {
        let mut m = m.borrow_mut();
        m.scheduled = false;
        m.pending.drain(..).collect()
    });
    for ptr in batch {
        run_runnable_ptr(ptr);
    }
}

/// Reconstructed a pinned runnable from its raw pointer and poll it once. Called from
/// the microtask drain (hot path) and from `__worker_drain`'s ready queue (cross-worker-wake cold path).
pub fn run_runnable_ptr(ptr: PinnedPtr) {
    let nn = NonNull::new(core::ptr::with_exposed_provenance_mut::<()>(ptr.0 as usize))
        .expect("null runnable pointer");
    // SAFETY: `ptr` came from `Run::into_raw` in `schedule`; this is its unique consumer (a queued
    // microtask fires exactly once; a ready-queue entry is popped exactly once), and we are on the
    // owner worker. async-task keeps at most one live Runnable per task.
    let runnable = unsafe { Run::from_raw(nn) };
    runnable.run();
}

/// Reconstruct a stealable runnable from its raw pointer and poll it once. May run on
/// any worker as the underlying future is `Send`.
pub fn run_steal_ptr(ptr: StealPtr) {
    let nn = NonNull::new(core::ptr::with_exposed_provenance_mut::<()>(ptr.0 as usize))
        .expect("null stealable runnable pointer");
    // SAFETY: `ptr` came from `Steal::into_raw` in `schedule_stealable`; it is popped from exactly
    // one deque/injector slot, so this is its unique consumer.
    let runnable = unsafe { Steal::from_raw(nn) };
    runnable.run();
}

/// `async-task`'s schedule hook for pinned tasks.
///
/// Invoked at the first spawn and on every wake. Routes the runnable
/// to its owner worker. must be a plain `fn` (Send + Sync + 'static),
/// because in the cold path, it runs on a non-owner worker.
pub fn schedule(runnable: Run) {
    let owner = *runnable.metadata();
    let ptr = PinnedPtr(runnable.into_raw().as_ptr().expose_provenance() as u32);
    if current_raw() == Some(owner) {
        // Hot path: woken on the owner (a worker, or the main thread for an `on_main` task). Run
        // locally on the owner's microtask queue.
        push_microtask(ptr);
    } else {
        // Cold path: woken from another worker (e.g. a cross-worker channel send). Hand the
        // Runnable back to the owner — it is the only worker allowed to poll this future. Routed
        // through `slot_for` so a task owned by the main thread (`owner == MAIN_ID`, the case for
        // every `on_main` task, since the main thread keeps `thread_id() == None`) lands in the
        // main slot instead of indexing the worker array out of bounds.
        STATE.slot_for(owner).ready.lock().unwrap().push_back(ptr);
        notify_worker(owner);
    }
}

/// `async-task`'s schedule hook for stealable tasks.
///
/// Invoked at first spawn and on every wake. Pushes the runnable onto a
/// work-stealing queue. The current worker's local deque for locality when
/// we are on a worker, else the global injector (e.g. produced on the main thread).
/// Any worker may then pop and poll it. must also be a plain `fn` (Send + Sync + 'static).
pub fn schedule_stealable(runnable: Steal) {
    let ptr = StealPtr(runnable.into_raw().as_ptr().expose_provenance() as u32);
    match current_raw() {
        // On a worker: push to its local deque for locality. On the main thread (`MAIN_ID`) or
        // off-runtime: push to the global injector — the main thread has no stealable deque of its
        // own (it never runs stealable work), and its `local` half is never stolen from.
        Some(id) if id != MAIN_ID => STATE.slots()[id as usize].local.0.push(ptr),
        _ => STATE.injector().push(ptr),
    }
    // Order the push BEFORE reading the idle set. Paired with the worker's
    // set-idle-then-recheck in `__worker_drain` (a Dekker handshake over the `idle` bitmask and
    // the queues), this guarantees no lost wakeup: a worker about to park either is seen idle here
    // (and woken) or observes this work in its pre-park double-check (and re-arms instead).
    fence(Ordering::SeqCst);
    wake_one();
}

/// Mark a worker `id` as parked in the wake-one bitmask. `SeqCst` so that
/// it orders against a producer's `wake_one` read (see [`schedule_stealable`]).
pub fn idle_set(id: u32) {
    STATE.idle.fetch_or(1 << id, Ordering::SeqCst);
}

/// Mark worker `id` active (not idle) in the wake-one bitmask.
pub fn idle_clear(id: u32) {
    STATE.idle.fetch_and(!(1 << id), Ordering::SeqCst);
}

/// Wake one parked worker, if any, to claim freshly-pushed stealable work, claiming
/// it out of the idle set so a batch of pushes fans out across distinct workers instead
/// of dog-piling one.
///
/// If no worker is parked, it wakes none and returns. Every worker marks itself idle
/// and then re-checks the queues before actually parking (an implementation detail of
/// `__worker_drain`), so a worker still in-drain will catch the work, and the producer
/// need not wake it. The claim is a CAS-free `fetch_and`: if our clear is the one that
/// removed the bit, we own the wake; otherwise, another producer claimed that worker,
/// so we try the next.
fn wake_one() {
    let mut idle = STATE.idle.load(Ordering::SeqCst);
    while idle != 0 {
        let w = idle.trailing_zeros();
        let bit = 1u32 << w;
        if STATE.idle.fetch_and(!bit, Ordering::SeqCst) & bit != 0 {
            notify_worker(w);
            return;
        }
        // Lost the claim (someone else cleared it first) — drop it and try the next idle worker.
        idle &= !bit;
    }
}

/// Defer a self-wake to a microtask so the JS drain loop parks (`Atomics.waitAsync`),
/// yielding to pinned microtasks and JS callbacks, before re-entering `__worker_drain`
/// to take the next stealable task.
///
/// A synchronous `notify_worker(self)` would spin the loop with no `await` and
/// starve those, so the wake must run from a microtask.
pub fn rearm_self(worker_id: u32) {
    debug_assert_eq!(thread_id(), Some(worker_id), "rearm_self off its worker");
    REARM.with(|cell| {
        let mut cell = cell.borrow_mut();
        if cell.is_none() {
            // Reads `thread_id()` at fire time (constant per thread), so one cached closure serves
            // every re-arm on this worker without per-call allocation.
            *cell = Some(Closure::wrap(Box::new(|| {
                if let Some(id) = thread_id() {
                    notify_worker(id);
                }
            }) as Box<dyn FnMut()>));
        }
        let f: &js_sys::Function = cell.as_ref().unwrap().as_ref().unchecked_ref();
        queue_microtask(f);
    });
}
