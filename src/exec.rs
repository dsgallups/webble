use core::ptr::NonNull;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering, fence};

use wasm_bindgen::prelude::*;
use web_sys::js_sys;

use crate::prelude::*;

pub type Run = async_task::Runnable<u32>;

pub type Steal = async_task::Runnable;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = queueMicrotask)]
    fn queue_microtask(cb: &js_sys::Function);
}

/// The `Int32Array` word index of worker `id`'s notify futex, for JS `Atomics.waitAsync`.
#[wasm_bindgen]
pub fn __notify_index(id: u32) -> u32 {
    let slot = &STATE.slots()[id as usize];
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

pub fn run_runnable_ptr(ptr: PinnedPtr) {
    let nn = NonNull::new(core::ptr::with_exposed_provenance_mut::<()>(ptr.0 as usize))
        .expect("null runnable pointer");
    // SAFETY: `ptr` came from `Run::into_raw` in `schedule`; this is its unique consumer (a queued
    // microtask fires exactly once; a ready-queue entry is popped exactly once), and we are on the
    // owner worker. async-task keeps at most one live Runnable per task.
    let runnable = unsafe { Run::from_raw(nn) };
    runnable.run();
}

pub fn run_steal_ptr(ptr: StealPtr) {
    let nn = NonNull::new(core::ptr::with_exposed_provenance_mut::<()>(ptr.0 as usize))
        .expect("null stealable runnable pointer");
    // SAFETY: `ptr` came from `Steal::into_raw` in `schedule_stealable`; it is popped from exactly
    // one deque/injector slot, so this is its unique consumer.
    let runnable = unsafe { Steal::from_raw(nn) };
    runnable.run();
}

pub fn schedule(runnable: Run) {
    let owner = *runnable.metadata();
    let ptr = PinnedPtr(runnable.into_raw().as_ptr().expose_provenance() as u32);
    if thread_id() == Some(owner) {
        // Hot path: woken on the owner (e.g. a WebTransport JS callback). Run locally.
        push_microtask(ptr);
    } else {
        // Cold path: woken from another worker (e.g. a cross-worker channel send). Hand the
        // Runnable back to the owner — it is the only worker allowed to poll this future.
        STATE.slots()[owner as usize]
            .ready
            .lock()
            .unwrap()
            .push_back(ptr);
        notify_worker(owner);
    }
}

pub fn schedule_stealable(runnable: Steal) {
    let ptr = StealPtr(runnable.into_raw().as_ptr().expose_provenance() as u32);
    match thread_id() {
        Some(id) => STATE.slots()[id as usize].local.0.push(ptr),
        None => STATE.injector().push(ptr),
    }
    // Order the push BEFORE reading the idle set. Paired with the worker's
    // set-idle-then-recheck in `__worker_drain` (a Dekker handshake over the `idle` bitmask and
    // the queues), this guarantees no lost wakeup: a worker about to park either is seen idle here
    // (and woken) or observes this work in its pre-park double-check (and re-arms instead).
    fence(Ordering::SeqCst);
    wake_one();
}

pub fn idle_set(id: u32) {
    STATE.idle.fetch_or(1 << id, Ordering::SeqCst);
}

/// Mark worker `id` active (not idle) in the wake-one bitmask.
pub fn idle_clear(id: u32) {
    STATE.idle.fetch_and(!(1 << id), Ordering::SeqCst);
}

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

thread_local! {
    static REARM: RefCell<Option<Closure<dyn FnMut()>>> = const { RefCell::new(None) };
}

/// Wake the worker `id` if it is parked in `Atomics.waitAsync` on its notify word. Producers MUST
/// push their work to the queue BEFORE calling this.
pub fn notify_worker(id: u32) {
    let slot = &STATE.slots()[id as usize];
    slot.notify.0.fetch_add(1, Ordering::Release);
    let _addr = &slot.notify.0 as *const AtomicU32 as *mut i32;

    // SAFETY: `addr` points at a live i32 in shared linear memory; wake the (single) waiter.
    #[cfg(target_arch = "wasm32")]
    unsafe {
        core::arch::wasm32::memory_atomic_notify(_addr, 1);
    }
}
