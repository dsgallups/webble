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

    idle_clear(worker_id);

    loop {
        let ptr = slot.ready.lock().unwrap().pop_front();
        match ptr {
            Some(ptr) => run_runnable_ptr(ptr),
            None => break,
        }
    }

    loop {
        let pending = slot.incoming.lock().unwrap().pop_front();
        match pending {
            Some(p) => p.run(worker_id),
            None => break,
        }
    }

    if let Some(ptr) = find_stealable_work(worker_id, slot) {
        run_steal_ptr(ptr);
    }

    idle_set(worker_id);
    fence(Ordering::SeqCst);
    if !slot.local.0.is_empty() || !STATE.injector().is_empty() {
        idle_clear(worker_id);
        rearm_self(worker_id);
    }

    true
}

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
