/// Virtual worker tests
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;
use web_sys::js_sys;

use crate::exec::{__notify_index, notify_worker};
use crate::pool::ThreadPool;
use crate::state::STATE;
use crate::tests::{
    N, YieldNow, flush_microtasks, memory_words, recv, recv_stealable, set_timeout,
};
use crate::worker::__worker_drain;

#[wasm_bindgen_test]
fn notify_index_is_distinct_per_worker() {
    let _pool = ThreadPool::for_test(N);
    let mut seen = std::collections::HashSet::new();
    for id in 0..N as u32 {
        let idx = __notify_index(id);
        assert!(
            idx > 0,
            "notify word index should be a real offset, got {idx}"
        );
        assert!(
            seen.insert(idx),
            "worker {id} shares notify word index {idx}"
        );
    }
}

#[wasm_bindgen_test]
fn notify_worker_bumps_its_own_word() {
    let _pool = ThreadPool::for_test(N);
    let arr = memory_words();

    let idx2 = __notify_index(2);
    let idx3 = __notify_index(3);
    let before2 = js_sys::Atomics::load(&arr, idx2).unwrap();
    let before3 = js_sys::Atomics::load(&arr, idx3).unwrap();

    notify_worker(2);

    assert_eq!(
        js_sys::Atomics::load(&arr, idx2).unwrap(),
        before2 + 1,
        "notify_worker(2) must increment worker 2's word"
    );
    assert_eq!(
        js_sys::Atomics::load(&arr, idx3).unwrap(),
        before3,
        "notify_worker(2) must not touch worker 3's word"
    );
}

#[wasm_bindgen_test]
fn available_parallelism_is_positive() {
    let cores = crate::available_parallelism();
    assert!(cores >= 1, "core count should be at least 1, got {cores}");
}

#[wasm_bindgen_test]
fn places_on_least_loaded_worker() {
    let pool = ThreadPool::for_test(N);
    let slots = STATE.slots();
    slots[0].load.0.store(5, Ordering::Release);
    slots[1].load.0.store(2, Ordering::Release);
    slots[2].load.0.store(9, Ordering::Release);
    slots[3].load.0.store(7, Ordering::Release);

    let _h = pool.spawn_local(|| async { 0u8 });

    assert_eq!(
        slots[1].incoming.lock().unwrap().len(),
        1,
        "work should be queued on the least-loaded worker (1)"
    );
    for i in [0usize, 2, 3] {
        assert_eq!(
            slots[i].incoming.lock().unwrap().len(),
            0,
            "worker {i} should have no queued work"
        );
    }
}

#[wasm_bindgen_test]
fn num_tasks_waiting_counts_pending_placements() {
    let pool = ThreadPool::for_test(N);
    assert_eq!(pool.num_tasks_waiting(), 0);
    let _a = pool.spawn_local(|| async { 1u8 });
    let _b = pool.spawn_local(|| async { 2u8 });
    assert_eq!(pool.num_tasks_waiting(), 2);
}

#[wasm_bindgen_test]
async fn delivers_immediate_result() {
    let pool = ThreadPool::for_test(N);
    let h = pool.spawn_local(|| async { 41u32 + 1 });
    __worker_drain(0);
    assert_eq!(recv(h).await, 42);
}

#[wasm_bindgen_test]
async fn yielding_future_completes_via_microtasks() {
    let pool = ThreadPool::for_test(N);
    let h = pool.spawn_local(|| async {
        YieldNow::new().await;
        YieldNow::new().await;
        7u32
    });
    __worker_drain(0);
    assert_eq!(recv(h).await, 7);
}

#[wasm_bindgen_test]
async fn non_send_value_held_across_await() {
    let pool = ThreadPool::for_test(N);
    let h = pool.spawn_local(|| async {
        let local = Rc::new(20u32);
        YieldNow::new().await;
        *local + 1
    });
    __worker_drain(0);
    assert_eq!(recv(h).await, 21);
}

#[wasm_bindgen_test]
async fn many_futures_run_concurrently_on_one_worker() {
    let pool = ThreadPool::for_test(N);
    let slots = STATE.slots();
    // Make every other worker look busy so all placements pick worker 0.
    for i in 1..N {
        slots[i].load.0.store(1000, Ordering::Release);
    }

    let mut handles = Vec::new();
    for n in 0..8u32 {
        handles.push(pool.spawn_local(move || async move {
            YieldNow::new().await;
            n * 2
        }));
    }
    assert_eq!(
        slots[0].load.0.load(Ordering::Relaxed),
        8,
        "all eight futures should be pinned to worker 0"
    );

    __worker_drain(0);
    for (n, h) in handles.into_iter().enumerate() {
        assert_eq!(recv(h).await, n as u32 * 2);
    }
    assert_eq!(
        slots[0].load.0.load(Ordering::Relaxed),
        0,
        "every LoadGuard should drop once its future completes"
    );
}

#[wasm_bindgen_test]
async fn load_returns_to_zero_after_completion() {
    let pool = ThreadPool::for_test(N);
    let h = pool.spawn_local(|| async {
        YieldNow::new().await;
        0u8
    });
    assert_eq!(
        STATE.slots()[0].load.0.load(Ordering::Relaxed),
        1,
        "placement should reserve load up front"
    );
    __worker_drain(0);
    let _ = recv(h).await;
    assert_eq!(
        STATE.slots()[0].load.0.load(Ordering::Relaxed),
        0,
        "completion should release the reserved load"
    );
}

#[wasm_bindgen_test]
async fn dropping_handle_does_not_cancel() {
    let pool = ThreadPool::for_test(N);
    let flag = Arc::new(AtomicBool::new(false));
    let f = flag.clone();
    let h = pool.spawn_local(move || async move {
        YieldNow::new().await;
        f.store(true, Ordering::SeqCst);
    });
    drop(h);

    __worker_drain(0);
    for _ in 0..10_000 {
        if flag.load(Ordering::SeqCst) {
            break;
        }
        flush_microtasks().await;
    }
    assert!(
        flag.load(Ordering::SeqCst),
        "a detached future must run even after its handle is dropped"
    );
}

#[wasm_bindgen_test]
async fn stealable_delivers_immediate_result() {
    let pool = ThreadPool::for_test(N);
    let h = pool.spawn_stealable(async { 41u32 + 1 });
    assert_eq!(recv_stealable(h).await, 42);
}

#[wasm_bindgen_test]
async fn stealable_yielding_future_completes_via_drains() {
    let pool = ThreadPool::for_test(N);
    let h = pool.spawn_stealable(async {
        YieldNow::new().await;
        YieldNow::new().await;
        7u32
    });
    assert_eq!(recv_stealable(h).await, 7);
}

#[wasm_bindgen_test]
async fn stealable_enqueues_until_drained() {
    let pool = ThreadPool::for_test(N);
    assert_eq!(pool.num_tasks_waiting(), 0);

    let h = pool.spawn_stealable(async { 5u8 });
    assert_eq!(
        pool.num_tasks_waiting(),
        1,
        "a freshly spawned stealable task waits in the injector"
    );

    assert_eq!(recv_stealable(h).await, 5);
    assert_eq!(
        pool.num_tasks_waiting(),
        0,
        "draining claims and completes it, emptying the queue"
    );
}

#[wasm_bindgen_test]
fn drain_marks_worker_idle_when_no_stealable_work() {
    let _pool = ThreadPool::for_test(N);
    __worker_drain(0);
    assert_eq!(
        STATE.idle.load(Ordering::SeqCst) & 1,
        1,
        "worker 0 should be marked idle after draining with nothing to do"
    );
}

#[wasm_bindgen_test]
fn schedule_stealable_wakes_exactly_one_idle_worker() {
    let pool = ThreadPool::for_test(N);
    let arr = memory_words();
    // workers 1, 2, 3 parked
    STATE.idle.store(0b1110, Ordering::SeqCst);

    let words = |i: u32| js_sys::Atomics::load(&arr, __notify_index(i)).unwrap();
    let before: Vec<i32> = (0..N as u32).map(words).collect();

    let _h = pool.spawn_stealable(async { 7u8 });

    let bumped: Vec<u32> = (0..N as u32)
        .filter(|&i| words(i) == before[i as usize] + 1)
        .collect();
    assert_eq!(
        bumped.len(),
        1,
        "wake-one must notify exactly one worker, but bumped {bumped:?}"
    );
    let w = bumped[0];
    assert!(
        (1..=3).contains(&w),
        "must wake a parked worker (1..=3), woke {w}"
    );
    assert_eq!(
        STATE.idle.load(Ordering::SeqCst) & (1 << w),
        0,
        "the woken worker must be claimed out of the idle set"
    );
}

#[wasm_bindgen_test]
fn schedule_stealable_with_no_idle_workers_wakes_none() {
    let pool = ThreadPool::for_test(N);
    let arr = memory_words();
    // nobody parked
    STATE.idle.store(0, Ordering::SeqCst);

    let words = |i: u32| js_sys::Atomics::load(&arr, __notify_index(i)).unwrap();
    let before: Vec<i32> = (0..N as u32).map(words).collect();

    let _h = pool.spawn_stealable(async { 1u8 });

    assert!(
        (0..N as u32).all(|i| words(i) == before[i as usize]),
        "with no idle workers, wake_one must bump no notify words"
    );
}

#[wasm_bindgen_test]
async fn waitasync_is_woken_by_notify_worker() {
    let _pool = ThreadPool::for_test(N);
    let arr = memory_words();
    let idx = __notify_index(1);
    let seen = js_sys::Atomics::load(&arr, idx).unwrap();

    // park asynchronously on worker 1's word at its current value.
    let res: JsValue = js_sys::Atomics::wait_async(&arr, idx, seen)
        .expect("waitAsync")
        .into();
    let is_async = js_sys::Reflect::get(&res, &JsValue::from_str("async"))
        .unwrap()
        .as_bool()
        .unwrap();
    assert!(
        is_async,
        "waitAsync should park (async=true) while the word still equals the snapshot"
    );
    let promise: js_sys::Promise = js_sys::Reflect::get(&res, &JsValue::from_str("value"))
        .unwrap()
        .unchecked_into();

    // fire the wake on a later macrotask, after we are already awaiting the promise.
    let cb = Closure::once(move || notify_worker(1));
    set_timeout(cb.as_ref().unchecked_ref(), 0);
    cb.forget();

    let value = wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .expect("waitAsync promise resolves");
    assert_eq!(
        value.as_string().as_deref(),
        Some("ok"),
        "notify_worker must wake the async waiter (resolve value 'ok')"
    );
    assert_eq!(
        js_sys::Atomics::load(&arr, idx).unwrap(),
        seen + 1,
        "the wake must be accompanied by the bumped word"
    );
}
