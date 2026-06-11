/// Real-worker integration tests
use std::collections::HashSet;
use wasm_bindgen_test::*;
use web_sys::js_sys;

use crate::{
    ThreadPool,
    handle::WorkerHandle,
    spawn::AsyncFnMarker,
    tests::{N, set_timeout},
    worker::thread_id,
};

fn spawn_real_pool() -> ThreadPool {
    // reset the shared STATE
    drop(ThreadPool::for_test(N));
    ThreadPool::new_with_num_workers(N, "/wasm-bindgen-test.js")
        .expect("failed to spawn real workers")
}

async fn sleep_ms(ms: i32) {
    let mut executor = |resolve: js_sys::Function, _reject: js_sys::Function| {
        set_timeout(&resolve, ms);
    };
    let p = js_sys::Promise::new(&mut executor);
    let _ = wasm_bindgen_futures::JsFuture::from(p).await;
}

async fn await_result<T>(mut h: WorkerHandle<T>) -> T {
    for _ in 0..500 {
        if h.try_recv().is_some() {
            return h.into_inner().expect("result present after try_recv");
        }
        sleep_ms(10).await;
    }
    panic!("worker produced no result within the timeout");
}

#[wasm_bindgen_test]
async fn real_workers_execute_on_a_worker_thread() {
    let pool = spawn_real_pool();
    let who = await_result(pool.spawn(thread_id)).await;
    assert!(
        who.is_some(),
        "work must run on a worker (thread_id set), not the main thread"
    );
    assert!(
        (who.unwrap() as usize) < N,
        "thread_id out of range: {who:?}"
    );
}

#[wasm_bindgen_test]
async fn real_workers_spread_placement_across_workers() {
    let pool = spawn_real_pool();

    let mut handles = Vec::new();
    let mut releases = Vec::new();
    for _ in 0..N {
        let (tx, rx) = async_channel::bounded::<()>(1);
        releases.push(tx);
        handles.push(pool.spawn::<AsyncFnMarker, _>(move || async move {
            let _ = rx.recv().await;
            thread_id()
        }));
    }

    sleep_ms(50).await;
    for tx in releases {
        let _ = tx.try_send(());
    }

    let mut seen: HashSet<u32> = HashSet::new();
    for h in handles {
        if let Some(w) = await_result(h).await {
            seen.insert(w);
        }
    }
    assert!(
        seen.len() >= 2,
        "placement should spread across workers; only saw {seen:?}"
    );
}

#[wasm_bindgen_test]
async fn cross_worker_wake_from_main_thread() {
    let pool = spawn_real_pool();
    let (tx, rx) = async_channel::bounded::<u32>(1);
    let h = pool.spawn::<AsyncFnMarker, _>(move || async move { rx.recv().await.unwrap() });

    sleep_ms(50).await;
    tx.try_send(777).unwrap();

    assert_eq!(await_result(h).await, 777);
}

#[wasm_bindgen_test]
async fn real_worker_runs_non_send_future() {
    let pool = spawn_real_pool();
    let (tx, rx) = async_channel::bounded::<u32>(1);
    let h = pool.spawn::<AsyncFnMarker, _>(move || async move {
        let local = std::rc::Rc::new(5u32);
        let got = rx.recv().await.unwrap();
        *local + got
    });

    sleep_ms(50).await;
    tx.try_send(10).unwrap();

    assert_eq!(await_result(h).await, 15);
}

#[wasm_bindgen_test]
async fn stealable_work_is_claimed_and_runs() {
    let pool = spawn_real_pool();

    let value = pool.spawn_stealable(async { 100u32 + 1 });
    assert_eq!(await_result(value).await, 101);

    let who = await_result(pool.spawn_stealable(async { thread_id() })).await;
    assert!(who.is_some(), "stolen work must run on a worker");
    assert!(
        (who.unwrap() as usize) < N,
        "thread_id out of range: {who:?}"
    );
}

#[wasm_bindgen_test]
async fn stealable_work_spreads_across_workers() {
    let pool = spawn_real_pool();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));

    let mut handles = Vec::new();
    let mut releases = Vec::new();
    for _ in 0..(2 * N) {
        let (tx, rx) = async_channel::bounded::<()>(1);
        releases.push(tx);
        let seen = seen.clone();
        handles.push(pool.spawn_stealable(async move {
            if let Some(w) = thread_id() {
                seen.lock().unwrap().insert(w);
            }
            // park so many tasks are in flight concurrently
            let _ = rx.recv().await;
            if let Some(w) = thread_id() {
                // may have migrated to another worker on resume
                seen.lock().unwrap().insert(w);
            }
        }));
    }

    sleep_ms(50).await;
    for tx in releases {
        let _ = tx.try_send(());
    }
    for h in handles {
        let _ = await_result(h).await;
    }

    let distinct = seen.lock().unwrap().len();
    assert!(
        distinct >= 2,
        "stealable work should spread across workers; only saw {distinct} distinct"
    );
}

#[wasm_bindgen_test]
async fn stealable_future_parks_and_resumes_to_completion() {
    let pool = spawn_real_pool();
    let (tx, rx) = async_channel::bounded::<u32>(1);
    let h = pool.spawn_stealable(async move {
        let first = thread_id();
        // park until woken from the main thread
        let got = rx.recv().await.unwrap();
        let second = thread_id();
        (first, second, got)
    });

    sleep_ms(50).await;
    tx.try_send(55).unwrap();

    let (first, second, got) = await_result(h).await;
    assert_eq!(
        got, 55,
        "the parked stealable future must resume and complete"
    );
    assert!(
        first.map(|w| (w as usize) < N).unwrap_or(false),
        "first-poll thread_id invalid: {first:?}"
    );
    assert!(
        second.map(|w| (w as usize) < N).unwrap_or(false),
        "resume thread_id invalid: {second:?}"
    );
}
