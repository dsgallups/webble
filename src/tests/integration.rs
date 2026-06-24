/// Real-worker integration tests
use std::collections::HashSet;
use wasm_bindgen_test::*;
use web_sys::js_sys;

use crate::{
    handle::WorkerHandle,
    spawn::AsyncFnMarker,
    state::Lifecycle,
    tests::{N, set_timeout},
    worker::{ThreadId, thread_id},
};

/// Reset the shared STATE (which also tears down any workers from a prior test), then start a fresh
/// real pool.
fn spawn_real_pool() {
    super::test_reset(N);
    crate::builder()
        .workers(N)
        .glue_path("/wasm-bindgen-test.js")
        .init()
        .expect("failed to spawn real workers");
}

async fn sleep_ms(ms: i32) {
    let mut executor = |resolve: js_sys::Function, _reject: js_sys::Function| {
        set_timeout(&resolve, ms);
    };
    let p = js_sys::Promise::new(&mut executor);
    _ = wasm_bindgen_futures::JsFuture::from(p).await;
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
    spawn_real_pool();
    let who = await_result(crate::spawn(thread_id)).await;
    assert!(
        who.is_some(),
        "work must run on a worker (thread_id set), not the main thread"
    );
    let ThreadId::Worker(who) = who.unwrap() else {
        panic!("thread_id is main!");
    };
    assert!((who as usize) < N, "thread_id out of range: {who:?}");
}

#[wasm_bindgen_test]
async fn on_main_runs_closure_on_the_main_thread() {
    spawn_real_pool();

    let h = crate::spawn(async {
        let on_worker = thread_id();
        let on_main = crate::on_main(|| async { thread_id() }).recv().await;
        (on_worker, on_main)
    });

    let (on_worker, on_main) = await_result(h).await;
    assert!(
        matches!(on_worker, Some(ThreadId::Worker(_))),
        "the spawning task must run on a worker, got {on_worker:?}"
    );
    assert_eq!(
        on_main,
        Some(Some(ThreadId::Main)),
        "the on_main closure must run on the main thread"
    );
}

#[wasm_bindgen_test]
async fn runtime_can_be_shut_down_and_restarted() {
    spawn_real_pool();
    assert_eq!(crate::lifecycle(), Lifecycle::Running);
    assert_eq!(await_result(crate::spawn(|| 1u32 + 1)).await, 2);

    // Shut down, then restart *without* `test_reset` — this drives the real `Shutdown → Running`
    // path that reuses the existing slots.
    crate::shutdown();
    assert!(crate::is_shutdown());
    assert_eq!(crate::lifecycle(), Lifecycle::Shutdown);

    crate::builder()
        .workers(N)
        .glue_path("/wasm-bindgen-test.js")
        .init()
        .expect("restart should succeed");
    assert_eq!(crate::lifecycle(), Lifecycle::Running);

    // Work must run on freshly-spawned workers after the restart.
    assert_eq!(await_result(crate::spawn(|| 40u32 + 2)).await, 42);
    let who = await_result(crate::spawn(thread_id)).await;
    assert!(who.is_some(), "restarted work must run on a worker");

    // `on_main` must still reach the main thread after a restart.
    let h = crate::spawn::<AsyncFnMarker, _>(|| async {
        crate::on_main(|| async { thread_id() }).recv().await
    });
    assert_eq!(await_result(h).await, Some(Some(ThreadId::Main)));
}

#[wasm_bindgen_test]
async fn real_workers_spread_placement_across_workers() {
    spawn_real_pool();

    let mut handles = Vec::new();
    let mut releases = Vec::new();
    for _ in 0..N {
        let (tx, rx) = async_channel::bounded::<()>(1);
        releases.push(tx);
        handles.push(crate::spawn::<AsyncFnMarker, _>(move || async move {
            _ = rx.recv().await;
            thread_id()
        }));
    }

    sleep_ms(50).await;
    for tx in releases {
        _ = tx.try_send(());
    }

    let mut seen: HashSet<u32> = HashSet::new();
    for h in handles {
        if let Some(w) = await_result(h).await {
            let ThreadId::Worker(w) = w else {
                panic!("Saw main thread!");
            };
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
    spawn_real_pool();
    let (tx, rx) = async_channel::bounded::<u32>(1);
    let h = crate::spawn(async move { rx.recv().await.unwrap() });

    sleep_ms(50).await;
    tx.try_send(777).unwrap();

    assert_eq!(await_result(h).await, 777);
}

#[wasm_bindgen_test]
async fn real_worker_runs_non_send_future() {
    spawn_real_pool();
    let (tx, rx) = async_channel::bounded::<u32>(1);
    let h = crate::spawn::<AsyncFnMarker, _>(|| async move {
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
    spawn_real_pool();

    let value = crate::spawn_stealable(async { 100u32 + 1 });
    assert_eq!(await_result(value).await, 101);

    let who = await_result(crate::spawn_stealable(async { thread_id() })).await;
    assert!(who.is_some(), "stolen work must run on a worker");
    let ThreadId::Worker(who) = who.unwrap() else {
        panic!("Found main thread");
    };
    assert!((who as usize) < N, "thread_id out of range: {who:?}");
}

#[wasm_bindgen_test]
async fn stealable_work_spreads_across_workers() {
    spawn_real_pool();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));

    let mut handles = Vec::new();
    let mut releases = Vec::new();
    for _ in 0..(2 * N) {
        let (tx, rx) = async_channel::bounded::<()>(1);
        releases.push(tx);
        let seen = seen.clone();
        handles.push(crate::spawn_stealable(async move {
            if let Some(w) = thread_id() {
                seen.lock().unwrap().insert(w);
            }
            // park so many tasks are in flight concurrently
            _ = rx.recv().await;
            if let Some(w) = thread_id() {
                // may have migrated to another worker on resume
                seen.lock().unwrap().insert(w);
            }
        }));
    }

    sleep_ms(50).await;
    for tx in releases {
        _ = tx.try_send(());
    }
    for h in handles {
        _ = await_result(h).await;
    }

    let distinct = seen.lock().unwrap().len();
    assert!(
        distinct >= 2,
        "stealable work should spread across workers; only saw {distinct} distinct"
    );
}

#[wasm_bindgen_test]
async fn stealable_future_parks_and_resumes_to_completion() {
    spawn_real_pool();
    let (tx, rx) = async_channel::bounded::<u32>(1);
    let h = crate::spawn_stealable(async move {
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
        first
            .map(|w| {
                let ThreadId::Worker(w) = w else {
                    panic!("found main thread!");
                };
                (w as usize) < N
            })
            .unwrap_or(false),
        "first-poll thread_id invalid: {first:?}"
    );
    assert!(
        second
            .map(|w| {
                let ThreadId::Worker(w) = w else {
                    panic!("found main thread!");
                };
                (w as usize) < N
            })
            .unwrap_or(false),
        "resume thread_id invalid: {second:?}"
    );
}
