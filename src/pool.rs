use std::sync::atomic::{AtomicU32, Ordering};

use crossbeam_deque::Injector;
use web_sys::{
    Worker, WorkerOptions, WorkerType,
    js_sys::{self, Array},
    wasm_bindgen::{self, JsValue},
};

use crate::prelude::*;

pub struct ThreadPool {
    workers: Vec<ThreadWorker>,
}

impl ThreadPool {
    pub fn new(num_workers: usize, worker_script_url: &str) -> Result<Self, JsValue> {
        assert!(
            num_workers <= 32,
            "ThreadPool supports at most 32 workers (the idle bitmask is a u32)"
        );
        STATE.shutdown.store(false, Ordering::Release);

        let slots: Box<[Slot]> = (0..num_workers).map(|_| Slot::new()).collect();
        let _ = STATE.slots.set(slots);
        let _ = STATE.injector.set(Injector::new());

        let module = wasm_bindgen::module();
        let memory = wasm_bindgen::memory();

        let opts = WorkerOptions::new();
        opts.set_type(WorkerType::Module);

        let mut workers = Vec::with_capacity(num_workers);

        for id in 0..num_workers {
            let worker = Worker::new_with_options(worker_script_url, &opts)?;

            let msg = Array::of3(&module, &memory, &JsValue::from(id as u32));
            worker.post_message(&msg)?;

            workers.push(ThreadWorker {
                id: id as u32,
                inner: worker,
            });
        }

        Ok(Self { workers })
    }
    pub fn with_available_parallelism(worker_script_url: &str) -> Result<Self, JsValue> {
        Self::new(available_parallelism(), worker_script_url)
    }

    pub fn spawn<M, S: Spawn<M>>(&self, work: S) -> S::Output {
        work.spawn(self)
    }

    pub fn spawn_stealable<M, S: SpawnStealable<M>>(&self, work: S) -> S::Output {
        work.spawn_stealable(self)
    }

    pub fn spawn_local<F, Fut, T>(&self, make: F) -> WorkerHandle<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        place_local(make)
    }

    pub fn shutdown(&mut self) {
        STATE.shutdown.store(true, Ordering::Release);
        if let Some(slots) = STATE.slots.get() {
            for i in 0..slots.len() as u32 {
                notify_worker(i);
            }
        }

        for worker in self.workers.drain(..) {
            worker.inner.terminate();
        }
    }

    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    pub fn num_tasks_waiting(&self) -> usize {
        let slots = STATE.slots();
        let pinned: usize = slots.iter().map(|s| s.incoming.lock().unwrap().len()).sum();
        let stealable: usize = slots.iter().map(|s| s.stealer.len()).sum();
        pinned + stealable + STATE.injector().len()
    }

    pub fn is_shutdown(&self) -> bool {
        STATE.shutdown.load(Ordering::Relaxed)
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if !self.workers.is_empty() {
            self.shutdown();
        }
    }
}

pub fn place_local<F, Fut, T>(make: F) -> WorkerHandle<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let (tx, rx) = async_channel::bounded(1);
    let w = pick_worker();
    let slot = &STATE.slots()[w as usize];
    slot.load.0.fetch_add(1, Ordering::AcqRel);
    let guard = LoadGuard { load: &slot.load.0 };

    let pending = PendingSpawn::new(move |worker_id| {
        let fut = make();
        spawn_on_worker(worker_id, async move {
            let _guard = guard;
            let result = fut.await;
            let _ = tx.try_send(result);
        });
    });

    slot.incoming.lock().unwrap().push_back(pending);
    notify_worker(w);

    WorkerHandle::new(rx)
}

pub fn place_stealable<Fut, T>(fut: Fut) -> WorkerHandle<T>
where
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = async_channel::bounded(1);
    let (runnable, task) = async_task::spawn(
        async move {
            let _ = tx.try_send(fut.await);
        },
        schedule_stealable,
    );
    task.detach();
    runnable.schedule();
    WorkerHandle::new(rx)
}

/// Spawn `fut` as a detached **pinned** task pinned to `owner`. MUST be called on the owner worker
/// so the first poll is local. The counterpart to [`place_stealable`] for the pinned track: unlike
/// that safe path, a `!Send` future forces the `unsafe` `spawn_unchecked`.
fn spawn_on_worker<F>(owner: u32, fut: F)
where
    F: Future<Output = ()> + 'static,
{
    // SAFETY: the future is built on and pinned to `owner` via metadata; `schedule` always routes
    // its Runnable back to `owner`, so it is never polled on another worker. It is `'static`, so it
    // outlives the spawn.
    let (runnable, task) = unsafe {
        async_task::Builder::new()
            .metadata(owner)
            .spawn_unchecked(move |_| fut, schedule)
    };
    // Detach so dropping the JoinHandle does NOT cancel the future (callers fire-and-forget;
    // results flow out-of-band through an async_channel). Forever-futures simply never finish.
    task.detach();
    runnable.run();
}

pub fn available_parallelism() -> usize {
    hardware_concurrency().filter(|&n| n > 0).unwrap_or(4)
}

fn hardware_concurrency() -> Option<usize> {
    let navigator =
        js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("navigator")).ok()?;
    js_sys::Reflect::get(&navigator, &JsValue::from_str("hardwareConcurrency"))
        .ok()?
        .as_f64()
        .map(|cores| cores as usize)
}

/// Pick the least-loaded worker for the next pinned placement.
fn pick_worker() -> u32 {
    let slots = STATE.slots();
    let mut best = 0u32;
    let mut best_load = u32::MAX;
    for (i, s) in slots.iter().enumerate() {
        let load = s.load.0.load(Ordering::Relaxed);
        if load < best_load {
            best_load = load;
            best = i as u32;
        }
    }
    best
}

struct LoadGuard {
    load: &'static AtomicU32,
}
impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.load.fetch_sub(1, Ordering::AcqRel);
    }
}
