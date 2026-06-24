use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, Ordering, fence};

use crossbeam_deque::Injector;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::{
    Worker, WorkerOptions, WorkerType,
    js_sys::{self, Array},
    wasm_bindgen::{self, JsValue},
};

use crate::prelude::*;

#[wasm_bindgen(inline_js = r#"
export function glue_url(path) {
	return self.location.origin + path;
}
export function make_worker_blob_url(glueUrl) {
	const script =
		'import init, { __worker_drain, __notify_index } from ' + JSON.stringify(glueUrl) + ';\n' +
		'self.onmessage = async ({ data }) => {\n' +
		'  const [module, memory, workerId] = data;\n' +
		'  const wasm = await init({ module_or_path: module, memory });\n' +
		'  const idx = __notify_index(workerId);\n' +
		'  while (true) {\n' +
		'	 const view = new Int32Array(wasm.memory.buffer);\n' +
		'	 const seen = Atomics.load(view, idx);\n' +
		'	 if (!__worker_drain(workerId)) break;\n' +
		'	 const r = Atomics.waitAsync(view, idx, seen);\n' +
		'	 if (r.async) { await r.value; }\n' +
		'  }\n' +
		'};\n';
	const blob = new Blob([script], { type: "text/javascript" });
	return URL.createObjectURL(blob);
}
"#)]
extern "C" {
    fn glue_url(path: &str) -> String;
    fn make_worker_blob_url(glue_url: &str) -> String;
}

// The web worker handles owned by the runtime.
//
// `web_sys::Worker` is `!Send`/`!Sync`, so the global `STATE` cannot hold them. Instead they live
// in a **main-thread** thread-local: `WebbleBuilder::init` (which runs on the main thread)
// populates it, and `shutdown` (also on the main thread) drains and terminates them. Worker
// threads see an empty `Vec` here, which is fine — they never create or terminate workers. This
// mirrors the existing `MICRO`/`REARM` thread-locals that park JS closures per thread.
thread_local! {
    static WORKERS: RefCell<Vec<ThreadWorker>> = const { RefCell::new(Vec::new()) };
}

/// Where the worker bootstrap script comes from.
enum WorkerSource {
    /// A path relative to the server origin pointing at your wasm-bindgen JS glue. Wrapped in a
    /// generated blob worker (see `make_worker_blob_url`).
    GluePath(String),
    /// An absolute worker-script URL, used verbatim.
    WorkerUrl(String),
}

/// Entry point for configuring and starting the global runtime, in the spirit of
/// `tracing_subscriber::fmt()`.
///
/// ```no_run
/// webble::builder().workers(8).glue_path("/app.js").init().unwrap();
/// ```
pub fn builder() -> WebbleBuilder {
    WebbleBuilder {
        num_workers: None,
        source: None,
    }
}

/// Builder for the global runtime. See [`crate::builder()`].
///
/// Each worker runs a non-blocking drain loop in JavaScript and an `async_task` executor on its
/// microtask queue. After [`init`](Self::init), work runs on one of **two tracks** via the free
/// functions [`spawn`] (pinned) and [`spawn_stealable`] (work-stealing), plus [`on_main`] to run a
/// closure on the main thread.
pub struct WebbleBuilder {
    num_workers: Option<usize>,
    source: Option<WorkerSource>,
}

impl WebbleBuilder {
    /// Set the worker count. Defaults to the browser's reported logical core count
    /// ([`available_parallelism`]). Capped at 32 (the idle bitmask is a `u32`).
    pub fn workers(mut self, num_workers: usize) -> Self {
        self.num_workers = Some(num_workers);
        self
    }

    /// Point the workers at your wasm-bindgen JS glue, relative to the server origin. For glue
    /// served at the root, this is `.glue_path("/wasm-bindgen-stuff.js")`. Workers are wrapped in a
    /// generated blob script that imports this glue. Mutually exclusive with [`Self::worker_url`].
    pub fn glue_path(mut self, path: impl Into<String>) -> Self {
        self.source = Some(WorkerSource::GluePath(path.into()));
        self
    }

    /// Use an absolute worker-script URL verbatim instead of generating a blob from the glue path.
    /// Mutually exclusive with [`Self::glue_path`].
    pub fn worker_url(mut self, url: impl Into<String>) -> Self {
        self.source = Some(WorkerSource::WorkerUrl(url.into()));
        self
    }

    /// Spawn the workers and install the global runtime. Must be called on the **main thread**.
    ///
    /// Panics if called a second time (like installing a global logger twice) or if neither
    /// [`glue_path`](Self::glue_path) nor [`worker_url`](Self::worker_url) was set. On a Web Worker
    /// construction error the runtime is left uninitialized so the call can be retried.
    pub fn init(self) -> Result<(), JsValue> {
        let num_workers = self.num_workers.unwrap_or_else(available_parallelism);
        assert!(
            num_workers <= 32,
            "webble supports at most 32 workers (the idle bitmask is a u32)"
        );

        // Resolve the worker URL before touching lifecycle state, so a missing source can't leave
        // the runtime half-transitioned.
        let worker_url = match self.source {
            Some(WorkerSource::GluePath(path)) => make_worker_blob_url(&glue_url(&path)),
            Some(WorkerSource::WorkerUrl(url)) => url,
            None => panic!("webble: call .glue_path(..) or .worker_url(..) before .init()"),
        };

        // Lifecycle transition. `Uninit` (fresh) or `Shutdown` (restart) → `Running`; initializing
        // while already `Running` panics, like installing a global logger twice.
        let prev = STATE.state.swap(Lifecycle::Running as u8, Ordering::AcqRel);
        assert!(
            prev != Lifecycle::Running as u8,
            "webble is already running; call webble::shutdown() before re-initializing"
        );

        // First init sizes the process-global slots. A restart reuses them — the count is fixed by
        // the `OnceLock`s — after clearing any state left over from the previous run.
        if STATE.slots.get().is_none() {
            let slots: Box<[Slot]> = (0..num_workers).map(|_| Slot::new()).collect();
            _ = STATE.slots.set(slots);
            _ = STATE.injector.set(Injector::new());
            _ = STATE.main_slot.set(Slot::new());
        } else {
            assert_eq!(
                STATE.slots().len(),
                num_workers,
                "webble: a restart must reuse the original worker count ({} workers were created)",
                STATE.slots().len()
            );
            clear_runtime_state();
        }

        let module = wasm_bindgen::module();
        let memory = wasm_bindgen::memory();
        let opts = WorkerOptions::new();
        opts.set_type(WorkerType::Module);

        // Build all workers fallibly; on error, undo `initialized` so a retry is possible.
        let result = (|| -> Result<Vec<ThreadWorker>, JsValue> {
            let mut workers = Vec::with_capacity(num_workers);
            for id in 0..num_workers {
                let worker = Worker::new_with_options(&worker_url, &opts)?;
                let msg = Array::of3(&module, &memory, &JsValue::from(id as u32));
                worker.post_message(&msg)?;
                workers.push(ThreadWorker {
                    id: id as u32,
                    inner: worker,
                });
            }
            Ok(workers)
        })();

        let workers = match result {
            Ok(workers) => workers,
            Err(e) => {
                STATE
                    .state
                    .store(Lifecycle::Uninit as u8, Ordering::Release);
                return Err(e);
            }
        };

        WORKERS.with(|w| *w.borrow_mut() = workers);

        // Make the main thread a pinned-track participant so `on_main` work can run here.
        crate::main_thread::start_main_loop();

        Ok(())
    }
}

/// Spawn `Send` work (a closure, a future, or an async closure) on the **pinned track**. Placed on
/// the least-loaded worker, which runs it to completion. Captured arguments must be `Send`, but the
/// future itself may be `!Send` (e.g. it can hold an `Rc`/`JsValue` across an `.await`).
///
/// Prefer this over [`spawn_stealable`] for I/O and CPU-light workloads.
pub fn spawn<M, S: Spawn<M>>(work: S) -> S::Output {
    work.spawn()
}

/// Spawn `Send` work onto the **work-stealing track**. Unlike [`spawn`], a returned future must
/// also be `Send`, as it may cross a worker boundary on every wake. Prefer this for CPU-heavy,
/// load-balancing-sensitive work.
pub fn spawn_stealable<M, S: SpawnStealable<M>>(work: S) -> S::Output {
    work.spawn_stealable()
}

/// Run a closure on the **main thread** and get its result back through a [`WorkerHandle`].
///
/// This is the sanctioned path for main-thread-only work (DOM, `window`, most `web_sys` APIs) from
/// a worker. The bounds mirror [`place_local`]: the `make` closure crosses to the main thread so it
/// must be `Send`, but the future it builds runs entirely on main and may be `!Send`.
pub fn on_main<F, Fut, T>(make: F) -> WorkerHandle<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let (tx, rx) = async_channel::bounded(1);
    let slot = STATE.slot_for(ThreadId::Main);

    let pending = PendingSpawn::new(move |id| {
        // Built and polled on the main thread, exactly like the worker pinned track.
        let fut = make();
        spawn_on_worker(id, async move {
            _ = tx.try_send(fut.await);
        });
    });

    slot.incoming.lock().unwrap().push_back(pending);
    notify_worker(ThreadId::Main);

    WorkerHandle::new(rx)
}

/// Signal all workers to stop and terminate them, moving the runtime to [`Lifecycle::Shutdown`].
/// Safe to call when uninitialized (no-op) and from the main thread only (it terminates the
/// main-thread-owned worker handles). A later [`webble::builder()`](crate::builder)`.init()` restarts the runtime.
pub fn shutdown() {
    // Store Shutdown then fence, so the `busy` reads below are ordered after it: this is the
    // `shutdown` half of the Dekker handshake in `__worker_drain` (store-Shutdown-then-read-busy vs.
    // store-busy-then-read-Shutdown). Either a worker observes the shutdown and bails before
    // touching the deques, or we observe its `busy` flag and wait for it below.
    STATE
        .state
        .store(Lifecycle::Shutdown as u8, Ordering::SeqCst);
    fence(Ordering::SeqCst);
    if let Some(slots) = STATE.slots.get() {
        for i in 0..slots.len() as u32 {
            notify_worker(ThreadId::Worker(i));
        }
    }
    // Wake the main loop so it observes the shutdown state and stops re-parking.
    if STATE.main_slot.get().is_some() {
        notify_worker(ThreadId::Main);
    }

    // Wait for any worker still mid-drain to reach a safe point before terminating it. Killing a
    // worker mid-deque-op leaves the shared, lock-free work-stealing deques in a corrupt state, and
    // those deques are reused on the next `init()` — so a stale terminate would surface as an
    // out-of-bounds access in a *later* run. Workers run on their own threads and make progress
    // while we spin here, so they settle quickly; the bound just prevents a runaway future from
    // hanging shutdown (in which case we fall back to terminating regardless, as before).
    if let Some(slots) = STATE.slots.get() {
        for _ in 0..QUIESCE_SPINS {
            if slots.iter().all(|s| !s.busy.0.load(Ordering::SeqCst)) {
                break;
            }
            core::hint::spin_loop();
        }
    }

    WORKERS.with(|w| {
        for worker in w.borrow_mut().drain(..) {
            worker.inner.terminate();
        }
    });
}

/// Upper bound on the busy-wait in [`shutdown`] before terminating workers regardless. Workers
/// progress on their own threads while the caller spins, so a quiescent pool breaks out almost
/// immediately; this only caps the wait if a worker is wedged in a long-running poll.
const QUIESCE_SPINS: u32 = 5_000_000;

/// The number of live workers in the runtime, or 0 when not [`Running`](Lifecycle::Running).
pub fn num_workers() -> usize {
    if STATE.lifecycle() == Lifecycle::Running {
        STATE.slots.get().map_or(0, |s| s.len())
    } else {
        0
    }
}

/// The number of tasks not currently being worked on (pinned placements + stealable work).
pub fn num_tasks_waiting() -> usize {
    let slots = STATE.slots();
    let pinned: usize = slots.iter().map(|s| s.incoming.lock().unwrap().len()).sum();
    let stealable: usize = slots.iter().map(|s| s.stealer.len()).sum();
    pinned + stealable + STATE.injector().len()
}

/// The current runtime [`Lifecycle`].
pub fn lifecycle() -> Lifecycle {
    STATE.lifecycle()
}

/// Whether the runtime has been shut down (i.e. [`lifecycle`] is [`Lifecycle::Shutdown`]).
pub fn is_shutdown() -> bool {
    STATE.is_shutdown()
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
        // `make` is `Send` and runs HERE, on the owner worker, so the future it produces
        // is created on the owner and never crosses a thread.
        let fut = make();
        spawn_on_worker(worker_id, async move {
            let _guard = guard;
            let result = fut.await;
            _ = tx.try_send(result);
        });
    });

    slot.incoming.lock().unwrap().push_back(pending);
    notify_worker(ThreadId::Worker(w));

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
            _ = tx.try_send(fut.await);
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
            .metadata(ThreadId::Worker(owner))
            .spawn_unchecked(move |_| fut, schedule)
    };
    // Detach so dropping the JoinHandle does NOT cancel the future (callers fire-and-forget;
    // results flow out-of-band through an async_channel). Forever-futures simply never finish.
    task.detach();
    runnable.run();
}

/// The number of logical cores available to this context, read from
/// `navigator.hardwareConcurrency` and clamped to at least 1.
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

/// Decrements a worker's load when the pinned future it guards completes (or is dropped). A
/// forever-future simply never drops its guard, which correctly reflects that it permanently
/// occupies a slot.
struct LoadGuard {
    load: &'static AtomicU32,
}
impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.load.fetch_sub(1, Ordering::AcqRel);
    }
}
