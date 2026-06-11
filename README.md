# Webble

A general-purpose async multithreaded runtime for the web.

Webble runs your Rust futures and closures across a pool of Web Workers that share the
WASM module's linear memory, real shared-memory threading in the browser, without
blocking the main thread or the workers' event loops.

## Overview

`ThreadPool` spawns one Web Worker per logical core (or a count you choose, up to 32).
Each worker runs a tiny JS drain loop that parks in `Atomics.waitAsync`, not a
blocking wait, so the worker's event loop stays alive and `fetch`, timers, and other JS
callbacks keep progressing inside your tasks. 

When work arrives, the worker is woken via a futex word in shared memory (`memory_atomic_notify` from Rust, `Atomics.waitAsync` from JS) and drains its queues on the microtask queue.

Work runs on one of two tracks:

| Track | Spawn with | Bounds | Behavior |
|---|---|---|---|
| Pinned (local) | [`ThreadPool::spawn`] / [`spawn_local`] | future may be `!Send` (captures must be `Send`) | placed on the least-loaded worker and owned by it forever |
| Stealable | [`ThreadPool::spawn_stealable`] | future must be `Send` | enters a work-stealing deque; may migrate between workers on every wake |

The pinned track is the right default for I/O and CPU-light workloads, and it lets you
hold `!Send` values like `Rc` or `JsValue` across `.await` points. The stealable track is
for CPU-heavy fan-out where load balancing matters. Stealing can be expensive, so defaulting
to `ThreadPool::spawn` is usually more performant.

## Example

```rust
use webble::ThreadPool;

// Path to your wasm-bindgen JS glue, relative to your server root.
let pool = ThreadPool::new("/my_app.js")?;

// Spawn a closure, which runs to completion on one worker.
let mut sum = pool.spawn(|| (0..1_000_000u64).sum::<u64>());

// Spawn an async fn. The future is built ON the worker, so it may be !Send:
let mut greeting = pool.spawn(|| async {
    let local = std::rc::Rc::new("hello"); // !Send is fine here
    format!("{local} from worker")
});

// CPU-heavy workload. spawn a stealable task to balance 
// the work across all workers.
let mut handles: Vec<_> = (0..64)
    .map(|i| pool.spawn_stealable(async move { expensive(i) }))
    .collect();

// Results arrive through a non-blocking handle. Poll it from your frame
// loop / main-thread executor:
if let Some(total) = sum.try_recv() {
    web_sys::console::log_1(&format!("sum = {total}").into());
}
```

`spawn` returns a [`WorkerHandle<T>`]. It is intentionally not a `Future` you block on —
the main thread must never block in the browser. Check it with `try_recv()` /
`check_release()`, or take ownership of the result with `into_inner()`.

## Requirements

Shared-memory WASM is a *tad* unstable, so you have to do a few silly things
to get this working. I plan on creating a starter template for Svelte/React/Vue in the future.

### **Nightly Rust**

webble uses `stdarch_wasm_atomic_wait`, and `std` must be rebuilts with atomics via `-Z build-std`.

### **Target features and linker flags**
Add to your project a `.cargo/config.toml`. Paste the following code:

```toml
[target.wasm32-unknown-unknown]
rustflags = [
    "-Ctarget-feature=+atomics,+bulk-memory,+mutable-globals",
    "-Clink-arg=--shared-memory",
    "-Clink-arg=--import-memory",
    "-Clink-arg=--max-memory=1073741824",
    "-Clink-arg=--export=__wasm_init_tls",
    "-Clink-arg=--export=__tls_size",
    "-Clink-arg=--export=__tls_align",
    "-Clink-arg=--export=__tls_base",
    "-Clink-arg=--export=__heap_base",
]

[unstable]
build-std = ["std", "panic_abort"]
```

### `wasm-bindgen`
You will need to pass in `--target web`. The workers are ES module workerws that import
your bindgen glue and re-initialize the module against shared memory.


I use something similar to this in a `Justfile` when building my wasm-binary
```just
wasm:
    RUSTUP_TOOLCHAIN=nightly cargo build --target wasm32-unknown-unknown -Z build-std=std,panic_abort
    wasm-bindgen --target web --typescript --out-dir frontend/src/lib/wasm/pkg target/wasm32-unknown-unknown/wasm_release/name_of_output.wasm
```

### `Cross-origin isolation**
`SharedArrayBuffer`, a required component of this runtime, requires your server to send:
```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: <choose require-corp or credentialless>
```

### Notes
No separate worker script is needed: the pool generates one as a blob URL at runtime. The
generated script imports your wasm-bindgen glue, which is why `ThreadPool::new` takes the
URL path to it (e.g. `ThreadPool::new("/my_app.js")`). If you'd rather serve your own
worker script, use `ThreadPool::new_from_absolute_url_and_count`.

## Questions
Feel free to DM me on discord if you have problems setting it up. my handle is `dsgallups`.

## Contributing
Yes please
