# Webble

[<img alt="github" src="https://img.shields.io/badge/github-dsgallups/webble?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/dsgallups/webble)
[<img alt="crates.io" src="https://img.shields.io/crates/v/webble.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/webble)
[![docs.rs](https://img.shields.io/static/v1?label=docs.rs&message=webble&color=green&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K)](https://docs.rs/webble/latest/webble)

A general-purpose async multithreaded runtime for the web.

Webble runs your Rust futures and closures across a pool of Web Workers that share the
WASM module's linear memory, real shared-memory threading in the browser, without
blocking the main thread or the workers' event loops.

## Overview

You initialize the runtime with `webble::builder().init()`. Because the scheduling state 
lives in the WASM module's shared linear memory (every worker sees the same global state), 
there is exactly one runtime per module — so after `init()` you spawn work through plain 
**free functions** (`webble::spawn`, `webble::spawn_stealable`, `webble::on_main`) rather 
than a handle.

The runtime has a single lifecycle, `Uninit → Running → Shutdown`, readable via
`webble::lifecycle()`. `webble::shutdown()` terminates the workers; calling `init()` again
**restarts** it (reusing the same worker count). Calling `init()` while already running panics,
like installing a global logger twice.

`init()` spawns one Web Worker per logical core (or a count you choose, up to 32).
Each worker runs a tiny JS drain loop that parks in `Atomics.waitAsync`, not a
blocking wait, so the worker's event loop stays alive and `fetch`, timers, and other JS
callbacks keep progressing inside your tasks. 

When work arrives, the worker is woken via a futex word in shared memory (`memory_atomic_notify` from Rust, `Atomics.waitAsync` from JS) and drains its queues on the microtask queue.

Work runs on one of two tracks:

| Track | Spawn with | Bounds | Behavior |
|---|---|---|---|
| Pinned (local) | [`webble::spawn`] | future may be `!Send` (captures must be `Send`) | placed on the least-loaded worker and owned by it forever |
| Stealable | [`webble::spawn_stealable`] | future must be `Send` | enters a work-stealing deque; may migrate between workers on every wake |

The pinned track is the right default for I/O and CPU-light workloads, and it lets you
hold `!Send` values like `Rc` or `JsValue` across `.await` points. The stealable track is
for CPU-heavy fan-out where load balancing matters. Stealing can be expensive, so defaulting
to `webble::spawn` is usually more performant.

The main thread is also a participant: from inside a worker task, [`webble::on_main`] runs a
closure on the main thread and returns its result, which is the sanctioned path for
main-thread-only work (DOM, `window`, most `web_sys` APIs).

## Example

```rust
use webble::Webble;

// Initialize once on the main thread. The path is your wasm-bindgen JS glue,
// relative to your server root.
Webble::builder().glue_path("/my_app.js").init()?;
// (or: Webble::builder().workers(8).glue_path("/my_app.js").init()?;)

// Spawn a closure, which runs to completion on one worker.
let mut sum = webble::spawn(|| (0..1_000_000u64).sum::<u64>());

// Spawn an async fn. The future is built ON the worker, so it may be !Send:
let mut greeting = webble::spawn(|| async {
    let local = std::rc::Rc::new("hello"); // !Send is fine here
    format!("{local} from worker")
});

// CPU-heavy workload. spawn a stealable task to balance 
// the work across all workers.
let mut handles: Vec<_> = (0..64)
    .map(|i| webble::spawn_stealable(async move { expensive(i) }))
    .collect();

// Results arrive through a non-blocking handle. Poll it from your frame
// loop / main-thread executor:
if let Some(total) = sum.try_recv() {
    web_sys::console::log_1(&format!("sum = {total}").into());
}
```

`spawn` returns a [`WorkerHandle<T>`]. It is intentionally not a `Future` you block on —
the main thread must never block in the browser. Check it with `try_recv()` /
`check_release()`, or take ownership of the result with `into_inner()`. From *inside* a
spawned task (where awaiting is fine), you can instead `.recv().await` the handle — this is
how you read an `on_main` result from a worker:

```rust
// running inside a worker task:
let id = webble::on_main(|| async { /* DOM work on main */ 42 }).recv().await;
```

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

### Cross-origin isolation
`SharedArrayBuffer`, a required component of this runtime, requires your server to send:
```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: <choose require-corp or credentialless>
```

### Notes
No separate worker script is needed: the runtime generates one as a blob URL at runtime. The
generated script imports your wasm-bindgen glue, which is why `glue_path` takes the
URL path to it (e.g. `Webble::builder().glue_path("/my_app.js")`). If you'd rather serve your
own worker script, use `Webble::builder().worker_url(absolute_url)`.

## Questions
Feel free to DM me on discord if you have problems setting it up. my handle is `dsgallups`.

## Contributing
Yes please
