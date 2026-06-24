use std::cell::RefCell;

use web_sys::{
    js_sys,
    wasm_bindgen::{JsCast, JsValue, prelude::Closure},
};

use crate::{
    exec::{__notify_index, run_runnable_ptr},
    state::{MAIN_ID, STATE},
    worker::{ThreadId, set_thread_id},
};

// Cached closure that re-enters `main_loop_tick` when the main thread's `Atomics.waitAsync`
// promise resolves. Cached so each re-park reuses one allocation instead of leaking a closure
// per wake.
thread_local! {
    static MAIN_TICK: RefCell<Option<Closure<dyn FnMut(JsValue)>>> = const { RefCell::new(None) };
}

/// `Int32Array` over the current shared wasm memory. Rebuilt each call because a `memory.grow` can
/// replace the underlying buffer.
fn main_words() -> js_sys::Int32Array {
    let mem = web_sys::wasm_bindgen::memory().unchecked_into::<js_sys::WebAssembly::Memory>();
    js_sys::Int32Array::new(&mem.buffer())
}

/// Start the main thread's non-blocking drain loop. Runs during [`init`](WebbleBuilder::init) (on
/// the main thread). It is zero-CPU when idle — just an awaited `Atomics.waitAsync` promise — so it
/// coexists with the app's own event loop. `waitAsync` is permitted on the main thread (unlike the
/// blocking `Atomics.wait`).
///
/// Each `init` starts its own loop; a loop stops as soon as it observes [`Lifecycle::Shutdown`]. In
/// production this means exactly one loop at a time (a `shutdown()` stops the running loop before
/// the next `init`). On a synchronous `shutdown()` + `init()` restart the old loop may not have
/// observed the shutdown yet and so lingers, parked, alongside the new one — harmless, since
/// `main_slot` is mutex-guarded and a single `memory_atomic_notify` wakes exactly one drainer.
pub(crate) fn start_main_loop() {
    set_thread_id(ThreadId::Main);
    main_loop_tick();
}

/// Drain the main thread's pinned-track work: cross-thread-woken runnables first, then newly placed
/// [`on_main`] closures. The main thread never runs stealable work (it must not jank the UI) and has
/// no idle bit, so the worker Dekker handshake is skipped.
fn drain_main() {
    let slot = STATE.slot_for(ThreadId::Main);

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
            Some(p) => p.run(MAIN_ID),
            None => break,
        }
    }
}

/// One iteration of the main loop: snapshot the notify word, drain pending [`on_main`] work, then
/// park on the word until a worker bumps it, re-entering on resolution via the cached [`MAIN_TICK`]
/// closure.
///
/// The snapshot is taken **before** draining (mirroring the worker blob loop): if a producer bumps
/// the word and notifies while we are draining, the word no longer equals `seen`, so `waitAsync`
/// returns immediately (`async = false`) and we re-drain instead of parking on stranded work. This
/// closes the lost-wakeup window without the worker-track Dekker handshake.
fn main_loop_tick() {
    let arr = main_words();
    let idx = __notify_index(MAIN_ID);
    let seen = match js_sys::Atomics::load(&arr, idx) {
        Ok(v) => v,
        Err(_) => return,
    };

    if STATE.is_shutdown() {
        return; // shut down — stop this loop; a later `init` starts a fresh one.
    }

    drain_main();

    let res: JsValue = match js_sys::Atomics::wait_async(&arr, idx, seen) {
        Ok(v) => v.into(),
        Err(_) => return,
    };

    let is_async = js_sys::Reflect::get(&res, &JsValue::from_str("async"))
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // When `async` is false the word already changed (not-equal / timed-out); loop again off a
    // resolved promise instead of recursing synchronously.
    let promise: js_sys::Promise = if is_async {
        js_sys::Reflect::get(&res, &JsValue::from_str("value"))
            .unwrap()
            .unchecked_into()
    } else {
        js_sys::Promise::resolve(&JsValue::UNDEFINED)
    };

    MAIN_TICK.with(|cell| {
        let mut cell = cell.borrow_mut();
        if cell.is_none() {
            *cell = Some(Closure::wrap(
                Box::new(|_: JsValue| main_loop_tick()) as Box<dyn FnMut(JsValue)>
            ));
        }
        _ = promise.then(cell.as_ref().unwrap());
    });
}
