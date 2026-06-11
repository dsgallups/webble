mod integration;
mod virt;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;
use web_sys::js_sys;

use crate::handle::WorkerHandle;
use crate::worker::__worker_drain;

wasm_bindgen_test_configure!(run_in_browser);

const N: usize = 4;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = setTimeout)]
    fn set_timeout(cb: &js_sys::Function, ms: i32);
}

/// A future that returns `Pending` exactly once, waking itself immediately.
struct YieldNow {
    yielded: bool,
}
impl YieldNow {
    fn new() -> Self {
        Self { yielded: false }
    }
}
impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn flush_microtasks() {
    let p = js_sys::Promise::resolve(&JsValue::UNDEFINED);
    let _ = wasm_bindgen_futures::JsFuture::from(p).await;
}

async fn recv<T>(mut h: WorkerHandle<T>) -> T {
    for _ in 0..10_000 {
        if h.try_recv().is_some() {
            return h.into_inner().expect("result present after try_recv");
        }
        flush_microtasks().await;
    }
    panic!("future did not complete within the microtask budget");
}

async fn recv_stealable<T>(mut h: WorkerHandle<T>) -> T {
    for _ in 0..10_000 {
        __worker_drain(0);
        if h.try_recv().is_some() {
            return h.into_inner().expect("result present after try_recv");
        }
        flush_microtasks().await;
    }
    panic!("stealable future did not complete within the drain budget");
}

/// `Int32Array` over the shared wasm memory, for reading/observing notify words.
fn memory_words() -> js_sys::Int32Array {
    let mem = wasm_bindgen::memory().unchecked_into::<js_sys::WebAssembly::Memory>();
    js_sys::Int32Array::new(&mem.buffer())
}
