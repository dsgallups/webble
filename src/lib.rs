#![cfg_attr(target_arch = "wasm32", feature(stdarch_wasm_atomic_wait))]
#![doc = r#"
# Webble

A general-purpose async multithreaded runtime for the web.

Initialize the runtime once on the main thread with [`webble::builder()`](crate::builder)`.init()`, then
spawn work through the free functions [`crate::spawn()`] (pinned track), [`crate::spawn_stealable()`]
(work-stealing track), and [`on_main`] (run a closure on the main thread). There is exactly
one runtime per WASM module, because the scheduler lives in shared linear memory that every
worker sees.

```no_run

webble::builder().glue_path("/my_app.js").init().unwrap();

let mut sum = webble::spawn(|| (0..1_000_000u64).sum::<u64>());
if let Some(total) = sum.try_recv() {
    // ...
}
```
"#]

pub mod exec;
pub mod handle;
pub mod pool;
#[doc(inline)]
pub use pool::*;
pub(crate) mod spawn;
pub mod state;
pub mod worker;

mod main_thread;

#[cfg(test)]
mod tests;

pub mod prelude {
    pub use crate::exec::*;
    pub use crate::handle::*;
    pub use crate::pool::*;
    pub(crate) use crate::spawn::*;
    pub use crate::state::*;
    pub use crate::worker::*;
}
