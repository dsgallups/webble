#![cfg_attr(target_arch = "wasm32", feature(stdarch_wasm_atomic_wait))]
#![doc = r#"
# Webble

:D (module docs wip)

"#]

pub mod exec;
pub mod handle;
pub mod pool;
#[doc(inline)]
pub use pool::*;
pub mod spawn;
pub mod state;
pub mod worker;

#[cfg(test)]
mod tests;

pub mod prelude {
    pub use crate::exec::*;
    pub use crate::handle::*;
    pub use crate::pool::*;
    pub use crate::spawn::*;
    pub use crate::state::*;
    pub use crate::worker::*;
}
