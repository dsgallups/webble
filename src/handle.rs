pub struct WorkerHandle<T> {
    pub(super) rx: async_channel::Receiver<T>,
    pub(super) result: Option<T>,
}
impl<T> WorkerHandle<T> {
    pub fn new(rx: async_channel::Receiver<T>) -> Self {
        Self { rx, result: None }
    }

    pub fn check_release(&mut self) -> Option<T> {
        if let Ok(output) = self.rx.try_recv() {
            return Some(output);
        }
        None
    }

    pub fn try_recv(&mut self) -> Option<&T> {
        if self.result.is_none()
            && let Ok(value) = self.rx.try_recv()
        {
            self.result = Some(value);
        }
        self.result.as_ref()
    }
    pub fn into_inner(mut self) -> Option<T> {
        if self.result.is_none()
            && let Ok(value) = self.rx.try_recv()
        {
            self.result = Some(value);
        }
        self.result
    }

    /// Await the result, returning `None` if the task is dropped before producing one.
    ///
    /// This is a normal cooperative `async` await — it never blocks a thread — so it is safe in any
    /// async context: inside a webble task, or in a main-thread future driven by an executor such
    /// as `wasm_bindgen_futures::spawn_local`. In the main thread's *synchronous* frame loop, where
    /// you cannot `.await`, poll [`try_recv`](Self::try_recv) / [`check_release`](Self::check_release)
    /// instead.
    pub async fn recv(mut self) -> Option<T> {
        if let Some(value) = self.result.take() {
            return Some(value);
        }
        self.rx.recv().await.ok()
    }
}
