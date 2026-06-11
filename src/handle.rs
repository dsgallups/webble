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
}
