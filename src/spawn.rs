use crate::prelude::*;

pub struct ClosureMarker;
pub struct FutureMarker;
pub struct AsyncFnMarker;

pub trait Spawn<M> {
    type Output;
    fn spawn(self) -> Self::Output;
}

impl<F, T> Spawn<ClosureMarker> for F
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    type Output = WorkerHandle<T>;
    fn spawn(self) -> WorkerHandle<T> {
        crate::place_local(move || async move { self() })
    }
}

impl<Fut, T> Spawn<FutureMarker> for Fut
where
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    type Output = WorkerHandle<T>;
    fn spawn(self) -> WorkerHandle<T> {
        crate::place_local(move || self)
    }
}

impl<F, Fut, T> Spawn<AsyncFnMarker> for F
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    type Output = WorkerHandle<T>;
    fn spawn(self) -> Self::Output {
        crate::place_local(self)
    }
}

pub trait SpawnStealable<M> {
    type Output;
    fn spawn_stealable(self) -> Self::Output;
}

impl<Fut, T> SpawnStealable<FutureMarker> for Fut
where
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    type Output = WorkerHandle<T>;
    fn spawn_stealable(self) -> WorkerHandle<T> {
        crate::place_stealable(self)
    }
}
