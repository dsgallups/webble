use crate::{pool, prelude::*};

pub struct ClosureMarker;
pub struct FutureMarker;
pub struct AsyncFnMarker;

pub trait Spawn<M> {
    type Output;
    fn spawn(self, pool: &ThreadPool) -> Self::Output;
}

impl<F, T> Spawn<ClosureMarker> for F
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    type Output = WorkerHandle<T>;
    fn spawn(self, _: &ThreadPool) -> WorkerHandle<T> {
        pool::place_local(move || async move { self() })
    }
}

impl<Fut, T> Spawn<FutureMarker> for Fut
where
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    type Output = WorkerHandle<T>;
    fn spawn(self, _: &ThreadPool) -> WorkerHandle<T> {
        pool::place_local(move || self)
    }
}

impl<F, Fut, T> Spawn<AsyncFnMarker> for F
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    type Output = WorkerHandle<T>;
    fn spawn(self, _: &ThreadPool) -> Self::Output {
        pool::place_local(self)
    }
}

pub trait SpawnStealable<M> {
    type Output;
    fn spawn_stealable(self, pool: &ThreadPool) -> Self::Output;
}

impl<Fut, T> SpawnStealable<FutureMarker> for Fut
where
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    type Output = WorkerHandle<T>;
    fn spawn_stealable(self, _: &ThreadPool) -> WorkerHandle<T> {
        pool::place_stealable(self)
    }
}
