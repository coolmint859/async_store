use std::{cell::Cell, time::{Duration, Instant}};

use crate::task::{HoldPolicy, TaskError};

use tokio::task::JoinHandle;

/// A stored resource
pub struct Resource<R, E> {
    /// The currect state of a resource
    pub state: ResourceState<R, E>,
    /// The generation counter of a resource (prevents stale overwrites)
    pub generation: u64
}

impl<R, E> Resource<R, E> {
    /// Returns the time the resource was requested if in the `Pending` state.
    pub fn requested_at(&self) -> Option<&Instant>{
        if let ResourceState::Pending(pending) = &self.state { Some(&pending.requested_at) } else { None }
    }

    /// Returns a reference the resource if in the `Ready` state
    pub fn get(&self) -> Option<&R> {
        if let ResourceState::Ready(ready) = &self.state { Some(&ready.rsc) } else { None }
    }

    /// Returns a mutable reference to the resource if in the `Ready` state.
    pub fn get_mut(&mut self) -> Option<&mut R> {
        if let ResourceState::Ready(ready) = &mut self.state { Some(&mut ready.rsc) } else { None }
    }

    /// Takes ownership of `self` and returns `R` if in the `Ready` state, else `None`.
    pub(crate) fn take(self) -> Option<R> {
        self.state.take()
    }

    /// Retreive the error message if the stored resource failed to complete.
    pub fn error(&self) -> Option<&TaskError<E>> {
        if let ResourceState::Failed(failed) = &self.state { Some(&failed.err) } else { None }
    }

    /// Returns `true` if this resource is in the `Ready` state, `false` otherwise.
    pub fn is_ready(&self) -> bool { self.get().is_some() }

    /// Returns `true` if this resource is in the `Pending` state, `false` otherwise.
    pub fn is_pending(&self) -> bool { self.requested_at().is_some() }

    /// Returns `true` if this resource is in the `Failed` state, `false` otherwise.
    pub fn is_failed(&self) -> bool { self.error().is_some() }

    /// Marks the resource as accessed if in the `Ready` state.
    pub fn mark_accessed(&self) {
        match &self.state {
            ResourceState::Ready(ready) => ready.accessed.set(Instant::now()),
            _ => {}
        }
    }

    /// Returns `true` if this resource should be retained in a `ResourceHandler`, `false` otherwise.
    pub(crate) fn eval_state(
        &mut self,
        now: &Instant, 
        thread_tmt: Duration, 
        failed_tmt: Duration
    ) -> bool {
        match &self.state {
            ResourceState::Ready(ready) => {
                return !ready.is_expired(&now);
            },
            ResourceState::Failed(failed) => {
                return !failed.is_expired(now, failed_tmt);
            },
            ResourceState::Pending(pending) => {
                if pending.is_timeout(&now, thread_tmt) {
                    pending.thread_handle.abort();
                    self.state = ResourceState::Failed(Failed { 
                        err: TaskError::ThreadTimeout, 
                        failed_at: *now
                    });
                }
                return true; // all former pending resources are kept
            }
        }
    }
}

/// The ready state of a resource, where `R` is the resource stored. 
/// 
/// Resources enter this state on the next call to `ResourceHandler::sync()` if the task's worker thread finishes successfully.
pub struct Ready<R> {
    /// The raw resource stored
    pub rsc: R,
    /// The time in seconds before this resource should be deallocated.
    pub hold_policy: HoldPolicy,
    /// The time stamp for when this resource was last accessed.
    pub accessed: Cell<Instant>,
}

impl<R> Ready<R> {
    /// Returns `true` if the resource has expired based on it's `HoldPolicy` and `accessed` time stamp, `false` otherwise
    pub(crate) fn is_expired(&self, now: &Instant) -> bool {
        match self.hold_policy {
            HoldPolicy::Indefinite => false,
            HoldPolicy::WithDuration(duration) => {
                now.saturating_duration_since(self.accessed.get()) > duration
            }
        }
    }
}

/// The failed state of a resource, where `E` is the error that occurred.
/// 
/// Resources enter this state on the next call to `ResourceHandler::sync()` if the task's worker thread did not finish successfully.
pub struct Failed<E> {
    /// The error for why the resource failed
    pub err: TaskError<E>,
    /// The time stamp for when the resource failed.
    pub failed_at: Instant
}

impl<E> Failed<E> {
    /// Returns `true` if the state has expired based on `timeout`, `false` otherwise.
    pub(crate) fn is_expired(&self, now: &Instant, timeout: Duration) -> bool {
        now.saturating_duration_since(self.failed_at) > timeout
    }
}

/// The pending state of a resource.
/// 
/// Resources enter this state when `ResourceHandler::request()` is called for the resource's task. 
/// They leave this state at the next call to `ResourceHandler::sync()` if the task's worker thread finishes, the thread panics, or a thread timeout occurs.
pub struct Pending {
    /// A handle to the tokio thread responsible for creating the resource
    pub thread_handle: JoinHandle<()>,
    /// The time stamp for when the resource was requested.
    pub requested_at: Instant,
}

impl Pending {
    /// Returns `true` if the worker thread timed out, `false` otherwise.
    pub(crate) fn is_timeout(&self, now: &Instant, timeout: Duration) -> bool {
        now.saturating_duration_since(self.requested_at) >= timeout
    }
}

/// Represents the state of a resource requested by the user of a handler instance.
pub enum ResourceState<R, E> {
    /// The pending state of a resource
    Pending(Pending),
    /// The ready state of a resource `R`
    Ready(Ready<R>),
    /// The failed state of a resource with error `E`
    Failed(Failed<E>),
}

impl<R, E> ResourceState<R, E> {
    /// Takes ownership of `self` and returns `R` if in the `Ready` state, else `None`
    pub(crate) fn take(self) -> Option<R> {
        if let ResourceState::Ready(ready) = self { Some(ready.rsc) } else { None }
    }
}