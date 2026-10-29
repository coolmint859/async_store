use std::{cell::Cell, time::{Duration, Instant}};

use crate::task::{HoldPolicy, TaskError};

use tokio::task::JoinHandle;

/// A stored resource
pub struct Resource<R, E> {
    /// The currect state of a resource
    pub(crate) state: ResourceState<R, E>,
    /// The generation counter of a resource (prevents stale overwrites)
    pub(crate) generation: u64
}

impl<R, E> Resource<R, E> {
    /// Returns the time the resource was requested if in the `Pending` state.
    pub fn requested_at(&self) -> Option<&Instant>{
        if let ResourceState::Pending(pending) = &self.state { Some(&pending.requested_at) } else { None }
    }

    /// Returns a reference the resource if in the `Ready` state. Resets the hold timer.
    pub fn get(&self) -> Option<&R> {
        if let ResourceState::Ready(ready) = &self.state {
            ready.mark_accessed();
            Some(&ready.rsc)
        } else { 
            None 
        }
    }

    /// Returns the error type if the resource if in the `Failed` state.
    pub fn get_error(&self) -> Option<&TaskError<E>> {
        if let ResourceState::Failed(failed) = &self.state { Some(&failed.err) } else { None }
    }

    /// Returns `true` if this resource is in the `Ready` state, `false` otherwise.
    pub fn is_ready(&self) -> bool { self.get().is_some() }

    /// Returns `true` if this resource is in the `Pending` state, `false` otherwise.
    pub fn is_pending(&self) -> bool { self.requested_at().is_some() }

    /// Returns `true` if this resource is in the `Failed` state, `false` otherwise.
    pub fn is_failed(&self) -> bool { self.get_error().is_some() }

    /// Runs a closure `f` on the resource if the resource is in the `Ready` state.
    pub fn is_ready_and(&self, f: impl FnOnce(&R)) {
        if let Some(rsc) = self.get() { f(rsc); }
    }

    /// Returns an `Instant` specifying the last time the resource was accessed if in the `Ready` state, else `None`.
    pub fn last_accessed(&self) -> Option<Instant> {
        match &self.state {
            ResourceState::Ready(ready) => Some(ready.last_accessed()),
            _ => None
        }
    }

    /// Marks the resource as accessed if in the `Ready` state.
    pub fn mark_accessed(&self) {
        match &self.state {
            ResourceState::Ready(ready) => ready.mark_accessed(),
            _ => {}
        }
    }

    /// Takes ownership of `self` and returns `R` if in the `Ready` state, else `None`.
    pub(crate) fn take(self) -> Option<R> {
        self.state.take()
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
    pub(crate) rsc: R,
    /// The time in seconds before this resource should be deallocated.
    pub(crate) hold_policy: HoldPolicy,
    /// The time stamp for when this resource was last accessed.
    pub(crate) accessed: Cell<Instant>,
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

    /// Marks the resource as accessed.
    pub fn mark_accessed(&self) {
        self.accessed.set(Instant::now());
    }

    /// Returns an `Instant` specifying the last time the resource was accessed.
    pub fn last_accessed(&self) -> Instant {
        self.accessed.get()
    }

    /// Returns a reference to the resource `R`. Marks it as accessed.
    pub fn get_rsc(&self) -> &R {
        self.mark_accessed();
        return &self.rsc
    }
}

/// The failed state of a resource, where `E` is the error that occurred.
/// 
/// Resources enter this state on the next call to `ResourceHandler::sync()` if the task's worker thread did not finish successfully.
pub struct Failed<E> {
    /// The error type for why the resource failed
    pub(crate) err: TaskError<E>,
    /// The time stamp for when the resource failed.
    pub(crate) failed_at: Instant
}

impl<E> Failed<E> {
    /// Returns `true` if the state has expired based on `timeout`, `false` otherwise.
    pub(crate) fn is_expired(&self, now: &Instant, timeout: Duration) -> bool {
        now.saturating_duration_since(self.failed_at) > timeout
    }

    /// Returns a copy of the `Instant` for when this resource was failed.
    pub fn failed_at(&self) -> Instant {
        self.failed_at
    }
}

/// The pending state of a resource.
/// 
/// Resources enter this state when `ResourceHandler::request()` is called for the resource's task. 
/// They leave this state at the next call to `ResourceHandler::sync()` if the task's worker thread finishes, the thread panics, or a thread timeout occurs.
pub struct Pending {
    /// A handle to the tokio thread responsible for creating the resource
    pub(crate) thread_handle: JoinHandle<()>,
    /// The time stamp for when the resource was requested.
    pub(crate) requested_at: Instant,
}

impl Pending {
    /// Returns `true` if the worker thread timed out, `false` otherwise.
    pub(crate) fn is_timeout(&self, now: &Instant, timeout: Duration) -> bool {
        now.saturating_duration_since(self.requested_at) >= timeout
    }

    /// Returns a copy of the `Instant` for when this resource was first requested.
    pub fn requested_at(&self) -> Instant {
        self.requested_at
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