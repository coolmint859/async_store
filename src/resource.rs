use std::{cell::Cell, time::{Duration, Instant}};

use crate::task::{HoldPolicy, TaskError};

use tokio::task::JoinHandle;

/// A stored resource
pub struct Resource<R, E> {
    pub state: ResourceState<R, E>,
    pub generation: u64
}

/// The ready state of a resource
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

/// The failed state of a resource, containing the error `E` and the time the resource failed.
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

/// The pending state of a resource, containing the handle to the worker thread and the time the resource was requested.
pub struct Pending {
    /// A handle to the tokio thread responsible for creating the resource
    pub thread_handle: JoinHandle<()>,
    /// The time stamp for when the resource was requested.
    pub requested_at: Instant,
}

impl Pending {
    /// Evaluates if the worker thread tied to this `Pending` state timed out. Returns a new `Failed` state if so, `None` otherwise.
    pub(crate) fn eval_into_failed<E>(&self, now: &Instant, timeout: Duration) -> Option<Failed<E>>{
        if now.saturating_duration_since(self.requested_at) >= timeout {
            self.thread_handle.abort();
            Some(Failed {
                err: TaskError::ThreadTimeout,
                failed_at: *now
            })
        } else {
            None
        }
    }
}

/// Represents the state of a resource requested by the user of a handler instance.
pub enum ResourceState<R, E> {
    /// Resource has been requested but is not yet ready
    Pending(Pending),
    /// Resource is ready for retrieval
    Ready(Ready<R>),
    /// Resource failed to complete
    Failed(Failed<E>),
}

impl<R, E> ResourceState<R, E> {
    /// Returns the time the resource was requested if in the `Pending` state.
    pub fn requested_at(&self) -> Option<&Instant>{
        if let ResourceState::Pending(pending) = self { Some(&pending.requested_at) } else { None }
    }

    /// Returns a reference the resource if in the `Ready` state
    pub fn get_rsc(&self) -> Option<&R> {
        if let ResourceState::Ready(ready) = self { Some(&ready.rsc) } else { None }
    }

    /// Returns a mutable reference to the resource if in the `Ready` state.
    pub fn get_rsc_mut(&mut self) -> Option<&mut R> {
        if let ResourceState::Ready(ready) = self { Some(&mut ready.rsc) } else { None }
    }

    /// Takes ownership of `self` and returns `R` if in the `Ready` state, else `None`
    pub fn take(self) -> Option<R> {
        if let ResourceState::Ready(ready) = self { Some(ready.rsc) } else { None }
    }

    /// Retreive the error message if the stored resource failed to complete.
    pub fn error(&self) -> Option<&TaskError<E>> {
        if let ResourceState::Failed(failed) = self { Some(&failed.err) } else { None }
    }

    /// Check if this resource is ready (has completed loading/creation)
    pub fn is_ready(&self) -> bool { self.get_rsc().is_some() }

    /// Check if this resource is pending (not yet loaded/created)
    pub fn is_pending(&self) -> bool { self.requested_at().is_some() }

    /// Check if this resource is failed (didn't load/wasn't created)
    pub fn is_failed(&self) -> bool { self.error().is_some() }

    /// Returns `true` if the resource with this `ResourceState` should be retained in a `ResourceHandler`, `false` otherwise.
    pub(crate) fn evaluate(
        &mut self,
        now: &Instant, 
        thread_tmt: Duration, 
        failed_tmt: Duration
    ) -> bool {
        match self {
            ResourceState::Ready(ready) => {
                return !ready.is_expired(&now);
            },
            ResourceState::Failed(failed) => {
                return !failed.is_expired(now, failed_tmt);
            },
            ResourceState::Pending(pending) => {
                if let Some(new_state) = pending.eval_into_failed::<E>(&now, thread_tmt) {
                    *self = ResourceState::Failed(new_state);
                }
                return true; // all former pending resources are kept
            }
        }
    }
}