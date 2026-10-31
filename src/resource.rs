use std::{cell::Cell, time::{Duration, Instant}};

use crate::task::{HoldPolicy, TaskError};

use tokio::task::JoinHandle;

/// The duration pending `Task`s can run being marked as failed.
#[derive(Debug, Clone, Copy)]
pub enum ThreadTimeout {
    /// The default thread timeout for the `ResourceHandler` will be used.
    Default,
    /// Tasks will be marked as failed if they do not complete within the specified duration. 
    /// 
    /// This overrides the `ResourceHandler`'s default thread timeout.
    WithDuration(Duration),
}

/// The duration failed resources are kept in memory before being removed from a `ResourceHandler`.
#[derive(Debug, Clone, Copy)]
pub enum FailedTimeout {
    /// The default failed timeout for the `ResourceHandler` will be used.
    Default,
    /// Resources in the `Failed` state will be kept in memory for the specified duration before being removed from the `ResourceHandler`. 
    /// 
    /// This overrides the `ResourceHandler`'s default failed timeout.
    WithDuration(Duration),
    /// Resources in the `Failed` state will be kept in memory indefinitely until manually removed.
    None,
    /// Resources in the `Failed` state will be removed from the `ResourceHandler` immediately after being marked as failed.
    Immediate,
}

/// Configuration for how a resource should be handled by a `ResourceHandler`.
/// 
/// The default configuration is `HoldPolicy::Indefinite` for the hold policy, and `None` for the thread timeout and failed timeout.
#[derive(Debug, Clone, Copy)]
pub struct ResourceConfig {
    /// The hold policy of a resource (how long it should be kept in memory). Default is `HoldPolicy::Indefinite`.
    pub hold_policy: HoldPolicy,
    /// The time limit for how long this resource can be pending before being marked as failed. Default is `None`, which means the `ResourceHandler`'s default thread timeout will be used.
    pub thread_timeout: ThreadTimeout,
    /// The time limit for how long a failed resource should be kept in memory before being removed. Default is `None`, which means the `ResourceHandler`'s default failed timeout will be used.
    pub failed_timeout: FailedTimeout,
}

impl ResourceConfig {
    /// Creates a `ResourceConfig` with the specified hold policy, and `None` values for thread_timeout and failed_timeout.
    pub fn default_with_policy(policy: HoldPolicy) -> Self {
        Self {
            hold_policy: policy,
            thread_timeout: ThreadTimeout::Default,
            failed_timeout: FailedTimeout::Default
        }
    }
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self::default_with_policy(HoldPolicy::Indefinite)
    }
}

/// A stored resource
#[derive(Debug)]
pub struct Resource<R, E> {
    /// The currect state of a resource
    pub(crate) state: ResourceState<R, E>,
    /// The number of times this resource has been requested (used for retry logic and generation tracking).
    pub(crate) request_count: u32,
    /// The time stamp for when the resource was requested.
    pub(crate) requested_at: Instant,
    /// Configuration for how this resource should be handled by a `ResourceHandler`.
    pub(crate) config: ResourceConfig,
}

impl<R, E> Resource<R, E> {
    /// Returns the time the resource was requested.
    pub fn requested_at(&self) -> &Instant {
        &self.requested_at
    }

    /// Returns a reference to the inner resource if in the `Ready` state.
    /// 
    /// Note: This marks the resource as accessed. If this is unintended behavior, call `peek()` 
    pub fn get(&self) -> Option<&R> {
        match &self.state {
            ResourceState::Ready{rsc, accessed} => {
                accessed.set(Instant::now());
                Some(&rsc)
            },
            _ => None
        }
    }

    /// Returns a reference to the inner resource if in the `Ready` state. 
    /// 
    /// Note: This does NOT mark the resource as accessed. If this is unintended behavior, call `get()`
    pub fn peek(&self) -> Option<&R> {
        match &self.state {
            ResourceState::Ready{rsc, ..} => Some(&rsc),
            _ => None
        }
    }

    /// Returns the error type if the resource if in the `Failed` state.
    pub fn get_error(&self) -> Option<&TaskError<E>> {
        match &self.state {
            ResourceState::Failed{err, ..} => Some(&err),
            _ => None
        }
    }

    /// Returns `true` if this resource is in the `Ready` state, `false` otherwise. Does not mark the resource as accessed.
    pub fn is_ready(&self) -> bool { 
        match &self.state {
            ResourceState::Ready{..} => true,
            _ => false
        }
    }

    /// Returns `true` if this resource is in the `Pending` state, `false` otherwise.
    pub fn is_pending(&self) -> bool { 
        match &self.state {
            ResourceState::Pending(_) => true,
            _ => false
        }
    }

    /// Returns `true` if this resource is in the `Failed` state, `false` otherwise.
    pub fn is_failed(&self) -> bool { 
        match &self.state {
            ResourceState::Failed{..} => true,
            _ => false
        }
    }

    /// Returns an `Instant` specifying the last time the resource was accessed if in the `Ready` state, else `None`.
    pub fn last_accessed(&self) -> Option<Instant> {
        match &self.state {
            ResourceState::Ready{ rsc: _, accessed} => Some(accessed.get()),
            _ => None
        }
    }

    /// Marks the resource as accessed if in the `Ready` state.
    pub fn mark_accessed(&self) {
        match &self.state {
            ResourceState::Ready{ rsc: _, accessed} => accessed.set(Instant::now()),
            _ => {}
        }
    }

    /// Takes ownership of `self` and returns `R` if in the `Ready` state, else `None`.
    pub(crate) fn take(self) -> Option<R> {
        self.state.take()
    }

    /// Returns `true` if this resource should be retained in a `ResourceHandler`, `false` otherwise.
    pub(crate) fn eval_state(&mut self, now: &Instant, thread_tmt: Duration, failed_tmt: FailedTimeout) -> bool {
        match &self.state {
            ResourceState::Ready{ rsc: _, accessed} => {
                !self.ready_expired(now, accessed.get())
            },
            ResourceState::Failed{ err: _, failed_at} => {
                !self.failed_expired(now, *failed_at, failed_tmt)
            },
            ResourceState::Pending(handle) => {
                if self.pending_timeout(now, thread_tmt) {
                    handle.abort();
                    self.state = ResourceState::Failed {
                        err: TaskError::ThreadTimeout, 
                        failed_at: *now
                    };
                }
                return true; // all pending resources are kept
            }
        }
    }

    /// Returns `true` if this resource is in the `Ready` state and has expired, `false` otherwise.
    fn ready_expired(&self, now: &Instant, accessed: Instant) -> bool {
        match self.config.hold_policy {
            HoldPolicy::Indefinite => false,
            HoldPolicy::WithDuration(duration) => {
                now.saturating_duration_since(accessed) > duration
            }
        }
    }

    /// Returns `true` if the resource's task is in the `Pending` state and has timed out, `false` otherwise.
    fn pending_timeout(&self, now: &Instant, thread_tmt: Duration) -> bool {
        let timeout = match self.config.thread_timeout {
            ThreadTimeout::Default => thread_tmt,
            ThreadTimeout::WithDuration(duration) => duration
        };

        now.saturating_duration_since(self.requested_at) > timeout
    }

    /// Returns `true` if the resource is in the `Failed` state and has expired, `false` otherwise.
    fn failed_expired(&self, now: &Instant, failed_at: Instant, failed_tmt: FailedTimeout) -> bool {
        let timeout = match self.config.failed_timeout {
            FailedTimeout::Default => match failed_tmt {
                FailedTimeout::Default | FailedTimeout::None => return false,
                FailedTimeout::WithDuration(duration) => duration,
                FailedTimeout::Immediate => return true
            },
            FailedTimeout::WithDuration(duration) => duration,
            FailedTimeout::None => return false,
            FailedTimeout::Immediate => return true
        };

        now.saturating_duration_since(failed_at) > timeout
    }
}

/// Represents the state of a resource requested by the user of a handler instance.
#[derive(Debug)]
pub enum ResourceState<R, E> {
    /// The pending state of a resource
    Pending(JoinHandle<()>),
    /// The ready state of a resource `R`
    Ready {
        rsc: R,
        accessed: Cell<Instant>
    },
    /// The failed state of a resource with error `E`
    Failed {
        err: TaskError<E>,
        failed_at: Instant
    },
}

impl<R, E> ResourceState<R, E> {
    /// Takes ownership of `self` and returns `R` if in the `Ready` state, else `None`
    pub(crate) fn take(self) -> Option<R> {
        if let ResourceState::Ready{rsc, ..} = self { Some(rsc) } else { None }
    }
}