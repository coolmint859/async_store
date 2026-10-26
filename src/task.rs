use core::fmt;
use std::{cell::Cell, fmt::{Debug, Display}, marker::PhantomData, sync::mpsc::Sender, time::{Duration, Instant}};

use futures_util::FutureExt;
use tokio::{runtime::Handle, task::JoinHandle};

use crate::resource::{Failed, Ready, Resource, ResourceState};

/// Represents the duration a resource is kept alive in a `ResourceHandler`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HoldPolicy {
    /// Resource persists for the duration of program unless manually removed
    Indefinite,
    /// Resource is kept alive for the provided duration since last accessed.
    WithDuration(Duration),
}

/// Specifies whether a `Task` is io-bound (Non-Blocking) or cpu-bound (Blocking)
#[derive(Debug)]
pub enum TaskType {
    /// task is cpu-bound
    CpuBound,
    /// task is io-bound
    IoBound
}

/// Error occurred from running a task
#[derive(Debug)]
pub enum TaskError<E> {
    /// Thread timed out before task completed
    ThreadTimeout,
    /// Task worker thread panicked while executing.
    TaskPanic,
    /// Error occurred while running the task
    TaskInternal(E)
}

impl<E: Display> Display for TaskError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskError::ThreadTimeout => write!(f, "Worker thread lost or stalled execution."),
            TaskError::TaskPanic => write!(f, "Task's worker thread panicked during execution"),
            TaskError::TaskInternal(err) => write!(f, "{}", err),
        }
    }
}
impl<E: Send + Debug + Display> std::error::Error for TaskError<E> {}
impl<E: Send + Debug + Display + 'static> From<E> for TaskError<E> {
    fn from(err: E) -> Self { Self::TaskInternal(err) }
}

/// An operation performed by a `tokio` thread on a future `F` which returns `Result<R, E>`, where R is stored in a
/// `ResourceHandler` of the matching resource 
#[derive(Debug)]
pub struct Task<F, R, E> {
    pub fut: F,
    pub ty: TaskType,
    pub hold_policy: HoldPolicy,
    _rsc: PhantomData<(R, E)>,
}

impl<F, R, E> Task<F, R, E>
where
    F: Future<Output = Result<R, E>> + Send + 'static,
    R: Send + Debug + 'static,
    E: Send + Debug + Display + 'static
{
    /// Create a io-bound task (non-blocking)
    pub fn io_bound(policy: HoldPolicy, fut: F) -> Self {
        Self {
            fut,
            ty: TaskType::IoBound,
            hold_policy: policy,
            _rsc: PhantomData,
        }
    }

    /// Create a cpu-bound task (blocking)
    pub fn cpu_bound(policy: HoldPolicy, fut: F) -> Self {
        Self {
            fut,
            ty: TaskType::CpuBound,
            hold_policy: policy,
            _rsc: PhantomData,
        }
    }

    /// Runs this task on a tokio thread and sends the result through `tx`.
    /// 
    /// Returns the handle to the tokio thread used to run the task.
    pub(crate) fn run<K>(self, 
        key: K,
        generation: u64, 
        tx: Sender<(K, Resource<R, E>)>
    ) -> JoinHandle<()> 
    where K: Send + 'static
    {
        match self.ty {
            TaskType::IoBound => {
                tokio::task::spawn(async move {
                    let result = std::panic::AssertUnwindSafe(self.fut)
                        .catch_unwind().await;
                    
                    let state =  match result {
                        Ok(task_result) => Self::result_to_state(task_result, self.hold_policy),
                        Err(_) => ResourceState::Failed(Failed {
                            err: TaskError::TaskPanic,
                            failed_at: Instant::now()
                        })
                    };

                    let resource = Resource { state, generation }; 
                    let _ = tx.send((key, resource));
                })
            },
            TaskType::CpuBound => {
                let handle = Handle::current();

                tokio::task::spawn_blocking(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handle.block_on(self.fut)
                    }));

                    let state = match result {
                        Ok(task_result) => Self::result_to_state(task_result, self.hold_policy),
                        Err(_) => ResourceState::Failed(Failed { 
                            err: TaskError::TaskPanic, 
                            failed_at: Instant::now()
                        })
                    };
                    
                    let resource = Resource { state, generation };
                    let _ = tx.send((key, resource));
                })
            }
        }
    }

    /// Converts the result of a task into a `ResourceState`.
    fn result_to_state(result: Result<R, E>, hold_policy: HoldPolicy) -> ResourceState<R, E> {
        match result {
            Ok(rsc) => ResourceState::Ready(Ready {
                rsc,
                hold_policy,
                accessed: Cell::new(Instant::now())
            }),
            Err(e) => ResourceState::Failed(Failed {
                err: TaskError::TaskInternal(e),
                failed_at: Instant::now()
            })
        }
    }
}