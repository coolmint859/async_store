use core::fmt;
use std::{cell::Cell, fmt::{Debug, Display}, marker::PhantomData, sync::mpsc::Sender, time::{Duration, Instant}};

use futures_util::FutureExt;
use tokio::{runtime::Handle, task::JoinHandle};

use crate::{FailedTimeout, ResourceConfig, ThreadTimeout, resource::ResourceState};

/// Describes the TTL policy for resources stored in a `ResourceHandler`.
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
    CpuBound,
    IoBound
}

/// A error that occurred while running a task
#[derive(Debug)]
pub enum TaskError<E> {
    /// Worker thread timed out before task completed
    ThreadTimeout,
    /// Worker thread panicked while executing.
    ThreadPanic,
    /// Error result was returned after running the task
    TaskInternal(E)
}

impl<E: Send + Debug + Display> std::error::Error for TaskError<E> {}
impl<E: Display> Display for TaskError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskError::ThreadTimeout => write!(f, "Worker thread lost or stalled execution."),
            TaskError::ThreadPanic => write!(f, "Worker thread panicked during execution"),
            TaskError::TaskInternal(err) => write!(f, "{}", err),
        }
    }
}

/// An operation performed by a `tokio` thread on a future `F` which returns `Result<R, E>`, where `R` is stored in a
/// `ResourceHandler` of the matching type. 
#[derive(Debug)]
pub struct Task<F, R, E> {
    pub fut: F,
    pub ty: TaskType,
    pub rsc_config: ResourceConfig,
    pub max_retries: Option<u32>,
    _rsc: PhantomData<(R, E)>,
}

impl<F, R, E> Task<F, R, E>
where
    F: Future<Output = Result<R, E>> + Send + 'static,
    R: Send + Debug + 'static,
    E: Send + Debug + Display + 'static
{
    /// Create a io-bound task (non-blocking). These run on `tokio`'s core thread pool.
    /// 
    /// * `policy` - A `HoldPolicy` determining the TTL of the stored resource.
    /// * `fut` A `Future` which resolves with a `Result<R, E>`
    pub fn io_bound(policy: HoldPolicy, fut: F) -> Self {
        Self {
            fut,
            ty: TaskType::IoBound,
            max_retries: None,
            rsc_config: ResourceConfig::default_with_policy(policy),
            _rsc: PhantomData,
        }
    }

    /// Create a cpu-bound task (blocking). These run on `tokio`'s blocking thread pool.
    ///
    /// * `policy` - A `HoldPolicy` determining the TTL of the stored resource.
    /// * `fut` A `Future` which resolves with a `Result<R, E>`
    pub fn cpu_bound(policy: HoldPolicy, fut: F) -> Self {
        Self {
            fut,
            ty: TaskType::CpuBound,
            rsc_config: ResourceConfig::default_with_policy(policy),
            max_retries: None,
            _rsc: PhantomData,
        }
    }

    /// Sets the timeout for how long this task can be pending before being marked as failed.
    pub fn with_thread_tmt(mut self, timeout: ThreadTimeout) -> Self {
        self.rsc_config.thread_timeout = timeout;
        self
    }

    /// Sets the max retry count for this task.
    pub fn with_max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the timeout for how long a failed resource should be kept in memory before being removed.
    pub fn with_failed_tmt(mut self, timeout: FailedTimeout) -> Self {
        self.rsc_config.failed_timeout = timeout;
        self
    }

    /// Runs this task on a tokio thread and sends the result through `tx`. Consumes `self`
    /// 
    /// Returns the handle to the tokio thread used to run the task.
    pub(crate) fn run<K: Send + 'static>(
        self, 
        key: K,
        request_count: u32, 
        tx: Sender<(K, u32, ResourceState<R, E>)>
    ) -> JoinHandle<()> 
    {
        match self.ty {
            TaskType::IoBound => {
                tokio::task::spawn(async move {
                    let result = std::panic::AssertUnwindSafe(self.fut)
                        .catch_unwind().await;
                    
                    let state =  match result {
                        Ok(task_result) => Self::result_to_state(task_result),
                        Err(_) => ResourceState::Failed {
                            err: TaskError::ThreadPanic,
                            failed_at: Instant::now()
                        }
                    };

                    let _ = tx.send((key, request_count, state));
                })
            },
            TaskType::CpuBound => {
                let handle = Handle::current();

                tokio::task::spawn_blocking(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handle.block_on(self.fut)
                    }));

                    let state = match result {
                        Ok(task_result) => Self::result_to_state(task_result),
                        Err(_) => ResourceState::Failed { 
                            err: TaskError::ThreadPanic, 
                            failed_at: Instant::now()
                        }
                    };
                    
                    let _ = tx.send((key, request_count, state));
                })
            }
        }
    }

    /// Converts the result of a `Task` into a `ResourceState`.
    fn result_to_state(result: Result<R, E>) -> ResourceState<R, E> {
        match result {
            Ok(rsc) => ResourceState::Ready {
                rsc,
                accessed: Cell::new(Instant::now())
            },
            Err(e) => ResourceState::Failed {
                err: TaskError::TaskInternal(e),
                failed_at: Instant::now()
            }
        }
    }
}