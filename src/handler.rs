use std::{cell::Cell, collections::HashMap, fmt::{Debug, Display}, hash::Hash, sync::mpsc, time::{Duration, Instant}};

use crate::{FailedTimeout, ResourceConfig, resource::{Resource, ResourceState}, task::{Task, TaskError}};

/// Stores and manages resource lifecycles by running `Task`s.
/// 
/// This struct is not thread-safe, and should only be used on the main thread or isolated contexts. It is imperative to call `sync()` every frame or update tick to ensure resources are updated in the `ResourceHandler`. 
/// 
/// Resources are stored in a `HashMap` and can be queried by key. Resources can be in one of three states:
/// - **Pending** - the `Task` is actively running on a worker thread. These can timeout if they take too long. The default timeout is 5 seconds. To configure this behavior, call `set_thread_tmt()` or `with_thread_tmt()`. You can also set a timeout for a specific `Task` by calling `with_thread_tmt()` on the `Task` itself, which overrides the `ResourceHandler`'s thread timeout for that specific task.
/// - **Ready** - the `Task` completed successfully and the resource is ready to use. Call `get()` or `peek()` to retrieve a reference to the completed resource. Resources will be removed automatically if they are not accessed based on their `HoldPolicy`. If the policy is set to `HoldPolicy::Indefinite`, this rule does not apply.
/// - **Failed** - the `Task` failed to complete, panicked, or timed out. Failed resources may be removed after some time. The default timeout is 3 seconds. To configure this behavior, call `set_failed_tmt()` or `with_failed_tmt()`. If the timeout is set to `None`, failed resources persist indefinitely unless manually removed.  You can also set a timeout for a specific `Task` by calling `with_failed_tmt()` on the `Task` itself, which overrides the `ResourceHandler`'s failed timeout for that specific task.
/// 
/// Resources can be requested by calling `get_or_request()` or `request()`. If a resource is already in the `Pending` or `Ready` state, these methods do nothing. 
/// If a resource is in the `Failed` state and the retry count is within the limit, these methods will run the provided `Task` again to attempt to create the resource, increment the retry count, and reset the state to `Pending`. 
/// The resource's retry count also doubles as a method to prevent older requests under the same key from overwriting new ones.
/// 
/// You can set the max retry count for a resource by calling `set_max_retries()` or `with_max_retries()`. If the resource fails to be created after the max retry count is reached, it will be removed from the handler. The default max retry count is 3. If the max retry count is set to `None`, the resource will be retried indefinitely.
/// You can also set the max retry count for a specific `Task` by calling `with_max_retries()` on the `Task` itself, which overrides the `ResourceHandler`'s max retry count for that specific task.
/// 
/// You can also store a resource directly by calling `store()`, which will overwrite any existing resource with the same key (This is similar to how `HashMap::insert()` works).
/// 
/// For *cpu-bound* tasks, thread timeouts may not immediately stop the thread, as the thread may be executing a blocking operation. 
/// In this case, the request count acts as a generation counter to prevent the thread from overwriting a newer resource in the handler.
/// This also applies for thread timeouts set on the `Task` itself, which overrides a `ResourceHandler`'s thread timeout for that specific task.
/// 
/// Resources can be accessed by calling `get()` or `peek()` after the resources are synced with the main thread if they are available. Calling `get()` marks the resource as accessed, resetting its hold time. Calling `peek()` does not mark the resource as accessed.
/// If you want to run a closure on a resource if it is ready, call `apply_if_ready()` or `peek_if_ready()`. 
/// If you want to run a closure on all resources that are ready, call `apply_for_each_ready()` or `peek_for_each_ready()`.
/// 
/// You can also query the state of a resource by calling `get_state_of()`, or get the error of a resource if it is in the `Failed` state by calling `get_err_of()`.
/// If you only need to know if a resource is in a specific state, call `is_ready()`, `is_pending()`, or `is_failed()`.
/// 
/// This struct also implements common HashMap-like methods such as `remove()`, `clear()`, `contains()`, and `len()`. It's worth noting that `remove()` will only return the resource if it exists and is in the `Ready` state.
/// You can also get the number of resources in a specific state by calling `count_ready()`, `count_pending()`, or `count_failed()`.
pub struct ResourceHandler<K, R, E> {
    resource_map: HashMap<K, Resource<R, E>>,

    tx: mpsc::Sender<(K, u32, ResourceState<R, E>)>,
    rx: mpsc::Receiver<(K, u32, ResourceState<R, E>)>,

    thread_timeout: Duration,           // time before a worker thread is considered 'dead' by the main thread
    failed_timeout: FailedTimeout,   // time before a failed resource is removed from the map
    max_retries: Option<u32>,           // max number of times a resource can be retried before being removed from the map
}

impl<K, R, E> ResourceHandler<K, R, E> 
where
    K: Send + Debug + Hash + Eq + PartialEq + Clone + 'static,
    R: Send + Debug + 'static,
    E: Send + Debug + Display + 'static
{
    /// Create a new `ResourceHandler`.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            resource_map: HashMap::new(),
            thread_timeout: Duration::from_secs(5),
            failed_timeout: FailedTimeout::WithDuration(Duration::from_secs(3)),
            max_retries: Some(3),
            tx, rx,
        }
    }

    /// Set the resource timeout for worker threads. The default is 5 seconds.
    /// 
    /// For cpu-bound tasks, this timeout may not immediately stop the thread, as the thread may be executing a blocking operation. 
    /// In this case, the request count is used to prevent the thread from overwriting a newer resource in the handler (Similar to generation counters).
    /// This also applies for thread timeouts set on the `Task` itself, which overrides this timeout for that specific task.
    pub fn with_thread_tmt(mut self, timeout: Duration) -> Self {
        self.set_thread_tmt(timeout);
        self
    }

    /// Set the resource timeout for worker threads.
    ///
    /// For cpu-bound tasks, this timeout may not immediately stop the thread, as the thread may be executing a blocking operation. 
    /// In this case, the request count is used to prevent the thread from overwriting a newer resource in the handler (Similar to generation counters).
    /// This also applies for thread timeouts set on the `Task` itself, which overrides this timeout for that specific task.
    pub fn set_thread_tmt(&mut self, timeout: Duration) {
        self.thread_timeout = timeout;
    }

    /// Set the timeout for failed resources.
    pub fn with_failed_tmt(mut self, timeout: FailedTimeout) -> Self {
        self.set_failed_tmt(timeout);
        self
    }

    /// Set the timeout for failed resources.
    pub fn set_failed_tmt(&mut self, timeout: FailedTimeout) {
        self.failed_timeout = timeout;
    }

    /// Set the max retry attempts for `Failed` resources.
    pub fn with_max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.set_max_retries(max_retries);
        self
    }

    /// Set the max retry attempts for `Failed` resources.
    pub fn set_max_retries(&mut self, max_retries: Option<u32>) {
        self.max_retries = max_retries;
    }

    /// Returns a resource if it is in the `Ready` state. If the resource has not yet been requested or previously failed, 
    /// the provided `Task` is run, and `None` is returned.
    /// 
    /// * `key` - A key of type `K` used query the handler for the resource
    /// * `task` - A `Task` that resolves with `Result<R, E>`
    pub fn get_or_request<F>(&mut self, key: K, task: Task<F, R, E>) -> Option<&R> 
    where 
        F: Future<Output = Result<R, E>> + Send + 'static
    {
        if !self.is_requested(&key) {
            self.request(key, task);
            return None;
        }

        self.get(&key)
    }

    /// Request a new resource by running a `Task`.
    /// 
    /// If the resource had not yet been requested or was previously requested but failed, this method runs the provided `Task` so long as it has not exceeded the max retry attempts.
    /// If the resource was previously requested but is pending or ready, this method does nothing.
    /// 
    /// For *cpu-bound* tasks, generation counters are used to prevent older requests that timed out from overwriting new ones.
    /// 
    /// * `key` - A key of type `K` used query the handler for the resource
    /// * `task` - A `Task` that resolves with `Result<R, E>`
    pub fn request<F>(&mut self, key: K, task: Task<F, R, E>) 
    where 
        F: Future<Output = Result<R, E>> + Send + 'static
    {
        let request_count = match self.eval_retries(&key, task.max_retries) {
            Some(count) => count,
            None => {
                // Propagate the resource config to last known request.
                // This allows for intuitive behavior for requests even if the task doesn't run due to max retries.
                if let Some(rsc) = self.resource_map.get_mut(&key) {
                    rsc.config = task.rsc_config;
                }
                return;
            }
        };

        let config = task.rsc_config;
        let thread_handle = task.run(
            key.clone(), 
            request_count, 
            self.tx.clone()
        );
        
        let resource = Resource {
            state: ResourceState::Pending(thread_handle),
            request_count,
            requested_at: Instant::now(),
            config
        };

        self.resource_map.insert(key, resource);
    }

    /// Evaluates whether a resource has exceeded the max retry count, and returns the next request count if it has not. 
    /// If the resource has exceeded its max retry count, `None` is returned. If the resource does not exist, `Some(1)` is returned.
    fn eval_retries(&self, key: &K, task_retry: Option<u32>) -> Option<u32> {
        if let Some(rsc) = self.resource_map.get(key) {
            if let Some(max_retries) = task_retry.or(self.max_retries) {
                if rsc.request_count >= max_retries {
                    return None;
                }
            }

            return Some(rsc.request_count + 1);
        }

        Some(1)
    }

    /// Store an already created resource.
    /// 
    /// If a resource mapped to the provided `key` already exists, this method overwrites that resource with the new one.
    /// 
    /// * `key` - A key of type `K` used query the `ResourceHandler`.
    /// * `resource` - An instance of the expected resource `R` this `ResourceHandler` stores
    /// * `config` - Configuration for how the resource should be handled by the `ResourceHandler`
    pub fn store(&mut self, key: &K, resource: R, config: ResourceConfig) {
        let resource = Resource {
            state: ResourceState::Ready { 
                rsc: resource,
                accessed: Cell::new(Instant::now())
            },
            request_count: 1,
            requested_at: Instant::now(),
            config
        };

        self.resource_map.insert(key.clone(), resource);
    }

    /// Remove a resource from the internal map (in any state). Returns the resource if in the `Ready` state, else `None`.
    pub fn remove(&mut self, key: &K) -> Option<R> {
        self.resource_map
            .remove(key)
            .and_then(|rsc| rsc.take())
    }

    /// Removes a resource from the internal map if it's in the `Ready` state, and returns it.
    pub fn remove_ready(&mut self, key: &K) -> Option<R> {
        if self.is_ready(key) { self.remove(key) } else { None }
    }

    /// Removes a resource from the internal map if it's in the `Failed` state.
    pub fn remove_failed(&mut self, key: &K) {
        if self.is_failed(key) { self.remove(key); }
    }

    /// Removes all resources.
    pub fn clear(&mut self) {
        // Generation counters automatically prevent still running tasks from polluting the map, so this is all thats needed.
        self.resource_map.clear();
    }

    /// Removes all resources that are in the `Failed` state.
    pub fn clear_failed(&mut self) {
        self.resource_map.retain(|_, rsc| {
            rsc.is_pending() || rsc.is_ready()
        });
    }
 
    /// Synchronize the task worker threads with the main thread, making available any completed resources. 
    ///
    /// This method should be called regularly (i.e. every frame)
    pub fn sync(&mut self) {
        while let Ok((key, request_count, new_state)) = self.rx.try_recv() {            
            // only update the resource if it exists and has more requests (newer generation)
            if let Some(rsc) = self.resource_map.get_mut(&key) {
                if rsc.request_count < request_count { continue; }
                rsc.state = new_state;
            }
        }

        let now = Instant::now();
        self.resource_map.retain(|_, rsc| {
            rsc.eval_state(&now, self.thread_timeout, self.failed_timeout)
        });
    }

    /// Returns `true` if a resource with `key` is stored in the map and is in the `Ready` state, `false` otherwise.
    /// 
    /// Note: This does not mark the resource as accessed.
    pub fn is_ready(&self, key: &K) -> bool {
        self.resource_map.get(key).is_some_and(|rsc| rsc.is_ready())
    }

    /// Returns `true` if a resource with `key` is stored in the map and is in the `Pending` state, `false` otherwise.
    pub fn is_pending(&self, key: &K) -> bool {
        self.resource_map.get(key).is_some_and(|rsc| rsc.is_pending())
    }

    /// Returns `true` if a resource with `key` is stored in the map and is in the `Failed` state, `false` otherwise.
    pub fn is_failed(&self, key: &K) -> bool {
        self.resource_map.get(key).is_some_and(|rsc| rsc.is_failed())
    }

    /// Returns `true` if a resource is in either the `Pending` or `Ready` states, `false` otherwise.
    pub fn is_requested(&self, key: &K) -> bool {
        self.resource_map.get(key).is_some_and(|rsc| {
            return rsc.is_pending() || rsc.is_ready()
        })
    }

    /// Returns the `Instant` the resource was requested if it exists, else `None`.
    pub fn requested_at(&self, key: &K) -> Option<&Instant> {
        self.resource_map.get(key).map(|rsc| rsc.requested_at())
    }

    /// Runs a closure `f` on a resource if exists and is in the `Ready` state. 
    /// 
    /// Note: This marks the resource as accessed. If this is unintented behavior, call `peek_if_ready()`.
    pub fn apply_if_ready(&self, key: &K, f: impl FnOnce(&R)) {
        if let Some(rsc) = self.get(key) { f(rsc); };
    }

    /// Runs a closure `f` on a resource if exists and is in the `Ready` state. 
    /// 
    /// Note: This does NOT mark the resource as accessed. If this is unintented behavior, call `apply_if_ready()`.
    pub fn peek_if_ready(&self, key: &K, f: impl FnOnce(&R)) {
        if let Some(rsc) = self.peek(key) { f(rsc); };
    }

    /// Runs a closure `f` for each resource if ready. 
    /// 
    /// Note: This marks each resource as accessed. If this is unintented behavior, call `peek_for_each_ready()`.
    pub fn apply_for_each_ready(&self, mut f: impl FnMut(&K, &R)) {
        for (key, rsc) in self.resource_map.iter() {
            if let Some(r) = rsc.get() { f(key, r); }
        }
    }

    /// Runs a closure `f` for each `Ready` resource. 
    /// 
    /// Note: This does not mark the resources as accessed. If this is unintented behavior, call `apply_for_each_ready()`.
    pub fn peek_for_each_ready(&self, mut f: impl FnMut(&K, &R)) {
        for (key, rsc) in self.resource_map.iter() {
            if let Some(r) = rsc.peek() { f(key, r); }
        }
    }

    /// Returns an `Instant` specifying the last time the resource was accessed if in the `Ready` state, else `None`.
    pub fn last_accessed(&self, key: &K) -> Option<Instant> {
        self.resource_map.get(key)?.last_accessed()
    }

    /// Get the error `E` of a resource with `key` if in the `Failed` state.
    pub fn get_err_of(&self, key: &K) -> Option<&TaskError<E>> {
        return self.resource_map.get(key)?.get_error();
    }

    /// Returns the `ResourceState` of a resource if exists, else `None`
    pub fn get_state_of(&self, key: &K) -> Option<&ResourceState<R, E>> {
        self.resource_map.get(key).map(|rsc| &rsc.state)
    }

    /// Returns a reference to a resource if in the `Ready` state, else `None`. 
    /// 
    /// Note: This marks the resource as accessed. If this is unintended behavior, call `peek()`  
    pub fn get(&self, key: &K) -> Option<&R> {
        if let Some(rsc) = self.resource_map.get(key) {
            return rsc.get();
        }
        None
    }

    /// Returns a reference to a resource if in the `Ready` state, else `None`. 
    /// 
    /// Note: This does NOT mark the resource as accessed. If this is unintended behavior, call `get()` 
    pub fn peek(&self, key: &K) -> Option<&R> {
        if let Some(rsc) = self.resource_map.get(key) {
            return rsc.peek();
        }
        None
    }

    /// Mark a `Ready` resource as accessed.
    /// 
    /// This is useful in the case the resource is a dependency to some other resource, but you don't need a reference to it.
    pub fn mark_accessed(&self, key: &K) {
        if let Some(rsc) = self.resource_map.get(key) {
            rsc.mark_accessed();
        }
    }

    /// Returns `true` if the internal map contains a resource with the specified `key` (in any state), `false` otherwise.
    /// 
    /// Note: This does not mark the resource as accessed if in the `Ready` state.
    pub fn contains(&self, key: &K) -> bool {
        self.resource_map.contains_key(key)
    }
    
    /// Get an iterator of the keys of all resources, regardless of state.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.resource_map.keys()
    }

    /// Get an iterator of the keys of all `Ready` resources.
    pub fn ready_keys(&self) -> impl Iterator<Item = &K> {
        self.resource_map.iter().filter_map(|(key, rsc)| {
            if rsc.is_ready() { Some(key) } else { None }
        })
    }

    /// Get an iterator of the keys of all `Pending` resources.
    pub fn pending_keys(&self) -> impl Iterator<Item = &K> {
        self.resource_map.iter().filter_map(|(key, rsc)| {
            if rsc.is_pending() { Some(key) } else { None }
        })
    }

    /// Get an iterator of the keys of all `Failed` resources.
    pub fn failed_keys(&self) -> impl Iterator<Item = &K> {
        self.resource_map.iter().filter_map(|(key, rsc)| {
            if rsc.is_failed() { Some(key) } else { None }
        })
    }

    /// Get an iterator of all resource keys and states in the form of a tuple.
    pub fn get_rsc_states(&self) -> impl Iterator<Item = (&K, &ResourceState<R, E>)> {
        self.resource_map.iter().map(|(key, resource)| {
            (key, &resource.state)
        })
    }

    /// Returns the number of resources in the internal map, regardless of state.
    pub fn len(&self) -> usize {
        self.resource_map.len()
    }

    /// Returns `true` if the internal map is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.resource_map.is_empty()
    }

    /// Returns the number of resources in the `Ready` state.
    pub fn count_ready(&self) -> usize {
        self.resource_map.values().filter(|rsc| rsc.is_ready()).count()
    }

    /// Returns the number of resources in the `Pending` state.
    pub fn count_pending(&self) -> usize {
        self.resource_map.values().filter(|rsc| rsc.is_pending()).count()
    }

    /// Returns the number of resources in the `Failed` state.
    pub fn count_failed(&self) -> usize {
        self.resource_map.values().filter(|rsc| rsc.is_failed()).count()
    }
}