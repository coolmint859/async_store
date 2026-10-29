use std::{cell::Cell, collections::HashMap, fmt::{Debug, Display}, hash::Hash, sync::mpsc, time::{Duration, Instant}};

use crate::{resource::{Pending, Ready, Resource, ResourceState}, task::{HoldPolicy, Task, TaskError}};

/// Manages and stores memory resources with concurrent creation through futures.
/// Allows any future as long as the output type is a `Result<R, E>`.
/// 
/// * `K`: The key type to store resources with
/// * `R`: the resource type that will be stored
/// * `E`: the error type in case a resource fails completion
pub struct ResourceHandler<K, R, E> {
    resource_map: HashMap<K, Resource<R, E>>,

    tx: mpsc::Sender<(K, Resource<R, E>)>,
    rx: mpsc::Receiver<(K, Resource<R, E>)>,

    thread_timeout: Duration, // time before a worker thread is considered 'dead' by the main thread
    failed_timeout: Duration, // time before a failed resource is removed from the map
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
            failed_timeout: Duration::from_secs(3),
            tx, rx,
        }
    }

    /// Set the resource timeout for worker threads, in seconds. The default is 5 seconds.
    /// 
    /// This is the amount of time before a thread is considered 'dead' and is told to stop executing.
    pub fn set_thread_tmt(&mut self,  timeout: u64) {
        self.thread_timeout = Duration::from_secs(timeout);
    }

    /// Set the timeout for failed resources, in seconds. The default is 3 seconds.
    /// 
    /// This is the amount of time before a failed resource is removed from the internal map. 
    pub fn set_failed_tmt(&mut self, timeout: u64) {
        self.failed_timeout = Duration::from_secs(timeout);
    }

    /// Returns a resource if it is in the `Ready` state. If the resource has not yet been requested or previously failed, 
    /// a worker thread is spawned to execute the `Future F`, and `None` is returned.
    /// 
    /// * 'key' - A key of type `K` used query the handler for the resource
    /// * 'task' - A `Task` whose `Future` resolves with the resource
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

    /// Request a worker thread to create a resource by running a `Task`.
    /// 
    /// If the resource is brand new or previously requested but failed, this method spawns a background tokio thread.
    /// If the resource was previously requested but is pending or ready, this method does nothing.
    /// 
    /// For cpu-bound tasks, generation counters are used to prevent older requests that timed out from overwriting new ones.
    /// 
    /// * 'key' - A key of type `K` used query the handler for the resource
    /// * 'task' - A `Task` that resolves with a resource `R`
    pub fn request<F>(&mut self, key: K, task: Task<F, R, E>) 
    where 
        F: Future<Output = Result<R, E>> + Send + 'static
    {
        let next_gen = match self.resource_map.get(&key) {
            Some(rsc) => match &rsc.state {
                ResourceState::Failed(_) => rsc.generation + 1,
                _ => return
            },
            None => 1
        };

        let resource = Resource {
            state: ResourceState::Pending(Pending {
                thread_handle: task.run(key.clone(), next_gen, self.tx.clone()),
                requested_at: Instant::now(),
            }),
            generation: next_gen
        };

        self.resource_map.insert(key, resource);
    }

    /// Store a preloaded resource into the internal map
    /// 
    /// * 'key' - A key of type `K` used query the handler for the resource
    /// * 'hold_time' - The time in seconds before a resource is considered 'dead' and is removed from the handler
    /// * 'resource' - An instance of the expected Resource this handler stores
    pub fn store(&mut self, key: &K, hold_policy: HoldPolicy, resource: R) {
        let resource = Resource {
            state: ResourceState::Ready(Ready { 
                rsc: resource, 
                hold_policy, 
                accessed: Cell::new(Instant::now())
            }),
            generation: 1
        };

        self.resource_map.insert(key.clone(), resource);
    }

    /// Remove a resource from the internal map (in any state). Returns the resource if found.
    pub fn remove(&mut self, key: &K) -> Option<R> {
        self.resource_map
            .remove(key)
            .and_then(|rsc| rsc.take())
    }

    /// Removes a resource from internal map if it's in the `Ready` state, and returns it.
    pub fn remove_ready(&mut self, key: &K) -> Option<R> {
        if self.is_ready(key) { self.remove(key) } else { None }
    }

    /// Removes a resource from internal map if it's in the `Failed` state.
    pub fn remove_failed(&mut self, key: &K) {
        if self.is_failed(key) { self.remove(key); }
    }

    /// Removes all resources.
    pub fn clear(&mut self) {
        self.resource_map.clear();
    }

    /// Removes all resources that are in the `Failed` state.
    pub fn clear_failed(&mut self) {
        self.resource_map.retain(|_, rsc| {
            rsc.is_pending() || rsc.is_ready()
        });
    }

    /// Syncronize the resource threads with the main thread, making available any completed resources. 
    ///
    /// Should be called regularly (i.e. every frame)
    pub fn sync(&mut self) {
        while let Ok((key, sent_rsc)) = self.rx.try_recv() {
            let key = key.clone();
            let matching_generation = match self.resource_map.get(&key) {
                Some(stored_rsc) => stored_rsc.generation == sent_rsc.generation,
                None => false
            };

            // skip mismatched generations - this prevents stale resources from overwriting more recent ones,
            // as well as ensuring a resource that was removed while pending is not inserted back into the map.
            if !matching_generation { continue; }

            self.resource_map.insert(key, sent_rsc);
        }

        let now = Instant::now();
        self.resource_map.retain(|_, rsc| {
            rsc.eval_state(&now, self.thread_timeout, self.failed_timeout)
        });
    }

    /// Returns `true` if a resource with `key` is stored in the map and is in the `Ready` state, `false` otherwise.
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

    /// Runs a closure `f` on a resource if exists and is in the `Ready` state.
    pub fn is_ready_and(&self, key: &K, f: impl FnOnce(&R)) {
        if let Some(rsc) = self.get(key) { f(rsc); };
    }

    /// Returns an `Instant` specifying the last time the resource was accessed if in the `Ready` state, else `None`.
    pub fn last_accessed(&self, key: &K) -> Option<Instant> {
        if let Some(rsc) = self.resource_map.get(key) {
            rsc.last_accessed()
        } else {
            None
        }
    }

    /// Get the error `E` of a resource with `key` if in the `Failed` state.
    pub fn get_err_of(&self, key: &K) -> Option<&TaskError<E>> {
        return self.resource_map.get(key)?.get_error();
    }

    /// Returns the `ResourceState` of a resource if exists, else `None`
    pub fn get_state_of(&self, key: &K) -> Option<&ResourceState<R, E>> {
        self.resource_map.get(key).map(|rsc| &rsc.state)
    }

    /// Returns a reference to a resource if in the `Ready` state, else `None`. Marks the resource as accessed.
    pub fn get(&self, key: &K) -> Option<&R> {
        match self.resource_map.get(key)?.state {
            ResourceState::Ready(ref ready) => {
                ready.accessed.set(Instant::now());
                Some(&ready.rsc)
            },
            _ => None
        }
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
    
    /// Get an iterator of the keys of all known resources, regardless of status
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
}