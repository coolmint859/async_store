# async-store

A lightweight, TTL-based, async resource storage and lifecycle handler built on top of tokio.

## Features
- Easily request resources to be created and stored with built in idempotency and retry logic
- Deterministic resource state tracking via an explicit sync call
- Uses a MPSC channel to drive resource updates
- Resources are stored in an internal `HashMap` for O(1) lookups
- Support for any kind of background work (io or cpu bound) through the use of futures
- Automatic TTL garbage collection based on resource access
- Automatic and configurable thread and failure timeouts
- Custom errors for failed tasks

### Limitations

- The `ResourceHandler` is not thread safe; it relies on the main thread to drive MPSC synchronization
- Resource synchronization has O(n) time complexity, where n is the number of resources
- Assumes resources to be unrelated to eachother except in their type

### Future Features
- Double-buffered resource updates (Old resources are available right up until the new one is ready at the next `sync()` call)
- Batching multiple similar tasks to run on the same worker thread

## What is this for?
- Custom lightweight game or graphics engines
- Procedural content generation
- Other real time systems that are primarily driven by the main thread

## Quick start

Add this to your `Cargo.toml`:

```toml
[dependencies]
async-store = "0.4.0"
```

### Simple Example Usage
Here is an example of running a task that resolves with the integer `42`, and then is stored for later use. In a real world use case, the worker thread would likely do more complex work.
```rust
use std::time::Duration;
use tokio::time::sleep;

use async_store::{HoldPolicy, ResourceHandler, Task};

#[tokio::main]
pub async fn main() {
    // The second `String` represents the error type
    let mut handler = ResourceHandler::<String, i32, String>::new();
    
    let key = String::from("unique_key");
    let hold_policy = HoldPolicy::WithDuration(Duration::from_secs(3));
    let task = Task::cpu_bound(hold_policy, async {
        // You would do likely do some more complicated work here

        Ok(42)
    });
    // Request the task to run and store result in a handler
    handler.request(key.clone(), task);

    // In a real world use case, sync() would typically be called every frame or update tick.
    sleep(Duration::from_millis(100)).await;
    handler.sync();

    // Calling get() here marks the resource as accessed, resetting it's hold time
    if let Some(rsc) = handler.get(&key) {
        println!("{}", rsc);
    }

    // Three seconds pass before the resource is accessed again, so sync() will remove the resource automatically. 
    // This is because the hold policy was set to expire the resource after three seconds.
    // If you want to keep the resource until you manually remove it, use `HoldPolicy::Indefinite` instead.
    sleep(Duration::from_secs(3)).await;
    handler.sync();

    // Calling get() will now return `None` because the resource was removed from the handler during the `sync()` call.
    if let None = handler.get(&key) {
        println!("Resource has expired and was removed from the handler.");
    }

    // The resource no longer exists in the handler, so contains() will return false.
    assert!(!handler.contains(&key));
}
```

# How it Works

At the core of `async-store` are two primary types: `Task`s and `ResourceHandler`s.

## Tasks

A `Task` is a wrapper over a `Future` and runs the `tokio` thread that a resource is constructed in. It's designed to work exclusively with a `ResourceHandler`. Tasks do not run until passed into `ResourceHandler::request()`.

### Task Types

A `Task` comes in two flavors: *io-bound* or *cpu-bound*. Io-bound tasks run on `tokio`'s core thread pool, and cpu-bound tasks run on `tokio`'s blocking thread pool. To understand why there is a distinction, see the [tokio docs](https://docs.rs/tokio/latest/tokio/).

### Hold Policies (TTL)

Tasks are created with a `HoldPolicy`, which determine how long a resource stays alive for once stored. You can opt out of the TTL feature by simply specifying `HoldPolicy::Indefinite` when creating the `Task`. This will have the stored resource persist indefinitely unless manually removed.

## ResourceHandlers

A `ResourceHandler` is the data structure that stores a resource created from a `Task`. Resources are stored internally in a `HashMap` for O(1) lookups.

`ResourceHandler`s are not thread safe and should only be used on the main thread or in isolated contexts. 

### Resource States and Lifecycles
Resources transition between three primary states:
- **Pending** - the `Task` is actively running on a worker thread. These can timeout if they take too long. To configure this behavior, call `set_thread_tmt()` on the `ResourceHandler`. You can also set the thread timeout on the `Task` itself, which overrides the `ResourceHandler`'s timeout.
- **Ready** - the `Task` completed successfully and the resource is ready to use. Resources will be removed automatically if they are not accessed based on their `HoldPolicy`. If the policy is set to `HoldPolicy::Indefinite`, this rule does not apply.
- **Failed** - the `Task` failed to complete, panicked, or timed out. Failed resources will be removed after some time. To configure this behavior, call `set_failed_tmt()` on the `ResourceHandler`.

To determine which state a resource is in, you can also call the `is_*()` methods on the `ResourceHandler`.

### Requesting Resources

There are two ways to store a resource in a `ResourceHandler`:
- calling `request()` or `get_or_request()`, providing it with a `Task`. `get_or_request()` will return a reference to the resource if available.
- calling `store()`, providing it with an already completed resource and a `HoldPolicy`.

Calling the first two methods will result in a retry under two conditions: 
1) The resource already exists under the same key and is currently in the failed state.
2) The `ResourceHandler` has a maximum retry limit set (default is 3) and the resource's retry count does not exceed it, or the max retry limit is set to `None`. Tasks can also be set with a maximum retry limit which overrides the `ResourceHandler`'s

Otherwise, they are fully idepontent: Calling them multiple times under the same key will not spawn a new worker thread.

In the case the retry limit is breached for a resource request, the resource will remain in the failed state until removed (either automatically or manually).

### Accessing Resources

Resources can be retrieved by calling `get()` or `peek()`. The main difference is that `get()` will mark the resource as accessed if available, while `peek()` does not. 

There are also several helper methods for running simple closures on resources or for bulk actions, such as `apply_if_ready()` or `peek_for_each_ready()`. For a list of all methods you can call, see the [docs](https://docs.rs/async-store/0.2.2/async_store).

### Thread Synchronization

`ResourceHandler`s avoid locks through the use of a MPSC channel. All worker threads are *producers* in this sense, where the `ResourceHandler` is the *consumer*. 

Because of this, **`sync()` should be called regularly** on the `ResourceHandler`, typically once per frame or update tick. This method drains the mpsc channel, processes completed tasks, and updates the internal resource map.

## Feedback

This is the first crate I've ever published. If you have any questions, feature requests, or issues you would like to see fixed, please open a discussion on the [GitHub repository](https://github.com/coolmint859/async_store/discussions) for the project!

## License

This project is licensed under:

- MIT license ([LICENSE-MIT](LICENSE) or http://opensource.org)
