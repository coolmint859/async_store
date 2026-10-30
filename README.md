# async-store

A lightweight, TTL-based, async resource storage and lifecycle handler built on top of tokio.

## Features
- Easily request resources to be created and stored with built in idempotency and retry logic
- Deterministic resource state tracking via an explicit sync call
- Lock-free via use of a MPSC channel
- Resources are stored in an internal `HashMap` for O(1) lookups
- Support for any kind of background work (io or cpu bound) through the use of futures
- Automatic TTL garbage collection based on resource access
- Automatic and configurable thread and failure timeouts
- Custom errors for failed tasks

### Limitations

- ResourceHandler is not inherently thread safe and relies on the main thread to drive MPSC synchronization
- Resource synchronization has O(n) time complexity, where n is the number of resources
- Assumes resources to be unrelated to eachother except in their type
- Resource mutability or updates after completion is not supported (yet)

## What is this for?
- Custom lightweight game or graphics engines
- Procedural content generation
- Other real time systems that are primarily driven by the main thread

## Quick start

Add this to your `Cargo.toml`:

```toml
[dependencies]
async-store = "0.3.0"
```

### Simple Example Usage
Here is an example of running a task that resolves with the integer `42`, and then is stored for later use. In a real world use case, the worker thread would likely do more complex work.
```rust
use std::time::Duration;
use tokio::time::sleep;

use async_store::{HoldPolicy, ResourceHandler, Task};

#[tokio::main]
pub async fn main() {
    // the second `String` represents the error type
    let mut handler = ResourceHandler::<String, i32, String>::new();
    
    let key = String::from("unique_key");
    let hold_policy = HoldPolicy::WithDuration(Duration::from_secs(3));
    let task = Task::cpu_bound(hold_policy, async {
        // you would do likely do some more complicated work here

        Ok(42)
    });
    // request the task to run and store result in a handler
    handler.request(key.clone(), task);

    // In a real world use case, sync() would typically be called every frame or update tick.
    sleep(Duration::from_millis(100)).await;
    handler.sync();

    // calling get() marks the resource as accessed, resetting it's hold time
    if let Some(rsc) = handler.get(&key) {
        println!("{}", rsc);
    }
}
```

# How it Works

At the core of `async-store` are two primary types: `Task`s and `ResourceHandler`s.

## Tasks

A `Task` is a wrapper over a `Future` and runs the `tokio` thread that a resource is constructed in. It's designed to work exclusively with a `ResourceHandler`.

### Task Types

A `Task` comes in two flavors: *io-bound* or *cpu-bound*. Io-bound tasks are run on `tokio`'s core thread pool, and cpu-bound tasks are run on `tokio`'s blocking thread pool. To understand why there is a distinction, see the [tokio docs](https://docs.rs/tokio/latest/tokio/).

### Hold Policies (TTL)

Tasks are created with a `HoldPolicy`, which determine how long a resource stays alive for once stored. You can opt out of the TTL feature by simply specifying `HoldPolicy::Indefinite` when creating the `Task`. This will have the stored resource persist indefinitely unless manually removed.

## ResourceHandlers

A `ResourceHandler` is the data structure that stores a resource created from a `Task`. Resources are stored internally in a `HashMap` for O(1) lookups.

### Resource States and Lifecycles
Resources transition between three primary states:
- **Pending** - the `Task` is actively running on a worker thread. These can timeout if they take too long. To configure this behavior, call `set_thread_tmt()` on the `ResourceHandler`.
- **Ready** - the `Task` completed successfully and the resource is ready to use. Resources will be removed automatically if they are not accessed based on their `HoldPolicy`. If the policy is set to `HoldPolicy::Indefinite`, this rule does not apply.
- **Failed** - the `Task` failed to complete, panicked, or timed out. Failed resources will be removed after some time. To configure this behavior, call `set_failed_tmt()` on the `ResourceHandler`.

To determine which state a resource is in, you can also call the `is_*()` methods on the `ResourceHandler`.

### Storing Resources

There are two ways to store a resource in a `ResourceHandler`:
- calling `request()` or `get_or_request()`, providing it with a `Task`. `get_or_request()` will return a reference to the resource if available.
- calling `store()`, providing it with an already completed resource and a `HoldPolicy`.

The first two methods are fully idempotent - you can call them as often as you like with the same key, and only one worker thread will spawn. 
* *Exception*: If the task previously failed under that same key, calling either method will spawn a *new* worker thread and safely reset the state to `Pending`.

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
