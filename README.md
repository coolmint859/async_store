# async-store

A lightweight, TTL-based, async resource storage and lifecycle handler created on top of tokio.

## Features
- Deterministic resource state tracking via explicit synconization calls.
- Lock-free via use of MPSC channels, making it extremely performant.
- Support for any kind of background work (io or cpu bound) through the use of Futures
- Automatic TTL garbage collection based on resource access.
- Custom Error types for when resources fail to complete.

## Quick start

Add this to your `Cargo.toml`:

```toml
[dependencies]
async-store = "0.1.0"
```

### Simple Example Usage
```rust
fn main() {
    let mut handler = ResourceHandler::new();
    
    let key = String::from("unique_key");
    let hold_policy = HoldPolicy::WithDuration(Duration::from_secs(3));
    let task = Task::cpu_bound(hold_policy, async || {
        // do some work on background thread, return completed resource
    });
    handler.request(key, task);

    // some time later...
    handler.sync();

    // calling get() marks the resource as accessed, resetting it's hold timer
    if let Some(rsc) = self.handler.get(&key) {
        // do something with resource
    }
}
```

## More Details

At the core of async-store is the `ResourceHandler` and `Task` structs. Together they allow easy resource creation by tracking the states of resources on background threads managed by `tokio`. Resources can either be `Pending`, `Ready` or `Failed`. All three states are easily discoverable at runtime.

Resources start out in the `Pending` state as the background thread does it's work. Depending on the result, it will either transition to `Ready` if the worker thread completes successfully, or `Failed` if it does not complete successfully.

### Tasks

A `Task` is a wrapper over some `Future` that you define, and manages the tokio thread the resource is created in. It's made to work directly with a `ResourceHandler`.

Tasks are created with a `HoldPolicy`, which is what determines how long a resource stays alive for once in the `Ready` state. You can opt out of the TTL feature by simply specifying `HoldPolicy::Indefinite`. This will have the resource created with the `Task` persist unless manually removed from a `ResourceHandler`.

Tasks can be either 'io-bound' or 'cpu-bound', which really just determines whether or not the future is run on tokio's work stealing pool. 

### ResourceHandlers

A `ResourceHandler` is the data structure that stores a resource created from a `Task`.

Requesting a task to run through the handler can be done by calling `request()`. This method is idempotent, meaning you can call it as much as you like with the same key, and only one task will spawn.

The only exception to this is in the case the resource with the key previously was in the `Failed` state, which can happen if the thread times out before completion. If this is the case, a new thread will spawn and the state will change to `Pending`. Any results from previously spawned threads created under the same key will be discarded.

The `ResourceHandler` avoids locks through the use of a MPSC channel. All worker threads are 'producers' in this sense, where the `ResourceHandler` is the 'consumer'. 

In order to get access to the resources created and stored in the handler, `sync()` must be called. If many resources are to be created, it's recommended to call this regularly. Accessing a resource can be done through either `get()` or `get_mut()`, which automatically resets it's hold timer.

## License

This project is licensed under:

- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org)
