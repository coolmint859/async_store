pub mod handler;
pub mod resource;
pub mod task;

pub use handler::*;
pub use resource::*;
pub use task::*;

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::time::sleep;
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    enum TestError {
        LoadFailed
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            return write!(f, "Test load failed");
        }
    }
    impl std::error::Error for TestError {}

    /// Ensures a resource made with an io-bound task completes successfully.
    #[tokio::test]
    async fn test_io_bound_lifecycle() {
        let mut handler: ResourceHandler<String, String, TestError> = ResourceHandler::new();
        let key = "asset_1".to_string();

        let task = Task::io_bound(
            HoldPolicy::Indefinite,
            async { Ok("Hello, Tokio-Store!".to_string()) }
        );

        let res = handler.get_or_request(key.clone(), task);
        assert!(res.is_none());
        assert!(handler.is_pending(&key));

        sleep(Duration::from_millis(50)).await;
        handler.sync();

        assert!(handler.is_ready(&key));
        let value = handler.get(&key);
        assert_eq!(value.map(|s| s.as_str()), Some("Hello, Tokio-Store!"));

        let removed = handler.remove(&key);
        assert_eq!(removed, Some("Hello, Tokio-Store!".to_string()));
        assert!(!handler.contains(&key));
    }

    /// Ensures a resource made with an cpu-bound task completes successfully.
    #[tokio::test]
    async fn test_cpu_bound_lifecycle() {
        let mut handler: ResourceHandler<i32, i32, TestError> = ResourceHandler::new();
        let key = 42;

        let task = Task::cpu_bound(
            HoldPolicy::Indefinite,
            async { Ok(123) }
        );

        handler.request(key, task);
        assert!(handler.is_pending(&key));

        sleep(Duration::from_millis(50)).await;
        handler.sync();

        assert!(handler.is_ready(&key));
        assert_eq!(handler.get(&key), Some(&123));

        let removed = handler.remove(&key);
        assert_eq!(removed, Some(123));
        assert!(!handler.contains(&key));
    }

    /// Ensures a resource that fails with a custom error is handled correctly.
    #[tokio::test]
    async fn test_task_internal_error_handling() {
        let mut handler: ResourceHandler<String, (), TestError> = ResourceHandler::new();
        let key = "bad_asset".to_string();

        let task = Task::io_bound(
            HoldPolicy::Indefinite,
            async { Err(TestError::LoadFailed) }
        );

        handler.request(key.clone(), task);
        sleep(Duration::from_millis(50)).await;
        handler.sync();

        assert!(handler.is_failed(&key));
        let err = handler.get_err_of(&key);
        
        match err {
            Some(TaskError::TaskInternal(TestError::LoadFailed)) => {},
            _ => panic!("Expected TaskInternal error"),
        }
    }

    /// Ensures a resource that is run with a panicking thread is handled correctly.
    #[tokio::test]
    async fn test_task_panic_handling() {
        let mut handler: ResourceHandler<String, i32, TestError> = ResourceHandler::new();
        let key = "panic_asset".to_string();

        let task = Task::io_bound(
            HoldPolicy::Indefinite,
            async {
                panic!("Intentional worker panic!");
            }
        );

        handler.request(key.clone(), task);
        sleep(Duration::from_millis(50)).await;
        handler.sync();

        assert!(handler.is_failed(&key));
        let err = handler.get_err_of(&key);

        match err {
            Some(TaskError::TaskPanic) => {},
            _ => panic!("Expected TaskPanic error"),
        }
    }

    /// Ensures that generation tracking is handled correctly, 
    /// and that successful tasks are priortized over failed tasks with the same key.
    #[tokio::test]
    async fn test_generation_tracking() {
        let mut handler: ResourceHandler<String, usize, TestError> = ResourceHandler::new();
        let key = "retry_asset".to_string();

        let failing_task = Task::io_bound(
            HoldPolicy::Indefinite,
            async { Err(TestError::LoadFailed) }
        );
        handler.request(key.clone(), failing_task);
        
        sleep(Duration::from_millis(30)).await;
        handler.sync();
        assert!(handler.is_failed(&key));

        let succeeding_task = Task::io_bound(
            HoldPolicy::Indefinite,
            async { Ok(42) }
        );
        handler.request(key.clone(), succeeding_task);
        assert!(handler.is_pending(&key));

        sleep(Duration::from_millis(30)).await;
        handler.sync();

        assert!(handler.is_ready(&key));
        assert_eq!(handler.get(&key), Some(&42));
    }

    /// Ensures pre-loaded data is stored in the handler correctly.
    #[tokio::test]
    async fn test_preloaded_storage() {
        let mut handler: ResourceHandler<String, &str, TestError> = ResourceHandler::new();
        let key = "config".to_string();

        handler.store(&key, HoldPolicy::Indefinite, "cached_data");

        assert!(handler.is_ready(&key));
        assert_eq!(handler.get(&key), Some(&"cached_data"));
    }
}