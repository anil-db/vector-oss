use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tower::{Layer, Service};
use vector_lib::stream::DriverResponse;

/// A layer that blocks all requests for 30 seconds when any clone receives a response code 555
#[derive(Clone)]
pub struct BlockingLayer {
    blocked_until: Arc<Mutex<Option<Instant>>>,
}

impl BlockingLayer {
    pub fn new() -> Self {
        Self {
            blocked_until: Arc::new(Mutex::new(None)),
        }
    }
}

impl<S> Layer<S> for BlockingLayer {
    type Service = BlockingService<S>;

    fn layer(&self, service: S) -> Self::Service {
        BlockingService {
            service,
            blocked_until: self.blocked_until.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BlockingService<S> {
    service: S,
    blocked_until: Arc<Mutex<Option<Instant>>>,
}

impl<S> BlockingService<S> {
    pub fn new(service: S) -> Self {
        Self {
            service,
            blocked_until: Arc::new(Mutex::new(None)),
        }
    }
}

impl<S, Request> Service<Request> for BlockingService<S>
where
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        // Check if we're currently blocked
        if let Some(blocked_until) = *self.blocked_until.lock().unwrap() {
            if blocked_until > Instant::now() {
                // Still blocked, return NotReady
                return std::task::Poll::Pending;
            } else {
                // Block has expired, clear it
                *self.blocked_until.lock().unwrap() = None;
            }
        }

        // Not blocked, check if inner service is ready
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let mut service = self.service.clone();
        let blocked_until = self.blocked_until.clone();

        Box::pin(async move {
            // Check if we're blocked before making the request
            if let Some(blocked_until) = *blocked_until.lock().unwrap() {
                if blocked_until > Instant::now() {
                    // Calculate remaining time
                    let remaining = blocked_until.duration_since(Instant::now());
                    tokio::time::sleep(remaining).await;
                } else {
                    // Block has expired, clear it
                    *blocked_until.lock().unwrap() = None;
                }
            }

            // Make the request
            let response = service.call(req).await?;

            // Check if we got a 555 response
            if response.event_status().is_rejected() {
                // Set the block for all clones
                *blocked_until.lock().unwrap() = Some(Instant::now() + Duration::from_secs(30));
            }

            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tower::ServiceBuilder;
    use vector_lib::stream::EventStatus;

    struct MockResponse;
    impl DriverResponse for MockResponse {
        fn event_status(&self) -> EventStatus {
            EventStatus::Rejected
        }
    }

    struct MockService;
    impl Service<()> for MockService {
        type Response = MockResponse;
        type Error = ();
        type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: ()) -> Self::Future {
            Box::pin(async move { Ok(MockResponse) })
        }
    }

    #[tokio::test]
    async fn test_blocking_layer() {
        let layer = BlockingLayer::new();
        let service = ServiceBuilder::new()
            .layer(layer)
            .service(MockService);

        // First request should succeed but trigger blocking
        let response = service.clone().call(()).await.unwrap();
        assert!(response.event_status().is_rejected());

        // Second request should be blocked
        let start = Instant::now();
        let response = service.clone().call(()).await.unwrap();
        let duration = start.elapsed();
        assert!(duration >= Duration::from_secs(30));
        assert!(response.event_status().is_rejected());
    }
}
