//! Graceful shutdown handling

use parking_lot::Mutex;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::signal;
use tracing::{info, warn};

/// Shutdown signal that can be cloned and awaited
#[derive(Clone)]
pub struct ShutdownSignal {
    inner: Arc<ShutdownInner>,
}

struct ShutdownInner {
    triggered: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl ShutdownSignal {
    /// Create a new shutdown signal
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownInner {
                triggered: AtomicBool::new(false),
                wakers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Check if shutdown has been triggered
    #[inline]
    pub fn is_triggered(&self) -> bool {
        self.inner.triggered.load(Ordering::Acquire)
    }

    /// Trigger the shutdown signal
    pub fn trigger(&self) {
        if !self.inner.triggered.swap(true, Ordering::Release) {
            info!("Shutdown signal triggered");
            let wakers = std::mem::take(&mut *self.inner.wakers.lock());
            for waker in wakers {
                waker.wake();
            }
        }
    }

    /// Wait for the shutdown signal
    pub fn wait(&self) -> ShutdownFuture {
        ShutdownFuture {
            signal: self.clone(),
        }
    }

    /// Install Ctrl+C handler that triggers this shutdown signal
    pub fn install_ctrl_c_handler(&self) {
        let signal = self.clone();
        tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => {
                    warn!("Received Ctrl+C, initiating graceful shutdown...");
                    signal.trigger();
                }
                Err(e) => {
                    warn!("Failed to listen for Ctrl+C: {}", e);
                }
            }
        });
    }

    /// Install both Ctrl+C and SIGTERM handlers
    #[cfg(unix)]
    pub fn install_signal_handlers(&self) {
        self.install_ctrl_c_handler();

        let signal = self.clone();
        tokio::spawn(async move {
            let mut sigterm =
                signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
            sigterm.recv().await;
            warn!("Received SIGTERM, initiating graceful shutdown...");
            signal.trigger();
        });
    }

    #[cfg(not(unix))]
    pub fn install_signal_handlers(&self) {
        self.install_ctrl_c_handler();
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Future that resolves when shutdown is triggered
pub struct ShutdownFuture {
    signal: ShutdownSignal,
}

impl Future for ShutdownFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.signal.is_triggered() {
            return Poll::Ready(());
        }

        // Register waker
        let mut wakers = self.signal.inner.wakers.lock();

        // Check again after acquiring lock
        if self.signal.is_triggered() {
            return Poll::Ready(());
        }

        wakers.push(cx.waker().clone());
        Poll::Pending
    }
}

/// Helper to run a future until shutdown or completion
pub async fn select_shutdown<F, T>(shutdown: ShutdownSignal, future: F) -> Option<T>
where
    F: Future<Output = T>,
{
    tokio::select! {
        result = future => Some(result),
        _ = shutdown.wait() => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_signal() {
        let signal = ShutdownSignal::new();
        assert!(!signal.is_triggered());

        let signal2 = signal.clone();
        let handle = tokio::spawn(async move {
            signal2.wait().await;
            true
        });

        // Give the task time to start waiting
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        signal.trigger();
        assert!(signal.is_triggered());

        let result = handle.await.unwrap();
        assert!(result);
    }
}

