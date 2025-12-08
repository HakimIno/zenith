//! WAL position tracking and management

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use zenith_storage::WalPositionStore;

/// WAL position tracker for exactly-once semantics
///
/// Tracks three key positions:
/// - received_lsn: Last LSN received from PostgreSQL
/// - processed_lsn: Last LSN whose events have been processed
/// - confirmed_lsn: Last LSN that has been durably flushed to sink and storage
pub struct WalPosition {
    /// Last LSN received from source
    received_lsn: AtomicU64,
    /// Last LSN fully processed (events extracted)
    processed_lsn: AtomicU64,
    /// Last LSN confirmed flushed to sink
    confirmed_lsn: AtomicU64,
    /// Persistent storage
    storage: Arc<WalPositionStore>,
}

impl WalPosition {
    /// Create a new WAL position tracker with storage
    pub fn new(storage: Arc<WalPositionStore>) -> Self {
        let confirmed_lsn = storage.confirmed_lsn();

        Self {
            received_lsn: AtomicU64::new(confirmed_lsn),
            processed_lsn: AtomicU64::new(confirmed_lsn),
            confirmed_lsn: AtomicU64::new(confirmed_lsn),
            storage,
        }
    }

    /// Get the starting LSN for resumption
    pub fn start_lsn(&self) -> u64 {
        self.confirmed_lsn.load(Ordering::Acquire)
    }

    /// Update received LSN
    #[inline]
    pub fn update_received(&self, lsn: u64) {
        self.received_lsn.fetch_max(lsn, Ordering::AcqRel);
    }

    /// Update processed LSN
    #[inline]
    pub fn update_processed(&self, lsn: u64) {
        self.processed_lsn.fetch_max(lsn, Ordering::AcqRel);
    }

    /// Update confirmed LSN with persistence
    ///
    /// This is called after successfully flushing to the sink.
    /// Uses batched persistence for performance.
    pub fn update_confirmed(&self, lsn: u64, rows_flushed: u64, force: bool) -> crate::Result<()> {
        let old = self.confirmed_lsn.fetch_max(lsn, Ordering::AcqRel);

        if lsn > old {
            self.storage.update_lsn(lsn, rows_flushed, force)?;
        }

        Ok(())
    }

    /// Force flush all pending LSN updates to disk
    pub fn flush(&self) -> crate::Result<()> {
        let lsn = self.confirmed_lsn.load(Ordering::Acquire);
        self.storage.update_lsn(lsn, 0, true)?;
        Ok(())
    }

    /// Get current received LSN
    #[inline]
    pub fn received_lsn(&self) -> u64 {
        self.received_lsn.load(Ordering::Acquire)
    }

    /// Get current processed LSN
    #[inline]
    pub fn processed_lsn(&self) -> u64 {
        self.processed_lsn.load(Ordering::Acquire)
    }

    /// Get current confirmed LSN
    #[inline]
    pub fn confirmed_lsn(&self) -> u64 {
        self.confirmed_lsn.load(Ordering::Acquire)
    }

    /// Calculate current lag (received - confirmed)
    #[inline]
    pub fn lag(&self) -> u64 {
        let received = self.received_lsn.load(Ordering::Acquire);
        let confirmed = self.confirmed_lsn.load(Ordering::Acquire);
        received.saturating_sub(confirmed)
    }

    /// Get all positions as a snapshot
    pub fn snapshot(&self) -> WalPositionSnapshot {
        WalPositionSnapshot {
            received_lsn: self.received_lsn.load(Ordering::Acquire),
            processed_lsn: self.processed_lsn.load(Ordering::Acquire),
            confirmed_lsn: self.confirmed_lsn.load(Ordering::Acquire),
        }
    }
}

/// Snapshot of WAL positions
#[derive(Debug, Clone, Copy)]
pub struct WalPositionSnapshot {
    pub received_lsn: u64,
    pub processed_lsn: u64,
    pub confirmed_lsn: u64,
}

impl WalPositionSnapshot {
    /// Calculate lag between received and confirmed
    #[inline]
    pub fn lag(&self) -> u64 {
        self.received_lsn.saturating_sub(self.confirmed_lsn)
    }

    /// Calculate pending (processed but not confirmed)
    #[inline]
    pub fn pending(&self) -> u64 {
        self.processed_lsn.saturating_sub(self.confirmed_lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_position() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = WalPositionStore::open(temp_dir.path()).unwrap();
        let position = WalPosition::new(storage);

        assert_eq!(position.start_lsn(), 0);

        position.update_received(1000);
        position.update_processed(1000);
        position.update_confirmed(1000, 100, true).unwrap();

        assert_eq!(position.received_lsn(), 1000);
        assert_eq!(position.processed_lsn(), 1000);
        assert_eq!(position.confirmed_lsn(), 1000);
        assert_eq!(position.lag(), 0);
    }

    #[test]
    fn test_lag_calculation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = WalPositionStore::open(temp_dir.path()).unwrap();
        let position = WalPosition::new(storage);

        position.update_received(1000);
        position.update_confirmed(500, 100, true).unwrap();

        assert_eq!(position.lag(), 500);
    }
}

