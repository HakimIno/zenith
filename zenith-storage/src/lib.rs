//! Zenith Storage - Embedded sled database wrapper for exactly-once semantics
//!
//! Provides persistent storage for confirmed LSN positions and checkpoint metadata.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use sled::{Db, Tree};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Storage errors
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Invalid data format")]
    InvalidFormat,
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Keys used in storage
const CONFIRMED_LSN_KEY: &[u8] = b"confirmed_lsn";
const SLOT_PREFIX: &[u8] = b"slot:";
const CHECKPOINT_TREE: &str = "checkpoints";
const METADATA_TREE: &str = "metadata";
const SNAPSHOT_TREE: &str = "snapshot_progress";

/// Checkpoint data for a replication slot
#[derive(Debug, Clone)]
pub struct SlotCheckpoint {
    pub slot_name: String,
    pub confirmed_lsn: u64,
    pub flushed_rows: u64,
    pub last_flush_time: u64, // Unix timestamp in milliseconds
}

impl SlotCheckpoint {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(32 + self.slot_name.len());
        // Length-prefixed slot name
        buf.put_u16(self.slot_name.len() as u16);
        buf.put_slice(self.slot_name.as_bytes());
        buf.put_u64(self.confirmed_lsn);
        buf.put_u64(self.flushed_rows);
        buf.put_u64(self.last_flush_time);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 26 {
            return Err(StorageError::InvalidFormat);
        }

        let mut buf = data;
        let name_len = buf.get_u16() as usize;
        if buf.len() < name_len + 24 {
            return Err(StorageError::InvalidFormat);
        }

        let slot_name = String::from_utf8(buf[..name_len].to_vec())
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        buf.advance(name_len);

        let confirmed_lsn = buf.get_u64();
        let flushed_rows = buf.get_u64();
        let last_flush_time = buf.get_u64();

        Ok(Self {
            slot_name,
            confirmed_lsn,
            flushed_rows,
            last_flush_time,
        })
    }
}

/// WAL position tracker with persistence
pub struct WalPositionStore {
    db: Db,
    checkpoints: Tree,
    metadata: Tree,
    snapshot_progress: Tree,
    cached_lsn: AtomicU64,
    pending_flush: RwLock<PendingFlush>,
}

struct PendingFlush {
    rows_since_flush: u64,
    last_flush_time: std::time::Instant,
}

impl Default for PendingFlush {
    fn default() -> Self {
        Self {
            rows_since_flush: 0,
            last_flush_time: std::time::Instant::now(),
        }
    }
}

impl WalPositionStore {
    /// Open or create a new WAL position store
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        let db = sled::Config::new()
            .path(path.as_ref())
            .cache_capacity(64 * 1024 * 1024) // 64MB cache
            .mode(sled::Mode::HighThroughput)
            .flush_every_ms(Some(1000))
            .open()?;

        let checkpoints = db.open_tree(CHECKPOINT_TREE)?;
        let metadata = db.open_tree(METADATA_TREE)?;
        let snapshot_progress = db.open_tree(SNAPSHOT_TREE)?;

        // Load cached LSN from storage
        let cached_lsn = metadata
            .get(CONFIRMED_LSN_KEY)?
            .map(|v| {
                if v.len() >= 8 {
                    u64::from_be_bytes(v[..8].try_into().unwrap())
                } else {
                    0
                }
            })
            .unwrap_or(0);

        info!("Opened WAL position store, confirmed_lsn: {}", cached_lsn);

        Ok(Arc::new(Self {
            db,
            checkpoints,
            metadata,
            snapshot_progress,
            cached_lsn: AtomicU64::new(cached_lsn),
            pending_flush: RwLock::new(PendingFlush {
                rows_since_flush: 0,
                last_flush_time: std::time::Instant::now(),
            }),
        }))
    }

    /// Get the current confirmed LSN (fast, from cache)
    #[inline]
    pub fn confirmed_lsn(&self) -> u64 {
        self.cached_lsn.load(Ordering::Acquire)
    }

    /// Update confirmed LSN with batching
    ///
    /// Only flushes to disk when:
    /// - 100k rows have been processed
    /// - 1 second has elapsed since last flush
    /// - force=true
    pub fn update_lsn(&self, lsn: u64, rows_processed: u64, force: bool) -> Result<bool> {
        // Always update cache
        self.cached_lsn.store(lsn, Ordering::Release);

        let should_flush = {
            let mut pending = self.pending_flush.write();
            pending.rows_since_flush += rows_processed;

            let elapsed = pending.last_flush_time.elapsed();
            let flush_by_rows = pending.rows_since_flush >= 100_000;
            let flush_by_time = elapsed.as_millis() >= 1000;

            if force || flush_by_rows || flush_by_time {
                pending.rows_since_flush = 0;
                pending.last_flush_time = std::time::Instant::now();
                true
            } else {
                false
            }
        };

        if should_flush {
            self.persist_lsn(lsn)?;
            debug!("Flushed confirmed_lsn: {} to disk", lsn);
            return Ok(true);
        }

        Ok(false)
    }

    /// Force persist LSN to disk immediately
    fn persist_lsn(&self, lsn: u64) -> Result<()> {
        self.metadata
            .insert(CONFIRMED_LSN_KEY, &lsn.to_be_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    /// Save checkpoint for a specific slot
    pub fn save_checkpoint(&self, checkpoint: &SlotCheckpoint) -> Result<()> {
        let key = [SLOT_PREFIX, checkpoint.slot_name.as_bytes()].concat();
        self.checkpoints.insert(key, checkpoint.encode().as_ref())?;
        Ok(())
    }

    /// Load checkpoint for a specific slot
    pub fn load_checkpoint(&self, slot_name: &str) -> Result<Option<SlotCheckpoint>> {
        let key = [SLOT_PREFIX, slot_name.as_bytes()].concat();
        match self.checkpoints.get(key)? {
            Some(data) => Ok(Some(SlotCheckpoint::decode(&data)?)),
            None => Ok(None),
        }
    }

    /// List all slot checkpoints
    pub fn list_checkpoints(&self) -> Result<Vec<SlotCheckpoint>> {
        let mut checkpoints = Vec::new();
        for result in self.checkpoints.scan_prefix(SLOT_PREFIX) {
            let (_, value) = result?;
            checkpoints.push(SlotCheckpoint::decode(&value)?);
        }
        Ok(checkpoints)
    }

    /// Force flush all pending writes to disk
    pub fn flush(&self) -> Result<()> {
        let lsn = self.cached_lsn.load(Ordering::Acquire);
        self.persist_lsn(lsn)?;
        info!("Force flushed all data to disk, confirmed_lsn: {}", lsn);
        Ok(())
    }

    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        StorageStats {
            confirmed_lsn: self.confirmed_lsn(),
            disk_size: self.db.size_on_disk().unwrap_or(0),
            checkpoint_count: self.checkpoints.len(),
        }
    }

    /// Compact the database
    pub fn compact(&self) -> Result<()> {
        warn!("Starting database compaction...");
        // Sled doesn't have explicit compaction, but we can flush
        self.db.flush()?;
        Ok(())
    }

    /// Record that a table has been snapshotted at a specific LSN
    pub fn set_table_snapshot_lsn(&self, table: &str, lsn: u64) -> Result<()> {
        self.snapshot_progress.insert(table.as_bytes(), &lsn.to_be_bytes())?;
        Ok(())
    }

    /// Get the LSN at which a table was snapshotted (if any)
    pub fn get_table_snapshot_lsn(&self, table: &str) -> Result<Option<u64>> {
        match self.snapshot_progress.get(table.as_bytes())? {
            Some(v) => {
                if v.len() >= 8 {
                    Ok(Some(u64::from_be_bytes(v[..8].try_into().unwrap())))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}

impl Drop for WalPositionStore {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            warn!("Failed to flush on drop: {}", e);
        }
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub confirmed_lsn: u64,
    pub disk_size: u64,
    pub checkpoint_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_checkpoint_encode_decode() {
        let checkpoint = SlotCheckpoint {
            slot_name: "test_slot".to_string(),
            confirmed_lsn: 123456789,
            flushed_rows: 1000000,
            last_flush_time: 1700000000000,
        };

        let encoded = checkpoint.encode();
        let decoded = SlotCheckpoint::decode(&encoded).unwrap();

        assert_eq!(decoded.slot_name, checkpoint.slot_name);
        assert_eq!(decoded.confirmed_lsn, checkpoint.confirmed_lsn);
        assert_eq!(decoded.flushed_rows, checkpoint.flushed_rows);
        assert_eq!(decoded.last_flush_time, checkpoint.last_flush_time);
    }

    #[test]
    fn test_wal_position_store() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = WalPositionStore::open(temp_dir.path()).unwrap();

        assert_eq!(store.confirmed_lsn(), 0);

        // Update LSN
        store.update_lsn(1000, 50000, false).unwrap();
        assert_eq!(store.confirmed_lsn(), 1000);

        // Force flush
        store.update_lsn(2000, 1, true).unwrap();
        assert_eq!(store.confirmed_lsn(), 2000);

        // Verify persistence
        drop(store);
        let store2 = WalPositionStore::open(temp_dir.path()).unwrap();
        assert_eq!(store2.confirmed_lsn(), 2000);
    }

    #[test]
    fn test_slot_checkpoints() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = WalPositionStore::open(temp_dir.path()).unwrap();

        let checkpoint = SlotCheckpoint {
            slot_name: "slot1".to_string(),
            confirmed_lsn: 999,
            flushed_rows: 500,
            last_flush_time: 1700000000000,
        };

        store.save_checkpoint(&checkpoint).unwrap();

        let loaded = store.load_checkpoint("slot1").unwrap().unwrap();
        assert_eq!(loaded.slot_name, "slot1");
        assert_eq!(loaded.confirmed_lsn, 999);

        let all = store.list_checkpoints().unwrap();
        assert_eq!(all.len(), 1);
    }
}

