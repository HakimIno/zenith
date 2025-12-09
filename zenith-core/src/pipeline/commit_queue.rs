//! Commit ordering queue using binary heap
//!
//! Ensures transactions are flushed in commit LSN order
//! to maintain exactly-once semantics.

use super::transaction_buffer::Transaction;
use crate::metrics::METRICS;
use parking_lot::Mutex;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use tracing::{debug, trace};

/// Wrapper for ordering transactions by commit_lsn (ascending)
struct OrderedTransaction(Transaction);

impl PartialEq for OrderedTransaction {
    fn eq(&self, other: &Self) -> bool {
        self.0.commit_lsn == other.0.commit_lsn
    }
}

impl Eq for OrderedTransaction {}

impl PartialOrd for OrderedTransaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedTransaction {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap behavior (lower LSN = higher priority)
        other.0.commit_lsn.cmp(&self.0.commit_lsn)
    }
}

/// Commit queue for ordering transactions by LSN
///
/// Uses a min-heap to maintain order by commit_lsn.
/// Only flushes transactions whose commit_lsn ≤ confirmed_lsn + 1.
pub struct CommitQueue {
    /// Priority queue ordered by commit_lsn (min-heap)
    heap: Mutex<BinaryHeap<OrderedTransaction>>,
    /// Current confirmed LSN (last successfully flushed)
    confirmed_lsn: AtomicU64,
    /// Maximum queue size before backpressure
    max_size: usize,
}

impl CommitQueue {
    /// Create a new commit queue
    pub fn new(max_size: usize) -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::with_capacity(max_size)),
            confirmed_lsn: AtomicU64::new(0),
            max_size,
        }
    }

    /// Get current confirmed LSN
    #[inline]
    pub fn confirmed_lsn(&self) -> u64 {
        self.confirmed_lsn.load(AtomicOrdering::Acquire)
    }

    /// Update confirmed LSN after successful flush
    pub fn update_confirmed_lsn(&self, lsn: u64) {
        let old = self.confirmed_lsn.fetch_max(lsn, AtomicOrdering::AcqRel);
        if lsn > old {
            debug!("Updated confirmed_lsn: {} -> {}", old, lsn);
            METRICS.set_confirmed_lsn(lsn);
        }
    }

    /// Get current queue size
    pub fn len(&self) -> usize {
        self.heap.lock().len()
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.heap.lock().is_empty()
    }

    /// Check if queue is at capacity
    pub fn is_full(&self) -> bool {
        self.len() >= self.max_size
    }

    /// Push a committed transaction onto the queue
    ///
    /// Returns false if the queue is full (backpressure).
    pub fn push(&self, txn: Transaction) -> bool {
        if txn.commit_lsn.is_none() {
            debug!("Ignoring uncommitted transaction xid={}", txn.xid);
            return true;
        }

        let mut heap = self.heap.lock();

        if heap.len() >= self.max_size {
            return false; // Backpressure
        }

        trace!(
            "Queueing transaction xid={} at lsn={}",
            txn.xid,
            txn.commit_lsn.unwrap()
        );

        heap.push(OrderedTransaction(txn));
        METRICS.set_commit_queue_size(heap.len());

        true
    }

    /// Pop transactions that are ready to be flushed
    ///
    /// Returns all transactions whose commit_lsn ≤ confirmed_lsn + gap_allowed.
    /// This allows for some reordering tolerance while maintaining consistency.
    pub fn pop_ready(&self, gap_allowed: u64) -> Vec<Transaction> {
        let confirmed = self.confirmed_lsn.load(AtomicOrdering::Acquire);
        let threshold = confirmed.saturating_add(gap_allowed);

        let mut heap = self.heap.lock();
        let mut ready = Vec::new();

        while let Some(OrderedTransaction(txn)) = heap.peek() {
            if let Some(commit_lsn) = txn.commit_lsn {
                if commit_lsn <= threshold {
                    if let Some(OrderedTransaction(txn)) = heap.pop() {
                        ready.push(txn);
                    }
                } else {
                    break; // No more ready transactions
                }
            } else {
                // This shouldn't happen, but remove it anyway
                heap.pop();
            }
        }

        if !ready.is_empty() {
            debug!("Popped {} ready transactions", ready.len());
            METRICS.set_commit_queue_size(heap.len());
        }

        ready
    }

    /// Pop a single transaction if ready
    pub fn pop_one(&self) -> Option<Transaction> {
        let confirmed = self.confirmed_lsn.load(AtomicOrdering::Acquire);
        let mut heap = self.heap.lock();

        if let Some(OrderedTransaction(txn)) = heap.peek() {
            if let Some(commit_lsn) = txn.commit_lsn {
                // Allow transaction if it's the next expected one
                if commit_lsn <= confirmed.saturating_add(1) || confirmed == 0 {
                    if let Some(OrderedTransaction(txn)) = heap.pop() {
                        METRICS.set_commit_queue_size(heap.len());
                        return Some(txn);
                    }
                }
            }
        }

        None
    }

    /// Drain all transactions (for shutdown)
    pub fn drain_all(&self) -> Vec<Transaction> {
        let mut heap = self.heap.lock();
        let mut all = Vec::with_capacity(heap.len());

        while let Some(OrderedTransaction(txn)) = heap.pop() {
            all.push(txn);
        }

        // Sort by commit_lsn for final flush
        all.sort_by_key(|t| t.commit_lsn);

        METRICS.set_commit_queue_size(0);
        all
    }

    /// Peek at the next transaction without removing it
    pub fn peek_next_lsn(&self) -> Option<u64> {
        self.heap
            .lock()
            .peek()
            .and_then(|OrderedTransaction(txn)| txn.commit_lsn)
    }

    /// Get statistics about the queue
    pub fn stats(&self) -> CommitQueueStats {
        let heap = self.heap.lock();
        let confirmed = self.confirmed_lsn.load(AtomicOrdering::Acquire);

        let (min_lsn, max_lsn) = heap
            .iter()
            .filter_map(|OrderedTransaction(t)| t.commit_lsn)
            .fold((u64::MAX, 0u64), |(min, max), lsn| {
                (min.min(lsn), max.max(lsn))
            });

        CommitQueueStats {
            size: heap.len(),
            confirmed_lsn: confirmed,
            min_pending_lsn: if min_lsn == u64::MAX { None } else { Some(min_lsn) },
            max_pending_lsn: if max_lsn == 0 { None } else { Some(max_lsn) },
            total_events: heap.iter().map(|OrderedTransaction(t)| t.events.len()).sum(),
        }
    }
}

impl Default for CommitQueue {
    fn default() -> Self {
        Self::new(10_000)
    }
}

/// Statistics about the commit queue
#[derive(Debug, Clone)]
pub struct CommitQueueStats {
    pub size: usize,
    pub confirmed_lsn: u64,
    pub min_pending_lsn: Option<u64>,
    pub max_pending_lsn: Option<u64>,
    pub total_events: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::transaction_buffer::Event;
    use crate::pipeline::Operation;
    use chrono::Utc;

    fn create_test_transaction(xid: u64, commit_lsn: u64, event_count: usize) -> Transaction {
        let mut txn = Transaction::new(xid, 0);
        txn.commit(commit_lsn, 0);

        for i in 0..event_count {
            txn.events.push(Event::new(
                commit_lsn,
                xid,
                Operation::Insert,
                "test.table".to_string(),
                "test_table_v1".to_string(), // selector
                serde_json::json!({"id": i}),
                None,
                Utc::now(),
                Some(serde_json::json!({"id": i})),
            ));
        }

        txn
    }

    #[test]
    fn test_commit_queue_ordering() {
        let queue = CommitQueue::new(100);

        // Push transactions out of order
        queue.push(create_test_transaction(3, 300, 1));
        queue.push(create_test_transaction(1, 100, 1));
        queue.push(create_test_transaction(2, 200, 1));

        assert_eq!(queue.len(), 3);

        // Pop should return in order
        let ready = queue.pop_ready(u64::MAX);
        assert_eq!(ready.len(), 3);
        assert_eq!(ready[0].commit_lsn, Some(100));
        assert_eq!(ready[1].commit_lsn, Some(200));
        assert_eq!(ready[2].commit_lsn, Some(300));
    }

    #[test]
    fn test_confirmed_lsn_threshold() {
        let queue = CommitQueue::new(100);

        queue.push(create_test_transaction(1, 100, 1));
        queue.push(create_test_transaction(2, 200, 1));
        queue.push(create_test_transaction(3, 300, 1));

        // With gap_allowed = 0, should only pop first if confirmed_lsn = 0
        let ready = queue.pop_ready(100);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].commit_lsn, Some(100));

        // Update confirmed LSN
        queue.update_confirmed_lsn(100);

        // Now can pop transactions up to 200
        let ready = queue.pop_ready(100);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].commit_lsn, Some(200));
    }

    #[test]
    fn test_drain_all() {
        let queue = CommitQueue::new(100);

        queue.push(create_test_transaction(1, 100, 5));
        queue.push(create_test_transaction(2, 200, 3));

        let all = queue.drain_all();
        assert_eq!(all.len(), 2);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_backpressure() {
        let queue = CommitQueue::new(2);

        assert!(queue.push(create_test_transaction(1, 100, 1)));
        assert!(queue.push(create_test_transaction(2, 200, 1)));
        assert!(!queue.push(create_test_transaction(3, 300, 1))); // Should fail
        assert!(queue.is_full());
    }
}

