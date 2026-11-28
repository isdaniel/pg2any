//! Byte-bounded admission control for snapshot rows.

use crate::error::{CdcError, Result};
use pg_walstream::{ColumnValue, RowData};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Column name plus value framing (quotes, separators, `Bytes` handle) in the rendered row.
const PER_COLUMN_OVERHEAD: usize = 32;

/// 1 permit = 1 KiB, so `acquire_many_owned`'s `u32` covers multi-TiB budgets.
fn kib_units(bytes: usize) -> u32 {
    u32::try_from(bytes.div_ceil(1024)).unwrap_or(u32::MAX)
}

pub(crate) struct ByteBudget {
    semaphore: Arc<Semaphore>,
    capacity_units: u32,
}

/// Returns its bytes to the budget on drop.
pub(crate) struct BytePermit {
    _permit: OwnedSemaphorePermit,
}

impl ByteBudget {
    pub(crate) fn new(capacity_bytes: usize) -> Self {
        let capacity_units = kib_units(capacity_bytes).max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(capacity_units as usize)),
            capacity_units,
        }
    }

    /// A request larger than the whole budget is clamped to it, so it is admitted alone.
    pub(crate) async fn acquire(&self, bytes: usize) -> Result<BytePermit> {
        let units = kib_units(bytes).max(1).min(self.capacity_units);
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(units)
            .await
            .map_err(|_| CdcError::generic("snapshot byte budget closed"))?;
        Ok(BytePermit { _permit: permit })
    }
}

pub(crate) fn row_byte_estimate(data: &RowData) -> usize {
    data.iter().fold(0usize, |acc, (_, value)| {
        let payload = match value {
            ColumnValue::Text(b) | ColumnValue::Binary(b) => b.len(),
            ColumnValue::Null => 0,
        };
        acc.saturating_add(payload.saturating_add(PER_COLUMN_OVERHEAD))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use pg_walstream::ColumnValue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::{pause, sleep, timeout};

    const STEP: Duration = Duration::from_millis(50);

    #[tokio::test]
    async fn oversize_request_is_admitted_alone() {
        pause();
        let budget = ByteBudget::new(4 * 1024);

        let big = timeout(STEP, budget.acquire(1024 * 1024))
            .await
            .expect("oversize request must not deadlock")
            .unwrap();
        // Holds the whole budget: nothing else fits.
        assert!(timeout(STEP, budget.acquire(1)).await.is_err());

        drop(big);
        timeout(STEP, budget.acquire(1)).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn second_acquire_waits_until_first_permit_dropped() {
        pause();
        let budget = Arc::new(ByteBudget::new(2 * 1024));
        let first = budget.acquire(2 * 1024).await.unwrap();

        let waiter = {
            let budget = budget.clone();
            tokio::spawn(async move { budget.acquire(1024).await.map(|_| ()) })
        };
        sleep(STEP).await;
        assert!(!waiter.is_finished());

        drop(first);
        timeout(STEP, waiter).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn total_outstanding_never_exceeds_capacity_with_normal_requests() {
        pause();
        const CAPACITY: usize = 8 * 1024;
        let budget = Arc::new(ByteBudget::new(CAPACITY));
        let outstanding = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let tasks: Vec<_> = (0..40usize)
            .map(|i| {
                let (budget, outstanding, peak) =
                    (budget.clone(), outstanding.clone(), peak.clone());
                tokio::spawn(async move {
                    let bytes = (i % 3 + 1) * 1024;
                    let permit = budget.acquire(bytes).await.unwrap();
                    let now = outstanding.fetch_add(bytes, Ordering::SeqCst) + bytes;
                    peak.fetch_max(now, Ordering::SeqCst);
                    sleep(Duration::from_millis(10)).await;
                    outstanding.fetch_sub(bytes, Ordering::SeqCst);
                    drop(permit);
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }

        let peak = peak.load(Ordering::SeqCst);
        assert!(peak <= CAPACITY, "peak {peak} exceeded capacity {CAPACITY}");
        assert!(peak > 1024, "tasks never overlapped; test is vacuous");
    }

    #[tokio::test]
    async fn saturated_sizes_do_not_overflow() {
        pause();
        let budget = ByteBudget::new(usize::MAX);
        let permit = timeout(STEP, budget.acquire(usize::MAX))
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        timeout(STEP, ByteBudget::new(0).acquire(0))
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn estimate_counts_text_and_binary_and_null_overhead() {
        let empty = RowData::from_pairs(vec![]);
        assert_eq!(row_byte_estimate(&empty), 0);

        let null_only = RowData::from_pairs(vec![("a", ColumnValue::Null)]);
        let overhead = row_byte_estimate(&null_only);
        assert!(overhead > 0, "NULL still costs per-column overhead");

        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("abc")),
            (
                "b",
                ColumnValue::binary_bytes(Bytes::from_static(&[0u8; 5])),
            ),
            ("c", ColumnValue::Null),
        ]);
        assert_eq!(row_byte_estimate(&row), 3 + 5 + 3 * overhead);
    }
}
