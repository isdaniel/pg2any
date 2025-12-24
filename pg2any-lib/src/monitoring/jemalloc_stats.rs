//! jemalloc Memory Statistics Module
//!
//! This module provides integration with jemalloc memory allocator for
//! memory statistics collection (allocated, resident, active bytes).
//!
//! # Features
//!
//! This module is only available when the `metrics` feature is enabled.
//!
//! # Usage
//!
//! ```rust,no_run
//! use pg2any_lib::monitoring::jemalloc_stats::{JemallocStats, get_jemalloc_stats};
//!
//! // Get current memory statistics
//! let stats = get_jemalloc_stats();
//! println!("Allocated: {} bytes", stats.allocated);
//! println!("Resident: {} bytes", stats.resident);
//! println!("Utilization: {:.2}%", stats.utilization_percent());
//! ```

#[cfg(feature = "metrics")]
use tikv_jemalloc_ctl::{epoch, stats};

/// Memory statistics from jemalloc
#[derive(Debug, Clone, Default)]
pub struct JemallocStats {
    /// Total number of bytes allocated by the application
    pub allocated: usize,
    /// Total number of bytes in physically resident data pages mapped by the allocator
    pub resident: usize,
    /// Total number of bytes in active pages allocated by the application
    pub active: usize,
    /// Total number of bytes in chunks mapped on behalf of the application
    pub mapped: usize,
    /// Total number of bytes dedicated to metadata
    pub metadata: usize,
    /// Total number of bytes retained (not returned to OS)
    pub retained: usize,
}

impl JemallocStats {
    /// Calculate memory overhead (metadata + retained)
    pub fn overhead(&self) -> usize {
        self.metadata.saturating_add(self.retained)
    }

    /// Calculate fragmentation (resident - active)
    pub fn fragmentation(&self) -> usize {
        self.resident.saturating_sub(self.active)
    }

    /// Calculate utilization percentage (allocated / resident * 100)
    pub fn utilization_percent(&self) -> f64 {
        if self.resident == 0 {
            0.0
        } else {
            (self.allocated as f64 / self.resident as f64) * 100.0
        }
    }
}

/// Get current jemalloc memory statistics
///
/// This function advances the jemalloc epoch to ensure fresh statistics
/// and then collects various memory metrics.
///
/// # Returns
///
/// Returns a `JemallocStats` struct containing memory statistics.
/// If jemalloc is not available or an error occurs, returns default values.
///
/// # Example
///
/// ```rust,no_run
/// let stats = pg2any_lib::monitoring::jemalloc_stats::get_jemalloc_stats();
/// println!("Memory allocated: {} MB", stats.allocated / 1024 / 1024);
/// ```
#[cfg(feature = "metrics")]
pub fn get_jemalloc_stats() -> JemallocStats {
    // Advance epoch to refresh cached statistics
    let _ = epoch::advance();

    JemallocStats {
        allocated: stats::allocated::read().unwrap_or(0),
        resident: stats::resident::read().unwrap_or(0),
        active: stats::active::read().unwrap_or(0),
        mapped: stats::mapped::read().unwrap_or(0),
        metadata: stats::metadata::read().unwrap_or(0),
        retained: stats::retained::read().unwrap_or(0),
    }
}

#[cfg(not(feature = "metrics"))]
pub fn get_jemalloc_stats() -> JemallocStats {
    JemallocStats::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jemalloc_stats_overhead() {
        let stats = JemallocStats {
            allocated: 1000,
            resident: 2000,
            active: 1500,
            mapped: 3000,
            metadata: 100,
            retained: 200,
        };

        assert_eq!(stats.overhead(), 300);
        assert_eq!(stats.fragmentation(), 500);
        assert!((stats.utilization_percent() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_jemalloc_stats_zero_resident() {
        let stats = JemallocStats {
            resident: 0,
            ..Default::default()
        };

        assert_eq!(stats.utilization_percent(), 0.0);
    }

    #[test]
    #[cfg(feature = "metrics")]
    fn test_get_jemalloc_stats() {
        let stats = get_jemalloc_stats();
        // Just ensure it doesn't panic and returns valid data
        // Allocated should be greater than 0 since we're running
        assert!(stats.allocated > 0);
    }
}
