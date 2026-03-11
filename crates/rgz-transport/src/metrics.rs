use std::sync::atomic::{AtomicU64, Ordering};

/// In-memory counters for queue saturation paths.
#[derive(Debug, Default)]
pub struct TransportMetrics {
    command_full_total: AtomicU64,
    control_full_total: AtomicU64,
    event_dropped_total: AtomicU64,
    io_event_dropped_total: AtomicU64,
    sub_cmd_full_total: AtomicU64,
}

/// Point-in-time copy of queue saturation counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportMetricsSnapshot {
    pub command_full_total: u64,
    pub control_full_total: u64,
    pub event_dropped_total: u64,
    pub io_event_dropped_total: u64,
    pub sub_cmd_full_total: u64,
}

impl TransportMetrics {
    pub fn inc_command_full(&self) {
        self.command_full_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_control_full(&self) {
        self.control_full_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_event_dropped(&self) {
        self.event_dropped_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_io_event_dropped(&self) {
        self.io_event_dropped_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sub_cmd_full(&self) {
        self.sub_cmd_full_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TransportMetricsSnapshot {
        TransportMetricsSnapshot {
            command_full_total: self.command_full_total.load(Ordering::Relaxed),
            control_full_total: self.control_full_total.load(Ordering::Relaxed),
            event_dropped_total: self.event_dropped_total.load(Ordering::Relaxed),
            io_event_dropped_total: self.io_event_dropped_total.load(Ordering::Relaxed),
            sub_cmd_full_total: self.sub_cmd_full_total.load(Ordering::Relaxed),
        }
    }
}
