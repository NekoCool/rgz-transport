//! Top-level transport APIs will be implemented here.

/// Placeholder type for the transport runtime.
pub struct TransportRuntime;

impl TransportRuntime {
    /// No-op constructor for bootstrap.
    pub fn new() -> Self {
        Self
    }
}

impl Default for TransportRuntime {
    fn default() -> Self {
        Self::new()
    }
}
