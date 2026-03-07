use serde::Deserialize;
use serde::Serialize;

/// Transport configuration for the new v2 implementation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportConfig {
    /// Logical name for observability and logs.
    pub node_name: String,
    /// Optional default timeout for startup/shutdown operations in milliseconds.
    pub timeout_ms: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            node_name: "rgz-node".to_string(),
            timeout_ms: 1_000,
        }
    }
}
