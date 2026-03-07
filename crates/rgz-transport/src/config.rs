use serde::Deserialize;
use serde::Serialize;

/// Transport configuration for the new v2 implementation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportConfig {
    /// Logical name for observability and logs.
    pub node_name: String,
    /// Optional default timeout for startup/shutdown operations in milliseconds.
    pub timeout_ms: u64,
    /// Capacity for standard command ingress queue.
    pub command_channel_capacity: usize,
    /// Capacity for control/shutdown ingress queue.
    pub control_channel_capacity: usize,
    /// Capacity for outbound actor event queue.
    pub event_channel_capacity: usize,
    /// Capacity for internal I/O event queue.
    pub io_event_channel_capacity: usize,
    /// Capacity for subscriber control queue.
    pub sub_cmd_channel_capacity: usize,
    /// Enable async ZeroMQ transport I/O wiring in the actor loop.
    pub enable_zeromq_io: bool,
    /// Optional local PUB socket bind endpoint (for publish path).
    pub zeromq_pub_bind: Option<String>,
    /// Additional PUB/SUB remote endpoints to connect for outbound publish messages.
    pub zeromq_pub_connect: Vec<String>,
    /// SUB endpoints to connect for receiving published messages.
    pub zeromq_sub_connect: Vec<String>,
    /// Optional local DEALER socket bind endpoint (for request/reply path).
    pub zeromq_req_bind: Option<String>,
    /// DEALER endpoints to connect for request/reply exchange.
    pub zeromq_req_connect: Vec<String>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            node_name: "rgz-node".to_string(),
            timeout_ms: 1_000,
            command_channel_capacity: 1024,
            control_channel_capacity: 128,
            event_channel_capacity: 2048,
            io_event_channel_capacity: 2048,
            sub_cmd_channel_capacity: 512,
            enable_zeromq_io: false,
            zeromq_pub_bind: None,
            zeromq_pub_connect: Vec::new(),
            zeromq_sub_connect: Vec::new(),
            zeromq_req_bind: None,
            zeromq_req_connect: Vec::new(),
        }
    }
}
