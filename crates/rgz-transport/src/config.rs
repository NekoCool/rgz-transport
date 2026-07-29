use serde::Deserialize;
use serde::Serialize;
use std::env;

const DEFAULT_DISCOVERY_MULTICAST_IP: &str = "239.255.0.7";
const DEFAULT_DISCOVERY_MSG_PORT: u16 = 10317;
const DEFAULT_DISCOVERY_SRV_PORT: u16 = 10318;

/// Opt-in configuration for legacy-compatible UDP discovery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscoveryConfig {
    /// Enables UDP multicast discovery. Disabled by default to preserve v2 behavior.
    pub enabled: bool,
    /// Multicast group used for message and service discovery.
    pub multicast_ip: String,
    /// UDP port for message publisher/subscriber discovery.
    pub message_port: u16,
    /// UDP port for service provider/requester discovery.
    pub service_port: u16,
    /// Optional IPv4 interface selected by `GZ_IP` when no explicit value is configured.
    pub interface_ip: Option<String>,
    /// Enables discovery diagnostics equivalent to legacy `GZ_VERBOSE=1`.
    pub verbose: bool,
}

impl DiscoveryConfig {
    /// Returns legacy-compatible defaults with optional environment overrides.
    pub fn from_environment() -> Self {
        let multicast_ip = env::var("GZ_DISCOVERY_MULTICAST_IP")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_DISCOVERY_MULTICAST_IP.to_string());
        let message_port = discovery_port("GZ_DISCOVERY_MSG_PORT", DEFAULT_DISCOVERY_MSG_PORT);
        let mut service_port = discovery_port("GZ_DISCOVERY_SRV_PORT", DEFAULT_DISCOVERY_SRV_PORT);
        if message_port == service_port {
            service_port = if message_port < u16::MAX {
                message_port + 1
            } else {
                message_port - 1
            };
        }

        Self {
            enabled: false,
            multicast_ip,
            message_port,
            service_port,
            interface_ip: env::var("GZ_IP").ok().filter(|value| !value.is_empty()),
            verbose: env::var("GZ_VERBOSE").is_ok_and(|value| value == "1"),
        }
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self::from_environment()
    }
}

fn discovery_port(name: &str, default: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::TransportConfig;

    #[test]
    fn discovery_is_opt_in_by_default() {
        let config = TransportConfig::default();

        assert!(!config.discovery.enabled);
        assert_ne!(config.discovery.message_port, config.discovery.service_port);
    }
}

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
    /// UDP discovery configuration. Discovery remains disabled unless explicitly enabled.
    pub discovery: DiscoveryConfig,
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
            discovery: DiscoveryConfig::default(),
        }
    }
}
